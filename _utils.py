# Databricks notebook source
# NOTEBOOK FILE: _utils.py
# Shared helpers for the ODD assessment agent.

# COMMAND ----------

import hashlib
import json
import os
import re
import traceback
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def uc_dbfs_to_local_path(path: str) -> str:
    """Normalize Databricks paths so Python libraries can read /Volumes/... paths."""
    if path is None:
        return None
    if path.startswith("dbfs:/Volumes/"):
        return "/Volumes/" + path[len("dbfs:/Volumes/"):]
    if path.startswith("/Volumes/"):
        return path
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):]
    if path.startswith("/dbfs/"):
        return path
    return path


def ensure_uc_objects(catalog: str, schema: str, volume: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")


def log_pipeline_error(
    table_fqn: str,
    stage: str,
    engagement_id: str = None,
    document_id: str = None,
    chunk_id: str = None,
    question_id: str = None,
    source_path: str = None,
    error: Exception = None,
    extra: dict | None = None,
):
    payload = {
        "error_ts": datetime.now(timezone.utc),
        "engagement_id": engagement_id,
        "stage": stage,
        "document_id": document_id,
        "chunk_id": chunk_id,
        "question_id": question_id,
        "source_path": source_path,
        "error_type": type(error).__name__ if error else None,
        "error_message": str(error) if error else None,
        "stacktrace": traceback.format_exc() if error else None,
        "extra_json": json.dumps(extra or {}, ensure_ascii=True),
    }
    spark.createDataFrame([payload]).write.mode("append").saveAsTable(table_fqn)


def show_validation_snapshot(catalog: str, schema: str, engagement_id: str = None):
    """Lightweight counts to run after each major stage."""

    def _safe_count(sql_text: str) -> int | None:
        try:
            return spark.sql(sql_text).collect()[0][0]
        except Exception:
            return None

    where = f"WHERE engagement_id = '{engagement_id}'" if engagement_id else ""
    docs = f"{catalog}.{schema}.documents"
    pages = f"{catalog}.{schema}.document_pages"
    chunks = f"{catalog}.{schema}.document_chunks"
    topics = f"{catalog}.{schema}.odd_report_topics"
    assessments = f"{catalog}.{schema}.odd_topic_assessments"
    summaries = f"{catalog}.{schema}.odd_chapter_summaries"

    rows = [
        ("documents_rows", _safe_count(f"SELECT count(*) FROM {docs} {where}")),
        ("documents_parse_pending", _safe_count(f"SELECT count(*) FROM {docs} {where} {'AND' if where else 'WHERE'} parse_status='pending'")),
        ("pages_rows", _safe_count(f"SELECT count(*) FROM {pages} {where}")),
        ("chunks_rows", _safe_count(f"SELECT count(*) FROM {chunks} {where}")),
        ("chunks_index_pending", _safe_count(f"SELECT count(*) FROM {chunks} {where} {'AND' if where else 'WHERE'} index_status='pending'")),
        ("odd_topics_rows", _safe_count(f"SELECT count(*) FROM {topics} {where}")),
        ("odd_assessments_rows", _safe_count(f"SELECT count(*) FROM {assessments} {where}")),
        ("odd_summaries_rows", _safe_count(f"SELECT count(*) FROM {summaries} {where}")),
    ]
    schema_t = T.StructType([
        T.StructField("metric", T.StringType(), nullable=False),
        T.StructField("value", T.LongType(), nullable=True),
    ])
    df = spark.createDataFrame(rows, schema=schema_t)
    try:
        display(df)
    except Exception:
        print(df.toPandas().to_string(index=False))


def assert_nonzero(df, what: str):
    c = df.count()
    if c == 0:
        raise ValueError(f"{what} produced 0 rows; stopping to prevent silent no-op.")
    return c


def normalize_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def classify_source_role(file_name: str) -> str:
    lower_name = (file_name or "").lower()
    _, ext = os.path.splitext(lower_name)
    for rule in SOURCE_ROLE_RULES:
        if ext not in rule.get("extensions", []):
            continue
        if any(keyword in lower_name for keyword in rule.get("keywords", [])):
            return rule["role"]
    if ext == ".pdf":
        return "appendix"
    return "other"


def assign_source_tier(file_name: str, source_path: str | None, profile: dict | None = None, source_role: str | None = None) -> int:
    role = source_role or classify_source_role(file_name)
    if role in SOURCE_TIER_BY_ROLE:
        return int(SOURCE_TIER_BY_ROLE[role])
    haystack = " ".join(filter(None, [file_name or "", source_path or ""])).lower()
    rules = (profile or {}).get("source_tier_rules", SOURCE_TIER_RULES)
    for rule in rules:
        keywords = [kw.lower() for kw in rule.get("keywords", [])]
        if any(kw in haystack for kw in keywords):
            return int(rule.get("tier", 5))
    return 5


def detect_legal_entity_hint(page_texts: list[str], profile: dict | None = None) -> str | None:
    sample = "\n".join((page_texts or [])[:3]).lower()
    patterns = (profile or {}).get("legal_entity_patterns", LEGAL_ENTITY_PATTERNS)
    for item in patterns:
        pattern = item.get("pattern")
        if pattern and re.search(pattern, sample, flags=re.IGNORECASE):
            return item.get("label")
    return None


def extract_appendix_references(text: str, regex_pattern: str | None = None) -> list[str]:
    if not text:
        return []
    pattern = re.compile(regex_pattern or APPENDIX_REF_REGEX, flags=re.IGNORECASE)
    refs = []
    for match in pattern.finditer(text):
        snippet = normalize_text(match.group(0))
        if snippet and snippet not in refs:
            refs.append(snippet)
    return refs


def has_substantive_answer(text: str) -> bool:
    cleaned = normalize_text(text)
    if len(cleaned) < 40:
        return False
    lower = cleaned.lower()
    placeholder_patterns = [
        "n/a",
        "not applicable",
        "not available",
        "none",
        "no response",
    ]
    if lower in placeholder_patterns:
        return False
    appendix_only = re.fullmatch(r"(please\s+)?refer\s+to\s+(the\s+)?appendix.*", lower)
    return appendix_only is None


def extract_label_value_metadata(rows: list[tuple[str, str]]) -> dict:
    mapping = {
        "mandate name": "mandate_name",
        "manager": "manager_name",
        "investment strategies": "investment_strategy",
        "report finalization date": "report_finalization_date",
        "portfolio/odd managers": "portfolio_odd_managers",
        "authors": "authors",
        "date": "report_finalization_date",
        "mandate": "mandate_name",
    }
    out = {}
    for label, value in rows:
        key = mapping.get(normalize_text(label).rstrip(":").lower())
        if key and normalize_text(value):
            out[key] = normalize_text(value)
    return out


def get_workflow_profile(profile_name: str) -> dict:
    profile = WORKFLOW_PROFILES.get(profile_name)
    if not profile:
        raise KeyError(f"Unknown workflow profile: {profile_name}")
    return profile


def get_questionnaire_profile(profile_name: str) -> dict:
    profile = QUESTIONNAIRE_PROFILES.get(profile_name)
    if not profile:
        raise KeyError(f"Unknown questionnaire profile: {profile_name}")
    return profile


def classify_question_type(text: str, profile: dict) -> str:
    if not text:
        return "free_text"
    t = text.strip()
    tl = t.lower()
    for kw in profile.get("numeric_keywords", []):
        if kw in tl:
            return "numeric"
    for pref in profile.get("yes_no_prefixes", []):
        if t.startswith(pref):
            return "yes_no"
    return "free_text"


def expand_query(question_text: str, profile: dict) -> list[str]:
    base = (question_text or "").strip()
    if not base:
        return []
    queries = [base]
    lower_q = base.lower()
    triggers = [t.lower() for t in profile.get("mandate_query_triggers", [])]
    if any(trigger in lower_q for trigger in triggers):
        extra_terms = " ".join(profile.get("mandate_query_terms", []))
        expanded = f"{base} {extra_terms}".strip()
        if expanded not in queries:
            queries.append(expanded)
    return queries


def is_high_risk(question_text: str, profile: dict) -> bool:
    lower_q = (question_text or "").lower()
    return any(topic.lower() in lower_q for topic in profile.get("high_risk_topics", []))


def get_doc_short_title(file_name: str, profile: dict | None = None) -> str:
    mapping = (profile or {}).get("doc_short_titles", DOC_SHORT_TITLES) or {}
    if file_name in mapping:
        return mapping[file_name]
    stem = os.path.splitext(file_name or "")[0]
    return stem.replace("_", " ").strip() or (file_name or "")


CITATION_REGEX = re.compile(r"\[([^\[\]]+?)(?:\s*\|\s*)?p\.?(\d+)(?:\s*\|\s*tier\s*([0-9]+))?\]", re.IGNORECASE)


def parse_citations(answer_text: str, evidence: list[dict]) -> list[dict]:
    """Extract [file p.N] markers from the model's answer and resolve to chunk_ids."""
    if not answer_text:
        return []
    found = []
    for m in CITATION_REGEX.finditer(answer_text):
        file_token, page_token = m.group(1).strip(), m.group(2).strip()
        try:
            page = int(page_token)
        except ValueError:
            continue
        match_chunk = None
        for ch in evidence:
            fname = ch.get("file_name") or ""
            short_title = os.path.splitext(fname)[0].replace("_", " ").strip()
            file_lower = file_token.lower()
            fname_lower = fname.lower()
            short_lower = short_title.lower()
            if fname and (
                file_lower in fname_lower
                or fname_lower in file_lower
                or (short_lower and file_lower in short_lower)
                or (short_lower and short_lower in file_lower)
            ):
                ps, pe = ch.get("page_start") or 0, ch.get("page_end") or 0
                if ps <= page <= pe or ps == page or pe == page:
                    match_chunk = ch
                    break
        found.append({
            "file": file_token,
            "page": page,
            "chunk_id": match_chunk["chunk_id"] if match_chunk else None,
        })
    return found


def parse_json_payload(text: str, default=None):
    if not text:
        return default
    stripped = text.strip()
    object_match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
    list_match = re.search(r"\[.*\]", stripped, flags=re.DOTALL)
    candidate = object_match.group(0) if object_match else list_match.group(0) if list_match else stripped
    try:
        return json.loads(candidate)
    except Exception:
        return default


def conservative_rollup_rating(ratings: list[str]) -> str:
    normalized = [normalize_text(r).title() for r in ratings if normalize_text(r)]
    if any(r == "Unacceptable" for r in normalized):
        return "Unacceptable"
    if any(r == "High" for r in normalized):
        return "High"
    if any(r == "Medium" for r in normalized):
        return "Medium"
    return "Low"
