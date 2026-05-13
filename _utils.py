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


def get_doc_citation_label(file_name: str, source_role: str | None = None, profile: dict | None = None) -> str:
    role = normalize_text(source_role).lower()
    if role == "manager_completed_ddq":
        return "DDQ"
    return get_doc_short_title(file_name, profile)


def build_source_locator_label(
    source_page_num: int | None = None,
    source_para_num: int | None = None,
    source_locator_type: str | None = None,
) -> str | None:
    locator_type = normalize_text(source_locator_type).lower()
    if locator_type == "page" and source_page_num is not None:
        return f"p.{int(source_page_num)}"
    if locator_type == "paragraph" and source_para_num is not None:
        return f"para {int(source_para_num)}"
    if locator_type == "page_paragraph" and source_page_num is not None and source_para_num is not None:
        return f"p.{int(source_page_num)}, para {int(source_para_num)}"
    if source_page_num is not None:
        return f"p.{int(source_page_num)}"
    if source_para_num is not None:
        return f"para {int(source_para_num)}"
    return None


def build_source_locator_range(
    source_page_start: int | None = None,
    source_page_end: int | None = None,
    source_para_start: int | None = None,
    source_para_end: int | None = None,
    source_locator_type: str | None = None,
) -> str | None:
    locator_type = normalize_text(source_locator_type).lower()
    if locator_type == "page" and source_page_start is not None:
        end = source_page_end if source_page_end is not None else source_page_start
        return f"p.{int(source_page_start)}" if int(end) == int(source_page_start) else f"p.{int(source_page_start)}-{int(end)}"
    if locator_type == "paragraph" and source_para_start is not None:
        end = source_para_end if source_para_end is not None else source_para_start
        return f"para {int(source_para_start)}" if int(end) == int(source_para_start) else f"para {int(source_para_start)}-{int(end)}"
    if locator_type == "page_paragraph":
        page_label = build_source_locator_range(source_page_start, source_page_end, None, None, "page")
        para_label = build_source_locator_range(None, None, source_para_start, source_para_end, "paragraph")
        if page_label and para_label:
            return f"{page_label}, {para_label}"
        return page_label or para_label
    return (
        build_source_locator_range(source_page_start, source_page_end, None, None, "page")
        or build_source_locator_range(None, None, source_para_start, source_para_end, "paragraph")
    )


def _locator_signature(locator_text: str | None) -> tuple[str | None, list[int]]:
    text = normalize_text(locator_text).lower()
    if not text:
        return None, []
    if text.startswith("p."):
        locator_type = "page"
    elif text.startswith("para "):
        locator_type = "paragraph"
    else:
        locator_type = None
    nums = [int(x) for x in re.findall(r"\d+", text)]
    return locator_type, nums


def locator_matches(citation_locator: str | None, evidence_locator: str | None) -> bool:
    citation_type, citation_nums = _locator_signature(citation_locator)
    evidence_type, evidence_nums = _locator_signature(evidence_locator)
    if not citation_locator or not evidence_locator:
        return False
    if normalize_text(citation_locator).lower() == normalize_text(evidence_locator).lower():
        return True
    if citation_type and evidence_type and citation_type != evidence_type:
        return False
    if not citation_nums or not evidence_nums:
        return False
    if len(evidence_nums) == 1:
        return citation_nums[0] == evidence_nums[0]
    return evidence_nums[0] <= citation_nums[0] <= evidence_nums[-1]


CITATION_REGEX = re.compile(r"\[([^\[\]|]+?)\s*\|\s*([^\[\]|]+?)\s*\|\s*tier\s*([0-9]+)\]", re.IGNORECASE)


def parse_citations(answer_text: str, evidence: list[dict]) -> list[dict]:
    """Extract [file | locator | tier X] markers from the model's answer and resolve to chunk_ids."""
    if not answer_text:
        return []
    found = []
    for m in CITATION_REGEX.finditer(answer_text):
        file_token = m.group(1).strip()
        locator_token = m.group(2).strip()
        nums = [int(x) for x in re.findall(r"\d+", locator_token)]
        page = nums[0] if nums else None
        match_chunk = None
        for ch in evidence:
            fname = ch.get("file_name") or ""
            citation_label = ch.get("citation_label") or ""
            file_lower = file_token.lower()
            fname_lower = fname.lower()
            label_lower = citation_label.lower()
            locator_label = ch.get("source_locator_label") or ""
            if fname and (
                file_lower in fname_lower
                or fname_lower in file_lower
                or (label_lower and file_lower in label_lower)
                or (label_lower and label_lower in file_lower)
            ):
                if locator_matches(locator_token, locator_label):
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
