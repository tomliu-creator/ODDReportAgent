# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install -U databricks-vectorsearch

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 07 Generate ODD topic assessments and Part 2 summaries

# COMMAND ----------

import json
import re
from databricks.vector_search.client import VectorSearchClient

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("workflow_profile", "odd_report_v1")
dbutils.widgets.text("vs_endpoint_name", DEFAULT_VS_ENDPOINT)
dbutils.widgets.text("assessment_model_name", DEFAULT_ASSESSMENT_MODEL)
dbutils.widgets.text("risk_model_name", DEFAULT_RISK_MODEL)
dbutils.widgets.text("top_k", str(DEFAULT_TOP_K))
dbutils.widgets.text("final_evidence_k", str(DEFAULT_FINAL_EVIDENCE_K))
dbutils.widgets.text("prompt_version", "odd_report_v1")
# Keep both retrieval modes on purpose.
# - `sql` is the safer default in this Databricks workspace because the Python Vector Search path
#   has previously hit serving/index environment constraints.
# - `python` remains valuable as a fallback/debug wedge and should not be removed just because the
#   ODD workflow currently prefers SQL.
dbutils.widgets.dropdown("retrieval_mode", "sql", ["sql", "python"])

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
PROFILE = get_workflow_profile(dbutils.widgets.get("workflow_profile").strip() or "odd_report_v1")
VS_ENDPOINT_NAME = dbutils.widgets.get("vs_endpoint_name").strip() or DEFAULT_VS_ENDPOINT
VS_INDEX_NAME = vs_index_name(ENGAGEMENT_ID)
ASSESSMENT_MODEL_NAME = dbutils.widgets.get("assessment_model_name").strip() or DEFAULT_ASSESSMENT_MODEL
RISK_MODEL_NAME = dbutils.widgets.get("risk_model_name").strip() or DEFAULT_RISK_MODEL
TOP_K = int(dbutils.widgets.get("top_k"))
FINAL_EVIDENCE_K = int(dbutils.widgets.get("final_evidence_k"))
PROMPT_VERSION = dbutils.widgets.get("prompt_version").strip() or "odd_report_v1"
RETRIEVAL_MODE = dbutils.widgets.get("retrieval_mode").strip().lower() or "sql"

metadata_row = (
    spark.table(ODD_REPORT_METADATA_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .orderBy(F.col("load_ts").desc())
    .limit(1)
    .collect()
)
if not metadata_row:
    raise ValueError("No ODD report metadata found; run 06_parse_odd_report first.")
metadata = metadata_row[0].asDict()

topics = [
    r.asDict()
    for r in (
        spark.table(ODD_REPORT_TOPICS_TABLE)
        .filter(F.col("engagement_id") == ENGAGEMENT_ID)
        .orderBy("topic_order")
        .collect()
    )
]
if not topics:
    raise ValueError("No ODD topics found; run 06_parse_odd_report first.")

risk_defs = [
    r.asDict()
    for r in (
        spark.table(ODD_RISK_DEFINITIONS_TABLE)
        .filter(F.col("engagement_id") == ENGAGEMENT_ID)
        .orderBy("sort_order")
        .collect()
    )
]
risk_def_text = "\n".join(f"- {r['rating_label']}: {r['rating_definition']}" for r in risk_defs)

print("RETRIEVAL_MODE:", RETRIEVAL_MODE)

vsc = None
index = None
if RETRIEVAL_MODE == "python":
    vsc = VectorSearchClient(disable_notice=True)
    index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
vs_cols = [
    "chunk_id",
    "engagement_id",
    "document_id",
    "file_name",
    "source_path_dbfs",
    "page_start",
    "page_end",
    "chunk_type",
    "section_hint",
    "chunk_index",
    "source_tier",
    "legal_entity_hint",
    "source_role",
    "section_code",
    "section_title",
    "chapter_code",
    "chapter_title",
    "question_number",
    "is_manager_answer_chunk",
    "has_substantive_answer",
    "referenced_appendices",
]


def _vs_extract_rows(resp: dict) -> list[dict]:
    if not isinstance(resp, dict):
        return []
    result = resp.get("result") or resp.get("results") or resp
    manifest = resp.get("manifest") or (result or {}).get("manifest") or {}
    data = (result or {}).get("data_array") or (result or {}).get("data") or []
    cols = manifest.get("columns") or []
    names = [c.get("name") for c in cols] if cols else None
    out = []
    for r in data:
        if names and len(names) == len(r):
            out.append({k: v for k, v in zip(names, r)})
        elif isinstance(r, dict):
            out.append(r)
    return out


def _safe_int(value, default=5) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _tier_priority(value) -> int:
    tier = _safe_int(value, 5)
    return 0 if tier == 0 else 1 if tier <= 2 else 2


def _search(query_text: str, filters: dict, num_results: int) -> list[dict]:
    if RETRIEVAL_MODE == "python":
        # Client-side path retained intentionally. Some workspaces need it for debugging or legacy behavior.
        resp = index.similarity_search(
            query_text=query_text,
            columns=vs_cols,
            num_results=num_results,
            filters=filters,
        )
        return _vs_extract_rows(resp)

    query_escaped = (query_text or "").replace("'", "''")
    search_results = int(num_results)
    if filters:
        # SQL vector_search does not receive our Python-style filters here, so it returns the top N
        # globally and Spark filters afterward. Over-fetch so prompt-driven source-role searches
        # are not starved by DDQ hits.
        # Databricks SQL vector_search currently caps num_results at 100.
        search_results = min(100, max(search_results * 8, 80))
    # SQL path retained intentionally as the safer default for this workspace. Do not collapse this
    # back into Python-only retrieval without checking Databricks serving/index constraints first.
    sql_text = f"""
    SELECT *
    FROM vector_search(
      index => '{VS_INDEX_NAME}',
      query => '{query_escaped}',
      num_results => {search_results}
    )
    """
    df = spark.sql(sql_text)
    if filters:
        for key, value in filters.items():
            if value is None:
                continue
            df = df.filter(F.col(key) == F.lit(value))
    filtered_df = df.select(*[c for c in vs_cols if c in df.columns])
    return [r.asDict() for r in filtered_df.limit(int(num_results)).collect()]


def _appendix_query_terms(chunk_rows: list[dict]) -> list[str]:
    refs = []
    for row in chunk_rows:
        raw = row.get("referenced_appendices")
        parsed = parse_json_payload(raw, default=None)
        if isinstance(parsed, list):
            for item in parsed:
                text = normalize_text(str(item))
                if text and text not in refs:
                    refs.append(text)
        elif normalize_text(raw):
            refs.append(normalize_text(raw))
    return refs


def _topic_query_text(topic: dict) -> str:
    parts = [
        topic.get("section_code"),
        topic.get("topic_title"),
        topic.get("chapter_title"),
        topic.get("topic_prompt"),
    ]
    return " ".join(normalize_text(part) for part in parts if normalize_text(part)).strip()


def _mandatory_source_hits(topic: dict) -> list[dict]:
    if not normalize_text(topic.get("topic_prompt")):
        return []

    # Generic topic-prompt path: if ODDAgent.md gives a topic-specific instruction, search
    # appendices with that same prompt text instead of hard-coding behavior for one section.
    # This keeps source expansion driven by the runtime behavior spec.
    return _search(
        _topic_query_text(topic),
        {"engagement_id": ENGAGEMENT_ID, "source_role": "appendix"},
        max(4, FINAL_EVIDENCE_K),
    )


def _retrieve_for_topic(topic: dict) -> tuple[list[dict], bool]:
    base_query = _topic_query_text(topic)
    section_hits = _search(
        base_query,
        {"engagement_id": ENGAGEMENT_ID, "source_role": "manager_completed_ddq", "section_code": topic["section_code"]},
        max(TOP_K, FINAL_EVIDENCE_K * 2),
    )
    section_substantive = any(bool(hit.get("has_substantive_answer")) for hit in section_hits)
    appendix_terms = _appendix_query_terms(section_hits)

    appendix_query = base_query
    if appendix_terms:
        appendix_query = f"{base_query} {' '.join(appendix_terms[:3])}".strip()
    appendix_hits = _search(
        appendix_query,
        {"engagement_id": ENGAGEMENT_ID, "source_role": "appendix"},
        max(4, FINAL_EVIDENCE_K),
    )
    mandatory_hits = _mandatory_source_hits(topic)

    fallback_used = False
    if not section_hits or not section_substantive:
        fallback_used = True
        global_ddq_hits = _search(
            base_query,
            {"engagement_id": ENGAGEMENT_ID, "source_role": "manager_completed_ddq"},
            max(TOP_K, FINAL_EVIDENCE_K * 2),
        )
        section_hits = global_ddq_hits

    combined = []
    for bucket_name, hits in (("section", section_hits), ("appendix", appendix_hits), ("mandatory_external", mandatory_hits)):
        for rank, hit in enumerate(hits, start=1):
            cid = hit.get("chunk_id")
            if not cid:
                continue
            combined.append({
                **hit,
                "bucket": bucket_name,
                "initial_rank": rank,
            })

    deduped = {}
    for item in combined:
        cid = item["chunk_id"]
        incumbent = deduped.get(cid)
        candidate_key = (_tier_priority(item.get("source_tier")), item["bucket"] != "section", item["initial_rank"])
        if incumbent is None:
            deduped[cid] = item
            continue
        incumbent_key = (_tier_priority(incumbent.get("source_tier")), incumbent["bucket"] != "section", incumbent["initial_rank"])
        if candidate_key < incumbent_key:
            deduped[cid] = item

    return sorted(
        deduped.values(),
        key=lambda item: (_tier_priority(item.get("source_tier")), item["bucket"] != "section", item["initial_rank"], item["chunk_id"]),
    ), fallback_used


candidate_map = {}
for topic in topics:
    candidate_map[topic["topic_id"]] = _retrieve_for_topic(topic)

all_chunk_ids = sorted({
    item["chunk_id"]
    for items, _fallback in candidate_map.values()
    for item in items
})
if not all_chunk_ids:
    raise RuntimeError("No evidence candidates were retrieved; verify the Vector Search index is ready.")

chunk_by_id = {
    r["chunk_id"]: r.asDict()
    for r in (
        spark.table(CHUNKS_TABLE)
        .filter(F.col("engagement_id") == ENGAGEMENT_ID)
        .filter(F.col("chunk_id").isin(all_chunk_ids))
        .collect()
    )
}

rerank_rows = []
for topic in topics:
    items, _fallback = candidate_map[topic["topic_id"]]
    for item in items[: max(TOP_K, FINAL_EVIDENCE_K * 2)]:
        chunk = chunk_by_id.get(item["chunk_id"])
        if not chunk:
            continue
        preview = (chunk.get("chunk_text") or "")[:2200]
        rerank_prompt = (
            "You are checking retrieval relevance for an ODD report topic.\n"
            f"TOPIC: {topic['section_code']} {topic['topic_title']}\n"
            f"CHAPTER: {topic['chapter_title']}\n\n"
            + (f"TOPIC-SPECIFIC MUST-COVER SCOPE:\n{topic.get('topic_prompt')}\n\n" if normalize_text(topic.get("topic_prompt")) else "")
            +
            f"CHUNK FILE: {chunk.get('file_name')}\n"
            f"PAGES: {chunk.get('page_start')}-{chunk.get('page_end')}\n"
            f"SOURCE ROLE: {chunk.get('source_role')}\n"
            f"TEXT:\n{preview}\n\n"
            "Relevance rule: if a topic-specific must-cover scope is provided, reply yes/partial only when "
            "the chunk contains factual evidence for at least one requested item. Do not mark source-name-only, "
            "certification, cover, table-of-contents, or generic control text as relevant merely because it comes "
            "from a required source document.\n"
            "Reply with valid JSON only: "
            "{\"supported\":\"yes|partial|no\",\"reason\":\"short reason\"}."
        )
        rerank_rows.append({
            "topic_id": topic["topic_id"],
            "chunk_id": item["chunk_id"],
            "source_tier": _safe_int(chunk.get("source_tier"), 5),
            "initial_rank": item["initial_rank"],
            "bucket": item["bucket"],
            "prompt": rerank_prompt,
        })

rerank_schema = T.StructType([
    T.StructField("topic_id", T.StringType()),
    T.StructField("chunk_id", T.StringType()),
    T.StructField("source_tier", T.IntegerType()),
    T.StructField("initial_rank", T.IntegerType()),
    T.StructField("bucket", T.StringType()),
    T.StructField("prompt", T.StringType()),
])

df_rerank_prompts = spark.createDataFrame(rerank_rows, schema=rerank_schema)
df_rerank_prompts.createOrReplaceTempView("odd_rerank_prompts")

df_rerank_raw = spark.sql(f"""
SELECT
  topic_id,
  chunk_id,
  source_tier,
  initial_rank,
  bucket,
  ai_query('{ASSESSMENT_MODEL_NAME}', prompt) AS rerank_response
FROM odd_rerank_prompts
""")


def _parse_rerank_label(text: str) -> str:
    if not text:
        return "no"
    match = re.search(r'"supported"\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
    if match:
        label = match.group(1).strip().lower()
        if label in {"yes", "partial", "no"}:
            return label
    lower_text = text.lower()
    for label in ("yes", "partial", "no"):
        if label in lower_text:
            return label
    return "no"


rerank_score_map = {"yes": 2, "partial": 1, "no": 0}
rerank_by_topic = {}
for row in df_rerank_raw.collect():
    label = _parse_rerank_label(row["rerank_response"] or "")
    rerank_by_topic.setdefault(row["topic_id"], []).append({
        "chunk_id": row["chunk_id"],
        "source_tier": _safe_int(row["source_tier"], 5),
        "initial_rank": _safe_int(row["initial_rank"], 999),
        "bucket": row["bucket"],
        "rerank_label": label,
        "rerank_score": rerank_score_map.get(label, 0),
    })


def _select_evidence(topic: dict) -> tuple[list[str], list[dict], bool]:
    items, fallback_used = candidate_map[topic["topic_id"]]
    scored = sorted(
        rerank_by_topic.get(topic["topic_id"], []),
        key=lambda item: (-item["rerank_score"], _tier_priority(item["source_tier"]), item["bucket"] != "section", item["initial_rank"]),
    )
    kept = [item for item in scored if item["rerank_label"] in {"yes", "partial"}][:FINAL_EVIDENCE_K]
    if not kept:
        kept = [{
            "chunk_id": item["chunk_id"],
            "source_tier": _safe_int(item.get("source_tier"), 5),
            "bucket": item.get("bucket"),
            "initial_rank": item.get("initial_rank"),
            "rerank_label": "fallback",
        } for item in items[:FINAL_EVIDENCE_K]]

    evidence = []
    for item in kept:
        chunk = chunk_by_id.get(item["chunk_id"])
        if not chunk:
            continue
        source_role = chunk.get("source_role")
        if not source_role and item.get("bucket") == "mandatory_external":
            source_role = "mandatory_external"
        evidence.append({
            "chunk_id": chunk["chunk_id"],
            "file_name": chunk["file_name"],
            "page_start": chunk["page_start"],
            "page_end": chunk["page_end"],
            "source_tier": _safe_int(chunk.get("source_tier"), 5),
            "source_role": source_role,
            "section_code": chunk.get("section_code"),
            "text_en": chunk["chunk_text"],
        })
    return [item["chunk_id"] for item in kept], evidence, fallback_used


def _format_evidence(evidence: list[dict]) -> str:
    lines = []
    for idx, item in enumerate(evidence, start=1):
        short_title = get_doc_short_title(item.get("file_name"), PROFILE)
        lines.append(
            f"[E{idx}] [{short_title} | p.{item.get('page_start')} | tier {item.get('source_tier')} | role {item.get('source_role')}]\n"
            f"{(item.get('text_en') or '')[:2500]}"
        )
    return "\n\n---\n\n".join(lines)


assessment_prompts = []
risk_prompts = []
evidence_by_topic = {}
retrieved_chunk_ids_by_topic = {}
fallback_flags = {}

for topic in topics:
    selected_chunk_ids, evidence, fallback_used = _select_evidence(topic)
    evidence_by_topic[topic["topic_id"]] = evidence
    retrieved_chunk_ids_by_topic[topic["topic_id"]] = selected_chunk_ids
    fallback_flags[topic["topic_id"]] = fallback_used
    evidence_block = _format_evidence(evidence)

    assessment_prompt = (
        "You are writing a formal operational due diligence report section.\n"
        f"AGENT BEHAVIOR SPEC:\n{metadata['prompt_text']}\n\n"
        f"TOPIC: {topic['section_code']} {topic['topic_title']}\n"
        f"CHAPTER: {topic['chapter_title']}\n"
        f"MANDATE: {metadata.get('mandate_name') or ''}\n"
        f"MANAGER: {metadata.get('manager_name') or ''}\n\n"
        + (f"TOPIC-SPECIFIC MUST-COVER SCOPE:\n{topic.get('topic_prompt')}\n\n" if normalize_text(topic.get("topic_prompt")) else "")
        +
        "Requirements:\n"
        "- Use neutral, factual, investor-facing language.\n"
        "- Prioritize the completed manager DDQ evidence when it is substantive.\n"
        "- Use appendices only to support, clarify, or challenge the DDQ response.\n"
        "- If the DDQ section is missing or thin, state the limitation carefully and rely on other evidence.\n"
        "- Return valid JSON only with keys `assessment_text` and `confidence`.\n"
        "- The `assessment_text` must cite material claims inline using `[file | p.N | tier X]` markers.\n\n"
        f"EVIDENCE:\n{evidence_block}"
    )
    assessment_prompts.append({
        "topic_id": topic["topic_id"],
        "prompt": assessment_prompt,
    })

    risk_prompt = (
        "You are assigning a risk rating for one ODD topic.\n"
        f"TOPIC: {topic['section_code']} {topic['topic_title']}\n"
        f"RISK DEFINITIONS:\n{risk_def_text}\n\n"
        "Use the evidence below and choose exactly one rating.\n"
        "Return valid JSON only with keys `risk_rating` and `risk_rationale`.\n\n"
        f"EVIDENCE:\n{evidence_block}"
    )
    risk_prompts.append({
        "topic_id": topic["topic_id"],
        "prompt": risk_prompt,
    })

assessment_df = spark.createDataFrame(assessment_prompts)
risk_df = spark.createDataFrame(risk_prompts)
assessment_df.createOrReplaceTempView("odd_assessment_prompts")
risk_df.createOrReplaceTempView("odd_risk_prompts")

df_assessment_raw = spark.sql(f"""
SELECT topic_id, ai_query('{ASSESSMENT_MODEL_NAME}', prompt) AS assessment_raw
FROM odd_assessment_prompts
""")
df_risk_raw = spark.sql(f"""
SELECT topic_id, ai_query('{RISK_MODEL_NAME}', prompt) AS risk_raw
FROM odd_risk_prompts
""")

assessment_payloads = {r["topic_id"]: parse_json_payload(r["assessment_raw"], default={}) or {} for r in df_assessment_raw.collect()}
risk_payloads = {r["topic_id"]: parse_json_payload(r["risk_raw"], default={}) or {} for r in df_risk_raw.collect()}

assessment_rows = []
for topic in topics:
    topic_id = topic["topic_id"]
    assessment_payload = assessment_payloads.get(topic_id, {})
    risk_payload = risk_payloads.get(topic_id, {})
    evidence = evidence_by_topic[topic_id]
    assessment_text = normalize_text(assessment_payload.get("assessment_text") or "")
    confidence = assessment_payload.get("confidence")
    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.8 if not fallback_flags[topic_id] else 0.6
    confidence = max(0.0, min(1.0, confidence))
    risk_rating = normalize_text(risk_payload.get("risk_rating") or "").title() or "Medium"
    risk_rationale = normalize_text(risk_payload.get("risk_rationale") or "")
    citations = parse_citations(assessment_text, evidence)
    source_tiers_used = sorted({_safe_int(item.get("source_tier"), 5) for item in evidence})
    manager_ddq_used = any(item.get("source_role") == "manager_completed_ddq" for item in evidence)
    appendices_used = any(item.get("source_role") in {"appendix", "mandatory_external"} for item in evidence)
    review_flag = "high" if fallback_flags[topic_id] or risk_rating in {"High", "Unacceptable"} else "medium" if confidence < 0.7 else "low"
    assessment_rows.append({
        "engagement_id": ENGAGEMENT_ID,
        "topic_id": topic_id,
        "section_code": topic["section_code"],
        "chapter_code": topic["chapter_code"],
        "topic_title": topic["topic_title"],
        "assessment_text": assessment_text,
        "risk_rating": risk_rating,
        "risk_rationale": risk_rationale,
        "citations": citations,
        "retrieved_chunk_ids": retrieved_chunk_ids_by_topic[topic_id],
        "source_tiers_used": source_tiers_used,
        "manager_ddq_used": manager_ddq_used,
        "appendices_used": appendices_used,
        "fallback_used": fallback_flags[topic_id],
        "confidence": confidence,
        "human_review_flag": review_flag,
        "assessment_model": ASSESSMENT_MODEL_NAME,
        "risk_model": RISK_MODEL_NAME,
    })

assessment_schema = T.StructType([
    T.StructField("engagement_id", T.StringType(), False),
    T.StructField("topic_id", T.StringType(), False),
    T.StructField("section_code", T.StringType(), True),
    T.StructField("chapter_code", T.StringType(), True),
    T.StructField("topic_title", T.StringType(), True),
    T.StructField("assessment_text", T.StringType(), True),
    T.StructField("risk_rating", T.StringType(), True),
    T.StructField("risk_rationale", T.StringType(), True),
    T.StructField(
        "citations",
        T.ArrayType(
            T.StructType([
                T.StructField("file", T.StringType(), True),
                T.StructField("page", T.IntegerType(), True),
                T.StructField("chunk_id", T.StringType(), True),
            ]),
            containsNull=True,
        ),
        True,
    ),
    T.StructField("retrieved_chunk_ids", T.ArrayType(T.StringType(), containsNull=True), True),
    T.StructField("source_tiers_used", T.ArrayType(T.IntegerType(), containsNull=True), True),
    T.StructField("manager_ddq_used", T.BooleanType(), True),
    T.StructField("appendices_used", T.BooleanType(), True),
    T.StructField("fallback_used", T.BooleanType(), True),
    T.StructField("confidence", T.DoubleType(), True),
    T.StructField("human_review_flag", T.StringType(), True),
    T.StructField("assessment_model", T.StringType(), True),
    T.StructField("risk_model", T.StringType(), True),
])

assessments_df = spark.createDataFrame(assessment_rows, schema=assessment_schema).withColumn("generated_at", F.current_timestamp())

spark.sql(f"DELETE FROM {ODD_TOPIC_ASSESSMENTS_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")
assessments_df.write.mode("append").saveAsTable(ODD_TOPIC_ASSESSMENTS_TABLE)

# COMMAND ----------

assessment_topic_df = (
    spark.table(ODD_TOPIC_ASSESSMENTS_TABLE).alias("a")
    .join(
        spark.table(ODD_REPORT_TOPICS_TABLE).alias("t"),
        on=[F.col("a.engagement_id") == F.col("t.engagement_id"), F.col("a.topic_id") == F.col("t.topic_id")],
        how="inner",
    )
    .filter(F.col("a.engagement_id") == ENGAGEMENT_ID)
    .select(
        F.col("a.engagement_id").alias("engagement_id"),
        F.col("t.chapter_code").alias("chapter_code"),
        F.col("t.chapter_title").alias("chapter_title"),
        F.col("t.section_code").alias("section_code"),
        F.col("t.topic_title").alias("topic_title"),
        F.col("a.assessment_text").alias("assessment_text"),
        F.col("a.risk_rating").alias("risk_rating"),
    )
)

chapter_groups = {}
for row in assessment_topic_df.orderBy("chapter_code", "section_code").collect():
    chapter_groups.setdefault((row["chapter_code"], row["chapter_title"]), []).append(row.asDict())

summary_prompt_rows = []
chapter_rating_rows = []
for (chapter_code, chapter_title), rows in chapter_groups.items():
    ratings = [row["risk_rating"] for row in rows]
    chapter_rating = conservative_rollup_rating(ratings)
    chapter_rating_rows.append((chapter_code, chapter_title, chapter_rating, len(rows)))
    topic_summaries = "\n".join(
        f"- {row['section_code']} {row['topic_title']} ({row['risk_rating']}): {(row['assessment_text'] or '')[:700]}"
        for row in rows
    )
    prompt = (
        "Write a high-level ODD chapter summary based on the topic assessments below.\n"
        f"CHAPTER: {chapter_title}\n"
        f"MANDATE: {metadata.get('mandate_name') or ''}\n"
        f"MANAGER: {metadata.get('manager_name') or ''}\n"
        f"TARGET RATING: {chapter_rating}\n"
        "Use neutral, concise report language. Return valid JSON only with key `summary_text`.\n\n"
        f"TOPIC ASSESSMENTS:\n{topic_summaries}"
    )
    summary_prompt_rows.append({
        "chapter_code": chapter_code,
        "chapter_title": chapter_title,
        "prompt": prompt,
    })

overall_rating = conservative_rollup_rating([row[2] for row in chapter_rating_rows])
overall_prompt = (
    "Write the overall ODD conclusion based on the chapter summaries below.\n"
    f"MANDATE: {metadata.get('mandate_name') or ''}\n"
    f"MANAGER: {metadata.get('manager_name') or ''}\n"
    f"TARGET RATING: {overall_rating}\n"
    "Use neutral, investor-facing report language. Return valid JSON only with key `summary_text`.\n\n"
    + "\n".join(f"- {code} {title} ({rating})" for code, title, rating, _count in chapter_rating_rows)
)
summary_prompt_rows.append({
    "chapter_code": "OVERALL",
    "chapter_title": "Overall conclusion",
    "prompt": overall_prompt,
})

summary_df = spark.createDataFrame(summary_prompt_rows)
summary_df.createOrReplaceTempView("odd_summary_prompts")
df_summary_raw = spark.sql(f"""
SELECT chapter_code, chapter_title, ai_query('{ASSESSMENT_MODEL_NAME}', prompt) AS summary_raw
FROM odd_summary_prompts
""")

summary_payloads = {
    (r["chapter_code"], r["chapter_title"]): parse_json_payload(r["summary_raw"], default={}) or {}
    for r in df_summary_raw.collect()
}

summary_rows = []
for chapter_code, chapter_title, rating, topic_count in chapter_rating_rows:
    payload = summary_payloads.get((chapter_code, chapter_title), {})
    summary_rows.append({
        "engagement_id": ENGAGEMENT_ID,
        "chapter_code": chapter_code,
        "chapter_title": chapter_title,
        "summary_text": normalize_text(payload.get("summary_text") or ""),
        "rating": rating,
        "topic_count": topic_count,
    })

overall_payload = summary_payloads.get(("OVERALL", "Overall conclusion"), {})
summary_rows.append({
    "engagement_id": ENGAGEMENT_ID,
    "chapter_code": "OVERALL",
    "chapter_title": "Overall conclusion",
    "summary_text": normalize_text(overall_payload.get("summary_text") or ""),
    "rating": overall_rating,
    "topic_count": len(topics),
})

chapter_summary_schema = T.StructType([
    T.StructField("engagement_id", T.StringType(), False),
    T.StructField("chapter_code", T.StringType(), False),
    T.StructField("chapter_title", T.StringType(), True),
    T.StructField("summary_text", T.StringType(), True),
    T.StructField("rating", T.StringType(), True),
    T.StructField("topic_count", T.IntegerType(), True),
])

chapter_summaries_df = spark.createDataFrame(summary_rows, schema=chapter_summary_schema).withColumn("generated_at", F.current_timestamp())
spark.sql(f"DELETE FROM {ODD_CHAPTER_SUMMARIES_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")
chapter_summaries_df.write.mode("append").saveAsTable(ODD_CHAPTER_SUMMARIES_TABLE)

# COMMAND ----------

display(
    spark.table(ODD_TOPIC_ASSESSMENTS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .select("section_code", "topic_title", "risk_rating", "confidence", "human_review_flag", "fallback_used")
    .orderBy("section_code")
)

display(
    spark.table(ODD_CHAPTER_SUMMARIES_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .orderBy("chapter_code")
)

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
