# Databricks notebook source
# NOTEBOOK FILE: 07_answer_ddq.py
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
# MAGIC **Legacy Notebook**
# MAGIC
# MAGIC This notebook belongs to the deprecated DDQ-fill workflow. The default orchestrated path for
# MAGIC this repository is now the ODD report workflow (`06_parse_odd_report` -> `07_assess_odd_report`
# MAGIC -> `08_fill_odd_report`).
# MAGIC
# MAGIC ## 07 Answer DDQ questions with mandate-aware retrieval, reranking, and claim checks

# COMMAND ----------

import json

from databricks.vector_search.client import VectorSearchClient

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("questionnaire_profile", "odd_ssga_v1")
dbutils.widgets.text("vs_endpoint_name", DEFAULT_VS_ENDPOINT)
dbutils.widgets.text("model_name", DEFAULT_LLM_ENDPOINT)
dbutils.widgets.text("top_k", str(DEFAULT_TOP_K))
dbutils.widgets.text("final_evidence_k", str(DEFAULT_FINAL_EVIDENCE_K))
dbutils.widgets.text("prompt_version", "v2")
dbutils.widgets.dropdown("retrieval_mode", "python", ["python", "sql"])

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
PROFILE_NAME = dbutils.widgets.get("questionnaire_profile").strip() or "odd_ssga_v1"
PROFILE = get_questionnaire_profile(PROFILE_NAME)
VS_ENDPOINT_NAME = dbutils.widgets.get("vs_endpoint_name").strip() or DEFAULT_VS_ENDPOINT
VS_INDEX_NAME = vs_index_name(ENGAGEMENT_ID)
MODEL_NAME = dbutils.widgets.get("model_name").strip() or DEFAULT_LLM_ENDPOINT
TOP_K = int(dbutils.widgets.get("top_k"))
FINAL_EVIDENCE_K = int(dbutils.widgets.get("final_evidence_k"))
PROMPT_VERSION = dbutils.widgets.get("prompt_version").strip() or "v2"
RETRIEVAL_MODE = dbutils.widgets.get("retrieval_mode").strip().lower() or "python"

print("ENGAGEMENT_ID:", ENGAGEMENT_ID)
print("QUESTIONNAIRE_PROFILE:", PROFILE_NAME)
print("VS endpoint:", VS_ENDPOINT_NAME)
print("VS index:", VS_INDEX_NAME)
print("MODEL_NAME:", MODEL_NAME)
print("TOP_K:", TOP_K)
print("FINAL_EVIDENCE_K:", FINAL_EVIDENCE_K)
print("RETRIEVAL_MODE:", RETRIEVAL_MODE)
if RETRIEVAL_MODE == "sql":
    print("INFO: advanced retrieval now uses the Python Vector Search client so source-tier metadata can be applied.")

# COMMAND ----------

questions_df = (
    spark.table(QUESTIONS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .filter(F.col("question_type") != "skipped_table")
    .filter(F.col("placeholder_paragraph_index").isNotNull())
    .select("question_id", "question_text", "question_type")
    .orderBy("question_id")
)
total = questions_df.count()
print("Answerable questions:", total)
if total == 0:
    raise ValueError("No answerable questions; run 06_parse_ddq first.")

questions = [r.asDict() for r in questions_df.collect()]

# COMMAND ----------

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
    return 0 if _safe_int(value, 5) <= 2 else 1


def _retrieval_limit_for_question(question_text: str) -> int:
    limit = TOP_K * (HIGH_RISK_RETRIEVAL_MULTIPLIER if is_high_risk(question_text, PROFILE) else 1)
    return max(limit, FINAL_EVIDENCE_K * 2)


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
    "section_hint",
    "chunk_index",
    "source_tier",
    "legal_entity_hint",
]


def _retrieve_candidates(question: dict) -> list[dict]:
    queries = expand_query(question["question_text"], PROFILE)
    limit = _retrieval_limit_for_question(question["question_text"])
    collected = []

    for query_ix, query_text in enumerate(queries):
        resp = index.similarity_search(
            query_text=query_text,
            columns=vs_cols,
            num_results=limit,
            filters={"engagement_id": ENGAGEMENT_ID},
        )
        hits = _vs_extract_rows(resp)
        for rank, hit in enumerate(hits, start=1):
            cid = hit.get("chunk_id")
            if not cid:
                continue
            collected.append({
                "question_id": question["question_id"],
                "chunk_id": cid,
                "file_name": hit.get("file_name"),
                "page_start": hit.get("page_start"),
                "page_end": hit.get("page_end"),
                "source_tier": _safe_int(hit.get("source_tier"), 5),
                "legal_entity_hint": hit.get("legal_entity_hint"),
                "initial_rank": rank,
                "query_variant": query_ix,
                "query_text": query_text,
            })

    deduped = {}
    for item in collected:
        cid = item["chunk_id"]
        incumbent = deduped.get(cid)
        candidate_key = (_tier_priority(item["source_tier"]), item["initial_rank"], item["query_variant"])
        if not incumbent:
            deduped[cid] = item
            continue
        incumbent_key = (_tier_priority(incumbent["source_tier"]), incumbent["initial_rank"], incumbent["query_variant"])
        if candidate_key < incumbent_key:
            deduped[cid] = item

    return sorted(
        deduped.values(),
        key=lambda item: (_tier_priority(item["source_tier"]), item["initial_rank"], item["query_variant"], item["chunk_id"]),
    )


candidate_map = {q["question_id"]: _retrieve_candidates(q) for q in questions}
all_chunk_ids = {item["chunk_id"] for items in candidate_map.values() for item in items}
print("Questions with retrieved candidates:", len([qid for qid, items in candidate_map.items() if items]))
print("Unique candidate chunk_ids:", len(all_chunk_ids))

if not all_chunk_ids:
    raise RuntimeError("No evidence candidates were retrieved; verify the Vector Search index is READY and scoped correctly.")

# COMMAND ----------

chunks_df = (
    spark.table(CHUNKS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .filter(F.col("chunk_id").isin(list(all_chunk_ids)))
    .select(
        "chunk_id",
        "file_name",
        "page_start",
        "page_end",
        "chunk_text",
        "source_tier",
        "legal_entity_hint",
    )
)
chunk_by_id = {r["chunk_id"]: r.asDict() for r in chunks_df.collect()}

rerank_rows = []
for q in questions:
    for item in candidate_map.get(q["question_id"], []):
        chunk = chunk_by_id.get(item["chunk_id"])
        if not chunk:
            continue
        preview = (chunk.get("chunk_text") or "")[:2500]
        rerank_prompt = (
            "You are checking retrieval relevance for a DDQ answer.\n"
            f"QUESTION:\n{q['question_text']}\n\n"
            "CHUNK:\n"
            f"File: {chunk.get('file_name')}\n"
            f"Pages: {chunk.get('page_start')}-{chunk.get('page_end')}\n"
            f"Tier: {chunk.get('source_tier')}\n"
            f"Text:\n{preview}\n\n"
            "Reply with valid JSON only: "
            "{\"supported\":\"yes|partial|no\",\"reason\":\"short reason\"}."
        )
        rerank_rows.append({
            "question_id": q["question_id"],
            "chunk_id": item["chunk_id"],
            "source_tier": chunk.get("source_tier"),
            "initial_rank": item["initial_rank"],
            "query_variant": item["query_variant"],
            "prompt": rerank_prompt,
        })

rerank_schema = T.StructType([
    T.StructField("question_id", T.StringType()),
    T.StructField("chunk_id", T.StringType()),
    T.StructField("source_tier", T.IntegerType()),
    T.StructField("initial_rank", T.IntegerType()),
    T.StructField("query_variant", T.IntegerType()),
    T.StructField("prompt", T.StringType()),
])
df_rerank_prompts = spark.createDataFrame(rerank_rows, schema=rerank_schema)
df_rerank_prompts.createOrReplaceTempView("ddq_rerank_prompts")

df_rerank_raw = spark.sql(f"""
SELECT
  question_id,
  chunk_id,
  source_tier,
  initial_rank,
  query_variant,
  ai_query('{MODEL_NAME}', prompt) AS rerank_response
FROM ddq_rerank_prompts
""")
rerank_results = [r.asDict() for r in df_rerank_raw.collect()]
print("Chunk rerank rows:", len(rerank_results))


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
rerank_by_qid: dict[str, list[dict]] = {}
for row in rerank_results:
    label = _parse_rerank_label(row.get("rerank_response") or "")
    rerank_by_qid.setdefault(row["question_id"], []).append({
        "chunk_id": row["chunk_id"],
        "source_tier": _safe_int(row.get("source_tier"), 5),
        "initial_rank": _safe_int(row.get("initial_rank"), 999),
        "query_variant": _safe_int(row.get("query_variant"), 0),
        "rerank_label": label,
        "rerank_score": rerank_score_map.get(label, 0),
    })


def _select_evidence(question: dict) -> tuple[list[str], list[dict]]:
    qid = question["question_id"]
    target_k = FINAL_EVIDENCE_K * (HIGH_RISK_RETRIEVAL_MULTIPLIER if is_high_risk(question["question_text"], PROFILE) else 1)
    scored = sorted(
        rerank_by_qid.get(qid, []),
        key=lambda item: (
            -item["rerank_score"],
            _tier_priority(item["source_tier"]),
            item["initial_rank"],
            item["query_variant"],
            item["chunk_id"],
        ),
    )
    kept = [item for item in scored if item["rerank_label"] in {"yes", "partial"}][:target_k]
    if not kept:
        fallback = candidate_map.get(qid, [])[:target_k]
        kept = [{
            "chunk_id": item["chunk_id"],
            "source_tier": _safe_int(item.get("source_tier"), 5),
            "initial_rank": _safe_int(item.get("initial_rank"), 999),
            "query_variant": _safe_int(item.get("query_variant"), 0),
            "rerank_label": "fallback",
            "rerank_score": -1,
        } for item in fallback]

    evidence = []
    for item in kept:
        chunk = chunk_by_id.get(item["chunk_id"])
        if not chunk:
            continue
        evidence.append({
            "chunk_id": chunk["chunk_id"],
            "file_name": chunk["file_name"],
            "page_start": chunk["page_start"],
            "page_end": chunk["page_end"],
            "source_tier": _safe_int(chunk.get("source_tier"), 5),
            "legal_entity_hint": chunk.get("legal_entity_hint"),
            "text_en": chunk["chunk_text"],
        })
    return [item["chunk_id"] for item in kept], evidence


prompt_rows = []
selected_evidence_by_question = {}
for q in questions:
    selected_chunk_ids, evidence = _select_evidence(q)
    selected_evidence_by_question[q["question_id"]] = evidence
    prompt_text = build_prompt(
        q["question_text"],
        q["question_type"],
        evidence,
        PROFILE,
        high_risk=is_high_risk(q["question_text"], PROFILE),
    )
    prompt_rows.append({
        "engagement_id": ENGAGEMENT_ID,
        "question_id": q["question_id"],
        "question_type": q["question_type"],
        "prompt": prompt_text,
        "retrieved_chunk_ids": selected_chunk_ids,
        "evidence_json": json.dumps(evidence, ensure_ascii=False),
    })

prompts_schema = T.StructType([
    T.StructField("engagement_id", T.StringType()),
    T.StructField("question_id", T.StringType()),
    T.StructField("question_type", T.StringType()),
    T.StructField("prompt", T.StringType()),
    T.StructField("retrieved_chunk_ids", T.ArrayType(T.StringType())),
    T.StructField("evidence_json", T.StringType()),
])
df_prompts = spark.createDataFrame(prompt_rows, schema=prompts_schema)
df_prompts.createOrReplaceTempView("ddq_prompts")
print("Prompts staged:", df_prompts.count())

# COMMAND ----------

df_raw = spark.sql(f"""
SELECT
  engagement_id,
  question_id,
  question_type,
  retrieved_chunk_ids,
  evidence_json,
  ai_query('{MODEL_NAME}', prompt) AS draft_answer_raw
FROM ddq_prompts
""")
raw_rows = [r.asDict() for r in df_raw.collect()]
print("Answer generation rows:", len(raw_rows))

# COMMAND ----------

question_by_id = {q["question_id"]: q for q in questions}
claim_check_rows = []
parsed_answers = {}
for row in raw_rows:
    question = question_by_id[row["question_id"]]
    parsed = parse_answer_envelope(row.get("draft_answer_raw") or "")
    parsed_answers[row["question_id"]] = parsed
    evidence = json.loads(row["evidence_json"]) if row.get("evidence_json") else []
    evidence_brief = []
    for item in evidence:
        evidence_brief.append(
            f"- {item.get('file_name')} p.{item.get('page_start')}-{item.get('page_end')} (tier {item.get('source_tier')}): "
            f"{(item.get('text_en') or '')[:1200]}"
        )
    claim_check_prompt = (
        "Check whether each factual claim in the draft answer is directly supported by the evidence.\n"
        f"QUESTION:\n{question['question_text']}\n\n"
        f"DRAFT ANSWER:\n{parsed.get('answer') or ''}\n\n"
        "EVIDENCE:\n"
        + "\n".join(evidence_brief)
        + "\n\nReturn valid JSON only as a list of objects with keys "
          "`claim`, `supported` (`yes`, `partial`, or `no`), and `suggestion`."
    )
    claim_check_rows.append({
        "question_id": row["question_id"],
        "prompt": claim_check_prompt,
    })

claim_check_schema = T.StructType([
    T.StructField("question_id", T.StringType()),
    T.StructField("prompt", T.StringType()),
])
df_claim_prompts = spark.createDataFrame(claim_check_rows, schema=claim_check_schema)
df_claim_prompts.createOrReplaceTempView("ddq_claim_check_prompts")

df_claim_raw = spark.sql(f"""
SELECT
  question_id,
  ai_query('{MODEL_NAME}', prompt) AS claim_check_raw
FROM ddq_claim_check_prompts
""")
claim_check_map = {r["question_id"]: parse_claim_check(r["claim_check_raw"] or "") for r in df_claim_raw.collect()}
print("Claim check rows:", len(claim_check_map))

# COMMAND ----------

def _coerce_confidence(value) -> float | None:
    try:
        conf = float(value)
    except Exception:
        return None
    if conf < 0:
        return 0.0
    if conf > 1:
        return 1.0
    return conf


def _derive_confidence(model_confidence, claim_checks: list[dict]) -> float:
    base = _coerce_confidence(model_confidence)
    if base is None:
        base = 0.5
    if not claim_checks:
        return base
    score_map = {"yes": 1.0, "partial": 0.5, "no": 0.0}
    scores = [score_map.get(item.get("supported"), 0.25) for item in claim_checks]
    return round((base + (sum(scores) / len(scores))) / 2, 3)


def _derive_review_flag(question_text: str, confidence: float, claim_checks: list[dict], source_tiers_used: list[int]) -> str:
    if is_high_risk(question_text, PROFILE):
        return "high"
    if any(item.get("supported") == "no" for item in claim_checks):
        return "high"
    if confidence < 0.45:
        return "high"
    if source_tiers_used and min(source_tiers_used) >= 4:
        return "medium"
    if any(item.get("supported") == "partial" for item in claim_checks):
        return "medium"
    if confidence < 0.7:
        return "medium"
    return "low"


final_rows = []
for row in raw_rows:
    qid = row["question_id"]
    parsed = parsed_answers.get(qid, {})
    answer_text = parsed.get("answer") or (row.get("draft_answer_raw") or "")
    evidence = json.loads(row["evidence_json"]) if row.get("evidence_json") else []
    citations = parse_citations(answer_text, evidence)
    cited_chunk_ids = [c.get("chunk_id") for c in citations if c.get("chunk_id")]
    if cited_chunk_ids:
        source_tiers_used = sorted({
            _safe_int(chunk_by_id[cid].get("source_tier"), 5)
            for cid in cited_chunk_ids
            if cid in chunk_by_id
        })
    else:
        source_tiers_used = sorted({_safe_int(item.get("source_tier"), 5) for item in evidence})

    claim_checks = claim_check_map.get(qid, [])
    confidence = _derive_confidence(parsed.get("confidence"), claim_checks)
    final_rows.append({
        "engagement_id": row["engagement_id"],
        "question_id": qid,
        "draft_answer": answer_text,
        "citations": citations,
        "retrieved_chunk_ids": row["retrieved_chunk_ids"] or [],
        "confidence": confidence,
        "human_review_flag": _derive_review_flag(
            question_by_id[qid]["question_text"],
            confidence,
            claim_checks,
            source_tiers_used,
        ),
        "missing_information": parsed.get("missing_information") or "",
        "source_tiers_used": source_tiers_used,
        "claim_check_json": json.dumps(claim_checks, ensure_ascii=False),
        "model": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
    })

answers_schema = T.StructType([
    T.StructField("engagement_id", T.StringType()),
    T.StructField("question_id", T.StringType()),
    T.StructField("draft_answer", T.StringType()),
    T.StructField("citations", T.ArrayType(T.StructType([
        T.StructField("file", T.StringType()),
        T.StructField("page", T.IntegerType()),
        T.StructField("chunk_id", T.StringType()),
    ]))),
    T.StructField("retrieved_chunk_ids", T.ArrayType(T.StringType())),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("human_review_flag", T.StringType()),
    T.StructField("missing_information", T.StringType()),
    T.StructField("source_tiers_used", T.ArrayType(T.IntegerType())),
    T.StructField("claim_check_json", T.StringType()),
    T.StructField("model", T.StringType()),
    T.StructField("prompt_version", T.StringType()),
])

df_answers = (
    spark.createDataFrame(final_rows, schema=answers_schema)
    .withColumn("generated_at", F.current_timestamp())
)

spark.sql(f"DELETE FROM {ANSWERS_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")
df_answers.write.mode("append").saveAsTable(ANSWERS_TABLE)

# COMMAND ----------

display(
    spark.table(ANSWERS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .select(
        "question_id",
        "human_review_flag",
        "confidence",
        F.size("citations").alias("n_citations"),
        F.size("source_tiers_used").alias("n_tiers"),
    )
    .orderBy(F.col("confidence").asc_nulls_first())
    .limit(20)
)

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
