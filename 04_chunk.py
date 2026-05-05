# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 04 Chunk parsed source rows into `document_chunks`
# MAGIC
# MAGIC Manager-completed DDQ rows are chunked by DDQ section so retrieval can target `A0100`,
# MAGIC `B0500`, and similar codes. Appendix PDFs continue to use sliding page windows.

# COMMAND ----------

import re

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("workflow_profile", "odd_report_v1")
ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
PROFILE = get_workflow_profile(dbutils.widgets.get("workflow_profile").strip() or "odd_report_v1")

MIN_CHARS = 2500
MAX_CHARS = 6000
MAX_PAGES = 5
OVERLAP_PAGES = 1


def _pick_section_hint(text: str) -> str | None:
    if not text:
        return None
    lines = [ln.strip() for ln in text.splitlines()[:60] if ln.strip()]
    for ln in lines:
        if re.match(r"^[A-Z]\d{4}\.\s+.+$", ln):
            return ln[:200]
        if re.match(r"^\d+(\.\d+)*\s+.{5,}$", ln):
            return ln[:200]
        if re.match(r"^[A-Z0-9][A-Z0-9\s\-'(),.:/]{10,}$", ln) and len(ln) <= 140:
            return ln[:200]
    return None


def _build_pdf_chunks(pages: list[dict], source_role: str, source_tier: int) -> list[dict]:
    legal_entity_hint = detect_legal_entity_hint([p.get("page_text") or "" for p in pages[:3]], PROFILE)
    chunks = []
    n = len(pages)
    i = 0
    chunk_idx = 0
    while i < n:
        start = i
        end = i - 1
        chars = 0
        while end + 1 < n and (end - start + 1) < MAX_PAGES:
            nxt = pages[end + 1]
            nxt_chars = len(nxt["page_text"] or "")
            if chars < MIN_CHARS:
                end += 1
                chars += nxt_chars
                continue
            if chars + nxt_chars <= MAX_CHARS:
                end += 1
                chars += nxt_chars
                continue
            break
        if end < start:
            end = start

        chunk_pages = pages[start:end + 1]
        page_start = chunk_pages[0]["page_num"]
        page_end = chunk_pages[-1]["page_num"]
        parts = [f"[PAGE {p['page_num']}]\n{p['page_text'] or ''}".strip() for p in chunk_pages]
        chunk_text = "\n\n".join(parts).strip()
        chunk_type = "sliding_pages"
        chunk_id = sha256_hex(
            f"{ENGAGEMENT_ID}||{pages[0]['document_id']}||{page_start}||{page_end}||{chunk_idx}||{chunk_type}"
        )
        chunks.append({
            "engagement_id": ENGAGEMENT_ID,
            "chunk_id": chunk_id,
            "document_id": pages[0]["document_id"],
            "file_name": pages[0]["file_name"],
            "source_path_dbfs": pages[0]["source_path_dbfs"],
            "source_path_local": pages[0]["source_path_local"],
            "chunk_index": chunk_idx,
            "page_start": page_start,
            "page_end": page_end,
            "chunk_type": chunk_type,
            "section_hint": _pick_section_hint(chunk_text),
            "source_tier": source_tier,
            "legal_entity_hint": legal_entity_hint,
            "source_role": source_role,
            "section_code": None,
            "section_title": None,
            "chapter_code": None,
            "chapter_title": None,
            "question_number": None,
            "is_manager_answer_chunk": False,
            "has_substantive_answer": None,
            "referenced_appendices": None,
            "chunk_text": chunk_text,
            "chunk_sha": sha256_hex(chunk_text),
            "chunk_char_len": len(chunk_text),
        })
        chunk_idx += 1
        i = (end + 1) - OVERLAP_PAGES
        if i <= start:
            i = end + 1
    return chunks


def _build_manager_ddq_chunks(rows: list[dict], source_tier: int) -> list[dict]:
    grouped = []
    current_code = None
    bucket = []
    for row in rows:
        if row.get("section_code") and row["section_code"] != current_code:
            if bucket:
                grouped.append(bucket)
            current_code = row["section_code"]
            bucket = [row]
        else:
            bucket.append(row)
    if bucket:
        grouped.append(bucket)

    chunks = []
    chunk_idx = 0
    for section_rows in grouped:
        section_code = section_rows[0].get("section_code")
        if not section_code:
            continue
        section_title = section_rows[0].get("section_title")
        chapter_code = section_rows[0].get("chapter_code")
        chapter_title = section_rows[0].get("chapter_title")
        has_substantive = bool(section_rows[0].get("has_substantive_answer"))
        referenced_appendices = section_rows[0].get("referenced_appendices")
        legal_entity_hint = detect_legal_entity_hint([r.get("page_text") or "" for r in section_rows[:3]], PROFILE)

        segments = []
        current_segment = []
        for row in section_rows:
            role = row.get("content_role") or "narrative"
            if role == "question" and current_segment:
                segments.append(current_segment)
                current_segment = [row]
            else:
                current_segment.append(row)
        if current_segment:
            segments.append(current_segment)

        for segment in segments:
            part_lines = []
            for row in segment:
                prefix = ""
                if row.get("content_role") == "section_heading":
                    prefix = f"{section_code}. {section_title}"
                elif row.get("content_role") == "question":
                    prefix = f"Question {row.get('question_number')}: {row.get('page_text')}"
                else:
                    prefix = row.get("page_text") or ""
                if normalize_text(prefix):
                    part_lines.append(prefix)
            chunk_text = "\n".join(part_lines).strip()
            if not chunk_text:
                continue
            chunk_id = sha256_hex(
                f"{ENGAGEMENT_ID}||{section_rows[0]['document_id']}||{section_code}||{chunk_idx}||ddq_section"
            )
            chunks.append({
                "engagement_id": ENGAGEMENT_ID,
                "chunk_id": chunk_id,
                "document_id": section_rows[0]["document_id"],
                "file_name": section_rows[0]["file_name"],
                "source_path_dbfs": section_rows[0]["source_path_dbfs"],
                "source_path_local": section_rows[0]["source_path_local"],
                "chunk_index": chunk_idx,
                "page_start": segment[0]["page_num"],
                "page_end": segment[-1]["page_num"],
                "chunk_type": "ddq_section",
                "section_hint": f"{section_code}. {section_title}",
                "source_tier": source_tier,
                "legal_entity_hint": legal_entity_hint,
                "source_role": "manager_completed_ddq",
                "section_code": section_code,
                "section_title": section_title,
                "chapter_code": chapter_code,
                "chapter_title": chapter_title,
                "question_number": segment[0].get("question_number"),
                "is_manager_answer_chunk": True,
                "has_substantive_answer": has_substantive,
                "referenced_appendices": referenced_appendices,
                "chunk_text": chunk_text,
                "chunk_sha": sha256_hex(chunk_text),
                "chunk_char_len": len(chunk_text),
            })
            chunk_idx += 1
    return chunks


pending_docs = (
    spark.table(DOCUMENTS_TABLE)
    .filter((F.col("engagement_id") == ENGAGEMENT_ID) & (F.col("is_present") == True))
    .filter(F.col("parse_status") == "done")
    .filter(F.col("chunk_status").isin(["pending", "error"]))
    .select("document_id", "file_name", "file_path_dbfs", "file_path_local", "source_role", "source_tier")
    .orderBy("source_role", "file_name")
)

pending_count = pending_docs.count()
print("Documents pending chunking:", pending_count)
if pending_count == 0:
    show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
    dbutils.notebook.exit("Nothing to chunk.")

docs = [r.asDict() for r in pending_docs.collect()]

if docs:
    spark.createDataFrame([(d["document_id"],) for d in docs], ["document_id"]).createOrReplaceTempView("docs_to_rechunk")
    spark.sql(f"""
    DELETE FROM {CHUNKS_TABLE}
    WHERE engagement_id = '{ENGAGEMENT_ID}'
      AND document_id IN (SELECT document_id FROM docs_to_rechunk)
    """)

all_chunks = []
ok_docs = []
failed_docs = []

for d in docs:
    doc_id = d["document_id"]
    try:
        rows_df = (
            spark.table(PAGES_TABLE)
            .filter((F.col("engagement_id") == ENGAGEMENT_ID) & (F.col("document_id") == doc_id))
            .orderBy("page_num")
        )
        rows = [r.asDict() for r in rows_df.collect()]
        if not rows:
            raise ValueError("No parsed rows found for document_id; ensure 03_parse_sources ran successfully.")

        if d["source_role"] == "manager_completed_ddq":
            built = _build_manager_ddq_chunks(rows, int(d["source_tier"]))
        else:
            built = _build_pdf_chunks(rows, d["source_role"], int(d["source_tier"]))

        all_chunks.extend(built)
        ok_docs.append(doc_id)
    except Exception as e:
        failed_docs.append((doc_id, str(e)[:4000]))
        log_pipeline_error(ERRORS_TABLE, stage="chunk_build", engagement_id=ENGAGEMENT_ID, document_id=doc_id, error=e)

print("Chunked OK docs:", len(ok_docs))
print("Chunked failed docs:", len(failed_docs))

if pending_count > 0 and len(all_chunks) == 0:
    raise RuntimeError("No chunks were created. Check document_pages content and chunking parameters.")

# COMMAND ----------

if all_chunks:
    df_new = (
        spark.createDataFrame(all_chunks)
        .withColumn("chunk_ts", F.current_timestamp())
        .withColumn("index_status", F.lit("pending"))
        .withColumn("index_ts", F.lit(None).cast("timestamp"))
        .withColumn("index_error", F.lit(None).cast("string"))
    )
    df_new.createOrReplaceTempView("new_chunks")

    spark.sql(f"""
    MERGE INTO {CHUNKS_TABLE} t
    USING new_chunks s
    ON t.chunk_id = s.chunk_id
    WHEN MATCHED AND (t.chunk_sha IS NULL OR t.chunk_sha <> s.chunk_sha) THEN UPDATE SET *
    WHEN MATCHED THEN UPDATE SET
      t.chunk_text = s.chunk_text,
      t.chunk_char_len = s.chunk_char_len,
      t.chunk_ts = current_timestamp()
    WHEN NOT MATCHED THEN INSERT *
    """)

if ok_docs:
    spark.createDataFrame([(x,) for x in ok_docs], ["document_id"]).createOrReplaceTempView("chunk_ok_docs")
    spark.sql(f"""
    MERGE INTO {DOCUMENTS_TABLE} t
    USING chunk_ok_docs s
    ON t.document_id = s.document_id
    WHEN MATCHED THEN UPDATE SET
      t.chunk_status = 'done',
      t.chunk_ts = current_timestamp(),
      t.chunk_error = null
    """)

if failed_docs:
    spark.createDataFrame(failed_docs, ["document_id", "chunk_error"]).createOrReplaceTempView("chunk_failed_docs")
    spark.sql(f"""
    MERGE INTO {DOCUMENTS_TABLE} t
    USING chunk_failed_docs s
    ON t.document_id = s.document_id
    WHEN MATCHED THEN UPDATE SET
      t.chunk_status = 'error',
      t.chunk_ts = current_timestamp(),
      t.chunk_error = s.chunk_error
    """)

# COMMAND ----------

display(
    spark.table(CHUNKS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .groupBy("document_id", "file_name", "source_role")
    .agg(F.count("*").alias("chunks"), F.sum("chunk_char_len").alias("chars"))
    .orderBy("source_role", "file_name")
)

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
