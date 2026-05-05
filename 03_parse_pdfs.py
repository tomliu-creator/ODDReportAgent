# Databricks notebook source
# NOTEBOOK FILE: 03_parse_pdfs.py
# COMMAND ----------
# MAGIC %pip install -U PyMuPDF

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 03 Parse PDFs (PyMuPDF) -> `document_pages`

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()

try:
    import fitz  # PyMuPDF
except Exception:
    import pymupdf as fitz

MAX_DOCS = None

pending = (
    spark.table(DOCUMENTS_TABLE)
    .filter((F.col("engagement_id") == ENGAGEMENT_ID) & (F.col("is_present") == True))
    .filter(F.col("parse_status").isin(["pending", "error"]))
    .select("document_id", "file_name", "file_path_dbfs", "file_path_local")
    .orderBy("file_name")
)

pending_count = pending.count()
print("Pending documents to parse:", pending_count)
if pending_count == 0:
    show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
    dbutils.notebook.exit("Nothing to parse.")

docs = pending.limit(MAX_DOCS).collect() if MAX_DOCS else pending.collect()
print("Will parse:", len(docs))

# COMMAND ----------

page_rows = []
ok_doc_ids = []
failed_docs = []

for d in docs:
    doc_id = d["document_id"]
    local_path = d["file_path_local"] or uc_dbfs_to_local_path(d["file_path_dbfs"])
    try:
        pdf = fitz.open(local_path)
        for idx in range(pdf.page_count):
            page = pdf.load_page(idx)
            text = page.get_text("text") or ""
            page_rows.append({
                "engagement_id": ENGAGEMENT_ID,
                "document_id": doc_id,
                "file_name": d["file_name"],
                "source_path_dbfs": d["file_path_dbfs"],
                "source_path_local": local_path,
                "page_num": idx + 1,
                "page_text": text,
                "page_char_count": len(text),
                "parse_method": "pymupdf_text",
            })
        pdf.close()
        ok_doc_ids.append(doc_id)
    except Exception as e:
        failed_docs.append((doc_id, str(e)[:4000]))
        log_pipeline_error(
            ERRORS_TABLE,
            stage="parse_pdf",
            engagement_id=ENGAGEMENT_ID,
            document_id=doc_id,
            source_path=d["file_path_dbfs"],
            error=e,
            extra={"file_path_local": local_path},
        )

print("Parsed OK docs:", len(ok_doc_ids))
print("Failed docs:", len(failed_docs))

if len(page_rows) == 0:
    raise RuntimeError("No pages were parsed. Check file paths and PyMuPDF availability.")

# COMMAND ----------

pages_schema = T.StructType([
    T.StructField("engagement_id", T.StringType(), nullable=False),
    T.StructField("document_id", T.StringType(), nullable=False),
    T.StructField("file_name", T.StringType(), nullable=True),
    T.StructField("source_path_dbfs", T.StringType(), nullable=True),
    T.StructField("source_path_local", T.StringType(), nullable=True),
    T.StructField("page_num", T.IntegerType(), nullable=False),
    T.StructField("page_text", T.StringType(), nullable=True),
    T.StructField("page_char_count", T.IntegerType(), nullable=True),
    T.StructField("parse_method", T.StringType(), nullable=True),
])

df_pages = spark.createDataFrame(page_rows, schema=pages_schema).withColumn("parse_ts", F.current_timestamp())
df_pages.createOrReplaceTempView("new_pages")

if ok_doc_ids:
    spark.createDataFrame([(x,) for x in ok_doc_ids], ["document_id"]).createOrReplaceTempView("docs_to_replace_pages")
    spark.sql(f"""
    DELETE FROM {PAGES_TABLE}
    WHERE engagement_id = '{ENGAGEMENT_ID}'
      AND document_id IN (SELECT document_id FROM docs_to_replace_pages)
    """)

spark.sql(f"""
MERGE INTO {PAGES_TABLE} t
USING new_pages s
ON t.engagement_id = s.engagement_id AND t.document_id = s.document_id AND t.page_num = s.page_num
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

if ok_doc_ids:
    spark.createDataFrame([(x,) for x in ok_doc_ids], ["document_id"]).createOrReplaceTempView("ok_docs")
    spark.sql(f"""
    MERGE INTO {DOCUMENTS_TABLE} t
    USING ok_docs s
    ON t.document_id = s.document_id
    WHEN MATCHED THEN UPDATE SET
      t.parse_status = 'done',
      t.parse_method = 'pymupdf_text',
      t.parse_ts = current_timestamp(),
      t.parse_error = null
    """)

if failed_docs:
    spark.createDataFrame(failed_docs, ["document_id", "parse_error"]).createOrReplaceTempView("failed_docs")
    spark.sql(f"""
    MERGE INTO {DOCUMENTS_TABLE} t
    USING failed_docs s
    ON t.document_id = s.document_id
    WHEN MATCHED THEN UPDATE SET
      t.parse_status = 'error',
      t.parse_ts = current_timestamp(),
      t.parse_error = s.parse_error
    """)

# COMMAND ----------

display(
    spark.table(PAGES_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .groupBy("document_id", "file_name")
    .agg(F.count("*").alias("pages"), F.sum("page_char_count").alias("chars"))
    .orderBy("file_name")
)

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
