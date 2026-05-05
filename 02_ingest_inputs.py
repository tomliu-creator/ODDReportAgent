# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 02 Ingest input evidence files (PDF + DOCX)
# MAGIC
# MAGIC Scans `engagements/<engagement_id>/inputs/*`, classifies each file, and MERGEs it into
# MAGIC `documents`. The completed manager DDQ is tagged as tier-0 evidence. The ODD report template
# MAGIC is tagged separately so later stages can parse/fill it but it is excluded from retrieval.

# COMMAND ----------

import os

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
assert ENGAGEMENT_ID, "engagement_id is required"

paths = engagement_paths(ENGAGEMENT_ID)
INPUTS_DBFS = paths["inputs_dbfs"]

REQUIRE_NONEMPTY = True
scan_glob = f"{INPUTS_DBFS}/*"
print("Scanning:", scan_glob)

df_files = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(scan_glob)
    .filter(F.lower(F.col("path")).rlike(r".*\.(pdf|docx)$"))
    .select(
        F.when(F.col("path").startswith("/Volumes/"), F.concat(F.lit("dbfs:"), F.col("path")))
        .otherwise(F.col("path"))
        .alias("file_path_dbfs"),
        F.col("length").alias("file_size_bytes"),
        F.col("modificationTime").alias("modification_ts"),
    )
    .withColumn("file_name", F.element_at(F.split(F.col("file_path_dbfs"), "/"), -1))
    .withColumn("file_ext", F.lower(F.regexp_extract(F.col("file_name"), r"(\.[^.]+)$", 1)))
)

if REQUIRE_NONEMPTY:
    assert_nonzero(df_files, f"binaryFile scan of {scan_glob}")

# COMMAND ----------

classify_source_role_udf = F.udf(classify_source_role, T.StringType())
assign_source_tier_udf = F.udf(lambda file_name, source_path, source_role: assign_source_tier(file_name, source_path, source_role=source_role), T.IntegerType())

df_docs = (
    df_files
    .withColumn("engagement_id", F.lit(ENGAGEMENT_ID))
    .withColumn("file_path_local", F.regexp_replace(F.col("file_path_dbfs"), r"^dbfs:", ""))
    .withColumn("source_role", classify_source_role_udf(F.col("file_name")))
    .withColumn("source_tier", assign_source_tier_udf(F.col("file_name"), F.col("file_path_dbfs"), F.col("source_role")))
    .withColumn("document_id", F.sha2(F.concat_ws("||", F.lit(ENGAGEMENT_ID), F.lower(F.col("file_path_dbfs"))), 256))
    .withColumn(
        "file_fingerprint",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("file_path_dbfs"),
                F.col("file_size_bytes").cast("string"),
                F.col("modification_ts").cast("string"),
            ),
            256,
        ),
    )
    .withColumn("is_present", F.lit(True))
    .withColumn("load_ts", F.current_timestamp())
)

display(df_docs.orderBy("source_role", "file_name"))

# COMMAND ----------

df_upsert = (
    df_docs.select(
        "engagement_id",
        "document_id",
        "file_path_dbfs",
        "file_path_local",
        "file_name",
        "file_ext",
        "file_size_bytes",
        "modification_ts",
        "file_fingerprint",
        "is_present",
        "source_role",
        "source_tier",
        "load_ts",
    )
    .withColumn("manager_name", F.lit(None).cast("string"))
    .withColumn("mandate_name", F.lit(None).cast("string"))
    .withColumn("investment_strategy", F.lit(None).cast("string"))
    .withColumn("report_finalization_date", F.lit(None).cast("string"))
    .withColumn("portfolio_odd_managers", F.lit(None).cast("string"))
    .withColumn("authors", F.lit(None).cast("string"))
    .withColumn("parse_status", F.lit("pending"))
    .withColumn("parse_method", F.lit(None).cast("string"))
    .withColumn("parse_ts", F.lit(None).cast("timestamp"))
    .withColumn("parse_error", F.lit(None).cast("string"))
    .withColumn("chunk_status", F.lit("pending"))
    .withColumn("chunk_ts", F.lit(None).cast("timestamp"))
    .withColumn("chunk_error", F.lit(None).cast("string"))
    .withColumn("index_status", F.lit("pending"))
    .withColumn("index_ts", F.lit(None).cast("timestamp"))
    .withColumn("index_error", F.lit(None).cast("string"))
    .withColumn("notes", F.lit(None).cast("string"))
)

df_upsert.createOrReplaceTempView("new_documents")

spark.sql(f"""
MERGE INTO {DOCUMENTS_TABLE} t
USING new_documents s
ON t.document_id = s.document_id
WHEN MATCHED AND (t.file_fingerprint IS NULL OR t.file_fingerprint <> s.file_fingerprint) THEN UPDATE SET
  t.engagement_id = s.engagement_id,
  t.file_path_dbfs = s.file_path_dbfs,
  t.file_path_local = s.file_path_local,
  t.file_name = s.file_name,
  t.file_ext = s.file_ext,
  t.file_size_bytes = s.file_size_bytes,
  t.modification_ts = s.modification_ts,
  t.file_fingerprint = s.file_fingerprint,
  t.is_present = true,
  t.source_role = s.source_role,
  t.source_tier = s.source_tier,
  t.load_ts = current_timestamp(),
  t.parse_status = 'pending',
  t.parse_method = null,
  t.parse_ts = null,
  t.parse_error = null,
  t.chunk_status = 'pending',
  t.chunk_ts = null,
  t.chunk_error = null,
  t.index_status = 'pending',
  t.index_ts = null,
  t.index_error = null
WHEN MATCHED THEN UPDATE SET
  t.engagement_id = s.engagement_id,
  t.file_path_dbfs = s.file_path_dbfs,
  t.file_path_local = s.file_path_local,
  t.file_name = s.file_name,
  t.file_ext = s.file_ext,
  t.file_size_bytes = s.file_size_bytes,
  t.modification_ts = s.modification_ts,
  t.file_fingerprint = s.file_fingerprint,
  t.is_present = true,
  t.source_role = s.source_role,
  t.source_tier = s.source_tier,
  t.load_ts = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  engagement_id, document_id, file_path_dbfs, file_path_local, file_name, file_ext,
  file_size_bytes, modification_ts, file_fingerprint, is_present,
  source_role, source_tier, manager_name, mandate_name, investment_strategy,
  report_finalization_date, portfolio_odd_managers, authors, load_ts,
  parse_status, parse_method, parse_ts, parse_error,
  chunk_status, chunk_ts, chunk_error,
  index_status, index_ts, index_error, notes
) VALUES (
  s.engagement_id, s.document_id, s.file_path_dbfs, s.file_path_local, s.file_name, s.file_ext,
  s.file_size_bytes, s.modification_ts, s.file_fingerprint, s.is_present,
  s.source_role, s.source_tier, s.manager_name, s.mandate_name, s.investment_strategy,
  s.report_finalization_date, s.portfolio_odd_managers, s.authors, s.load_ts,
  'pending', null, null, null,
  'pending', null, null,
  'pending', null, null, null
)
""")

spark.sql(f"""
UPDATE {DOCUMENTS_TABLE}
SET is_present = false
WHERE engagement_id = '{ENGAGEMENT_ID}'
  AND file_path_dbfs NOT IN (SELECT file_path_dbfs FROM new_documents)
""")

# COMMAND ----------

display(
    spark.table(DOCUMENTS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .groupBy("source_role", "parse_status", "chunk_status", "index_status", "is_present")
    .count()
    .orderBy("source_role")
)

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
