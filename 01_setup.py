# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 01 Catalog / Schema / Volume / Tables Setup
# MAGIC
# MAGIC Idempotently creates the configured catalog/schema, the `engagements` UC volume,
# MAGIC the per-engagement folder layout, and the Delta tables used by the ODD workflow.

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
assert ENGAGEMENT_ID, "engagement_id is required"
print("ENGAGEMENT_ID:", ENGAGEMENT_ID)

# COMMAND ----------

ensure_uc_objects(CATALOG, SCHEMA, VOLUME)
paths = engagement_paths(ENGAGEMENT_ID)
for p in (paths["inputs_dbfs"], paths["questionnaire_dbfs"], paths["output_dbfs"]):
    dbutils.fs.mkdirs(p)
print("Engagement folders ready under:", f"{VOLUME_DBFS_ROOT}/{ENGAGEMENT_ID}/")
display(dbutils.fs.ls(f"{VOLUME_DBFS_ROOT}/{ENGAGEMENT_ID}"))

# COMMAND ----------

def _add_column_if_missing(table_fqn: str, column_def: str):
    col_name = column_def.split()[0]
    existing = {
        row["col_name"].lower()
        for row in spark.sql(f"SHOW COLUMNS IN {table_fqn}").collect()
        if row["col_name"]
    }
    if col_name.lower() not in existing:
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({column_def})")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DOCUMENTS_TABLE} (
  engagement_id STRING,
  document_id STRING,
  file_path_dbfs STRING,
  file_path_local STRING,
  file_name STRING,
  file_ext STRING,
  file_size_bytes BIGINT,
  modification_ts TIMESTAMP,
  file_fingerprint STRING,
  is_present BOOLEAN,
  source_role STRING,
  source_tier INT,
  manager_name STRING,
  mandate_name STRING,
  investment_strategy STRING,
  report_finalization_date STRING,
  portfolio_odd_managers STRING,
  authors STRING,
  load_ts TIMESTAMP,
  parse_status STRING,
  parse_method STRING,
  parse_ts TIMESTAMP,
  parse_error STRING,
  chunk_status STRING,
  chunk_ts TIMESTAMP,
  chunk_error STRING,
  index_status STRING,
  index_ts TIMESTAMP,
  index_error STRING,
  notes STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PAGES_TABLE} (
  engagement_id STRING,
  document_id STRING,
  file_name STRING,
  source_path_dbfs STRING,
  source_path_local STRING,
  page_num INT,
  source_page_num INT,
  source_para_num INT,
  source_locator_type STRING,
  source_locator_label STRING,
  page_text STRING,
  page_char_count INT,
  parse_method STRING,
  source_role STRING,
  source_tier INT,
  section_code STRING,
  section_title STRING,
  chapter_code STRING,
  chapter_title STRING,
  content_role STRING,
  question_number INT,
  has_substantive_answer BOOLEAN,
  referenced_appendices STRING,
  parse_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
  engagement_id STRING,
  chunk_id STRING,
  document_id STRING,
  file_name STRING,
  source_path_dbfs STRING,
  source_path_local STRING,
  chunk_index INT,
  page_start INT,
  page_end INT,
  source_page_start INT,
  source_page_end INT,
  source_para_start INT,
  source_para_end INT,
  source_locator_type STRING,
  source_locator_label STRING,
  chunk_type STRING,
  section_hint STRING,
  source_tier INT,
  legal_entity_hint STRING,
  source_role STRING,
  section_code STRING,
  section_title STRING,
  chapter_code STRING,
  chapter_title STRING,
  question_number INT,
  is_manager_answer_chunk BOOLEAN,
  has_substantive_answer BOOLEAN,
  referenced_appendices STRING,
  embedding_text STRING,
  chunk_text STRING,
  chunk_sha STRING,
  chunk_char_len INT,
  chunk_ts TIMESTAMP,
  index_status STRING,
  index_ts TIMESTAMP,
  index_error STRING
) USING DELTA
""")

for col in [
    "file_ext STRING",
    "source_role STRING",
    "source_tier INT",
    "manager_name STRING",
    "mandate_name STRING",
    "investment_strategy STRING",
    "report_finalization_date STRING",
    "portfolio_odd_managers STRING",
    "authors STRING",
]:
    _add_column_if_missing(DOCUMENTS_TABLE, col)

for col in [
    "source_page_num INT",
    "source_para_num INT",
    "source_locator_type STRING",
    "source_locator_label STRING",
    "source_role STRING",
    "source_tier INT",
    "section_code STRING",
    "section_title STRING",
    "chapter_code STRING",
    "chapter_title STRING",
    "content_role STRING",
    "question_number INT",
    "has_substantive_answer BOOLEAN",
    "referenced_appendices STRING",
]:
    _add_column_if_missing(PAGES_TABLE, col)

for col in [
    "source_page_start INT",
    "source_page_end INT",
    "source_para_start INT",
    "source_para_end INT",
    "source_locator_type STRING",
    "source_locator_label STRING",
    "source_role STRING",
    "section_code STRING",
    "section_title STRING",
    "chapter_code STRING",
    "chapter_title STRING",
    "question_number INT",
    "is_manager_answer_chunk BOOLEAN",
    "has_substantive_answer BOOLEAN",
    "referenced_appendices STRING",
    "embedding_text STRING",
]:
    _add_column_if_missing(CHUNKS_TABLE, col)

spark.sql(f"ALTER TABLE {CHUNKS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUESTIONS_TABLE} (
  engagement_id STRING,
  question_id STRING,
  questionnaire_profile STRING,
  section_id STRING,
  section_title STRING,
  question_number INT,
  question_text STRING,
  question_type STRING,
  placeholder_paragraph_index INT,
  source_docx_path STRING,
  load_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ANSWERS_TABLE} (
  engagement_id STRING,
  question_id STRING,
  draft_answer STRING,
  citations ARRAY<STRUCT<file:STRING, page:INT, chunk_id:STRING>>,
  retrieved_chunk_ids ARRAY<STRING>,
  confidence DOUBLE,
  human_review_flag STRING,
  missing_information STRING,
  source_tiers_used ARRAY<INT>,
  claim_check_json STRING,
  model STRING,
  prompt_version STRING,
  generated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ODD_REPORT_METADATA_TABLE} (
  engagement_id STRING,
  report_template_document_id STRING,
  report_template_path STRING,
  report_template_name STRING,
  agent_spec_path STRING,
  mandate_name STRING,
  manager_name STRING,
  investment_strategy STRING,
  report_finalization_date STRING,
  portfolio_odd_managers STRING,
  authors STRING,
  prompt_text STRING,
  part1_table_index INT,
  part2_table_index INT,
  load_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ODD_REPORT_TOPICS_TABLE} (
  engagement_id STRING,
  topic_id STRING,
  topic_order INT,
  table_index INT,
  topic_row_index INT,
  answer_row_index INT,
  chapter_code STRING,
  chapter_title STRING,
  section_code STRING,
  topic_title STRING,
  topic_prompt STRING,
  raw_topic_text STRING,
  load_ts TIMESTAMP
) USING DELTA
""")

for col in [
    "agent_spec_path STRING",
]:
    _add_column_if_missing(ODD_REPORT_METADATA_TABLE, col)

for col in [
    "topic_prompt STRING",
]:
    _add_column_if_missing(ODD_REPORT_TOPICS_TABLE, col)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ODD_RISK_DEFINITIONS_TABLE} (
  engagement_id STRING,
  rating_label STRING,
  rating_definition STRING,
  sort_order INT,
  load_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ODD_TOPIC_ASSESSMENTS_TABLE} (
  engagement_id STRING,
  topic_id STRING,
  section_code STRING,
  chapter_code STRING,
  topic_title STRING,
  assessment_text STRING,
  risk_rating STRING,
  risk_rationale STRING,
  citations ARRAY<STRUCT<file:STRING, page:INT, chunk_id:STRING>>,
  retrieved_chunk_ids ARRAY<STRING>,
  source_tiers_used ARRAY<INT>,
  manager_ddq_used BOOLEAN,
  appendices_used BOOLEAN,
  fallback_used BOOLEAN,
  confidence DOUBLE,
  human_review_flag STRING,
  assessment_model STRING,
  risk_model STRING,
  generated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ODD_CHAPTER_SUMMARIES_TABLE} (
  engagement_id STRING,
  chapter_code STRING,
  chapter_title STRING,
  summary_text STRING,
  rating STRING,
  topic_count INT,
  generated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERRORS_TABLE} (
  error_ts TIMESTAMP,
  engagement_id STRING,
  stage STRING,
  document_id STRING,
  chunk_id STRING,
  question_id STRING,
  source_path STRING,
  error_type STRING,
  error_message STRING,
  stacktrace STRING,
  extra_json STRING
) USING DELTA
""")

# COMMAND ----------

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
