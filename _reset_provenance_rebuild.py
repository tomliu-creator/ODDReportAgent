# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()

spark.sql(f"""
UPDATE {DOCUMENTS_TABLE}
SET
  parse_status = CASE WHEN source_role = 'report_template' THEN parse_status ELSE 'pending' END,
  parse_method = CASE WHEN source_role = 'report_template' THEN parse_method ELSE null END,
  parse_ts = CASE WHEN source_role = 'report_template' THEN parse_ts ELSE null END,
  parse_error = CASE WHEN source_role = 'report_template' THEN parse_error ELSE null END,
  chunk_status = CASE WHEN source_role = 'report_template' THEN chunk_status ELSE 'pending' END,
  chunk_ts = CASE WHEN source_role = 'report_template' THEN chunk_ts ELSE null END,
  chunk_error = CASE WHEN source_role = 'report_template' THEN chunk_error ELSE null END,
  index_status = CASE WHEN source_role = 'report_template' THEN index_status ELSE 'pending' END,
  index_ts = CASE WHEN source_role = 'report_template' THEN index_ts ELSE null END,
  index_error = CASE WHEN source_role = 'report_template' THEN index_error ELSE null END
WHERE engagement_id = '{ENGAGEMENT_ID}'
  AND is_present = true
""")

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
dbutils.notebook.exit(f"Reset rebuild state for {ENGAGEMENT_ID}")
