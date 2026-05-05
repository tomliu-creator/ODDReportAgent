# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 09 Orchestrate: run the ODD workflow end to end
# MAGIC
# MAGIC Prerequisites:
# MAGIC 1. Evidence files uploaded to `/Volumes/cmi_agent/ddq_agent/engagements/<engagement_id>/inputs/`.
# MAGIC 2. The completed manager DDQ `.docx` and the `ODD Reports Blank.docx` template are both present there.

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("workflow_profile", "odd_report_v1")
dbutils.widgets.text("vs_endpoint_name", "emd-default-vs")
dbutils.widgets.text("assessment_model_name", "databricks-gpt-oss-20b")
dbutils.widgets.text("risk_model_name", "databricks-gpt-oss-20b")
dbutils.widgets.text("embedding_model_endpoint_name", "databricks-gte-large-en")
dbutils.widgets.text("report_template_name", "")
# retrieval_mode:
# - sql: use Databricks SQL VECTOR_SEARCH(...) inside the platform; safer default for this workspace.
#   Keep this wedge because the Python Vector Search path has caused environment-specific issues in
#   Databricks workspaces, including model-serving / endpoint constraints. Do not remove for cleanup convenience.
# - python: use the Python Vector Search client similarity_search(...) directly.
#   Keep this wedge because some environments or future debugging sessions may still need the client path.
dbutils.widgets.dropdown("retrieval_mode", "sql", ["sql", "python"])

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
WORKFLOW_PROFILE = dbutils.widgets.get("workflow_profile").strip() or "odd_report_v1"
VS_ENDPOINT_NAME = dbutils.widgets.get("vs_endpoint_name").strip()
ASSESSMENT_MODEL_NAME = dbutils.widgets.get("assessment_model_name").strip()
RISK_MODEL_NAME = dbutils.widgets.get("risk_model_name").strip()
EMBED_ENDPOINT = dbutils.widgets.get("embedding_model_endpoint_name").strip()
REPORT_TEMPLATE_NAME = dbutils.widgets.get("report_template_name").strip()
RETRIEVAL_MODE = dbutils.widgets.get("retrieval_mode").strip().lower() or "sql"

print("ENGAGEMENT_ID:", ENGAGEMENT_ID)
print("WORKFLOW_PROFILE:", WORKFLOW_PROFILE)
print("VS endpoint:", VS_ENDPOINT_NAME)
print("Assessment model:", ASSESSMENT_MODEL_NAME)
print("Risk model:", RISK_MODEL_NAME)
print("Embedding model:", EMBED_ENDPOINT)
print("Report template override:", REPORT_TEMPLATE_NAME or "<auto-detect>")
print("Retrieval mode:", RETRIEVAL_MODE)

DEFAULT_TIMEOUT_SEC = 7200

# COMMAND ----------

r = dbutils.notebook.run("./01_setup", DEFAULT_TIMEOUT_SEC, {"engagement_id": ENGAGEMENT_ID})
print("01_setup:", r)

r = dbutils.notebook.run("./02_ingest_inputs", DEFAULT_TIMEOUT_SEC, {"engagement_id": ENGAGEMENT_ID})
print("02_ingest_inputs:", r)

r = dbutils.notebook.run("./03_parse_sources", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
})
print("03_parse_sources:", r)

r = dbutils.notebook.run("./04_chunk", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
})
print("04_chunk:", r)

r = dbutils.notebook.run("./05_vector_index", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "vs_endpoint_name": VS_ENDPOINT_NAME,
    "embedding_model_endpoint_name": EMBED_ENDPOINT,
})
print("05_vector_index:", r)

r = dbutils.notebook.run("./06_parse_odd_report", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
    "report_template_name": REPORT_TEMPLATE_NAME,
})
print("06_parse_odd_report:", r)

r = dbutils.notebook.run("./07_assess_odd_report", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
    "vs_endpoint_name": VS_ENDPOINT_NAME,
    "assessment_model_name": ASSESSMENT_MODEL_NAME,
    "risk_model_name": RISK_MODEL_NAME,
    "retrieval_mode": RETRIEVAL_MODE,
})
print("07_assess_odd_report:", r)

r = dbutils.notebook.run("./08_fill_odd_report", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
})
print("08_fill_odd_report:", r)

print(f"Pipeline complete for engagement: {ENGAGEMENT_ID}")
print(f"Output: /Volumes/cmi_agent/ddq_agent/engagements/{ENGAGEMENT_ID}/output/")
