# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## 09 Orchestrate: run the ODD workflow end to end
# MAGIC
# MAGIC Prerequisites:
# MAGIC 1. Evidence files uploaded to the configured Unity Catalog volume under `<engagement_id>/inputs/`.
# MAGIC 2. The completed manager DDQ `.docx` and the `ODD Reports Blank.docx` template are both present there.
# MAGIC 3. `ODDAgent.md` is available to the Databricks runtime, either alongside the workspace notebooks
# MAGIC    or via an explicit `agent_spec_path` widget override.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.dropdown("workflow_mode", "odd_report", ["odd_report", "ddq_fill"])
dbutils.widgets.text("workflow_profile", "odd_report_v1")
dbutils.widgets.text("assessment_model_name", DEFAULT_ASSESSMENT_MODEL)
dbutils.widgets.text("risk_model_name", DEFAULT_RISK_MODEL)
dbutils.widgets.text("manager_ddq_name", "")
dbutils.widgets.text("report_template_name", "")
dbutils.widgets.text("agent_spec_path", "ODDAgent.md")
# retrieval_mode:
# - sql: use Databricks SQL VECTOR_SEARCH(...) inside the platform; safer default for this workspace.
#   Keep this wedge because the Python Vector Search path has caused environment-specific issues in
#   Databricks workspaces, including model-serving / endpoint constraints. Do not remove for cleanup convenience.
# - python: use the Python Vector Search client similarity_search(...) directly.
#   Keep this wedge because some environments or future debugging sessions may still need the client path.
dbutils.widgets.dropdown("retrieval_mode", "sql", ["sql", "python"])

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
WORKFLOW_MODE = dbutils.widgets.get("workflow_mode").strip() or "odd_report"
WORKFLOW_PROFILE = dbutils.widgets.get("workflow_profile").strip() or "odd_report_v1"
ASSESSMENT_MODEL_NAME = dbutils.widgets.get("assessment_model_name").strip()
RISK_MODEL_NAME = dbutils.widgets.get("risk_model_name").strip()
MANAGER_DDQ_NAME = dbutils.widgets.get("manager_ddq_name").strip()
REPORT_TEMPLATE_NAME = dbutils.widgets.get("report_template_name").strip()
AGENT_SPEC_PATH = dbutils.widgets.get("agent_spec_path").strip()
RETRIEVAL_MODE = dbutils.widgets.get("retrieval_mode").strip().lower() or "sql"

print("ENGAGEMENT_ID:", ENGAGEMENT_ID)
print("WORKFLOW_MODE:", WORKFLOW_MODE)
print("WORKFLOW_PROFILE:", WORKFLOW_PROFILE)
print("VS endpoint: configured in _config.py / 05_vector_index.py")
print("Assessment model:", ASSESSMENT_MODEL_NAME)
print("Risk model:", RISK_MODEL_NAME)
print("Embedding model: configured in _config.py / 05_vector_index.py")
print("Manager DDQ override:", MANAGER_DDQ_NAME or "<auto-detect>")
print("Report template override:", REPORT_TEMPLATE_NAME or "<auto-detect>")
print("Agent spec path:", AGENT_SPEC_PATH or "<default>")
print("Retrieval mode:", RETRIEVAL_MODE)

DEFAULT_TIMEOUT_SEC = 7200

if WORKFLOW_MODE != "odd_report":
    raise ValueError(
        "workflow_mode='ddq_fill' is deprecated in this notebook. "
        "The default supported run path is the ODD report workflow."
    )

# COMMAND ----------

r = dbutils.notebook.run("./01_setup", DEFAULT_TIMEOUT_SEC, {"engagement_id": ENGAGEMENT_ID})
print("01_setup:", r)

r = dbutils.notebook.run("./02_ingest_inputs", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "manager_ddq_name": MANAGER_DDQ_NAME,
    "report_template_name": REPORT_TEMPLATE_NAME,
})
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
    "workflow_profile": WORKFLOW_PROFILE,
})
print("05_vector_index:", r)

r = dbutils.notebook.run("./06_load_odd_agent_spec", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
    "report_template_name": REPORT_TEMPLATE_NAME,
    "agent_spec_path": AGENT_SPEC_PATH,
})
print("06_load_odd_agent_spec:", r)

r = dbutils.notebook.run("./07_assess_odd_report", DEFAULT_TIMEOUT_SEC, {
    "engagement_id": ENGAGEMENT_ID,
    "workflow_profile": WORKFLOW_PROFILE,
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
print(f"Output: {VOLUME_FUSE_ROOT}/{ENGAGEMENT_ID}/output/")
