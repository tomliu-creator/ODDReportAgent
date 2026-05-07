# Databricks notebook source
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 06 Load the ODD agent markdown spec into structured metadata
# MAGIC
# MAGIC `ODDAgent.md` is the runtime behavior spec for the ODD workflow. This notebook reads the
# MAGIC markdown spec, extracts the global prompt guidance, risk scale, and topic list, then writes
# MAGIC the normalized tables consumed by `07_assess_odd_report.py` and `08_fill_odd_report.py`.

# COMMAND ----------

import os
import re

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("workflow_profile", "odd_report_v1")
dbutils.widgets.text("report_template_name", "")
dbutils.widgets.text("agent_spec_path", "ODDAgent.md")

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
PROFILE = get_workflow_profile(dbutils.widgets.get("workflow_profile").strip() or "odd_report_v1")
REPORT_TEMPLATE_NAME = dbutils.widgets.get("report_template_name").strip() or None
AGENT_SPEC_PATH = dbutils.widgets.get("agent_spec_path").strip() or "ODDAgent.md"


def _current_workspace_dir() -> str | None:
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        if notebook_path.startswith("/"):
            notebook_path = "/Workspace" + notebook_path
        return os.path.dirname(notebook_path)
    except Exception:
        return None


def _candidate_spec_paths(raw_path: str) -> list[str]:
    raw_path = raw_path.strip()
    candidates = []
    if raw_path:
        candidates.append(raw_path)
        if not os.path.isabs(raw_path):
            candidates.append(os.path.join(os.getcwd(), raw_path))
            workspace_dir = _current_workspace_dir()
            if workspace_dir:
                candidates.append(os.path.join(workspace_dir, raw_path))
    deduped = []
    for path in candidates:
        normalized = uc_dbfs_to_local_path(path)
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _read_text_file(raw_path: str) -> tuple[str, str]:
    attempts = []
    for candidate in _candidate_spec_paths(raw_path):
        attempts.append(candidate)
        if not os.path.exists(candidate):
            continue
        for encoding in ("utf-8", "utf-8-sig", "cp1252"):
            try:
                with open(candidate, "r", encoding=encoding) as fh:
                    return candidate, fh.read()
            except UnicodeDecodeError:
                continue
    raise FileNotFoundError(f"Could not locate readable agent spec for {raw_path}. Tried: {attempts}")


def _extract_section(markdown_text: str, heading_text: str) -> str:
    pattern = rf"(?ms)^##\s+{re.escape(heading_text)}\s*$\n(.*?)(?=^##\s+|\Z)"
    match = re.search(pattern, markdown_text)
    if not match:
        raise RuntimeError(f"Missing section '## {heading_text}' in ODD agent spec.")
    return match.group(1).strip()


def _parse_bullets(section_text: str) -> list[str]:
    bullets = []
    for line in section_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(normalize_text(stripped[2:]))
    return bullets


def _strip_md(text: str) -> str:
    cleaned = (text or "").replace("**", "").replace("`", "")
    cleaned = (
        cleaned
        .replace("\u2014", "-")
        .replace("\u2013", "-")
        .replace("\u2192", "->")
        .replace("â€”", "-")
        .replace("â€“", "-")
        .replace("â†’", "->")
        .replace("Â§", "Section ")
    )
    return normalize_text(cleaned)


spec_local_path, spec_text = _read_text_file(AGENT_SPEC_PATH)
spec_text = (
    spec_text
    .replace("\r\n", "\n")
    .replace("\u2014", "-")
    .replace("\u2013", "-")
    .replace("â€”", "-")
    .replace("â€“", "-")
)
print("Loaded agent spec:", spec_local_path)

general_prompt_section = _extract_section(spec_text, "1. Role and General Prompt")
risk_section = _extract_section(spec_text, "2. Risk Rating Scale")
topics_section = _extract_section(spec_text, "3. Part 1 - DDQ Topics")

prompt_text = "\n".join(_parse_bullets(general_prompt_section)).strip()
if not prompt_text:
    raise RuntimeError("No behavior bullets were extracted from section 1 of the ODD agent spec.")

risk_rows = []
for idx, line in enumerate(_parse_bullets(risk_section)):
    match = re.match(r"^\*\*([^*]+)\*\*\s*-\s*(.+)$", line)
    if not match:
        match = re.match(r"^([^-\*][^-]+?)\s*-\s*(.+)$", _strip_md(line))
    if not match:
        continue
    risk_rows.append({
        "engagement_id": ENGAGEMENT_ID,
        "rating_label": _strip_md(match.group(1)).title(),
        "rating_definition": _strip_md(match.group(2)),
        "sort_order": idx,
    })

if not risk_rows:
    raise RuntimeError("No risk rating definitions were extracted from section 2 of the ODD agent spec.")

topic_rows = []
topic_order = 0
current_chapter_code = None
current_chapter_title = None
current_topic_idx = None

for raw_line in topics_section.splitlines():
    line = raw_line.rstrip()
    stripped = line.strip()
    if not stripped:
        continue

    chapter_match = re.match(r"^###\s+([A-D])\.\s+(.+)$", stripped)
    if chapter_match:
        current_chapter_code = chapter_match.group(1)
        current_chapter_title = _strip_md(chapter_match.group(2))
        current_topic_idx = None
        continue

    topic_match = re.match(r"^- \*\*([A-D]\d{4})\s+(.+?)\*\*$", stripped)
    if topic_match:
        topic_order += 1
        section_code = topic_match.group(1)
        topic_title = _strip_md(topic_match.group(2))
        raw_topic_text = f"{current_chapter_code}. {current_chapter_title} - {section_code}. {topic_title}"
        topic_rows.append({
            "engagement_id": ENGAGEMENT_ID,
            "topic_id": sha256_hex(f"{ENGAGEMENT_ID}||{section_code}||{topic_title}"),
            "topic_order": topic_order,
            "table_index": 2,
            "topic_row_index": (topic_order - 1) * 2,
            "answer_row_index": (topic_order - 1) * 2 + 1,
            "chapter_code": current_chapter_code,
            "chapter_title": current_chapter_title,
            "section_code": section_code,
            "topic_title": topic_title,
            "topic_prompt": None,
            "raw_topic_text": raw_topic_text,
        })
        current_topic_idx = len(topic_rows) - 1
        continue

    prompt_match = re.match(r"^Prompt:\s*(.+)$", stripped)
    if prompt_match and current_topic_idx is not None:
        existing = topic_rows[current_topic_idx].get("topic_prompt")
        addition = _strip_md(prompt_match.group(1))
        topic_rows[current_topic_idx]["topic_prompt"] = addition if not existing else f"{existing} {addition}".strip()

if not topic_rows:
    raise RuntimeError("No Part 1 topics were extracted from section 3 of the ODD agent spec.")

template_docs = (
    spark.table(DOCUMENTS_TABLE)
    .filter((F.col("engagement_id") == ENGAGEMENT_ID) & (F.col("source_role") == "report_template") & (F.col("is_present") == True))
    .orderBy("file_name")
)
if REPORT_TEMPLATE_NAME:
    template_docs = template_docs.filter(F.col("file_name") == REPORT_TEMPLATE_NAME)
template_doc_rows = [r.asDict() for r in template_docs.collect()]
if len(template_doc_rows) != 1:
    raise ValueError(
        f"Expected exactly 1 report template for engagement {ENGAGEMENT_ID}; found {[d['file_name'] for d in template_doc_rows]}"
    )
template_doc = template_doc_rows[0]

manager_doc_rows = (
    spark.table(DOCUMENTS_TABLE)
    .filter((F.col("engagement_id") == ENGAGEMENT_ID) & (F.col("source_role") == "manager_completed_ddq") & (F.col("is_present") == True))
    .orderBy(F.col("load_ts").desc(), F.col("file_name"))
    .limit(1)
    .collect()
)
manager_doc = manager_doc_rows[0].asDict() if manager_doc_rows else {}

metadata_df = spark.createDataFrame([{
    "engagement_id": ENGAGEMENT_ID,
    "report_template_document_id": template_doc["document_id"],
    "report_template_path": template_doc["file_path_dbfs"],
    "report_template_name": template_doc["file_name"],
    "agent_spec_path": spec_local_path,
    "mandate_name": manager_doc.get("mandate_name"),
    "manager_name": manager_doc.get("manager_name"),
    "investment_strategy": manager_doc.get("investment_strategy"),
    "report_finalization_date": manager_doc.get("report_finalization_date"),
    "portfolio_odd_managers": manager_doc.get("portfolio_odd_managers"),
    "authors": manager_doc.get("authors"),
    "prompt_text": prompt_text,
    "part1_table_index": 2,
    "part2_table_index": 3,
}], schema=T.StructType([
    T.StructField("engagement_id", T.StringType(), False),
    T.StructField("report_template_document_id", T.StringType(), True),
    T.StructField("report_template_path", T.StringType(), True),
    T.StructField("report_template_name", T.StringType(), True),
    T.StructField("agent_spec_path", T.StringType(), True),
    T.StructField("mandate_name", T.StringType(), True),
    T.StructField("manager_name", T.StringType(), True),
    T.StructField("investment_strategy", T.StringType(), True),
    T.StructField("report_finalization_date", T.StringType(), True),
    T.StructField("portfolio_odd_managers", T.StringType(), True),
    T.StructField("authors", T.StringType(), True),
    T.StructField("prompt_text", T.StringType(), True),
    T.StructField("part1_table_index", T.IntegerType(), True),
    T.StructField("part2_table_index", T.IntegerType(), True),
])).withColumn("load_ts", F.current_timestamp())

topics_df = spark.createDataFrame(topic_rows, schema=T.StructType([
    T.StructField("engagement_id", T.StringType(), False),
    T.StructField("topic_id", T.StringType(), False),
    T.StructField("topic_order", T.IntegerType(), False),
    T.StructField("table_index", T.IntegerType(), False),
    T.StructField("topic_row_index", T.IntegerType(), False),
    T.StructField("answer_row_index", T.IntegerType(), False),
    T.StructField("chapter_code", T.StringType(), True),
    T.StructField("chapter_title", T.StringType(), True),
    T.StructField("section_code", T.StringType(), True),
    T.StructField("topic_title", T.StringType(), True),
    T.StructField("topic_prompt", T.StringType(), True),
    T.StructField("raw_topic_text", T.StringType(), True),
])).withColumn("load_ts", F.current_timestamp())

risk_df = spark.createDataFrame(risk_rows, schema=T.StructType([
    T.StructField("engagement_id", T.StringType(), False),
    T.StructField("rating_label", T.StringType(), False),
    T.StructField("rating_definition", T.StringType(), True),
    T.StructField("sort_order", T.IntegerType(), False),
])).withColumn("load_ts", F.current_timestamp())

spark.sql(f"DELETE FROM {ODD_REPORT_METADATA_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")
spark.sql(f"DELETE FROM {ODD_REPORT_TOPICS_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")
spark.sql(f"DELETE FROM {ODD_RISK_DEFINITIONS_TABLE} WHERE engagement_id = '{ENGAGEMENT_ID}'")

metadata_df.write.mode("append").saveAsTable(ODD_REPORT_METADATA_TABLE)
topics_df.write.mode("append").saveAsTable(ODD_REPORT_TOPICS_TABLE)
risk_df.write.mode("append").saveAsTable(ODD_RISK_DEFINITIONS_TABLE)

display(spark.table(ODD_REPORT_TOPICS_TABLE).filter(F.col("engagement_id") == ENGAGEMENT_ID).orderBy("topic_order"))
display(spark.table(ODD_RISK_DEFINITIONS_TABLE).filter(F.col("engagement_id") == ENGAGEMENT_ID).orderBy("sort_order"))

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
