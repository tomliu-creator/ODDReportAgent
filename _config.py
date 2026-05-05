# Databricks notebook source
# NOTEBOOK FILE: _config.py
# This notebook is intended to be `%run` from the pipeline notebooks.

# COMMAND ----------

# Core UC locations for the ODD assessment agent.
CATALOG = "cmi_agent"
SCHEMA = "ddq_agent"
VOLUME = "engagements"

# Shared storage tables.
DOCUMENTS_TABLE = f"{CATALOG}.{SCHEMA}.documents"
PAGES_TABLE = f"{CATALOG}.{SCHEMA}.document_pages"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.document_chunks"
QUESTIONS_TABLE = f"{CATALOG}.{SCHEMA}.ddq_questions"  # legacy
ANSWERS_TABLE = f"{CATALOG}.{SCHEMA}.ddq_answers"  # legacy
ERRORS_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_errors"

# ODD-specific tables.
ODD_REPORT_METADATA_TABLE = f"{CATALOG}.{SCHEMA}.odd_report_metadata"
ODD_REPORT_TOPICS_TABLE = f"{CATALOG}.{SCHEMA}.odd_report_topics"
ODD_RISK_DEFINITIONS_TABLE = f"{CATALOG}.{SCHEMA}.odd_report_risk_definitions"
ODD_TOPIC_ASSESSMENTS_TABLE = f"{CATALOG}.{SCHEMA}.odd_topic_assessments"
ODD_CHAPTER_SUMMARIES_TABLE = f"{CATALOG}.{SCHEMA}.odd_chapter_summaries"

# Volume layout.
VOLUME_DBFS_ROOT = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
VOLUME_FUSE_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


def engagement_paths(engagement_id: str) -> dict:
    """Return per-engagement folder paths (both dbfs:/ and /Volumes/ flavors)."""
    return {
        "inputs_dbfs": f"{VOLUME_DBFS_ROOT}/{engagement_id}/inputs",
        "inputs_local": f"{VOLUME_FUSE_ROOT}/{engagement_id}/inputs",
        "questionnaire_dbfs": f"{VOLUME_DBFS_ROOT}/{engagement_id}/questionnaire",
        "questionnaire_local": f"{VOLUME_FUSE_ROOT}/{engagement_id}/questionnaire",
        "output_dbfs": f"{VOLUME_DBFS_ROOT}/{engagement_id}/output",
        "output_local": f"{VOLUME_FUSE_ROOT}/{engagement_id}/output",
    }


def vs_index_name(engagement_id: str) -> str:
    """One Vector Search index per engagement."""
    return f"{CATALOG}.{SCHEMA}.idx_{engagement_id}"


def uc_safe_suffix(raw: str) -> str:
    """Normalize free-form ids into UC-safe identifier suffixes."""
    import re

    cleaned = re.sub(r"[^0-9A-Za-z_]", "_", (raw or "").strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    return cleaned or "default"


def vs_source_table_name(engagement_id: str) -> str:
    """Dedicated Delta Sync source table per engagement."""
    return f"{CATALOG}.{SCHEMA}.document_chunks_vs_{uc_safe_suffix(engagement_id)}"


DEFAULT_VS_ENDPOINT = "emd-default-vs"
DEFAULT_EMBEDDING_MODEL = "databricks-gte-large-en"
EMBEDDING_MODEL_CANDIDATES = [
    "databricks-gte-large-en",
    "databricks-bge-large-en",
    "bge_base_en_v1_5",
    "bge_large_en_v1_5",
]

DEFAULT_LLM_ENDPOINT = "databricks-gpt-oss-20b"
DEFAULT_ASSESSMENT_MODEL = DEFAULT_LLM_ENDPOINT
DEFAULT_RISK_MODEL = DEFAULT_LLM_ENDPOINT
DEFAULT_TOP_K = 20
DEFAULT_FINAL_EVIDENCE_K = 8
HIGH_RISK_RETRIEVAL_MULTIPLIER = 2

SECTION_HEADING_REGEX = r"^([A-Z]\d{4})[\.]?\s+(.+)$"
QUESTION_NUMBERING_REGEX = r"^\s*(\d{1,3})[\.\)]\s+(.+)"
APPENDIX_REF_REGEX = r"(?i)\bappendix(?:es)?\s+([A-Za-z0-9,\-\sand]+)"
TOPIC_ROW_REGEX = r"^([A-D])\.\s+(.+?)\s+-\s+([A-Z]\d{4})\.\s+(.+)$"

SOURCE_ROLE_RULES = [
    {"role": "report_template", "keywords": ["odd reports blank", "odd report"], "extensions": [".docx"]},
    {"role": "manager_completed_ddq", "keywords": ["final responses", "completed ddq", "ddq"], "extensions": [".docx"]},
]

SOURCE_TIER_BY_ROLE = {
    "manager_completed_ddq": 0,
    "appendix": 1,
    "policy": 2,
    "assurance_report": 3,
    "public_disclosure": 4,
    "report_template": 9,
    "other": 5,
}

SOURCE_TIER_RULES = [
    {"tier": 2, "keywords": ["policy", "procedure", "compliance", "code of ethics", "mnpi"]},
    {"tier": 3, "keywords": ["soc", "isae", "assurance"]},
    {"tier": 4, "keywords": ["annual", "10-k", "10k", "public report"]},
]

LEGAL_ENTITY_PATTERNS = [
    {"label": "State Street Investment Management", "pattern": r"state street investment management"},
    {"label": "SSGA Europe Limited", "pattern": r"ssga europe limited"},
    {"label": "State Street Corporation", "pattern": r"state street corporation"},
]

DOC_SHORT_TITLES = {
    "appx_11_remun.pdf": "Appendix 11 - Remuneration Policy Overview",
}

WORKFLOW_PROFILES = {
    "odd_report_v1": {
        "section_heading_regex": SECTION_HEADING_REGEX,
        "question_numbering_regex": QUESTION_NUMBERING_REGEX,
        "topic_row_regex": TOPIC_ROW_REGEX,
        "appendix_ref_regex": APPENDIX_REF_REGEX,
        "source_tier_rules": SOURCE_TIER_RULES,
        "legal_entity_patterns": LEGAL_ENTITY_PATTERNS,
        "doc_short_titles": DOC_SHORT_TITLES,
        "default_assessment_model": DEFAULT_ASSESSMENT_MODEL,
        "default_risk_model": DEFAULT_RISK_MODEL,
        "high_risk_topics": [
            "ownership",
            "governance",
            "conflict",
            "key person",
            "reputation",
            "integrity",
            "risk management",
            "assurance",
            "compliance",
            "legal",
            "tax",
            "valuation",
            "reporting",
            "service providers",
            "systems",
            "it security",
            "business continuity",
            "disaster recovery",
            "cyber",
        ],
    },
}

# Legacy DDQ profile retained so existing notebooks remain readable if referenced manually.
QUESTIONNAIRE_PROFILES = {
    "odd_ssga_v1": {
        "answer_placeholder_pattern": r"<Provide your answer here\.>",
        "section_heading_regex": SECTION_HEADING_REGEX,
        "question_numbering_regex": QUESTION_NUMBERING_REGEX,
        "skip_tables": True,
        "yes_no_prefixes": [
            "Do you", "Does ", "Is ", "Are ", "Has ", "Have ", "Will ", "Can ", "Did ",
        ],
        "numeric_keywords": [
            "how many", "what percentage", "what is the number", "number of",
        ],
        "source_tier_rules": SOURCE_TIER_RULES,
        "legal_entity_patterns": LEGAL_ENTITY_PATTERNS,
        "doc_short_titles": DOC_SHORT_TITLES,
        "mandate_query_triggers": [
            "mandate", "product", "strategy", "investment process",
            "portfolio manager", "staff", "team", "target investors",
            "allocated", "allocation", "resources", "emerging market", "msci em",
        ],
        "mandate_query_terms": [
            "Emerging Market Equity",
            "MSCI EM Index Strategy",
            "SSGA Europe",
            "State Street Investment Management",
            "Emerging Markets Indexing",
        ],
        "high_risk_topics": WORKFLOW_PROFILES["odd_report_v1"]["high_risk_topics"],
    },
}
