# Databricks notebook source
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
# MAGIC ## 05 Per-engagement Delta Sync Vector Search index
# MAGIC
# MAGIC One index per engagement: `cmi_agent.ddq_agent.idx_<engagement_id>`.
# MAGIC The synced metadata includes section and source-role columns so retrieval can target DDQ
# MAGIC sections such as `A0100` before falling back globally.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import json
import requests
import time

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("vs_endpoint_name", DEFAULT_VS_ENDPOINT)
dbutils.widgets.text("embedding_model_endpoint_name", DEFAULT_EMBEDDING_MODEL)
dbutils.widgets.dropdown("pipeline_type", "TRIGGERED", ["TRIGGERED", "CONTINUOUS"])

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
VS_ENDPOINT_NAME = dbutils.widgets.get("vs_endpoint_name").strip() or DEFAULT_VS_ENDPOINT
VS_INDEX_NAME = vs_index_name(ENGAGEMENT_ID)
VS_SOURCE_TABLE = vs_source_table_name(ENGAGEMENT_ID)
EMBEDDING_MODEL_ENDPOINT = dbutils.widgets.get("embedding_model_endpoint_name").strip() or DEFAULT_EMBEDDING_MODEL
PIPELINE_TYPE = dbutils.widgets.get("pipeline_type").strip().upper()

print("ENGAGEMENT_ID:", ENGAGEMENT_ID)
print("VS endpoint:", VS_ENDPOINT_NAME)
print("VS index:", VS_INDEX_NAME)
print("VS source table:", VS_SOURCE_TABLE)
print("Embedding model:", EMBEDDING_MODEL_ENDPOINT)
print("Pipeline type:", PIPELINE_TYPE)


def _workspace_host() -> str:
    return f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"


def _workspace_token() -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def _serving_endpoint_exists(name: str) -> tuple[bool, dict | None]:
    if not name:
        return False, None
    url = f"{_workspace_host()}/api/2.0/serving-endpoints/{name}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {_workspace_token()}"}, timeout=30)
    if resp.status_code == 200:
        return True, resp.json()
    if resp.status_code == 404:
        return False, None
    raise RuntimeError(f"Serving endpoint lookup failed for '{name}': HTTP {resp.status_code} {resp.text[:300]}")


def _embedding_endpoint_supports_delta_sync(payload: dict | None) -> bool:
    if not payload:
        return False
    task = (payload or {}).get("task")
    if task and task != "llm/v1/embeddings":
        return False

    served_entities = ((payload or {}).get("config") or {}).get("served_entities") or []
    served_models = ((payload or {}).get("config") or {}).get("served_models") or []
    combined = list(served_entities) + list(served_models)
    if not combined:
        # Managed foundation endpoints may not expose scale settings. If the task is embeddings and
        # the endpoint is READY, treat it as eligible.
        return ((payload or {}).get("state") or {}).get("ready") == "READY"

    for entity in combined:
        if entity.get("scale_to_zero_enabled") is True:
            return False
        if entity.get("min_provisioned_concurrency") == 0:
            return False
    return True


def _resolve_embedding_endpoint(requested_name: str) -> str:
    candidates = []
    if requested_name:
        candidates.append(requested_name)
    for name in EMBEDDING_MODEL_CANDIDATES:
        if name not in candidates:
            candidates.append(name)

    for name in candidates:
        exists, payload = _serving_endpoint_exists(name)
        if not exists:
            continue
        state = (payload or {}).get("state", {}).get("ready")
        task = (payload or {}).get("task")
        print(f"Embedding endpoint check: name={name}, state={state}, task={task}")
        if not _embedding_endpoint_supports_delta_sync(payload):
            print(f"Skipping embedding endpoint '{name}' because it is not eligible for Delta Sync Vector Search.")
            continue
        return name

    raise ValueError(f"No usable embedding serving endpoint found. Tried: {candidates}.")


EMBEDDING_MODEL_ENDPOINT = _resolve_embedding_endpoint(EMBEDDING_MODEL_ENDPOINT)
print("Embedding model (effective):", EMBEDDING_MODEL_ENDPOINT)

# COMMAND ----------

chunk_count = (
    spark.table(CHUNKS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .filter(F.col("source_role").isNotNull())
    .filter(F.col("source_role") != "report_template")
    .count()
)
print("Chunks for engagement:", chunk_count)
if chunk_count == 0:
    raise ValueError(f"No chunks for engagement_id={ENGAGEMENT_ID}; run 04_chunk first.")

spark.sql(f"DROP TABLE IF EXISTS {VS_SOURCE_TABLE}")
spark.sql(f"""
CREATE TABLE {VS_SOURCE_TABLE}
USING DELTA
AS
SELECT *
FROM {CHUNKS_TABLE}
WHERE 1 = 0
""")

spark.sql(f"ALTER TABLE {VS_SOURCE_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"DELETE FROM {VS_SOURCE_TABLE}")
spark.sql(f"""
INSERT INTO {VS_SOURCE_TABLE}
SELECT *
FROM {CHUNKS_TABLE}
WHERE engagement_id = '{ENGAGEMENT_ID}'
  AND source_role IS NOT NULL
  AND coalesce(source_role, '') <> 'report_template'
""")

vs_source_count = spark.table(VS_SOURCE_TABLE).count()
print("Rows in per-engagement VS source table:", vs_source_count)
if vs_source_count != chunk_count:
    raise RuntimeError(
        f"Expected {chunk_count} source rows for engagement_id={ENGAGEMENT_ID}, found {vs_source_count} in {VS_SOURCE_TABLE}."
    )

# COMMAND ----------

vsc = VectorSearchClient()

if not vsc.endpoint_exists(VS_ENDPOINT_NAME):
    existing = vsc.list_endpoints()
    if isinstance(existing, dict) and "endpoints" in existing:
        endpoints = existing.get("endpoints") or []
    elif isinstance(existing, list):
        endpoints = existing
    else:
        endpoints = []
    existing_names = sorted({
        (e.get("name") or e.get("endpoint_name") or e.get("endpointName"))
        for e in endpoints if isinstance(e, dict)
    } - {None})
    if existing_names:
        raise ValueError(
            f"VS endpoint '{VS_ENDPOINT_NAME}' missing but workspace already has {existing_names}. "
            "Set widget 'vs_endpoint_name' to one of those."
        )
    print("Creating Vector Search endpoint:", VS_ENDPOINT_NAME)
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
else:
    print("VS endpoint exists:", VS_ENDPOINT_NAME)

# COMMAND ----------

columns_to_sync = [
    "engagement_id",
    "document_id",
    "file_name",
    "source_path_dbfs",
    "page_start",
    "page_end",
    "chunk_type",
    "section_hint",
    "chunk_index",
    "source_tier",
    "legal_entity_hint",
    "source_role",
    "section_code",
    "section_title",
    "chapter_code",
    "chapter_title",
    "question_number",
    "is_manager_answer_chunk",
    "has_substantive_answer",
    "referenced_appendices",
    "embedding_text",
]


def _index_has_expected_metadata_columns(index_handle, expected_columns: list[str]) -> bool | None:
    try:
        desc = index_handle.describe()
    except Exception as e:
        print(f"Unable to inspect columns for existing index {VS_INDEX_NAME}: {e}")
        return None
    payload = json.dumps(desc).lower()
    if all(col.lower() in payload for col in expected_columns):
        return True
    if "columns_to_sync" not in payload and "schema_json" not in payload and "schema" not in payload:
        print(f"WARNING: index description for {VS_INDEX_NAME} does not expose synced-column metadata.")
        return None
    return False


def _index_uses_expected_embedding_endpoint(index_handle, expected_endpoint: str) -> bool | None:
    try:
        desc = index_handle.describe()
    except Exception as e:
        print(f"Unable to inspect embedding endpoint for existing index {VS_INDEX_NAME}: {e}")
        return None
    payload = json.dumps(desc).lower()
    if expected_endpoint.lower() in payload:
        return True
    if "embedding" not in payload and "model_endpoint" not in payload:
        print(f"WARNING: index description for {VS_INDEX_NAME} does not expose embedding-endpoint metadata.")
        return None
    return False


def _index_uses_expected_embedding_source(index_handle, expected_source_column: str) -> bool | None:
    try:
        desc = index_handle.describe()
    except Exception as e:
        print(f"Unable to inspect embedding source column for existing index {VS_INDEX_NAME}: {e}")
        return None
    payload = json.dumps(desc).lower()
    if expected_source_column.lower() in payload:
        return True
    if "embedding_source_column" not in payload and "embedding" not in payload and "schema" not in payload:
        print(f"WARNING: index description for {VS_INDEX_NAME} does not expose embedding-source metadata.")
        return None
    return False


def _delete_index(endpoint_name: str, index_name: str):
    encoded_index = requests.utils.quote(index_name, safe="")
    url = f"{_workspace_host()}/api/2.0/vector-search/indexes/{encoded_index}"
    resp = requests.delete(url, headers={"Authorization": f"Bearer {_workspace_token()}"}, timeout=60)
    if resp.status_code in (200, 202, 204, 404):
        return
    raise RuntimeError(f"Failed to delete Vector Search index '{index_name}': HTTP {resp.status_code} {resp.text[:300]}")


created_index = False
if not vsc.index_exists(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME):
    print("Creating Delta Sync index:", VS_INDEX_NAME)
    create_error = None
    for attempt in range(1, 4):
        try:
            vsc.create_delta_sync_index_and_wait(
                endpoint_name=VS_ENDPOINT_NAME,
                index_name=VS_INDEX_NAME,
                primary_key="chunk_id",
                source_table_name=VS_SOURCE_TABLE,
                pipeline_type=PIPELINE_TYPE,
                # Use embedding_text for relevance matching. For manager-completed DDQ chunks this
                # includes the original DDQ question plus the manager answer; downstream prompts
                # still use chunk_text, which excludes question wording and contains evidence only.
                embedding_source_column="embedding_text",
                embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
                columns_to_sync=columns_to_sync,
            )
            create_error = None
            break
        except Exception as e:
            create_error = e
            if vsc.index_exists(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME):
                print(f"Index create attempt {attempt} timed out after the backend accepted it; proceeding.")
                create_error = None
                break
            if attempt == 3:
                raise
            print(f"Index create attempt {attempt} failed: {e}")
            time.sleep(20 * attempt)
    if create_error is not None:
        raise create_error
    created_index = True
else:
    print("Index exists, validating metadata columns:", VS_INDEX_NAME)

index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
uses_expected_embedding = _index_uses_expected_embedding_endpoint(index, EMBEDDING_MODEL_ENDPOINT)
uses_expected_embedding_source = _index_uses_expected_embedding_source(index, "embedding_text")
if uses_expected_embedding is False or uses_expected_embedding_source is False:
    print(
        f"Existing index '{VS_INDEX_NAME}' was built with a different embedding configuration. "
        "Deleting and recreating it so Vector Search embeds embedding_text while prompts use answer-only chunk_text."
    )
    _delete_index(VS_ENDPOINT_NAME, VS_INDEX_NAME)
    created_index = False
    while vsc.index_exists(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME):
        time.sleep(10)
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        primary_key="chunk_id",
        source_table_name=VS_SOURCE_TABLE,
        pipeline_type=PIPELINE_TYPE,
        # Keep Vector Search relevance on embedding_text while assessment evidence remains chunk_text.
        embedding_source_column="embedding_text",
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        columns_to_sync=columns_to_sync,
    )
    created_index = True
    index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)

has_expected_columns = _index_has_expected_metadata_columns(
    index,
    ["source_role", "section_code", "chapter_code", "has_substantive_answer", "referenced_appendices", "embedding_text"],
)
if has_expected_columns is False:
    raise RuntimeError(
        f"Existing Vector Search index '{VS_INDEX_NAME}' is missing one or more required metadata columns. "
        "Recreate the index once, then rerun this notebook."
    )

if PIPELINE_TYPE == "TRIGGERED":
    print("Running triggered sync")
    sync_error = None
    for attempt in range(1, 7):
        try:
            index.sync()
            sync_error = None
            break
        except Exception as e:
            sync_error = e
            message = str(e)
            if "not ready to sync yet" not in message.lower():
                raise
            print(f"Index sync attempt {attempt} deferred because the pipeline is still setting up. Waiting before retrying.")
            time.sleep(30 * attempt)
            index.wait_until_ready(verbose=True, wait_for_updates=False)
    if sync_error is not None:
        raise sync_error
index.wait_until_ready(verbose=True, wait_for_updates=(PIPELINE_TYPE == "TRIGGERED"))

if created_index:
    print("Created and synced:", VS_INDEX_NAME)

spark.sql(f"""
UPDATE {CHUNKS_TABLE}
SET index_status = 'done',
    index_ts = current_timestamp(),
    index_error = null
WHERE engagement_id = '{ENGAGEMENT_ID}'
  AND index_status = 'pending'
""")
spark.sql(f"""
UPDATE {DOCUMENTS_TABLE}
SET index_status = CASE
      WHEN source_role = 'report_template' THEN index_status
      ELSE 'done'
    END,
    index_ts = current_timestamp()
WHERE engagement_id = '{ENGAGEMENT_ID}'
  AND (
      (parse_status = 'done' AND chunk_status = 'done')
      OR source_role = 'report_template'
  )
""")

show_validation_snapshot(CATALOG, SCHEMA, ENGAGEMENT_ID)
