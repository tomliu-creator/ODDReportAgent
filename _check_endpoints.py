# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick endpoint health check
# MAGIC Paste this into a Databricks notebook cell and run.

# COMMAND ----------
import requests
import os

# --- CONFIGURE THESE ---
EMBEDDING_ENDPOINT = "bge_large_en_v1_5"
LLM_ENDPOINT = "gpt-oss-20b"
# -----------------------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

headers = {"Authorization": f"Bearer {token}"}

def check_endpoint(name, expected_task=None):
    url = f"{host}/api/2.0/serving-endpoints/{name}"
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        data = r.json()
        state = data.get("state", {}).get("ready", "UNKNOWN")
        task = data.get("task", "N/A")
        print(f"[OK] {name} — state={state}, task={task}")
        if expected_task and task != expected_task:
            print(f"  WARNING: expected task='{expected_task}', got '{task}'")
        return True
    elif r.status_code == 404:
        print(f"[MISSING] {name} — endpoint not found")
        return False
    else:
        print(f"[ERROR] {name} — HTTP {r.status_code}: {r.text[:200]}")
        return False

print(f"Host: {host}\n")

print("=== Embedding model ===")
emb_ok = check_endpoint(EMBEDDING_ENDPOINT, expected_task="llm/v1/embeddings")

print("\n=== LLM model ===")
llm_ok = check_endpoint(LLM_ENDPOINT, expected_task="llm/v1/chat")

print("\n=== Summary ===")
print(f"Embedding '{EMBEDDING_ENDPOINT}': {'READY' if emb_ok else 'MISSING'}")
print(f"LLM '{LLM_ENDPOINT}': {'READY' if llm_ok else 'MISSING'}")
print(f"\nAll good to run pipeline: {emb_ok and llm_ok}")
