# Databricks notebook source
# NOTEBOOK FILE: 10_evaluate.py
# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %run ./_utils

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10 Evaluate DDQ answers against a curated gold set

# COMMAND ----------

dbutils.widgets.text("engagement_id", "odd_ssga_2025")
dbutils.widgets.text("gold_csv_path", "")

ENGAGEMENT_ID = dbutils.widgets.get("engagement_id").strip()
GOLD_CSV_PATH = dbutils.widgets.get("gold_csv_path").strip()

if not GOLD_CSV_PATH:
    raise ValueError("Set `gold_csv_path` to a CSV with columns like question_id,gold_answer,gold_evidence_files,is_high_risk.")

answers_df = (
    spark.table(ANSWERS_TABLE)
    .filter(F.col("engagement_id") == ENGAGEMENT_ID)
    .select(
        "question_id",
        "draft_answer",
        "citations",
        "source_tiers_used",
        "human_review_flag",
        "missing_information",
    )
)
gold_df = (
    spark.read.option("header", True).csv(uc_dbfs_to_local_path(GOLD_CSV_PATH))
    .select(
        F.col("question_id"),
        F.col("gold_answer"),
        F.col("gold_evidence_files"),
        F.coalesce(F.col("is_high_risk").cast("boolean"), F.lit(False)).alias("is_high_risk"),
    )
)

joined = (
    gold_df.join(answers_df, on="question_id", how="left")
    .withColumn("draft_lower", F.lower(F.coalesce(F.col("draft_answer"), F.lit(""))))
    .withColumn("gold_has_answer", F.length(F.trim(F.coalesce(F.col("gold_answer"), F.lit("")))) > 0)
    .withColumn("is_insufficient", F.col("draft_lower").contains("insufficient evidence"))
    .withColumn("false_insufficient", F.col("gold_has_answer") & F.col("is_insufficient"))
    .withColumn(
        "tier_drift",
        F.when(
            F.col("source_tiers_used").isNull() | (F.size(F.col("source_tiers_used")) == 0),
            F.lit(False),
        ).otherwise(F.array_min(F.col("source_tiers_used")) >= 4)
    )
    .withColumn(
        "citation_validity_ok",
        F.when(
            F.col("citations").isNull(),
            F.lit(False),
        ).otherwise(F.expr("aggregate(citations, true, (acc, x) -> acc AND x.chunk_id IS NOT NULL)"))
    )
    .withColumn(
        "wrong_scope_proxy",
        F.lower(F.coalesce(F.col("gold_evidence_files"), F.lit(""))).contains("ssga")
        & F.col("tier_drift"),
    )
)

metrics = joined.agg(
    F.count("*").alias("gold_questions"),
    F.avg(F.when(F.col("false_insufficient"), 1.0).otherwise(0.0)).alias("false_insufficient_rate"),
    F.avg(F.when(F.col("wrong_scope_proxy"), 1.0).otherwise(0.0)).alias("wrong_scope_rate"),
    F.avg(F.when(F.col("tier_drift"), 1.0).otherwise(0.0)).alias("tier_drift_rate"),
    F.avg(F.when(F.col("citation_validity_ok"), 1.0).otherwise(0.0)).alias("citation_validity"),
)

display(metrics)
display(
    joined.select(
        "question_id",
        "is_high_risk",
        "false_insufficient",
        "wrong_scope_proxy",
        "tier_drift",
        "citation_validity_ok",
        "human_review_flag",
        "missing_information",
    ).orderBy("question_id")
)
