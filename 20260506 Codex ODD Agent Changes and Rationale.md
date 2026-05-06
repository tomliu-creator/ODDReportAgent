# 20260506 Codex ODD Agent Changes and Rationale

This document summarizes the main ODD Agent changes made on 2026-05-06, why they were made, and what still needs to be rerun in Databricks for the changes to affect generated outputs.

## Executive Summary

The work focused on making the ODD Agent more faithful to the completed manager DDQ, `ODDAgent.md`, and the source hierarchy. The main fixes were:

- Prevent DDQ question wording from leaking into generated assessments as evidence.
- Preserve DDQ question wording for vector relevance by separating `embedding_text` from answer-only `chunk_text`.
- Use `topic_prompt` from `ODDAgent.md` in retrieval queries for all topics.
- Diagnose and fix appendix metadata staleness that prevented appendix chunks, including Appendix 8 annual-report chunks, from being retrieved.
- Retain both SQL and Python Vector Search retrieval modes with comments explaining why both wedges exist.
- Sync the changed notebooks to the Databricks workspace.

## Files Changed

- `01_setup.py`
- `02_ingest_inputs.py`
- `04_chunk.py`
- `05_vector_index.py`
- `07_assess_odd_report.py`

The synced Databricks notebook paths are under:

`/Workspace/Users/tomliushopping@gmail.com/notebooks_ddq/`

## A0400 Question Text Leakage Fix

### Problem

The generated assessment for `A0400 Firm governance structure` contained too much DDQ question wording rather than only manager-provided answers.

The focused test showed that `03_parse_sources.py` correctly separated `content_role='question'` and `content_role='answer'`, but `04_chunk.py` recombined question rows into `chunk_text` as:

`Question {n}: ...`

Because `07_assess_odd_report.py` uses `chunk_text` as evidence text, the assessment model could quote or summarize the DDQ question instead of only the manager answer.

Evidence log:

`logs/a0400_question_leak_test_20260506_063549.log`

### Change

`04_chunk.py` now excludes `content_role='question'` rows from answer/evidence `chunk_text`.

### Reason

The ODD assessment should assess and cite the manager's answer and supporting evidence, not restate the questionnaire prompt as if it were evidence.

### Verification

Post-fix test:

`logs/a0400_question_leak_fix_test_20260506_070724.log`

Result:

`Chunks with detected A0400 question text after fix: 0`

## Separate `embedding_text` From `chunk_text`

### Problem

Removing DDQ question text from `chunk_text` also removed it from Vector Search similarity because `05_vector_index.py` originally used:

`embedding_source_column="chunk_text"`

That meant question wording was no longer available to help retrieve the relevant manager answer.

### Change

`01_setup.py` adds an `embedding_text` column to `document_chunks`.

`04_chunk.py` writes:

- `embedding_text`: includes DDQ question wording plus manager answer text for vector relevance.
- `chunk_text`: excludes DDQ question wording and contains evidence text only.

`05_vector_index.py` now uses:

`embedding_source_column="embedding_text"`

### Reason

This preserves the useful retrieval signal from the DDQ question while preventing question wording from being passed to the assessment model as evidence.

### Verification

Split test:

`logs/a0400_embedding_vs_evidence_split_test_20260506_071400.log`

Result:

- `embedding_text includes question wording for relevance: True`
- `chunk_text excludes question wording for evidence: True`
- `PASS`

## Topic Prompt Driven Retrieval

### Problem

`ODDAgent.md` contains topic-specific prompts, for example `A0300 Financial condition of the firm`, but retrieval originally used only:

`section_code + topic_title + chapter_title`

The `topic_prompt` was added only later to the final assessment prompt, meaning Vector Search could miss evidence that the topic prompt explicitly required.

### Change

`07_assess_odd_report.py` now builds retrieval query text through `_topic_query_text(topic)`, which includes:

`section_code + topic_title + chapter_title + topic_prompt`

This is used for:

- same-section manager DDQ retrieval
- appendix retrieval
- global manager DDQ fallback retrieval
- A0300 mandatory external-source pass

If `topic_prompt` is empty, it is skipped and retrieval uses the section/topic/chapter text only.

### Reason

`ODDAgent.md` is the runtime behavior spec. Retrieval should be driven by the topic instruction itself, not by a separate hand-written translation of the prompt.

## A0300 Annual Report Retrieval Investigation

### Problem

The `A0300` prompt says not to rely only on the DDQ and to always check the latest annual report, audited financial statements, credit-rating evidence, and the DDQ response. However, the agent did not retrieve evidence from:

`Appendix 8 - 2024 Annual Report - State Street Corporation.pdf`

### Findings

The investigation found four issues:

- The `A0300` `topic_prompt` was loaded correctly from `ODDAgent.md`.
- Appendix 8 existed in `documents` and contained financial-performance chunks, including revenue, net income, EPS / earnings-per-share, and margin-related text.
- Existing Appendix 8 chunks had `source_role=NULL` and `source_tier=NULL`, so `source_role='appendix'` retrieval filtered them out.
- SQL Vector Search retrieval applies filters after `vector_search(...)` returns top N globally, so filtered appendix retrieval can be starved by DDQ hits unless it over-fetches.

Evidence log:

`logs/a0300_annual_report_retrieval_test_20260506_074052.log`

Post-fix source-selection check:

`logs/a0300_mandatory_source_postfix_test_20260506_074318.log`

### Change

`07_assess_odd_report.py` now gives `A0300` a mandatory external evidence pass against appendices using the `topic_prompt` itself as the retrieval query.

It does not hard-code Appendix 8 by filename.

It does not use a separate hand-written financial keyword taxonomy.

### Reason

For A0300, annual-report / audited-financial / credit-rating evidence is not optional. The retrieval workflow should search the required source class using the actual topic prompt, while the reranker still decides which chunks are relevant enough to keep.

## Appendix Metadata Staleness Fix

### Problem

All 25 appendix documents were correctly classified in `documents` as:

`source_role='appendix'`

`source_tier=1`

But their existing `document_pages` and `document_chunks` rows had null `source_role` and null `source_tier`.

Root-cause log:

`logs/appendix_metadata_root_cause_20260506_075141.log`

### Root Cause

`02_ingest_inputs.py` updated `documents.source_role/source_tier` for unchanged files without resetting:

- `parse_status`
- `chunk_status`
- `index_status`

Because `03_parse_sources.py` and `04_chunk.py` only process documents whose status is `pending` or `error`, the stale page/chunk rows were never rebuilt after metadata classification improved.

### Change

`02_ingest_inputs.py` now detects source role/tier changes even when file fingerprints are unchanged. If either changes, it resets derived-stage statuses to pending:

- `parse_status='pending'`
- `chunk_status='pending'`
- `index_status='pending'`

### Reason

Source role and tier flow into parsed pages, chunks, retrieval filters, and Vector Search metadata. If classification changes, derived rows must be rebuilt.

## Chunk Merge Metadata Refresh

### Problem

`04_chunk.py` previously updated matched chunks only when `chunk_sha` changed, or updated only text/timestamps on unchanged chunks. That could leave metadata stale when the chunk text stayed the same but fields such as `source_role`, `source_tier`, or `embedding_text` changed.

### Change

`04_chunk.py` now uses full `UPDATE SET *` for matched chunks.

### Reason

Chunk metadata is part of retrieval correctness. If metadata changes, the stored chunk row should reflect it even when text content is unchanged.

## SQL And Python Retrieval Wedges

### Current Design

`07_assess_odd_report.py` keeps two retrieval modes:

- `sql`
- `python`

The workflow defaults to `sql`.

### Reason

The SQL path is safer for this Databricks workspace because the Python Vector Search client path previously had environment-specific serving/index constraints, including model serving constraints around endpoints that cannot scale to zero.

The Python path remains useful as a fallback/debug wedge and should not be removed just to simplify the code.

The code now includes comments explaining this so the split is not removed later for convenience.

## SQL Vector Search Over-Fetch

### Problem

The SQL retrieval path calls `vector_search(...)` first and applies filters afterward. If `num_results` is small, a source-role-filtered search can return zero rows even when relevant rows exist, because the initial global top N may be dominated by DDQ chunks.

### Change

`07_assess_odd_report.py` over-fetches in SQL mode before applying filters.

### Reason

This makes filtered source-class retrieval more reliable, especially for appendix searches and A0300 external-evidence requirements.

## Current Databricks Follow-Up Required

The code has been patched and synced, but existing Databricks tables still contain stale derived rows until the pipeline is rerun or repaired.

Recommended rerun order:

1. `01_setup.py`
2. `02_ingest_inputs.py`
3. `03_parse_sources.py`
4. `04_chunk.py`
5. `05_vector_index.py`
6. `07_assess_odd_report.py`
7. `08_fill_odd_report.py`

Important note:

Because the current `documents` metadata is already correct, `02_ingest_inputs.py` may not by itself detect a new role/tier change for already-stale appendices. To repair the current Databricks state, either:

- mark affected appendix `parse_status`, `chunk_status`, and `index_status` as `pending`, then rerun parse/chunk/index; or
- run a targeted metadata backfill from `documents` into `document_pages` and `document_chunks`, then refresh/recreate the Vector Search index.

The cleaner long-term route is a full pending-status repair and rerun so `embedding_text`, `chunk_text`, source role/tier, and index metadata are all rebuilt consistently.

## Verification Artifacts

- `logs/a0400_question_leak_test_20260506_063549.log`
- `logs/a0400_question_leak_fix_test_20260506_070724.log`
- `logs/a0400_embedding_vs_evidence_split_test_20260506_071400.log`
- `logs/a0300_annual_report_retrieval_test_20260506_074052.log`
- `logs/a0300_mandatory_source_postfix_test_20260506_074318.log`
- `logs/appendix_metadata_root_cause_20260506_075141.log`

