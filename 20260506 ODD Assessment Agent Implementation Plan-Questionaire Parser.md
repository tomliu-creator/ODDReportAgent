# ODD Assessment Agent - Implementation Plan

## Context

The current `notebooks_ddq` workflow was originally built to fill a DDQ questionnaire: it ingests source PDFs, chunks them, builds a Vector Search index, parses a questionnaire DOCX into questions, drafts answers, and writes those answers back into the same questionnaire.

The revised requirement is not to fill the questionnaire anymore. The completed manager DDQ is now an input evidence document, and the ODD report is the output:

1. Treat `APG DDQ 16.01.2026 - Final responses.docx` as the highest-priority source, because it contains the manager's completed DDQ answers.
2. Use that completed DDQ, plus appendices where relevant, to generate operational due diligence assessments into `ODD Reports Blank.docx`.
3. Preserve the existing notebook pipeline shape as much as possible, but repurpose the later stages for ODD report assessment rather than DDQ answering.

Initial document inspection shows:

- `APG DDQ 16.01.2026 - Final responses.docx` has 23 DDQ section headings such as `A0100. Firm ownership structure`, `B0500. Compliance`, and `D0400. Business continuity, back-up and recovery`.
- `ODD Reports Blank.docx` has a Part 1 table with 23 matching assessment topics, one for each DDQ section code.
- `ODD Reports Blank.docx` has a Part 2 chapter summary table with ratings for chapters A-D plus an overall conclusion.
- The report template includes a reusable prompt paragraph that should drive every Part 1 topic assessment.
- The first report table contains engagement-level metadata: mandate name, manager, investment strategy, report date, portfolio/ODD managers, and authors.

This means the best design is not a generic global RAG pass. It should be a section-aware assessment workflow where the completed manager DDQ is treated as privileged evidence and appendices are used as supporting or fallback evidence.

## Target Behavior

For each Part 1 topic in `ODD Reports Blank.docx`:

1. Identify the topic's DDQ section code, for example `A0100`.
2. Retrieve from the completed manager DDQ section with the same `section_code` first.
3. If that section is missing or has no substantive manager answer, broaden retrieval globally across the manager DDQ and appendices.
4. Use appendices when they support, clarify, or challenge the manager response.
5. Generate a neutral, factual ODD assessment paragraph using the prompt from the report template.
6. Generate a risk rating and short rationale using a separate risk-assessment model endpoint. For now, configure this endpoint to the same LLM endpoint used for drafting.
7. Fill Part 1 of the ODD report with the generated assessment and rating.

Important workflow boundary: `APG DDQ 16.01.2026 - Final responses.docx` is never treated as a questionnaire to be completed. It is parsed and indexed as tier-0 evidence. If the manager left a DDQ section empty, the ODD report assessment should not try to fill that DDQ section; it should instead broaden evidence retrieval across the completed DDQ and appendices, then write the assessment directly into the ODD report.

For Part 2:

1. Aggregate Part 1 assessments by chapter A-D.
2. Generate high-level chapter summaries and ratings.
3. Generate an overall conclusion and rating.
4. Fill the Part 2 table in the ODD report.

## Recommended Architecture

Keep the existing pipeline shape:

```text
01_setup
02_ingest_inputs
03_parse_sources
04_chunk
05_vector_index
06_parse_odd_report
07_assess_odd_topics
08_fill_odd_report
09_orchestrate
10_evaluate
```

The structural change is that stages 06-08 become report-assessment stages for the ODD report workflow. Stages 01-05 should remain shared where possible, but they now ingest and index the completed DDQ as evidence rather than prepare a questionnaire for answering.

Do not preserve DDQ questionnaire filling as a first-class workflow for this implementation. The old notebooks may remain in the repository as legacy/reference wrappers if that minimizes disruption, but the normal orchestrated workflow should run the ODD report pipeline:

- Replace the active stage-06 behavior with `06_parse_odd_report.py`.
- Replace the active stage-07 behavior with `07_assess_odd_report.py`.
- Replace the active stage-08 behavior with `08_fill_odd_report.py`.
- Update `09_orchestrate.py` so the default and expected run path is ODD report generation.
- If keeping `06_parse_ddq.py`, `07_answer_ddq.py`, or `08_fill_docx.py`, mark them as legacy and exclude them from the default orchestrated run.

This keeps the useful notebook sequencing intact while removing the obsolete questionnaire-fill deliverable from the main workflow.

## Source Priority

Add a stricter source hierarchy:

| Tier | Source type | Example |
|---:|---|---|
| 0 | Completed manager DDQ responses | `APG DDQ 16.01.2026 - Final responses.docx` |
| 1 | Mandate-specific appendices | key personnel, org charts, risk team charts, ownership structure |
| 2 | Policies and procedures | compliance, conflicts, MNPI, code of ethics |
| 3 | Assurance reports | SOC reports |
| 4 | Annual report / public corporate disclosure | 2024 Annual Report |
| 5 | Generic fallback | other supporting material |

Tier 0 should dominate retrieval. Appendices should not displace the completed DDQ unless the DDQ section is empty, generic, or explicitly points to an appendix.

## Metadata Additions

### Document-level metadata

Add metadata to `documents`:

- `source_role`: `manager_completed_ddq`, `appendix`, `report_template`, `other`
- `source_tier`: integer as above
- `manager_name`
- `mandate_name`
- `investment_strategy`
- `report_finalization_date`
- `portfolio_odd_managers`
- `authors`

The first table in `ODD Reports Blank.docx` should be parsed into engagement metadata. The first mandate table in the completed DDQ should also be parsed where possible and used as a cross-check.

### Chunk-level metadata

Add metadata to `document_chunks`:

- `section_code`: `A0100`, `A0200`, `B0500`, etc.
- `section_title`
- `chapter_code`: `A`, `B`, `C`, `D`
- `chapter_title`
- `question_number`
- `is_manager_answer_chunk`: boolean
- `has_substantive_answer`: boolean
- `referenced_appendices`: array/string, for examples such as `Appendix 7 - Ownership Structure`
- `source_role`
- `source_tier`

The table-of-contents style codes should absolutely be stored as metadata. They make retrieval more precise and faster because Part 1 topics map directly to DDQ sections.

## Completed DDQ Parsing

Add DOCX parsing support to the source ingestion path. Current parsing is PDF-oriented; the manager-completed DDQ is a DOCX and should be indexed as evidence.

Recommended change:

- Rename or extend `03_parse_pdfs.py` into `03_parse_sources.py`.
- Keep existing PDF parsing unchanged.
- Add DOCX parsing for input documents with `source_role='manager_completed_ddq'`.
- Preserve paragraph order and heading context.
- Treat each `Heading 2` matching `^([A-Z]\d{4})\.\s+(.+)` as a DDQ section boundary.
- Within each section, capture question paragraphs and answer paragraphs.
- Mark a section as non-substantive if its answer text is empty, placeholder-like, or only says "please refer to appendix" without additional explanation.

Fallback rule:

- If `section_code` is found and `has_substantive_answer=true`, retrieve primarily from that section in the completed DDQ.
- If `section_code` is missing or non-substantive, retrieve globally from the completed DDQ plus appendices.
- If the manager response references an appendix, add a targeted appendix retrieval pass using the referenced appendix name.

## ODD Report Parsing

Add `06_parse_odd_report.py`.

Responsibilities:

- Locate `ODD Reports Blank.docx` in the ODD input/template folder.
- Extract the report prompt paragraph under the `Prompt` heading.
- Extract risk rating definitions from the risk table.
- Extract engagement metadata from the first table.
- Extract Part 1 topics from table 2:
  - topic row index
  - answer row index
  - chapter
  - chapter title
  - section code
  - topic title
- Extract Part 2 summary rows from table 3:
  - chapter title
  - rating cell
  - overall conclusion row

Persist this into new tables:

- `odd_report_topics`
- `odd_report_metadata`
- `odd_report_risk_definitions`

## Retrieval Design

For each topic:

1. Section pass:
   - Filter `source_role='manager_completed_ddq'`
   - Filter `section_code=<topic.section_code>`
   - Retrieve top 6-10 chunks.
2. Appendix pass:
   - If the DDQ section references appendices, filter or query those appendix names.
   - Retrieve top 4-6 chunks.
3. Global fallback pass:
   - Trigger only when the section pass has no substantive evidence or reranking rejects all section evidence.
   - Search manager DDQ globally first, appendices second.
4. Rerank:
   - Reuse the current lightweight LLM reranking pattern.
   - Prefer manager DDQ chunks over appendices unless appendix evidence is clearly more specific.

Vector Search should include these synced columns:

- `source_role`
- `source_tier`
- `section_code`
- `section_title`
- `chapter_code`
- `has_substantive_answer`
- `referenced_appendices`

Keep two retrieval wedges in the implementation:

- `sql` retrieval via Databricks SQL `VECTOR_SEARCH(...)`
- `python` retrieval via the Python Vector Search client

Reason:

- The ODD workflow should default to `sql` because this Databricks workspace has previously had issues with the Python Vector Search path, including environment-specific serving/index constraints.
- The `python` path should still be preserved as a fallback and debugging wedge, because workspace behavior can vary and it remains useful in older or differently configured environments.
- This is a deliberate compatibility decision, not temporary duplication. Do not remove one branch purely for simplification unless the workspace constraint has been re-tested and the implementation plan is updated accordingly.

## Assessment Generation

Add `07_assess_odd_report.py`.

Use two model widgets:

- `assessment_model_name`: default to current `DEFAULT_LLM_ENDPOINT`
- `risk_model_name`: default to the same value for now

Assessment prompt:

- Use the prompt extracted from `ODD Reports Blank.docx`.
- Inject the topic name, section code, manager DDQ evidence, appendix evidence, and metadata.
- Require neutral third-party investor language.
- Require 200-500 words for each Part 1 topic unless the template prompt is later changed.
- Require citation markers in the same structured style already used by the DDQ agent.

Risk prompt:

- Use the risk definitions extracted from the report template.
- Ask the risk model to choose exactly one of: `Low`, `Medium`, `High`, `Unacceptable`.
- Require a short rationale and evidence references.
- Store rating separately from the assessment paragraph.

Persist into a new table:

- `odd_topic_assessments`

Suggested schema:

- `engagement_id`
- `topic_id`
- `section_code`
- `chapter_code`
- `topic_title`
- `assessment_text`
- `risk_rating`
- `risk_rationale`
- `citations`
- `retrieved_chunk_ids`
- `source_tiers_used`
- `manager_ddq_used`
- `appendices_used`
- `fallback_used`
- `confidence`
- `human_review_flag`
- `assessment_model`
- `risk_model`
- `generated_at`

## Part 2 Summary Generation

Part 2 should be generated only after all Part 1 topics are complete.

Add a second pass in `07_assess_odd_report.py` or a small `07b_summarize_odd_report.py`.

Inputs:

- All Part 1 assessments
- All Part 1 ratings
- Risk definitions
- Engagement metadata

Outputs:

- Chapter A-D summary text
- Chapter A-D rating
- Overall conclusion text
- Overall rating

Persist into:

- `odd_chapter_summaries`

Rating aggregation should not be a blind average. Use conservative escalation:

- If any topic is `Unacceptable`, chapter rating should normally be `Unacceptable`.
- If any topic is `High`, chapter rating should normally be at least `High`.
- If several topics are `Medium`, chapter rating may be `Medium` even if none is `High`.
- The model may downgrade only with explicit rationale.

## Filling the ODD Report

Add `08_fill_odd_report.py`.

Responsibilities:

- Open `ODD Reports Blank.docx`.
- Fill table 0 metadata if values were extracted or overridden.
- Fill Part 1 table:
  - Put topic assessment text into the blank row immediately following each topic row.
  - Add rating either at the start/end of the assessment or in a consistent label, depending on the report table structure.
- Fill Part 2 table:
  - Update chapter ratings.
  - Add or append generated chapter summary text where the template supports it.
  - Update overall conclusion rating.
- Save to `output/<template_basename>.filled.docx`.

Because this is table-heavy, use explicit `python-docx` table handling rather than paragraph placeholder replacement.

## Minimal Code Changes by File

### `_config.py`

Add:

- `WORKFLOW_PROFILES["odd_report_v1"]`
- source role/tier rules
- DDQ section regex
- report topic regex
- default model config:
  - `DEFAULT_ASSESSMENT_MODEL`
  - `DEFAULT_RISK_MODEL`

### `01_setup.py`

Add tables:

- `odd_report_metadata`
- `odd_report_topics`
- `odd_report_risk_definitions`
- `odd_topic_assessments`
- `odd_chapter_summaries`

Add columns to `documents` and `document_chunks` for source role and section metadata.

### `02_ingest_inputs.py`

Ingest `.docx` files from `inputs/`, not only PDFs.

Classify:

- `APG DDQ 16.01.2026 - Final responses.docx` as `manager_completed_ddq`
- `ODD Reports Blank.docx` as `report_template` if placed in the ODD input/template folder
- PDFs as appendices unless matched otherwise

### `03_parse_pdfs.py`

Either:

- Rename to `03_parse_sources.py`, or
- Keep the filename and add DOCX handling despite the imperfect name.

Recommendation: rename only in the implementation plan and later code, but preserve the old notebook as a wrapper if needed.

### `04_chunk.py`

Add section-aware chunking:

- For completed DDQ DOCX chunks, do not use only sliding page windows.
- Chunk by DDQ section where possible.
- If a section is too long, split within the section while preserving the same `section_code`.

### `05_vector_index.py`

Sync the new metadata columns.

If an existing index lacks the metadata, recreate it once, as we already had to do for `source_tier`.

### `06_parse_odd_report.py`

Active stage-06 notebook for report template parsing. This replaces the old DDQ-question parsing step for the ODD workflow.

### `07_assess_odd_report.py`

Active stage-07 notebook for topic-level assessment, risk rating, claim checking, and Part 2 roll-up. This replaces the old questionnaire-answering step.

### `08_fill_odd_report.py`

Active stage-08 notebook for table-based ODD report filling. This replaces the old DDQ DOCX filling step.

### `09_orchestrate.py`

Add:

- `workflow_mode`: optional, default `odd_report`
- `retrieval_mode`: default `sql`, optional `python`
- `report_template_name`
- `manager_ddq_name`
- `assessment_model_name`
- `risk_model_name`

Default run:

```text
01_setup
02_ingest_inputs
03_parse_sources
04_chunk
05_vector_index
06_parse_odd_report
07_assess_odd_report
08_fill_odd_report
```

If a legacy `ddq_fill` mode is kept temporarily, it should be explicitly labelled as deprecated and should not be the default in the Databricks workflow.

## Verification Plan

1. Parse check:
   - Completed DDQ yields 23 section codes.
   - ODD report Part 1 yields 23 topics.
   - Every ODD Part 1 topic maps to exactly one DDQ section code.
2. Metadata check:
   - `APG DDQ 16.01.2026 - Final responses.docx` has `source_role='manager_completed_ddq'` and `source_tier=0`.
   - Chunks from A0100 carry `section_code='A0100'`.
   - Report metadata table extracts mandate name, manager, strategy, dates, managers, and authors.
3. Retrieval check:
   - For `A0100`, first-pass evidence comes from the completed DDQ A0100 section.
   - For any non-substantive section, fallback evidence includes manager DDQ global results and appendices.
   - Referenced appendices are retrieved when manager answers point to them.
4. Generation check:
   - Part 1 has 23 assessments.
   - Every assessment has a rating, rationale, citations, and retrieved chunk IDs.
   - Risk model field is populated separately from assessment model field, even while both point to the same endpoint.
5. DOCX fill check:
   - `ODD Reports Blank.filled.docx` exists.
   - Part 1 blank rows are filled.
   - Part 2 ratings are updated.
   - No generated text is written into the prompt or rating definition sections.
6. Human spot checks:
   - A0100 ownership structure
   - A0900 key person risk
   - B0500 compliance
   - B0600 legal and tax
   - D0300 systems and IT security
   - D0400 business continuity

## Open Decisions

1. Whether Part 1 should include citations visibly in the final report or only store citations in Delta audit tables.
2. Whether risk ratings should be inserted into the current two-column Part 1 table or kept in Part 2 only.
3. Whether the report template should be lightly modified to add explicit rating cells per topic.
4. Whether `ODD Reports Blank.docx` should live in the existing `questionnaire/` folder or a new `templates/` folder under the engagement.

## Recommendation

Implement this as the primary `odd_report_v1` workflow, not as a sidecar to the old DDQ-fill workflow. The critical design choice is to make the completed manager DDQ a tier-0, section-coded evidence source. That gives the agent the right default behavior: assess from the manager's own completed questionnaire first, use appendices as support, and fall back globally only when a section is missing or too thin.

Also keep the retrieval split between SQL and Python on purpose. Even though the logic should stay aligned across both modes, the SQL mode should remain the default because it has been safer in this Databricks workspace. The Python client mode should remain available as a fallback/debug wedge and should not be removed merely to simplify the code.

The table-of-contents codes such as `A0100` should be chunk metadata and Vector Search metadata. They are the natural join key between the manager DDQ and Part 1 of the ODD report, and they will make retrieval both more precise and cheaper.
