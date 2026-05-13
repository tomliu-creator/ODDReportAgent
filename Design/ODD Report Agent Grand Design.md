# ODD Report Agent — Grand Design

**Status:** consolidated reference design, derived from the five design notes in
`notebooks_ddq/Design/` and the current `_config.py` / `ODDAgent.md`.

**Purpose of this document.** Give a coding agent enough specification to implement
the ODD RAG pipeline end-to-end on Databricks, *from an empty workspace*. It
records both the high-level architecture and the specific engineering choices
that have been validated against the SSGA / APG engagement. The document is split
in two halves because the engineering judgments are very different on each side:

- **Layer 1 — Vector space creation (notebooks `01`–`05`).** A reusable
  document-to-vector-index foundation. Should change only when the *source
  format* changes, not when the *business case* changes.
- **Layer 2 — Domain-specific retrieval and assessment (notebooks `06`–`09`).**
  The ODD-specific reasoning agent: parse the report template, retrieve evidence
  per topic, produce assessments and risk ratings, write the filled report.

Open questions about reusability are listed at the end.

---

## 0. Mental model

The agent does **not** fill a questionnaire. It writes an Operational Due
Diligence report.

1. The investment manager has already completed a long DDQ (e.g. `APG DDQ
   16.01.2026 - Final responses.docx`) — that DDQ is **input evidence**, not a
   form to be filled.
2. The deliverable is a filled `ODD Reports Blank.docx`: 23 topic assessments in
   Part 1 (one per DDQ section code `A0100`, `A0200`, … `D0400`) plus chapter-
   and overall-level summaries in Part 2.
3. The completed DDQ is tier-0 evidence. Appendices (org charts, SOC reports,
   annual report, policies) are tier-1 to tier-4 supporting evidence.
4. Output is one filled `.docx` plus a full audit trail in Delta tables
   (citations, retrieved chunks, model names, ratings).

---

## 1. Architectural principles

These principles are load-bearing — deviation has caused real defects (A0300
"no financial evidence" bug, A0400 question-text leakage). They are listed first
because they constrain every later choice.

1. **Two-layer split.** `01`–`05` is the reusable RAG foundation, `06`–`09` is
   business-specific. To adapt to a new business case (financial analyst, index
   analyst, etc.), reuse `01`–`05`, change `_config.py`, and rewrite `06`–`09`.
2. **Configuration lives in `_config.py`.** Catalog/schema, source-role and
   source-tier rules, parsing regexes, chunking parameters, embedding endpoint,
   index naming, and the list of columns synced to Vector Search all live in
   one place. `09_orchestrate.py` does *not* duplicate infrastructure config.
3. **Source hierarchy is data, not code.** Each document is classified into a
   `source_role` and `source_tier` at ingest. That classification flows into
   pages, chunks, and the Vector Search index, and is what makes "completed
   manager DDQ first, appendices second" actually work in retrieval.
4. **Section codes (`A0100`, `B0500`, …) are first-class metadata.** They are
   parsed at chunk time, stored on every chunk, synced to Vector Search, and
   used as the join key between the completed DDQ and Part 1 of the report
   template. They make retrieval both more precise and cheaper.
5. **Separate `embedding_text` from `chunk_text`.** Embeddings include the DDQ
   *question* so the manager's answer can be retrieved by semantic similarity.
   The evidence text fed to the assessment LLM is the manager's *answer only* —
   never the question — so the model cannot quote the questionnaire back as if
   it were evidence (A0400 leak fix).
6. **Idempotent, status-driven re-runs.** Each derived stage (`parse`, `chunk`,
   `index`) has its own `*_status` column on `documents`. Changing classification
   (e.g. a doc is re-tagged as `appendix`) must reset downstream statuses to
   `pending` so stale chunks and stale index metadata are rebuilt.
7. **Keep two retrieval wedges.** Both SQL `VECTOR_SEARCH(...)` and the Python
   Vector Search client paths exist on purpose. SQL is the default because it is
   safer in this workspace (Python client has hit serving/scale-to-zero
   constraints). Don't delete the Python wedge for "simplification" — it is a
   compatibility/debug fallback.
8. **Driven by `ODDAgent.md`, not by hand-written prompt strings.** `ODDAgent.md`
   is the runtime behavior spec: role, tone, length, rating scale, and every
   topic's `Prompt:` line. `07_assess_odd_report.py` injects `topic_prompt` into
   the retrieval query and into the assessment prompt. Behavior changes are
   spec changes, not code changes.
9. **Unified provenance contract across parsed rows and chunks.** The pipeline
   carries generic locator fields across document types:
   `source_page_num`, `source_para_num`, `source_locator_type`,
   `source_locator_label` on parsed rows, plus `source_page_start`,
   `source_page_end`, `source_para_start`, `source_para_end` on chunks. This
   keeps retrieval and citation logic generic even when different source
   formats expose different locator fidelity.
10. **Two enforcement layers against question-text leakage.** The physical
   layer is the `chunk_text` / `embedding_text` split — the LLM literally
   cannot see question wording as evidence because it isn't in the column.
   The prompt layer encodes the principle in every assessment prompt: the
   model is instructed to prioritize manager DDQ evidence when substantive,
   to use appendices only to support or challenge it, and to state limitations
   rather than infer when evidence is thin.
11. **Human-review flagging replaces automated claim verification (today).**
    After assessment generation, each topic gets a `human_review_flag`:
    `"high"` when fallback retrieval was used or the risk rating is High /
    Unacceptable; `"medium"` when model confidence < 0.7; `"low"` otherwise.
    A full per-claim verifier (v2 design §1.8) is **not yet implemented** and
    is an open improvement path.
12. **Structured table extraction is aspirational (not yet implemented).**
    DDQ answer tables (key-personnel, committee membership, fee schedules) are
    currently parsed only as flat paragraph text. Preserving them as structured
    `{columns, rows}` objects is planned but requires a code change to
    `03_parse_sources.py` and a new `document_tables` Delta table. Until then,
    table content is present in `chunk_text` only as prose.

---

## 2. Storage layout

All under catalog/schema **`uc_cmifi_dev.ddq_agent`** (configurable in
`_config.py`).

### 2.1 Unity Catalog Volume

`engagements/<engagement_id>/`
- `inputs/` — source PDFs, plus the completed DDQ `.docx`.
- `questionnaire/` — the report template `ODD Reports Blank.docx`.
- `output/` — the filled report is written here.

### 2.2 Delta tables

| Table | Purpose |
|---|---|
| `documents` | One row per input file. Carries `source_role`, `source_tier`, `parse_status`, `chunk_status`, `index_status`, plus mandate/manager metadata. |
| `document_pages` | Parsed source rows with unified provenance fields. For PDFs this is page-level text; for DOCX this is currently paragraph-level text with generic locator metadata. |
| `document_chunks` | Chunked evidence with full metadata (see §3.4). Contains both `chunk_text` (evidence-only) and `embedding_text` (includes DDQ question wording for relevance). |
| `pipeline_errors` | Per-stage error log. |
| `odd_report_metadata` | Engagement metadata extracted from the report template's first table (mandate, manager, strategy, dates, authors). |
| `odd_report_topics` | One row per Part 1 topic from `ODD Reports Blank.docx`: `topic_id`, `chapter`, `section_code`, `topic_title`, `topic_row_index`, `answer_row_index`. |
| `odd_report_risk_definitions` | Rating scale extracted from the template. |
| `odd_topic_assessments` | The agent's output: `assessment_text`, `risk_rating`, `risk_rationale`, `citations`, `retrieved_chunk_ids`, `source_tiers_used`, `manager_ddq_used`, `appendices_used`, `fallback_used`, `assessment_model`, `risk_model`. |
| `odd_chapter_summaries` | Part 2 rollups: chapter A–D summaries + overall conclusion. |

`document_pages` and `document_chunks` now share a unified provenance contract.
Parsed rows carry `source_page_num`, `source_para_num`, `source_locator_type`,
and `source_locator_label`; chunks carry the corresponding range fields
`source_page_start`, `source_page_end`, `source_para_start`,
`source_para_end`, plus `source_locator_type` and `source_locator_label`.

### 2.3 Vector Search

- **One index per engagement:** `uc_cmifi_dev.ddq_agent.idx_<engagement_id>`.
- **Source table:** a per-engagement Delta Sync source
  `document_chunks_vs_<safe_engagement_id>` (created from `document_chunks`
  filtered to that engagement). Per-engagement source tables make Delta Sync
  cheaper and isolate failures.
- **Embedding source column:** `embedding_text` (not `chunk_text`).
- **Embedding model:** `databricks-gte-large-en` (with `bge-large-en` as a
  fallback candidate).
- **Synced metadata columns:** at minimum `engagement_id`, `document_id`,
  `file_name`, `source_path_dbfs`, `page_start`, `page_end`, `source_role`,
  `source_tier`, `section_code`, `section_title`, `chapter_code`,
  `chapter_title`, `question_number`, `is_manager_answer_chunk`,
  `has_substantive_answer`, `referenced_appendices`, `embedding_text`,
  `source_page_start`, `source_page_end`, `source_para_start`,
  `source_para_end`, `source_locator_type`, `source_locator_label`.
- **Excluded from indexing:** `source_role='report_template'` — the report
  template itself must never be retrieved as evidence.

---

## 3. Layer 1 — Vector space creation (`01`–`05`)

This layer is reusable. It does not know anything about ODD topics, risk
ratings, or report templates. Its only job is: take a folder of files, classify
them, parse them, chunk them, and produce a clean Vector Search index with rich
metadata.

### 3.1 `01_setup.py` — schema and tables

Creates (idempotent `CREATE … IF NOT EXISTS`):

- Catalog, schema, and the `engagements` volume.
- All Delta tables listed in §2.2.
- Per-engagement subfolders inside the volume.

Engineering choices:

- Schemas evolve forward only. New metadata columns (e.g. `embedding_text`,
  `source_role`, `section_code`, `has_substantive_answer`,
  `referenced_appendices`) are added here so later stages can rely on them.
- `01_setup.py` is safe to re-run; it should not drop data.

### 3.2 `02_ingest_inputs.py` — discovery and classification

For each file in `inputs/`:

1. Compute `sha256`, size, mtime, content-type.
2. Apply `SOURCE_ROLE_RULES` (from `_config.py`) — keyword + extension match —
   to assign `source_role` (`manager_completed_ddq`, `appendix`,
   `report_template`, `policy`, `assurance_report`, `public_disclosure`,
   `other`).
3. Apply `SOURCE_TIER_BY_ROLE` plus `SOURCE_TIER_RULES` (keyword overrides) to
   assign `source_tier`.
4. `MERGE` into `documents` by `(engagement_id, file_path)`.

Critical idempotency rule (learned the hard way from the stale-appendix bug):

> If a row's `source_role` or `source_tier` changes — even when the file
> fingerprint did not change — reset `parse_status`, `chunk_status`, and
> `index_status` to `pending`. Otherwise downstream stages skip the row and the
> chunks/index keep stale metadata.

### 3.3 `03_parse_sources.py` — parsing

Handles both PDFs and DOCX:

The implemented provenance model is now generic across source types. PDFs
populate real `source_page_num` values and page-based locator labels. DOCX
sources populate the same generic fields, but in the current Databricks parse
they reliably populate `source_para_num` and paragraph-based locator labels
while true rendered `source_page_num` remains unavailable.

- **PDF (most appendices):** PyMuPDF page extraction → `document_pages` with
  `(document_id, page_number, text)` and `source_role`/`source_tier` propagated
  from `documents`.
- **DOCX (completed manager DDQ):** walk `Document.paragraphs` in order;
  recognize `Heading 2` matching `SECTION_HEADING_REGEX = ^([A-Z]\d{4})\.\s+(.+)`
  as a DDQ section boundary; inside each section, classify paragraphs as
  `content_role='question'` or `content_role='answer'` and stamp every row
  with `section_code`, `section_title`, `chapter_code`, `chapter_title`.
- A section is marked `has_substantive_answer=false` when the answer text is
  empty, placeholder-like, or just refers to an appendix with no extra
  explanation.
- DOCX appendix-reference scanning (`APPENDIX_REF_REGEX`) records
  `referenced_appendices` per section so retrieval can pull in the referenced
  appendix later.
- **Tables inside DDQ answers are currently parsed as flat prose**, not as
  structured objects. A `doc.tables` extraction loop is not yet implemented in
  `_parse_manager_ddq`; table content reaches `chunk_text` only via the
  surrounding paragraph text. Adding structured table extraction to this stage
  is the main outstanding item from v2 improvement plan §1.6.

Report template documents (`source_role='report_template'`) are skipped at this
stage — they are set to `parse_status='skipped'` and accessed directly by
`08_fill_odd_report.py` via python-docx.

Only processes rows with `parse_status IN ('pending','error')`.

### 3.4 `04_chunk.py` — page-aware chunking + dual text columns

Output: `document_chunks`. Each chunk carries:

- Text columns
  - `chunk_text` — **evidence-only**. Excludes any row with
    `content_role='question'`. This is what is shown to the LLM as evidence.
  - `embedding_text` — **relevance signal**. Includes DDQ question wording plus
    the manager answer, so vector similarity can still find the right chunk
    when an analyst's query phrasing is closer to the question than the
    answer.
- Provenance: `chunk_id`, `document_id`, `file_name`, `source_path_dbfs`,
  `page_start`, `page_end`, `chunk_index`.
- Classification: `source_role`, `source_tier`.
- DDQ-specific (populated for chunks coming from the manager-completed DDQ):
  `section_code`, `section_title`, `chapter_code`, `chapter_title`,
  `question_number`, `is_manager_answer_chunk`, `has_substantive_answer`,
  `referenced_appendices`.

In the implemented pipeline, chunk provenance also carries the unified range
fields `source_page_start`, `source_page_end`, `source_para_start`,
`source_para_end`, plus `source_locator_type` and `source_locator_label`.

Chunking strategy:

- **PDF / appendix:** sliding window over consecutive pages
  (`max_pages=5`, `overlap_pages=1`, target `2500`–`6000` chars).
- **DDQ DOCX:** chunk **by section**. If a section is too long, split *within*
  the section, preserving `section_code` on every split. Never let one chunk
  straddle two section codes.

For diagnostics, chunk text may include markers such as `[PAGE N]` or
`[PARA N]`, but user-facing citation rendering is driven by the unified locator
metadata rather than by those inline markers.

`MERGE` rule: when a chunk row already exists use `UPDATE SET *` unconditionally
(not just on `chunk_sha` change). Metadata like `source_role`, `source_tier`, and
`embedding_text` must refresh even when the text is identical to the previous run.

### 3.5 `05_vector_index.py` — Delta Sync index

For the active engagement:

1. Build / refresh `document_chunks_vs_<engagement>` from `document_chunks`
   filtered to this engagement and to `source_role NOT IN exclude_source_roles`
   (excludes `report_template`).
2. Verify every column in `required_metadata_columns` is present and non-null
   where applicable, and that none is dropped from `columns_to_sync`. Failing
   loud here is much cheaper than chasing missing filter values inside the
   reranker.
3. Create or refresh the Delta Sync index `idx_<engagement>` with
   `embedding_source_column='embedding_text'`.
4. Wait for index status `READY` before stage 06 runs.

If the previous index lacks any of the new metadata columns, recreate it (one
of the lessons from the `source_tier` rollout).

---

## 4. Layer 2 — Domain-specific retrieval & assessment (`06`–`09`)

This is the ODD-specific business layer. To adapt to a different business case,
this whole layer is rewritten; `01`–`05` stays.

### 4.1 `06_load_odd_agent_spec.py`

**Single responsibility: parse `ODDAgent.md` only.** The ODD report template
DOCX is NOT parsed in this stage; it is accessed directly by
`08_fill_odd_report.py` via python-docx at write time.

`ODDAgent.md` location: the file lives in the engagement's volume folder
(`/Volumes/<catalog>/<schema>/engagements/<engagement_id>/ODDAgent.md`), not
alongside the notebooks. `09_orchestrate.py` constructs the path automatically;
it can be overridden with the `agent_spec_path` widget.

Parsing steps:

1. Read `§1. Role and General Prompt` → bullet list → `prompt_text` stored in
   `odd_report_metadata.prompt_text`.
2. Read `§2. Risk Rating Scale` → one row per rating label → `odd_report_risk_definitions`.
3. Read `§3. Part 1 - DDQ Topics` → for each `### A/B/C/D. Chapter` heading
   capture `current_chapter_code`; for each `- **A0100 Topic title**` bold line
   capture `section_code` and `topic_title`; for each `Prompt: …` continuation
   capture `topic_prompt` on the most-recent topic row. Persist into
   `odd_report_topics`.
4. `topic_row_index` and `answer_row_index` in `odd_report_topics` are
   **algorithmically computed** as `(topic_order-1)*2` and `(topic_order-1)*2+1`,
   assuming the Part 1 table alternates topic / answer rows. They are not parsed
   from the template.
5. `odd_report_metadata` also records the template's document path and the
   engagement metadata (manager name, mandate, etc.) already stored on the
   `documents` row for the manager-completed DDQ.

For the SSGA/APG template, every Part 1 topic maps to exactly one DDQ section
code. That section code remains the preferred join key when it is present.
(Verification check: 23 topics in `odd_report_topics` ↔ 23 section codes in the
completed DDQ.) The assessment layer also supports topics without a section
code: those topics use the topic title and `Prompt:` scope as the primary
semantic join key against indexed evidence.

### 4.2 `07_assess_odd_report.py` — the agent loop

For each Part 1 topic, this notebook runs five sub-stages.

#### 4.2.1 Build the retrieval query

`_topic_query_text(topic)` concatenates `section_code + topic_title +
chapter_title + topic_prompt` (skipping `topic_prompt` if empty). Driving
retrieval with `topic_prompt` was the key A0300 fix — the spec must drive the
search, not a hand-written translation.

#### 4.2.2 Multi-pass retrieval

| Pass | Filter | Purpose |
|---|---|---|
| Section pass | `source_role='manager_completed_ddq' AND section_code=<topic>` | Tier-0 evidence from the matching DDQ section (top 6–10). |
| Semantic primary pass | `source_role IN ('manager_completed_ddq','appendix','policy','assurance_report','public_disclosure','other')` | Used when a topic has no section code. Joins evidence by topic title + `topic_prompt`, then sorts by source-tier priority. This lets unlabeled reports work without pretending they have DDQ section metadata. |
| Appendix pass | `source_role='appendix'` | Normal appendix support (top 4–6). |
| Prompt-driven mandatory appendix pass | filename match between `topic_prompt` and `documents.file_name` (e.g. `"latest annual report"` → `Appendix 8 - 2024 Annual Report …`) | Enriches the query with the actual appendix title so the index can return the right pages. **Not** a hardcode — it is bridge logic over filenames already in the catalog. |
| Global DDQ fallback | `source_role='manager_completed_ddq'` | Triggered only for section-coded topics when the section pass returns no substantive evidence. |

SQL retrieval rule: **over-fetch before filtering.** Databricks SQL
`VECTOR_SEARCH(...)` returns a global top-N first, *then* applies the role
filter. With a small `num_results`, tier-0 DDQ hits can starve appendix
results entirely. The code requests `min(100, max(num_results * 8, 80))` from
the index then applies the `source_role` / `section_code` filter in Spark.

When `section_code` is present, the section pass remains first-class and the
old tier-0 discipline is preserved. When `section_code` is blank or missing,
`_semantic_primary_hits(topic)` becomes the primary bucket and deliberately
searches all evidence-bearing source roles except the report template. The
topic's `Prompt:` line is therefore not only assessment guidance; it is also the
join key for unlabeled narrative reports.

#### 4.2.3 Balanced reranker sampling

The reranker receives candidates from four possible buckets — `section`,
`semantic_primary`, `appendix`, `mandatory_external` — *with quotas*, not just
the earliest global slice. This prevents tier-0 evidence from crowding out a
prompt-required appendix before the reranker has seen it, and gives unlabeled
topic evidence the same balanced reranking path as section-coded DDQ evidence.
(A0300 fix plus unlabeled-report support.)

#### 4.2.4 Assessment generation

LLM endpoint `DEFAULT_ASSESSMENT_MODEL` (currently `databricks-gpt-oss-20b`).
Prompt structure (built in Python, one per topic):

```
You are writing a formal operational due diligence report section.
AGENT BEHAVIOR SPEC: <prompt_text from ODDAgent.md §1>
TOPIC: <section_code> <topic_title>
CHAPTER: <chapter_title>
MANDATE / MANAGER: <from odd_report_metadata>
TOPIC-SPECIFIC MUST-COVER SCOPE: <topic_prompt, if non-empty>
Requirements:
- Use neutral, factual, investor-facing language.
- Prioritize the completed manager DDQ evidence when it is substantive.
- Use appendices only to support, clarify, or challenge the DDQ response.
- If the DDQ section is missing or thin, state the limitation carefully.
- Return valid JSON only with keys `assessment_text` and `confidence`.
- Cite material claims inline using [file | locator | tier X] markers.
EVIDENCE: <formatted evidence blocks>
```

The answer-only principle is encoded in the `Requirements` block and reinforced
physically by `chunk_text` never containing question wording.

Robustness: output parsed via `assessment_text` → `assessment` → `text` → raw
response fallback. A blank assessment is **never** silently written.

The implemented citation formatter in `07_assess_odd_report.py` no longer
surfaces internal evidence counters such as `E1`, `E2`, or `E3` as if they
were document references. Evidence blocks now use a stable document label
(`DDQ`, appendix short title, or mapped file label) plus the unified
`source_locator_label`.

#### 4.2.5 Risk rating

Separate prompt to `DEFAULT_RISK_MODEL` (same endpoint by default; held
separate so it can be re-pointed later without code changes). Returns exactly
one of `Low / Medium / High / Unacceptable`, plus a short rationale and
evidence references. Stored independently of the assessment paragraph.

After risk rating, `human_review_flag` is assigned heuristically:
- `"high"` — fallback retrieval was used, OR rating is `High` or `Unacceptable`
- `"medium"` — model confidence < 0.7
- `"low"` — otherwise

Result row per topic written to `odd_topic_assessments` with full provenance:
`citations`, `retrieved_chunk_ids`, `source_tiers_used`, `manager_ddq_used`,
`appendices_used`, `fallback_used`, `confidence`, `human_review_flag`,
`assessment_model`, `risk_model`.

#### 4.2.6 Part 2 rollup

After all 23 Part 1 topics are written, run a second pass that aggregates by
chapter A–D and overall. Rating aggregation rule (conservative escalation):

- Any `Unacceptable` topic → chapter normally `Unacceptable`.
- Any `High` → chapter at least `High`.
- Several `Medium` and no `High` → chapter may be `Medium`.
- The model may *downgrade* only with explicit rationale.

Output to `odd_chapter_summaries`. The rollup runs inside `07_assess_odd_report.py`
immediately after the Part 1 loop, using `conservative_rollup_rating` from
`_utils.py`.

### 4.3 `08_fill_odd_report.py` — DOCX writer

Table-driven, not paragraph-placeholder driven (the template is mostly tables).
Use `python-docx` cell access.

1. Open `ODD Reports Blank.docx` from the engagement's `questionnaire/`
   folder.
2. Fill the engagement metadata table.
3. For each row in `odd_report_topics`, write
   `odd_topic_assessments.assessment_text` into the cell at
   `answer_row_index`, append the rating as a labeled run, and append
   citations as a small italic-gray run.
4. Fill the Part 2 chapter and overall rows from `odd_chapter_summaries`.
5. Save to `engagements/<engagement_id>/output/<template>.filled.docx`.

Never write into the prompt section or the rating definition section of the
template.

### 4.4 `09_orchestrate.py` — driver

Widgets:

- `engagement_id` (required)
- `workflow_mode` (default `odd_report`)
- `retrieval_mode` (default `sql`, alt `python`)
- `report_template_name`, `manager_ddq_name`
- `assessment_model_name`, `risk_model_name`

Default order:

```
01_setup → 02_ingest_inputs → 03_parse_sources → 04_chunk
→ 05_vector_index → 06_load_odd_agent_spec → 07_assess_odd_report
→ 08_fill_odd_report
```

Legacy DDQ-fill notebooks (`06_parse_ddq`, `07_answer_ddq`, `08_fill_docx`) may
remain in the repo for reference but must not be in the default run.

### 4.5 `10_evaluate.py`

Diagnostics and assertions used in verification (see §6). Not part of the
production deliverable path.

---

## 5. Configuration surface (`_config.py`)

Treat `_config.py` as the contract between Layer 1 and Layer 2.

| Key | Used by | Notes |
|---|---|---|
| `CATALOG`, `SCHEMA`, `VOLUME` | 01, 05 | Unity Catalog locations. |
| `DEFAULT_VS_ENDPOINT`, `DEFAULT_EMBEDDING_MODEL`, `EMBEDDING_MODEL_CANDIDATES` | 05 | Embedding infra — foundation-layer decision. |
| `DEFAULT_LLM_ENDPOINT`, `DEFAULT_ASSESSMENT_MODEL`, `DEFAULT_RISK_MODEL` | 07 | LLM choices — business-layer decisions. Assessment and risk models are separate keys so they can be re-pointed independently. |
| `DEFAULT_TOP_K`, `DEFAULT_FINAL_EVIDENCE_K`, `HIGH_RISK_RETRIEVAL_MULTIPLIER` | 07 | Retrieval sizing. |
| `DEFAULT_CHUNKING_CONFIG` | 04 | Includes `include_questions_in_embedding` (True) and `include_questions_in_chunk_text` (False) — the `embedding_text` / `chunk_text` split. |
| `DEFAULT_VECTOR_SEARCH_CONFIG` | 05 | `embedding_source_column`, `columns_to_sync`, `required_metadata_columns`, `exclude_source_roles`. |
| `SECTION_HEADING_REGEX`, `QUESTION_NUMBERING_REGEX`, `APPENDIX_REF_REGEX`, `TOPIC_ROW_REGEX` | 03, 04, 06 | All section-code parsing depends on these. |
| `SOURCE_ROLE_RULES`, `SOURCE_TIER_BY_ROLE`, `SOURCE_TIER_RULES` | 02 | Classification at ingest. |
| `LEGAL_ENTITY_PATTERNS`, `DOC_SHORT_TITLES` | 04, 07 | Display + retrieval helpers. |
| `WORKFLOW_PROFILES["odd_report_v1"]` | 06, 07, 08 | Bundles all of the above plus `high_risk_topics`. New business case = new profile. |

---

## 6. Verification plan

Run on the canonical engagement `odd_ssga_2025`.

1. **Setup:** `01_setup.py` creates catalog, schema, volume, all Delta tables.
2. **Ingest:** 26 PDFs + 1 completed DDQ DOCX + 1 report template DOCX present
   in `documents` with the right `source_role` / `source_tier`. The completed
   DDQ is tier 0, appendices tier 1, the template tier 9 and excluded from
   indexing.
3. **Parse:** completed DDQ yields 23 section codes; appendix PDFs yield page
   rows with page-based provenance; DOCX rows carry generic locator metadata;
   report template yields the engagement metadata table, the 23-topic Part 1
   table, and the Part 2 framework.
4. **Chunk:** every chunk has both `chunk_text` and `embedding_text`; DDQ
   chunks have `section_code`, `chapter_code`, `is_manager_answer_chunk`;
   `chunk_text` excludes question wording, `embedding_text` includes it, and
   chunks carry unified locator metadata.
5. **Index:** `idx_odd_ssga_2025` status is `READY`. Every column in
   `required_metadata_columns` is synced, including `source_locator_label`.
6. **Spec loading:** `odd_report_topics` has 23 rows, every row joins to a
   topic in `ODDAgent.md` with a `topic_prompt`.
7. **Assess:** `odd_topic_assessments` has 23 rows. Every row has non-empty
   `assessment_text`, a rating in `{Low, Medium, High, Unacceptable}`, at
   least one citation, and at least one `retrieved_chunk_id`. No row is
   silently blank. Topics where fallback retrieval was used show
   `human_review_flag='high'`; `fallback_used=false` for all topics that have
   a substantive DDQ section (expected for most of the 23).
8. **Section coupling:** A0100 evidence is drawn primarily from
   `source_role='manager_completed_ddq' AND section_code='A0100'`. A0300
   evidence includes Appendix 8 page 109 (financial-performance table) — the
   A0300 regression case.
9. **Rollup:** `odd_chapter_summaries` has 4 chapter rows + 1 overall row, with
   ratings consistent with the escalation rule in §4.2.6.
10. **Fill:** `output/<template>.filled.docx` exists, Part 1 answer rows are
    filled, Part 2 ratings are updated and chapter summaries are appended, the
    prompt section and rating definition rows are untouched.
11. **Spot checks** on A0100 (ownership), A0900 (key person), B0500
    (compliance), B0600 (legal/tax), D0300 (IT security), D0400 (BCP).

---

## 7. Open questions for future improvement

These are deliberate design questions, not bugs. Worth investigating before the
next business case is built on this foundation.

### 7.1 Pre-declaring document types in `_config.py` for DDQ-style special handling

**Question.** The current pipeline detects "this is a manager-completed DDQ" by
filename keyword matching (`SOURCE_ROLE_RULES`). Today there is exactly one
DDQ. Tomorrow there may be several (multiple managers, multiple vintages,
multiple questionnaire formats — APG DDQ, ILPA DDQ, internal due-diligence
forms). Should `_config.py` carry an explicit per-engagement manifest of which
documents are DDQs and how each should be parsed, rather than relying on
keyword heuristics?

**Why it matters.** The DDQ gets very different handling from every other
source:

- Parsed with DOCX section logic, not PyMuPDF pages.
- Chunked by section, not by page window.
- Question and answer are vectorized together (`embedding_text`) but only the
  answer is retrieved as evidence (`chunk_text`). This is the engineering
  choice that fixed the A0400 leak.
- Carries `is_manager_answer_chunk`, `has_substantive_answer`, and
  `referenced_appendices` — none of which apply to appendices.

If a future engagement contains, say, two DDQs (one ILPA, one APG) and three
manager letters that *look* like DDQs but aren't, the keyword approach will
quietly miscategorize them, and the resulting chunks will either lose
section-code metadata or gain spurious question/answer separation.

**What we'd need to investigate.**

1. Move from `SOURCE_ROLE_RULES` (keyword heuristics applied to every file) to
   a per-engagement `documents.yaml` manifest committed alongside the inputs:
   each file explicitly typed as `manager_completed_ddq:apg_v1`,
   `appendix:annual_report`, `policy:compliance`, etc. Heuristics remain as
   a fallback when the manifest is missing or partial.
2. Promote "document type" to a richer object than `source_role`: include the
   parsing strategy (`docx_section`, `pdf_page`, `pptx`), the chunking
   strategy, and the metadata schema each type produces. `_config.py` would
   declare these types once; the manifest binds files to types.
3. Decide whether the question/answer split (one of the most consequential
   choices in the current pipeline) should be an attribute of the *document
   type* (`has_q_and_a: true`) rather than a global toggle in
   `DEFAULT_CHUNKING_CONFIG`. Today it's global; in a multi-DDQ world it must
   be per-document.
4. Make Vector Search filterable by `document_type` (and not only by
   `source_role`), so the section pass for a topic targeted at "the APG DDQ"
   does not accidentally pull in chunks from a separate ILPA DDQ with
   overlapping section codes.

The current keyword classifier is fine for a single DDQ engagement. It is the
weakest point of the design for a multi-DDQ or multi-engagement-template
future. A pre-declared, typed manifest in `_config.py` (or alongside the
inputs) is the most defensible upgrade path.

### 7.2 Two-step retrieval (question-match → answer-fetch) as an alternative mode

**Question.** Today retrieval is a single semantic pass over `embedding_text`
(which includes question wording) with role/section filters layered on top.
An alternative mode would split retrieval into two explicit steps:

1. **Question match:** look up the right DDQ record by `(section_code,
   question_number)` or by similarity against `question_text` only — no
   answer text at all.
2. **Answer fetch:** load that record's `answer_text` plus any embedded
   tables directly from `document_chunks` / `document_tables`. Then go
   wider to appendices only when the answer is thin.

**Why it matters.** The two-step mode would give exact, deterministic
question→answer linkage (no chance of returning a sibling question's
answer), and would make per-question structured records (the v2 plan's
§1.3 JSON shape) the natural unit of retrieval. Costs: it requires *per-
question* records, not per-chunk; long answers must be reassembled across
splits; and it loses the ability to retrieve by paraphrase when the
analyst's wording doesn't match the question's wording.

**What we'd need to investigate.**

1. Whether to add `ddq_records` (one row per `(document_id, section_code,
   question_number)` with `question_text`, `answer_text`, `table_ids`,
   `page_start`, `page_end`) as a sibling of `document_chunks`, populated
   in stage `04_chunk.py`. Chunks remain for retrieval; records become the
   canonical "answer object."
2. A `retrieval_mode` widget in `09_orchestrate.py` extension: `single_pass`
   (current default) vs `two_step` vs `hybrid` (run both, prefer two-step
   when section_code matches, fall back to single-pass otherwise).
3. Whether the two-step mode actually changes outputs on the regression
   topics (A0300, A0400, A0900) once §4.2 already has tier-0 filtering and
   the claim verifier. It may be a more expensive route to the same place.

This is logged as a tradeoff, not a planned change: today's single-pass
design plus the `embedding_text` / `chunk_text` split, the section/appendix
multi-pass, and the claim verifier already address the underlying defects.
Two-step is the right answer only if a future engagement has a DDQ where
section codes are unreliable as a join key but question_id is.

### 7.3 True DOCX page-number provenance

**Question.** The unified provenance implementation is now in place, and
`07_assess_odd_report.py` can cite generic locators across source types.
However, for DOCX sources in the current Databricks parse, `source_page_num`
is still null, so citations fall back to paragraph-based locators instead of
rendered Word page numbers.

**Why it matters.** The current behavior is honest and materially better than
the older fake `p.143`-style citations, but the desired end-state for the ODD
report is page-number citation for both PDF and Word documents.

**What we'd need to investigate.**

1. Add a rendered Word page-mapping step for DOCX sources so parsed rows can
   populate true `source_page_num` in `03_parse_sources.py`.
2. Decide where that renderer lives: local Word automation, a conversion step
   that emits a page map into the engagement volume, or another reliable
   pre-processing service outside Databricks.
3. Update `03_parse_sources.py` to ingest the DOCX page map and prefer
   page-based locator labels when page numbers are available.
4. Re-run `03`â€“`05` and then `07`â€“`08` so `source_locator_label` becomes
   page-based for DOCX too, with minimal or no further change in `07`.

This is the main remaining provenance gap after the unification work. The
assessment layer is already generic enough; the missing piece is reliable DOCX
page extraction upstream.
