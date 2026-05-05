# DDQ RAG Agent — Operational Due Diligence Auto-Fill

## Context

The Operational Due Diligence (ODD) team manually fills lengthy Due Diligence Questionnaires (DDQs) — e.g., the SSGA `DDQ SURVEY-001288` has **121 questions across 30 sections** — by reading through ~26 source PDFs (policies, org charts, bios, SOC 1, annual report, ~50 MB total) per engagement. This is slow and error-prone.

We will build a RAG agent on Databricks that:
1. Ingests a per-engagement folder of source PDFs into Unity Catalog + Vector Search.
2. Parses the DDQ `.docx` into a structured questions table.
3. For each question, retrieves evidence and drafts an answer with inline citations using `ai_query`.
4. Writes the drafts back into the source `.docx` placeholders so the analyst gets a pre-filled document to review and edit.
5. Emits an audit Delta table (`ddq_answers`) with citations and retrieved chunk IDs as a parallel "table-mode" output and as the fallback if in-place fill quality is unsatisfactory.

**Generalization:** the design parameterizes engagement and questionnaire profile so the same pipeline can later fill other questionnaire types (e.g., Investment Due Diligence) by swapping inputs and a profile config — no code rewrite.

**Reuses** the existing 8-notebook pipeline patterns at `C:\Users\sunno\Projects\DataBrick RAG Agent\notebooks\` (idempotent MERGE ingest, page-aware chunking, Delta Sync Vector Search, `ai_query` answer generation as in `07_llm_query_demo.py` and `08_off_balance_sheet_study.py`) — but as a **new sibling notebook set** under `notebooks_ddq/` so the off-balance-sheet pipeline stays untouched. English-only inputs, so the `04_chunk_translate.py` translate stage is dropped.

## Decisions (settled in brainstorming)

| Decision | Choice |
|---|---|
| Output mode | Fill the `.docx` in place (primary); `ddq_answers` Delta table as fallback |
| Review workflow | Draft-only with inline `[Source: file, p.N]` citations; analyst edits all 121 |
| Pipeline reuse | New `notebooks_ddq/` set, parameterized for future questionnaire types |
| Language | English-only; skip translate stage |
| Index strategy | One Vector Search index per engagement (`cmi_agent.ddq_agent.idx_<engagement_id>`) |
| Trigger mode | Notebook-driven batch run (`09_orchestrate.py` with widgets) |
| Tables in DDQ | Skip in v1 (only fill 121 free-text / yes-no / numeric placeholders) |
| Citation depth | Inline `[Source: filename, p.N]` in docx + structured citations in `ddq_answers` |
| Orchestration | Spark batched `ai_query` over a questions table joined with `vector_search()` |
| Catalog / schema | `cmi_agent.ddq_agent` |

## Storage layout

All under catalog/schema **`cmi_agent.ddq_agent`**:

- **UC Volume `engagements`** with per-engagement subfolders:
  - `engagements/<engagement_id>/inputs/` — raw input PDFs (analyst uploads here)
  - `engagements/<engagement_id>/questionnaire/` — source `.docx`
  - `engagements/<engagement_id>/output/` — filled `.docx` written here
- **Delta tables** (all carry `engagement_id`):
  - `documents` — one row per input PDF (path, sha256, size, mtime, parse_status)
  - `document_pages` — page-level extracted text
  - `document_chunks` — page-aware sliding-window chunks (English; no translation column)
  - `ddq_questions` — `engagement_id, question_id, section_id, section_title, question_text, question_type, placeholder_paragraph_index, source_docx_path`
  - `ddq_answers` — `engagement_id, question_id, draft_answer, citations array<struct<file:string, page:int, chunk_id:string>>, retrieved_chunk_ids array<string>, model, prompt_version, generated_at`
  - `pipeline_errors` — same shape as existing pipeline
- **Vector Search index per engagement:** `cmi_agent.ddq_agent.idx_<engagement_id>` — Delta Sync on `document_chunks` filtered by `engagement_id`, embedding model `databricks-bge-large-en` (matches existing notebook 05).

## Notebook structure (`notebooks_ddq/`)

Reuses helpers from existing `_config.py` / `_utils.py` patterns; ODD-specific code is engagement- and profile-parameterized.

| Notebook | Purpose | Reuse from existing |
|---|---|---|
| `_config.py` | Widgets (`engagement_id`, `questionnaire_profile`, catalog=`cmi_agent`, schema=`ddq_agent`, model endpoints), `QUESTIONNAIRE_PROFILES` dict | New, mirrors existing `_config.py` |
| `_utils.py` | docx walk helpers, paragraph indexing, citation formatter, prompt builders | New, plus shared error-logging helper from existing `_utils.py` |
| `01_setup.py` | Create catalog/schema/volume/tables (idempotent CREATE IF NOT EXISTS); create per-engagement folder | Adapt of existing `01_catalog_setup.py` |
| `02_ingest_inputs.py` | Scan `inputs/`, sha256-hash, MERGE into `documents` | Adapt of existing `02_inventory_ingest.py` |
| `03_parse_pdfs.py` | PyMuPDF page extraction → `document_pages` | Adapt of existing `03_parse_reports.py` |
| `04_chunk.py` | Page-aware sliding-window chunking → `document_chunks` (no translate) | Adapt of existing `04_chunk_translate.py`, drop `ai_translate` step |
| `05_vector_index.py` | Create per-engagement Delta Sync index | Adapt of existing `05_vector_index.py` |
| `06_parse_ddq.py` | **New** — read the source `.docx` via `python-docx`, extract questions per profile, write `ddq_questions` | New |
| `07_answer_ddq.py` | **New** — Spark SQL: join `ddq_questions` with `vector_search()` lateral, batch `ai_query`, write `ddq_answers` | Inspired by existing `07_llm_query_demo.py` and `08_off_balance_sheet_study.py` (multi-prompt extraction pattern) |
| `08_fill_docx.py` | **New** — read `ddq_answers`, replace placeholders in source docx, save to `output/` | New |
| `09_orchestrate.py` | Driver that runs 02 → 08 in sequence with widget overrides | New |

## DDQ parsing (notebook 06)

Profile-driven so other questionnaires plug in later:

```python
QUESTIONNAIRE_PROFILES = {
    "odd_ssga_v1": {
        "answer_placeholder_pattern": r"<Provide your answer here\.>",
        "section_heading_regex": r"^([A-Z]\d{4})\s+(.+)$",
        "question_numbering_regex": r"^\s*(\d{1,3})[\.\)]\s+(.+)",
        "skip_tables": True,
        "question_type_rules": {
            "yes_no_prefixes": ["Do you", "Does ", "Is ", "Are ", "Has ", "Have ", "Will "],
            "numeric_keywords": ["how many", "what percentage", "what is the number"],
        },
    },
    # future: "idd_v1": { ... }
}
```

Walks `Document.paragraphs` in order, tracks the most-recent section heading, captures (question_text, paragraph_index) on numbered questions, and pairs each with the next placeholder paragraph for the `placeholder_paragraph_index`. Tables are recorded with `question_type='skipped_table'` for audit but not answered. Expected output for the SSGA DDQ: 121 answerable rows + 8 skipped-table rows.

## Retrieval + generation (notebook 07)

Single Spark SQL pass — pattern mirrors notebook 08's multi-prompt extraction:

```sql
WITH retrieved AS (
  SELECT q.engagement_id, q.question_id, q.question_text, q.question_type,
         vs.chunk_id, vs.text, vs.file_path, vs.page_start, vs.page_end, vs.score
  FROM cmi_agent.ddq_agent.ddq_questions q
  LATERAL VIEW vector_search(
    index => 'cmi_agent.ddq_agent.idx_' || q.engagement_id,
    query => q.question_text,
    num_results => 8
  ) vs
  WHERE q.engagement_id = '${engagement_id}'
    AND q.question_type IN ('free_text', 'yes_no', 'numeric')
),
prompts AS (
  SELECT engagement_id, question_id, question_text, question_type,
         collect_list(struct(chunk_id, text, file_path, page_start, page_end)) AS evidence
  FROM retrieved
  GROUP BY engagement_id, question_id, question_text, question_type
)
INSERT INTO cmi_agent.ddq_agent.ddq_answers
SELECT engagement_id, question_id,
       ai_query('${endpoint}', build_prompt(question_text, question_type, evidence)) AS draft_answer,
       parse_citations(draft_answer, evidence) AS citations,
       transform(evidence, e -> e.chunk_id) AS retrieved_chunk_ids,
       '${endpoint}' AS model,
       'v1' AS prompt_version,
       current_timestamp() AS generated_at
FROM prompts;
```

`build_prompt` (Python UDF) emits two distinct prompt templates:

- **yes_no:** instruct the model to answer `Yes / No / Partially` plus a one-sentence rationale, citing as `[file p.N]`. If evidence is insufficient, answer `Insufficient evidence` and explain.
- **free_text:** concise 2–5 sentence answer grounded in evidence, citing as `[file p.N]`. Same insufficient-evidence escape hatch.

`parse_citations` post-extracts `[file p.N]` markers from the model output and joins back to `evidence` to produce the structured `citations` array.

## Docx fill (notebook 08)

Uses `python-docx` (new dependency). Algorithm:

1. Open source docx from `engagements/<engagement_id>/questionnaire/`.
2. Load `ddq_answers` for this engagement into a dict keyed by `question_id`.
3. Walk `doc.paragraphs` with index; for each row in `ddq_questions` where `question_type != 'skipped_table'`, locate `paragraphs[placeholder_paragraph_index]`, clear its runs, insert `draft_answer` as a normal run, then append a small italic-gray run with `' [Source: <file>, p.<N>; ...]'` per citation.
4. Save to `engagements/<engagement_id>/output/<original_basename>.filled.docx`.

## Critical files to be created

- `notebooks_ddq/_config.py`
- `notebooks_ddq/_utils.py`
- `notebooks_ddq/01_setup.py` … `09_orchestrate.py` (10 notebooks total, per the table above)

No existing files modified.

## Verification (end-to-end)

1. **Setup:** in `09_orchestrate.py` set `engagement_id=odd_ssga_2025`. Run `01_setup.py` — confirm `cmi_agent.ddq_agent` schema, `engagements` volume, and the 6 Delta tables exist.
2. **Stage inputs:** copy the 26 PDFs into `engagements/odd_ssga_2025/inputs/` and the source docx into `engagements/odd_ssga_2025/questionnaire/` via `databricks fs cp` (using the `dev` profile per AGENTS.md).
3. **Run pipeline:** execute `09_orchestrate.py` end to end.
4. **Assertions:**
   - `documents` has 26 rows with `parse_status='ok'`.
   - `document_chunks` is non-empty; `idx_odd_ssga_2025` index status is `READY`.
   - `ddq_questions` has 121 answerable rows + 8 `skipped_table` rows.
   - `ddq_answers` has 121 rows; every row has at least one entry in `citations`.
   - `pipeline_errors` is empty.
   - Filled docx exists in `engagements/odd_ssga_2025/output/`; assert that `<Provide your answer here.>` no longer appears anywhere in it.
5. **Spot-check** 5 answers in the filled docx: 2 free-text against actual policy PDFs, 2 yes/no, 1 numeric. Open each citation source page and confirm it supports the answer.
6. **Generalization smoke test (optional):** add a stub `idd_v1` profile and confirm `06_parse_ddq.py` accepts it without code change (no answerable run needed).

## Out of scope (v2 candidates)

- Filling the 8 embedded tables (personnel, service providers, attachments).
- Confidence-scored auto-acceptance (gate by score; flag low-confidence for analyst).
- Deployed Databricks App with chat UI on the same per-engagement index (per AGENTS.md `deploy` skill / `agent-*` naming).
- MLflow `ResponsesAgent` wrapper for unit testing and reuse from a chat surface.

---

## Brainstorming Q&A log

The decisions in this plan came from the following exchange. Recorded for traceability.

### Q1. How should the agent deliver answers for the 121 DDQ questions?
- **Options offered:** (1) Fill the `.docx` in place — write drafts directly into `<Provide your answer here.>` placeholders; (2) Draft answers in a side-by-side Delta/CSV/Excel table; (3) Interactive chat agent only.
- **Choice:** Option 1 as primary; **keep Option 2 open as a fallback** if Option 1 quality is unsatisfactory.

### Q2. What review workflow do you want around the agent's answers?
- **Options offered:** (1) Draft-only with citations, human edits everything; (2) Confidence-gated: auto-accept high, flag low; (3) Fully autonomous.
- **Choice:** Option 1 first; **Option 2 as a potential follow-up.**

### Q3. How should this DDQ agent relate to the existing 8-notebook pipeline?
- **Options offered:** (1) Fork & adapt into a new `notebooks_odd/` set; (2) Extend in place via a `use_case` widget; (3) Greenfield, ideas only.
- **Choice:** New notebook set, but designed so **the same agent can later fill other questionnaires (e.g., Investment Due Diligence) based on different inputs.** Drove the questionnaire-profile config and per-engagement parameterization.

### Q4. Are the 26 input PDFs in English already, or do they need translation like the existing French pipeline?
- **Options offered:** (1) All English — skip translate; (2) Mixed — keep `ai_translate`; (3) Auto-detect per document.
- **Choice:** Option 1. Translate stage dropped from `04_chunk.py`.

### Q5. How should we organize indexed content across engagements (given future reuse for IDD etc.)?
- **Options offered:** (1) One Vector Search index per engagement; (2) Single shared index with `engagement_id` filter; (3) One index per questionnaire type.
- **Choice:** Option 1 — `cmi_agent.ddq_agent.idx_<engagement_id>`.

### Q6. How should the agent be triggered — notebook batch run, or a deployed app?
- **Options offered:** (1) Notebook-driven batch run; (2) Deployed Databricks App; (3) Both — phased.
- **Choice:** Option 1 for v1. App deferred to v2.

### Q7. The DDQ has 8 embedded tables — how should v1 handle them?
- **Options offered:** (1) Skip tables, fill only the 121 free-text/yes-no Q&A placeholders; (2) Best-effort table fill.
- **Choice:** Option 1. Tables recorded as `skipped_table` rows in `ddq_questions` for audit but not answered.

### Q8. What metadata should each draft answer carry into the filled docx?
- **Options offered:** (1) Inline `[Source: filename, p.N]` citations + Delta audit table; (2) Citations + retrieved-passage quotes appended; (3) Answer text only, audit-table-only citations.
- **Choice:** Option 1.

### Q9. Which orchestration approach for the fill step?
- **Options offered (proposed conversationally):** (A) Spark batched `ai_query` over a questions table — recommended; (B) Python loop in the orchestrator; (C) MLflow `ResponsesAgent` wrapper.
- **Choice:** A. Drives the single-SQL `vector_search()` + `ai_query` pattern in `07_answer_ddq.py`.

### Q10. Catalog and schema?
- **Choice:** Catalog **`cmi_agent`**, schema **`ddq_agent`**. Used throughout the storage layout and SQL.
