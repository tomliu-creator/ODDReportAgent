# DDQ RAG Agent — Improvement Plan

## Context

The DataBricks DDQ RAG agent at `C:\Users\sunno\Projects\DataBrick RAG Agent\notebooks_ddq\` scored **46/100** on the human evaluation in [DataBricks_Agent_Evaluation.md](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/DataBricks_Agent_Evaluation.md). It can produce coherent first-draft ODD answers but suffers from four diagnosable failure modes:

1. **Wrong-scope answers** — pulls generic State Street corporate material when the question targets *State Street Investment Management / SSGA Europe / MSCI EM Index Strategy* (Q16, Q17, Q18, Q56).
2. **False "Insufficient evidence"** — claims missing data when it is in fact present in the source pack (Q3, Q10, Q20, Q45).
3. **Citation overreach / over-interpretation** — supports mandate-specific conclusions with annual-report or generic risk-disclosure passages (Q43).
4. **Soft handling of high-risk sections** — collapses legal/regulatory/fraud questions into clean "no evidence" answers (Q45, Q46, Q47, Q40).

The evaluation's headline diagnosis is correct: the bottleneck is **retrieval discipline and source hierarchy**, not writing quality. The current code retrieves with a single top-k=8 vector search over an undifferentiated chunk pool ([07_answer_ddq.py:74-88](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py)), uses one prompt template per `question_type` ([_utils.py:147-189](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py)), and has no post-generation verification step. This plan proposes targeted, surgical fixes mapped to specific files.

---

## Recommended improvements (ranked by impact)

### 1. Source-type metadata + tiered retrieval (fixes wrong-scope, generic-corporate answers)

**Problem:** every chunk is equal in the vector index. A page from the SSGA EM Index strategy appendix competes with a page from the State Street Corporation 10-K on cosine similarity alone.

**Change in [04_chunk.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/04_chunk.py):**
- Add a `source_tier` column to `document_chunks` derived from the file name / path. Tiers map to the evaluation's recommended hierarchy:
  - `tier=1` — manager DDQ response, mandate appendices (filename keywords: `DDQ`, `appendix`, `mandate`, `EM`, `MSCI`, `SSGA Europe`)
  - `tier=2` — policy documents (`policy`, `procedure`, `compliance`)
  - `tier=3` — SOC reports (`SOC`)
  - `tier=4` — annual report / 10-K (`annual`, `10-K`, `10K`)
  - `tier=5` — generic corporate (default)
- Add a `legal_entity_hint` column (regex over first 3 pages: `State Street Investment Management`, `SSGA Europe Limited`, `State Street Corporation`).
- Configure these as a small lookup dict in `_config.py` so it is questionnaire-agnostic.

**Change in [05_vector_index.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/05_vector_index.py):** include `source_tier` and `legal_entity_hint` in the indexed columns so they can be filtered/boosted at query time.

**Change in [07_answer_ddq.py:71-107](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py):** retrieve in two passes per question:
- Pass A: filter `source_tier IN (1,2)`, `num_results=6`.
- Pass B: unfiltered, `num_results=4`.
- Concatenate, dedupe by `chunk_id`, keep tier in the evidence dict so the prompt can show it.

This is the single highest-leverage change — it directly attacks the evaluation's #1 finding.

### 2. Mandate-aware query expansion (fixes Q20, Q56 staffing/process misses)

**Problem:** the question "Investment staff allocated to the mandate" embeds no mandate keywords, so the vector search drifts to generic firm-wide passages.

**Change in [_utils.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py):** add `expand_query(question_text, profile) -> list[str]`. For the `odd_ssga_v1` profile, when the question contains trigger words (`mandate`, `product`, `strategy`, `investment process`, `portfolio manager`, `staff`, `team`), append a parallel query with mandate terms appended: `"<question> Emerging Market Equity MSCI EM Index Strategy SSGA Europe"`.

**Change in [07_answer_ddq.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py):** issue both queries, dedupe hits by `chunk_id`, keep the union. Trigger words and expansion terms live in `QUESTIONNAIRE_PROFILES` so other questionnaires (IDD) can define their own.

### 3. Higher top-k + lightweight LLM reranking (fixes false "insufficient evidence")

**Problem:** Q3 (ESOP figures), Q10 (AUM), Q45 (litigation) all exist in the source pack but are missed because top-8 vector hits don't surface them.

**Change in [07_answer_ddq.py:33,82-88](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py):**
- Bump default `top_k` from 8 → 20 for retrieval.
- Add a rerank step using `ai_query` with a cheap model (e.g. `databricks-meta-llama-3-3-70b-instruct` or whatever is configured): for each retrieved chunk, ask "Does this chunk contain information that could answer Q? Reply yes/no/partial." Keep the top 8 ranked `yes`+`partial`.
- This is the same pattern as notebook 08's multi-prompt extraction, applied to retrieval rather than answer generation.

### 4. High-risk-section mode (fixes Q45, Q46, Q47, Q40 ODD failures)

**Problem:** for legal/regulatory/fraud questions, the agent returns "Insufficient evidence" instead of either (a) finding the matter in the pack or (b) explicitly saying "to the best of available evidence, none disclosed."

**Change in [_utils.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py):** add a `high_risk_topics` list to the profile (`legal`, `regulatory`, `litigation`, `fraud`, `wrongdoing`, `complaint`, `investigation`, `conflict of interest`, `valuation`, `cyber`, `outsourcing`, `BCP`, `disaster recovery`, `insurance`). Add a `is_high_risk(question_text, profile) -> bool` helper.

**Change in [_utils.py `build_prompt`](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py:147):** when `is_high_risk`, append a stricter instruction:
- Retrieve `top_k * 2` chunks (handled in 07).
- Distinguish three outcomes in the answer: `"Material matter disclosed: <quote+cite>"`, `"None disclosed in source pack"`, `"Insufficient evidence to determine"`. Forbid the model from collapsing the latter two.
- Quote at least one phrase verbatim if any candidate evidence exists.

### 5. Claim verification pass (fixes citation overreach, Q43)

**Problem:** the agent links indemnity insurance to securities lending and labels a class action as material, neither directly supported by the cited chunks.

**Change in [07_answer_ddq.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py):** after the main `ai_query` pass, run a second batched `ai_query` per (question, draft_answer, evidence) that asks: *"For each factual claim in DRAFT, is it directly supported by the EVIDENCE? Output a JSON list of `{claim, supported: yes/partial/no, suggestion}`."* Persist the result to a new `claim_check_json` column on `ddq_answers`. Lower-cost than a full rewrite, and gives analysts a flag column to triage.

### 6. Richer answer schema (fixes analyst-usability gap)

**Change in [07_answer_ddq.py:249-261](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/07_answer_ddq.py):** extend the `ddq_answers` schema with:
- `confidence` (float, 0-1) — derived from rerank score + claim-check pass rate.
- `human_review_flag` (`low` | `medium` | `high`) — auto-set to `high` if `is_high_risk` or any claim-check `no`.
- `missing_information` (string) — empty if answer is complete; otherwise the model's structured statement of what was not found.
- `source_tiers_used` (array<int>) — flags answers built only from tier 4-5 evidence so analysts can spot generic-corporate drift.

**Change in [_utils.py `build_prompt`](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py:147):** request the model to emit a JSON envelope `{answer, missing_information, confidence}` rather than free prose; parse in driver. Keep the existing `[file p.N]` citation convention inside the `answer` field.

### 7. Better citation labels (analyst usability)

**Change in [_utils.py `parse_citations`](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/_utils.py:192) and [08_fill_docx.py](C:/Users/sunno/Projects/DataBrick%20RAG%20Agent/notebooks_ddq/08_fill_docx.py):** when rendering citations into the docx, expand to `[<doc_short_title> | p.N | tier]` instead of a raw filename. `doc_short_title` is a new mapping in `_config.py` (e.g. `"Appendix 11 - Remuneration Policy Overview"` for filename `appx_11_remun.pdf`). Falls back to filename if unmapped.

### 8. Evaluation harness (closes the loop)

**New file `notebooks_ddq/10_evaluate.py`:** load the human gold answers (manually curated CSV: `question_id, gold_answer, gold_evidence_files, is_high_risk`). Compute and persist:
- `false_insufficient_rate` — % of questions where draft says "Insufficient evidence" but gold has an answer.
- `wrong_scope_rate` — auto-flag using `legal_entity_hint` of cited chunks vs. mandate keywords in the gold.
- `tier_drift_rate` — % of answers where all citations are tier ≥4.
- `citation_validity` — % of `[file p.N]` markers that resolve to a real chunk page.

These metrics make every change in items 1-7 measurable rather than vibes-based, and directly answer the evaluation's "Measure false-insufficient and wrong-scope rates" recommendation.

---

## Out of scope (acknowledged but not proposed here)

- Filling the 8 embedded tables (key-person table is the analyst-pain example in §6 of the eval). The original plan deferred this to v2; addressing it requires `python-docx` table-cell logic in `08_fill_docx.py` plus a new `table_extractor` profile entry. Worth a separate plan.
- Confidence-gated auto-acceptance — the new `confidence` column enables it, but the gating rule itself is a v2 product decision.
- Switching from per-engagement Vector Search index to a single shared index — orthogonal and would not move the eval score.

---

## Critical files to be modified

- `notebooks_ddq/_config.py` — `QUESTIONNAIRE_PROFILES` additions (high-risk topics, mandate expansion terms, source-tier rules, doc short titles).
- `notebooks_ddq/_utils.py` — `expand_query`, `is_high_risk`, updated `build_prompt`, updated `parse_citations`, new JSON-envelope parser.
- `notebooks_ddq/04_chunk.py` — emit `source_tier` and `legal_entity_hint` on every chunk.
- `notebooks_ddq/05_vector_index.py` — include the two new columns in the index spec.
- `notebooks_ddq/07_answer_ddq.py` — two-pass retrieval, query expansion, rerank, claim-check, richer output schema.
- `notebooks_ddq/08_fill_docx.py` — friendlier citation rendering using `doc_short_title`.
- `notebooks_ddq/10_evaluate.py` — **new** — eval harness against a small human-graded set.

## Verification

1. Curate a 15-question gold set (mix of the eval's named bad/good examples: Q3, Q10, Q16, Q18, Q20, Q28, Q42, Q44, Q45, Q48, Q50, Q56, Q60 plus 2 numeric).
2. Run `09_orchestrate.py` end-to-end on `engagement_id=odd_ssga_2025` before any change → record baseline metrics from `10_evaluate.py`.
3. Apply changes 1-2 (tiers + mandate expansion), rerun, measure tier-drift and wrong-scope rate.
4. Apply changes 3-4 (top-k+rerank, high-risk mode), rerun, measure false-insufficient rate and high-risk recall.
5. Apply changes 5-7 (claim check, richer schema, citations), rerun, measure citation validity and human-review-flag distribution.
6. Spot-check Q18 ("target investors"), Q45 ("legal proceedings"), Q56 ("investment process") — the three most diagnostic failures from the eval — and confirm the new answers (a) cite tier-1/2 sources, (b) are mandate-specific, and (c) for Q45 either surface the matters or explicitly say "none disclosed."

The expected directional win: false-insufficient rate down materially on items 3-4, wrong-scope rate down materially on items 1-2, with overall eval score moving from 46 toward the 65-75 band where the agent becomes useful enough to reduce analyst rework rather than just reorder it.
