# 20260506 Codex A0300 Retrieval Refinements

## Context

The A0300 financial-condition assessment was incorrectly concluding that no relevant financial performance data was available, even though Appendix 8, the State Street 2024 Annual Report, contained revenue, net income, EPS, margin, ROE, and expense data.

A diagnostic test showed that the good annual-report chunks existed in `document_chunks`, had valid appendix metadata, and were searchable through Databricks Vector Search. The failure was not ingestion or indexing. The failure was evidence selection inside `07_assess_odd_report.py`.

## Problem Found

For A0300, the final selected evidence contained:

- Several manager DDQ chunks.
- Two irrelevant SOC 1 chunks.
- Appendix 8 page 243, which was a Form 10-K certification page.

The useful Appendix 8 pages, especially page 109 with the financial-performance table, were available but did not reach the final answer. The reranker only inspected an early slice of the candidate list, and that early slice was crowded by tier-0 DDQ evidence and less useful appendix pages.

In simple terms, the right evidence was in the library, but it was not being handed to the judge.

## Refinement 1: Prompt-to-Appendix Title Bridge

`07_assess_odd_report.py` now loads available appendix filenames from the `documents` table and compares them against the topic prompt.

If a topic prompt refers to a source type that resembles an appendix filename, the matching appendix title is added to the vector-search query.

Example:

- Topic prompt says: `Always check the latest annual report`.
- Appendix filename is: `Appendix 8 - 2024 Annual Report - State Street Corporation.pdf`.
- The query is enriched with that appendix title.

This also supports other appendix references, such as:

- Topic prompt says: `Check the Certificate of Insurance`.
- Appendix filename is: `Appendix 14 - Certificate of Insurance.pdf`.
- The search query is enriched with the Certificate of Insurance appendix title.

This is intentionally generic. It is not an A0300 hardcode and does not force the model to use a specific appendix. It improves recall by making the relevant source more likely to appear in the candidate pool.

## Refinement 2: Balanced Reranker Candidate Sampling

Previously, the reranker inspected only the first slice of candidates after sorting. Because manager DDQ evidence is tier 0, it could crowd out appendix evidence before the reranker had a chance to judge it.

`07_assess_odd_report.py` now builds a balanced reranker input from three buckets:

- `section`: evidence from the matching DDQ section.
- `appendix`: normal appendix retrieval hits.
- `mandatory_external`: prompt-driven appendix retrieval hits.

The reranker receives candidates from each bucket instead of only the earliest global slice. This ensures that prompt-required appendix evidence is actually evaluated.

## Refinement 3: Wider Prompt-Driven Appendix Pool

For topics with a non-empty topic prompt, prompt-driven appendix retrieval now requests a wider candidate pool before final evidence selection.

This matters because useful annual-report fact pages can rank behind generic risk, certification, or table-of-contents pages from the same source. A wider pool gives the reranker enough candidates to reject decoys and keep factual evidence.

## Refinement 4: No Silent Blank Assessment Text

One rerun produced a blank A0300 assessment even though useful evidence was selected. This likely happened because the model response did not parse into the expected `assessment_text` JSON key.

`07_assess_odd_report.py` now falls back to alternate keys such as `assessment` or `text`, and finally to the raw model response. This prevents blank report sections from being silently stored.

## Validation Result

After rerunning `07_assess_odd_report.py`, A0300 selected Appendix 8 page 109 and generated a substantive financial-condition assessment.

The updated A0300 assessment now discusses:

- Total revenue.
- Net income.
- Diluted EPS.
- Pretax margin.
- Return on average common equity.
- Management-fee revenue.
- Expense and cost-base information.

Validation log:

`logs/a0300_after_source_title_fix_validation_20260506_220955.log`

Databricks reruns:

- `07_assess_odd_report`: run `1105651798398456`, success.
- `08_fill_odd_report`: run `571914050487596`, success.

## Why This Should Stay

These changes protect the workflow from a common RAG failure mode: relevant evidence exists and is searchable, but is removed before the reranker or assessment model can inspect it.

The fixes keep the workflow generic and prompt-driven:

- They do not hardcode A0300.
- They do not hardcode Appendix 8.
- They use the topic prompt and appendix metadata to improve retrieval.
- They preserve the manager-completed DDQ as tier-0 evidence while still allowing required appendix evidence to be considered.

