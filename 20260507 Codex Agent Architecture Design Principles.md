# 20260507 Codex Agent Architecture Design Principles

## Core Principle

The agent architecture is split into two layers:

- `01-05`: reusable RAG foundation.
- `06-09`: business-specific agent workflow.

The purpose of this split is to make the lower layer stable, reusable, and centrally configurable, while allowing the upper layer to be rewritten for different business cases without disturbing ingestion, parsing, chunking, indexing, and retrieval infrastructure.

## Layer 1: Reusable RAG Foundation (`01-05`)

The first five notebooks should remain broadly reusable across business cases.

- `01_setup.py`: creates shared Delta tables and schemas.
- `02_ingest_inputs.py`: discovers input files and assigns document roles/tiers.
- `03_parse_sources.py`: parses supported source files into page/row text.
- `04_chunk.py`: chunks parsed text and creates chunk-level metadata.
- `05_vector_index.py`: creates/syncs the Databricks Vector Search index.

These notebooks form the document-to-vector-index pipeline.

For new business cases, prefer changing `_config.py` before changing `01-05`.

## Configuration For `01-05`

Infrastructure and reusable RAG behavior should be centrally managed in `_config.py`.

Examples include:

- Unity Catalog catalog/schema/volume paths.
- Source-role rules, such as `manager_completed_ddq`, `appendix`, `annual_report`, `index_methodology`, or `factsheet`.
- Source-tier rules.
- Parsing regexes and metadata patterns.
- Chunk size, page overlap, and question/answer embedding behavior.
- Vector Search endpoint default.
- Embedding model endpoint default and fallback candidates.
- Vector index naming.
- Vector Search metadata columns to sync.
- Vector Search embedding source column.
- Source roles excluded from indexing.

`09_orchestrate.py` may call `01-05`, but should not duplicate infrastructure configuration that belongs in `_config.py`.

## Layer 2: Business-Specific Workflow (`06-09`)

The later notebooks are allowed to be business-specific and may be replaced for another agent.

In the current ODD workflow:

- `06_load_odd_agent_spec.py`: loads `ODDAgent.md` and ODD report topics.
- `07_assess_odd_report.py`: retrieves evidence, reranks, writes topic assessments, and assigns risk.
- `08_fill_odd_report.py`: exports the ODD report to Word.
- `09_orchestrate.py`: runs the ODD workflow in order.

For another use case, these can become different notebooks, for example:

- `06_load_financial_analyst_spec.py`
- `07_analyze_annual_reports.py`
- `08_export_financial_memo.py`
- `09_orchestrate_financial_analysis.py`

Or:

- `06_load_index_analyst_spec.py`
- `07_analyze_index_methodologies.py`
- `08_export_index_review.py`
- `09_orchestrate_index_analysis.py`

## Configuration For `06-09`

Business-specific runtime settings may live in `09_orchestrate.py` or in the business-specific agent spec.

Examples include:

- Agent behavior spec path, such as `ODDAgent.md`.
- Assessment model name.
- Risk model name.
- Retrieval mode, such as `sql` or `python`.
- Output format and output notebook.
- Business-specific topic definitions.
- Risk-rating or conclusion framework.
- Prompt templates and output schema.

The LLM model choices are business-layer decisions because different workflows may need different model behavior. The embedding model and vector-index configuration remain foundation-layer decisions and should stay in `_config.py`.

## Practical Rule

When adapting the agent to a new business case:

1. Reuse `01-05` first.
2. Add or adjust a workflow profile in `_config.py`.
3. Recode `06-09` only for business-specific behavior, prompts, outputs, and domain logic.
4. Avoid duplicating foundation configuration in `09`.
5. Only recode `01-05` if the new source documents require a new parsing or chunking algorithm that configuration cannot express.

## Examples

For a financial analyst reading annual reports:

- Reuse `01-05`.
- Configure source roles such as `annual_report`, `earnings_transcript`, `investor_presentation`, and `financial_statement`.
- Reuse vector indexing and metadata sync.
- Replace `06-09` with notebooks that load a financial analyst spec, define financial analysis topics, produce evidence-backed analysis, and export a memo.

For an index analyst reading index methodologies:

- Reuse `01-05`.
- Configure source roles such as `index_methodology`, `factsheet`, `rebalance_notice`, and `benchmark_policy`.
- Tune chunk sizes if methodology sections are long or clause-like.
- Replace `06-09` with notebooks that extract methodology questions, analyze eligibility/rebalance/weighting rules, and export an index review.

## Design Intent

This architecture keeps the system modular:

- The foundation layer turns documents into reliable searchable evidence.
- The business layer turns evidence into a domain-specific deliverable.

That separation makes it easier to reuse the agent, debug retrieval independently from reasoning, and avoid accidental coupling between one business case and the underlying RAG infrastructure.

