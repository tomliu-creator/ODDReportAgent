# ODD RAG Agent Improvement Plan

## Purpose

This plan improves the ODD report-generation agent so it produces evidence-bounded, analyst-usable drafts. The core issues observed were:

1. The agent sometimes confused DDQ **question text** with **manager answer text**.


---

# 1. Questionnaire Question/Answer Boundary Parser

## 1.1 Problem

The filled questionnaire should not be ingested as undifferentiated raw chunks. Many DDQ questions contain examples of what the manager should answer, such as:

- shareholder general meeting;
- executive management;
- Investment Committee;
- Risk Committee;
- Audit Committee;
- Valuation Committee;
- Remuneration Committee;
- chair;
- meeting frequency;
- voting mechanism;
- escalation routes.

These terms are part of the **question**, not necessarily part of the **manager’s answer**. If the vector search retrieves a chunk containing both the question and the answer, the LLM may promote the question examples into factual findings.

Note that the work flow should accomendate different questionaire (meaning the user will specify the name of the questionaire for specical parsing treatment) and also the possiblity of mulitple questionaires to be paresed 

## 1.2 Required behavior

The retrieval process should use the DDQ question text to find the right record, but the generation process should use only the manager’s answer text and supporting attachments as evidence.

The question text is useful for retrieval. It is not factual evidence.

## 1.3 Proposed parsed record format

Each DDQ question should be stored as a structured record:

```json
{
  "document_name": "APG DDQ 16.01.2026 - Final responses",
  "section_id": "A0400",
  "section_title": "Firm governance structure",
  "question_id": "14",
  "question_text": "Describe the governance structure of the company...",
  "answer_text": "State Street Investment Management maintains a comprehensive governance structure...",
  "tables": [],
  "page_start": 181,
  "page_end": 185,
  "source_type": "filled_questionnaire",
  "evidence_type": "manager_answer",
  "evidence_tier": 0
}
```

Important: only `answer_text` and linked tables should be treated as Tier 0 factual evidence. `question_text` should be retrieval metadata only.

## 1.4 Retrieval workflow

Use a two-step retrieval process:

1. **Question matching**
   - Search section ID, question ID, section title and question text.
   - Purpose: identify the right DDQ record.

2. **Answer evidence retrieval**
   - Retrieve the corresponding manager answer text and embedded tables.
   - Then retrieve supporting appendices if needed.
   - Pass question text to the model only as “what is being asked,” not as evidence.

Recommended source priority:

| Priority | Source type |
|---:|---|
| 1 | Manager-filled DDQ answer text |
| 2 | Tables embedded in the DDQ answer |
| 3 | Mandate-specific appendices and ODD meeting notes |
| 4 | Policy documents and procedure manuals |
| 5 | SOC1/SOC2 reports |
| 6 | Annual report / audited financial statements |
| 7 | Public or generic corporate materials |

Annual report evidence should be mandatory for A0300 financial condition, but not the default source for every section.

## 1.5 Parser logic

The parser should:

1. Detect section headings, e.g. `A0400. Firm governance structure`.
2. Detect question numbers, e.g. `14. Describe the governance structure...`.
3. Capture question text until the start of the manager answer.
4. Capture manager answer text until the next question or section heading.
5. Preserve tables as structured data.
6. Attach page numbers, source document names and evidence tier.
7. Tag each text block as either `question_text` or `answer_text`.

## 1.6 Handling tables

Tables should not be flattened into paragraphs only. Store them as structured objects:

```json
{
  "table_id": "A0900_Q34_key_personnel",
  "section_id": "A0900",
  "question_id": "34",
  "columns": ["Name", "Years in industry", "Years at firm", "Current position"],
  "rows": [
    ["John Tucker, CFA", "37", "37", "Systematic Equity CIO"],
    ["Julian Harding", "30", "9", "Head of Index - EMEA and APAC PM"]
  ]
}
```

This prevents the agent from leaving tables blank when the information exists.

## 1.7 Mandatory answer-only rule

Add this to the system/developer prompt for DDQ workflows:

```text
When using a filled questionnaire, distinguish strictly between QUESTION TEXT and MANAGER ANSWER TEXT.

Use the question text only to understand what is being asked and to retrieve the correct answer record.

Do not treat examples or requested fields in the question as evidence.

Only facts appearing in the manager answer text, embedded answer tables, or supporting attachments may be stated as facts.

If the manager does not provide a requested item, write “not disclosed” rather than inferring it.
```

## 1.8 Claim verification layer

After drafting each section, run a second verification step.

Verifier prompt:

```text
Review the draft section against the supplied evidence.

Extract every factual claim about:
- governance bodies;
- named roles;
- committees;
- systems/tools;
- reporting frequency;
- voting mechanisms;
- escalation routes;
- audit/testing;
- controls;
- risk ratings;
- financial figures.

For each claim, classify it as:
SUPPORTED, PARTIALLY SUPPORTED, or UNSUPPORTED.

Delete all UNSUPPORTED claims.
Qualify all PARTIALLY SUPPORTED claims.
Return only the revised section.
```

This would catch most “best-practice hallucination” problems.


