# Evaluation of DataBricks RAG Agent for ODD DDQ Completion

## Executive view

The DataBricks agent is useful as a **first-draft generator**, but it is not safe enough for direct ODD questionnaire completion. Its main strength is that it often attempts a substantive answer instead of leaving fields blank. Its main weakness is that it sometimes answers from **generic State Street corporate material** rather than the specific manager / mandate DDQ evidence.

**Overall score: 46 / 100**

| Criterion | Weight | Score |
|---|---:|---:|
| Completeness / answerability | 20 | 11 |
| Factual accuracy vs source pack | 25 | 10 |
| Product / mandate specificity | 15 | 5 |
| Evidence grounding and traceability | 15 | 8 |
| ODD risk judgment | 15 | 7 |
| Analyst usability | 10 | 5 |
| **Total** | **100** | **46** |

---

## 1. Completeness / answerability

**Score: 11 / 20**

### Assessment

The agent makes a real attempt to answer many questions and produces narrative drafts that can be edited by an analyst. That is valuable. However, it still misses many answers that are available in the source pack and often says “insufficient evidence” when the human final response contains the information.

### Bad examples

#### Q3 — Employee ownership

The human answer contains concrete ownership figures, including ESOP ownership and director / executive ownership. The DataBricks agent says there is insufficient evidence.

**Why this matters:** ownership alignment is a standard ODD item. Missing available figures creates avoidable human rework.

#### Q10 — AUM by firm and strategy

The human answer gives firm AUM and MSCI Emerging Markets Index Strategy AUM as of November 30, 2025. The DataBricks agent says the evidence only gives consolidated total assets and does not provide fund / strategy AUM.

**Why this matters:** this is a basic quantitative DDQ field. It should be retrieved exactly.

#### Q45 — Legal, regulatory, and litigation matters

The human final response contains a detailed legal / regulatory proceedings section. The DataBricks agent says there is insufficient evidence.

**Why this matters:** this is a high-risk DDQ section. Missing it is more serious than missing a generic background item.

### Good examples

#### Q28 — Employment scheme and incentive alignment

The agent gives a reasonably useful answer on remuneration, including base salary, benefits, cash incentives, deferred compensation, external benchmarking, firm performance, individual performance, and long-term incentive alignment.

#### Q38 — Backup plan / business continuity for critical employees

The agent gives a reasonably grounded answer that the BCP identifies critical personnel and backup personnel, although it does not fully reproduce the human answer’s formal succession-planning detail.

### Improvement needed

The agent should distinguish between:

1. **Information not present anywhere**
2. **Information present but not retrieved**
3. **Information present but only partially retrieved**

For ODD production, false “insufficient evidence” answers should be tracked as a separate metric.

---

## 2. Factual accuracy vs source pack

**Score: 10 / 25**

### Assessment

The agent is factually correct in some operational and risk-control areas, but it also produces material wrong-scope or misleading answers. The most serious issue is that it sometimes answers with corporate-level State Street facts when the question asks about State Street Investment Management, SSGA Europe, or the Emerging Market Equity mandate.

### Bad examples

#### Q16 / Q17 — Importance and profile of the product

The human answer says the product is part of the **Equity Indexing / Emerging Markets Indexing** franchise and is strategically important within State Street Investment Management.

The DataBricks agent instead describes **State Street Alpha and related digital / custody capabilities**.

**Problem:** this is the wrong product context. It may be true for State Street generally, but it does not answer the mandate-specific DDQ question.

#### Q18 — Target investors

The human report identifies clients such as asset managers and owners, insurance companies, wealth managers, official institutions, and central banks.

The DataBricks agent says the firm targets **individual, retail investors**.

**Problem:** this is materially wrong for the institutional ODD context.

#### Q43 — Indemnity insurance and material claims

The agent links indemnity insurance to securities lending / repurchase indemnification and identifies a class-action complaint as a material claim. The human answer is more cautious and states that State Street maintains a comprehensive insurance program and communicates with insurance carriers regarding legal proceedings where coverage may be available.

**Problem:** the agent over-interprets and may create an unsupported conclusion about who bears cost and whether a claim is an indemnity-insurance claim.

### Good examples

#### Q50 / Q51 — Risk management framework and periodic risk assessments

The agent gives a broadly credible description of annual review, ERM, operational risk, credit risk, stress testing, risk categories, and control testing. The answer is not perfect, but it is directionally useful.

#### Q44 — Background checks

The agent correctly identifies that pre-employment background checks are performed and notes the lack of evidence for periodic re-checks. This is a reasonable partial answer.

### Improvement needed

The agent needs a claim-verification layer that checks:

- Is the answer about the correct legal entity?
- Is the answer about the correct product / mandate?
- Does the cited evidence actually support the exact claim?
- Is the claim too strong relative to the source?

---

## 3. Product / mandate specificity

**Score: 5 / 15**

### Assessment

This is one of the weakest areas. The agent often answers from a broad State Street Corporation perspective, while the final report is specific to **State Street Investment Management**, **SSGA Europe Limited**, and the **Emerging Market Equity Index / MSCI EM Index Strategy**.

### Bad examples

#### Q20 — Investment staff / resources allocated to the mandate

The human answer gives mandate-specific staffing: nearly 60 Global Systematic Equity portfolio managers, lead PM Richard Hamilton, backup PM Mark Davey, over 30 global trading team members, and over 100 dedicated operations personnel.

The DataBricks agent says there is insufficient evidence.

**Problem:** the agent missed a highly relevant mandate-specific staffing answer.

#### Q56 — Investment process

The human answer is very specific to index equity management: source, design, implement, monitor; RITS; Cortex; replication vs optimization; Axioma; tracking error; EM liquidity and settlement constraints.

The DataBricks agent gives a generic process answer involving account extraction, IMS, Bloomberg / Portia reconciliations, and oversight.

**Problem:** the answer is not wrong in every sentence, but it does not capture the actual product investment process.

#### Q32 — Multi-location operations

The human answer explains regional portfolio-management structure, global trading centers, operations centers, Global Process Owners, COO oversight, and Strategic Partner Oversight.

The DataBricks answer focuses on outsourcing and backup monitoring, with limited link to the actual operating model described in the final report.

### Good examples

#### Q31 — Similar strategy and allocation policy

The agent identifies fair and equitable allocation across similar products, though its examples are less precise than the human answer.

### Improvement needed

The retrieval logic should prioritize mandate-specific passages when the question contains words such as:

- product under consideration
- mandate
- strategy
- investment process
- portfolio manager
- Emerging Market Equity
- index
- MSCI EM
- SSGA Europe
- State Street Investment Management

Annual report evidence should be a fallback, not the default.

---

## 4. Evidence grounding and traceability

**Score: 8 / 15**

### Assessment

The agent usually provides citation-like markers such as `[C1]`, `[C2]`, and sometimes page references. That is better than unsupported prose. However, the traceability is not sufficient for ODD audit use because the citation labels are not always transparent, and some claims appear weakly supported or unsupported.

### Bad examples

#### Vague citation labels

Many answers refer to `[C1]`, `[C2]`, etc. without making it easy for the reviewer to know the exact document name, appendix, and page. In an ODD workflow, reviewers need evidence that can be checked quickly.

#### Over-broad citations

Some answers cite general risk disclosure or annual-report language to support mandate-specific conclusions. That creates a false sense of grounding.

#### Missing source hierarchy

The agent does not appear to consistently prefer the manager-provided DDQ response over annual reports or generic corporate documents.

### Good examples

#### Q42 — Insurance coverage

The agent identifies a certificate of liability insurance, policy number, limit, insurer, and broker. This is the right kind of answer style: concrete, document-based, and reviewable.

#### Q51 — Risk assessments

The agent includes page-level references and specific concepts such as annual review, ERM, risk categories, and controls testing. Even if it needs refinement, the answer is relatively traceable.

### Improvement needed

The agent should output source references in this format:

`Appendix name | page | quoted/near-quoted support | confidence`

Example:

`Appendix 11 - Remuneration Policy Overview | p.1-2 | supports pay-for-performance, deferred compensation, LTI`

This would make analyst review much faster.

---

## 5. ODD risk judgment

**Score: 7 / 15**

### Assessment

The agent has some awareness of ODD-sensitive themes such as governance, controls, risk management, background checks, insurance, compliance, and BCP. But it misses or softens several high-risk areas, especially legal/regulatory proceedings and mandate-specific operational controls.

### Bad examples

#### Q45 — Legal / regulatory proceedings

The agent says insufficient evidence, while the human report includes multiple material matters. This is a critical failure.

#### Q46 / Q47 — Complaints, investigations, fraud, wrongdoing

The agent gives overly clean “no evidence” style answers. The human answer is more nuanced: State Street is involved in ordinary-course disputes and regulatory inquiries, and it is not practical to provide every inquiry.

**Problem:** in ODD, “no evidence” should not be turned into an implied clean bill of health unless the source explicitly supports it.

#### Q40 — Reputation incidents

The agent says insufficient evidence. The human final answer says no significant known incidents for the period under review but notes confidentiality. This distinction matters: “insufficient evidence” and “to the best of knowledge, none known” are not the same.

### Good examples

#### Q48 / Q49 — Operational risk management

The agent gives a credible three-lines-of-defense answer and identifies the CRO, ERM, second-line functions, and Corporate Audit. This is useful, although the final response contains more specific State Street Investment Management information.

#### Q60 — Stress tests and liquidity analyses

The agent identifies regular stress tests and historical stress scenarios. This is a reasonable ODD-style response.

### Improvement needed

The agent needs a high-risk-section mode for:

- legal and regulatory proceedings
- fraud / wrongdoing
- conflicts of interest
- valuation
- custody / client money
- outsourcing
- cyber / IT security
- BCP / DR
- insurance

In these sections, the agent should be conservative but exhaustive. Missing a source-backed red flag should be treated as a critical failure.

---

## 6. Analyst usability

**Score: 5 / 10**

### Assessment

The output is readable and often useful as a first draft. However, the analyst still needs to spend significant time correcting wrong-scope answers, filling missed fields, checking citations, and restoring tables.

### Bad examples

#### Tables not populated

The key-person table is left blank, while the human final report contains names, years in industry, years at firm, current position, and role details.

#### Excessive generic prose

Some answers are polished but generic. That can be dangerous because the language sounds credible even when it does not answer the specific DDQ question.

#### N/A handling

The agent often says insufficient evidence where the human final response says “Not applicable” or gives a concise explanation. These are different outcomes in DDQ completion.

### Good examples

#### Draft quality

Where the retrieval is correct, the agent’s writing is clear and can be reused with moderate editing.

#### Controls language

For internal controls, risk management, and remuneration, the agent produces reasonably professional ODD-style language.

### Improvement needed

The output should separate:

- **Answer**
- **Evidence**
- **Missing information**
- **Human-review flag**
- **Confidence**

For example:

```text
Answer: ...
Evidence: Appendix X, page Y
Missing information: none / specific missing item
Human-review flag: low / medium / high
Confidence: 0.78
```

---

## Recommended remediation for the DataBricks agent

### 1. Add source-ranking rules

Prioritize sources in this order:

1. Manager-provided final DDQ response / questionnaire response
2. Mandate-specific appendices
3. Policy documents
4. SOC reports
5. Annual report
6. Generic public information

### 2. Add mandate-specific retrieval filters

When the DDQ question asks about the product or mandate, force retrieval using terms such as:

- Emerging Market Equity
- Emerging Markets Indexing
- MSCI EM Index Strategy
- Systematic Equity
- Richard Hamilton
- Mark Davey
- Cortex
- Axioma
- tracking error

### 3. Add citation verification

Before finalizing an answer, require the model to check whether each factual claim is directly supported by the retrieved text.

### 4. Add critical-question guardrails

For legal, regulatory, conflicts, fraud, valuation, cyber, outsourcing, and BCP questions, the agent should:

- retrieve more sources
- avoid broad “no evidence” conclusions
- quote or summarize specific proceedings where available
- label unresolved items clearly

### 5. Measure false-insufficient and wrong-scope rates

The DataBricks agent’s key risks are:

- false insufficient answers
- wrong product / entity scope
- overconfident generic answers
- citation overreach

These should be measured separately in the evaluation harness.

---

## Final judgment

The DataBricks agent has potential because it can draft coherent ODD answers. But today it is not reliable enough for production without a human reviewer. The most important fix is not better writing; it is **better retrieval discipline and source hierarchy**.

The agent should be redesigned to retrieve mandate-specific evidence first, verify each claim against source text, and flag uncertain answers instead of filling gaps with generic corporate material.
