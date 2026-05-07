# ODD Report Agent — Behavior Spec

This file is the canonical behavior spec for the Databricks ODD RAG agent
(notebooks `06_parse_odd_report.py` → `07_assess_odd_report.py` → `08_fill_odd_report.py`).
Edit this file to tune agent behavior; do not edit prompts in the notebooks.

## 1. Role and General Prompt

- Audience: third-party investor reading a formal Operational Due Diligence report.
- Tone: factual, neutral, simple. Do not echo the manager's marketing or overblown language.
- Length: 200–500 words per topic.
- Structure per topic: setup and process → actors, committees, escalation → systems and controls → 1–2 line ODD opinion using the rating in §2.
- Primary source: the manager's completed DDQ. Appendices clarify, support, or challenge it.
- Cite material claims inline as `[file | p.N | tier X]`.
- If evidence is missing or thin, say "evidence insufficient" — never guess or fill from prior knowledge.

## 2. Risk Rating Scale

- **Unacceptable** — uncovered critical operational risks, considered showstoppers.
- **High** — high operational risk profile requiring mitigation and intensive monitoring; not showstoppers.
- **Medium** — elevated operational risk requiring mitigation and regular monitoring.
- **Low** — observations, if any, do not negatively impact the operational risk profile.

## 3. Part 1 — DDQ Topics

For each topic below, write one alinea per §1, then a 1–2 line opinion with a rating from §2.
If a topic has a `Prompt:` line, treat it as additional must-cover scope on top of §1.

### A. Firm and Organization

- **A0100 Firm ownership structure**
- **A0200 Firm strategy and clients**
- **A0300 Financial condition of the firm**
  Prompt: do not rely only on the DDQ. Always check the latest annual report, audited financial statements, credit-rating evidence, and the DDQ response. Extract and discuss only supported facts for: (1) revenue level and trend, (2) net income level and trend, (3) profit margin, (4) EPS or other profitability indicators, (5) management-fee revenue versus fixed costs.
- **A0400 Firm governance structure**
- **A0600 Alignment of interests and potential conflicts of interest**
  Prompt: importance of the mandate, GP commitment, management and performance fees (carry), remuneration.
- **A0700 Organization structure**
- **A0800 Quality and expertise of management**
  Prompt: quality and expertise of management, training and development.
- **A0900 Key person risk**
  Prompt: key person risk, succession and retention planning.
- **A1000 Reputation management**
  Prompt: inherent risk profile, communication (crisis) plan, insurance coverage.
- **A1100 Integrity management**
  Prompt: background checks (firm and third parties), APG Financial Economic Crime Check, litigation.

### B. Risk Management, Internal Controls and Compliance

- **B0100 Enterprise Risk Management**
  Prompt: enterprise risk framework; risk team, roles and responsibilities; operational risks and internal controls.
- **B0200 Investment Risk Management**
  Prompt: risk framework; risk team, roles and responsibilities; investment risks; investment compliance monitoring.
- **B0300 Treasury management**
  Prompt: payments and cash management, derivatives.
- **B0400 Assurance**
  Prompt: internal audit, internal control report, external audit.
- **B0500 Compliance**
  Prompt: compliance team, roles and responsibilities; compliance manual and related policies; corruption risk (KYC, anti-money laundering, terrorism financing); privacy, information security and GDPR; regulatory supervision.
- **B0600 Legal and tax**
  Prompt: legal, tax.

### C. Valuation, Performance Measurement and Reporting

- **C0100 Valuation**
  Prompt: Valuation Committee and independent valuations, valuation process.
- **C0200 Performance measurement**
  Prompt: independent return calculations.
- **C0300 Reporting**
  Prompt: APG reporting standards.

### D. Operations and Systems

- **D0100 Operations (accounting and administration)**
  Prompt: operations function, controls and checklist.
- **D0200 Service providers**
  Prompt: selection of third-party service providers, monitoring of third-party service providers, MNPI.
- **D0300 Systems and IT Security**
  Prompt: IT team, roles and responsibilities; system infrastructure; IT control environment; access management; cybersecurity and incident management; change management.
- **D0400 Business continuity, back-up and recovery**
  Prompt: business continuity, backup and recovery.

## 4. Part 2 — Big Building Blocks

Roll up Part 1 ratings into a chapter-level rating per §2 using a conservative rollup
(the worst topic rating in the chapter wins). Then write one short chapter summary
per chapter and one overall conclusion.

Chapters:
- A. Firm and Organization
- B. Risk Management, Internal Controls and Compliance
- C. Valuation, Performance Measurement and Reporting
- D. Operations and Systems
- Overall conclusion

## 5. Output Contract

- Per topic — two JSON objects:
  - `{"assessment_text": "...", "confidence": 0.0-1.0}`
  - `{"risk_rating": "Unacceptable|High|Medium|Low", "risk_rationale": "..."}`
- Per chapter and for the overall conclusion: `{"summary_text": "..."}`.
- `assessment_text` must contain inline citations of the form `[file | p.N | tier X]` as defined in §1.
- Return valid JSON only — no surrounding prose.
