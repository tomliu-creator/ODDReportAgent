# ODD Analyst Interview — Questionnaire

**Purpose:** Gather the analyst's tacit knowledge to improve the ODD RAG agent's retrieval quality, output accuracy, and interaction design. Also scope the next use case.

**Context:** The analyst has already seen the agent's output.

---

## A. Evidence Quality and Document Tiering

**A1. Document tiering**
The agent currently treats the completed DDQ as the primary source (tier 0) and everything else as supporting. How would you rank the other document types by reliability and importance? For example:
- Completed DDQ
- Annual report / audited financial statements
- SOC 1 report
- Appendices (org charts, policies, bios, insurance certificates)
- Code of Ethics / Standard of Conduct
- Other?

Should any of these be treated as equally authoritative to the DDQ for certain topics?

**A2. Document-to-topic mapping**
For each of the four chapters (A–D), which documents do you typically reach for first? For example, do you always check the annual report for A0300 (financial condition), or the SOC 1 for B0400 (assurance)? We want to know your mental "go-to" list per topic so the agent retrieves the right evidence first.

**A3. High-value sections within documents**
Are there specific parts of a document you re-use across multiple topics? For example:
- The balance sheet or P&L from the annual report
- The organizational chart for both A0700 and A0900
- A specific section of the SOC 1 report

Knowing these helps us pre-extract and tag them so they're always available.

**A4. Negative relevance — what to ignore**
Are there things the agent should actively skip for certain topics? For example: the manager's general valuation methodology is irrelevant if the mandate uses its own. What other exclusions come to mind? This is one of the hardest things for the agent to learn without being told.

**A5. Topic-specific prompts and keywords**
For the topics you've reviewed in the agent's output, were there keywords or angles you expected the agent to cover but it missed? Do you have specific instructions or search terms you'd add for any topic? We can add these directly to the agent's spec.

---

## B. Output Quality — Reaction to Current Agent Output

**B6. First impression**
Having seen the generated report — what was your overall reaction? What surprised you positively, and what was clearly wrong or missing?

**B7. Tone and depth**
Is the writing style appropriate for the audience (investment committee, board)? Too shallow? Too detailed? Too generic? Should certain topics get more depth than others?

**B8. The ODD opinion line**
The agent writes a 1–2 line risk opinion per topic. How does this compare to what you'd write? Is the rating methodology (Unacceptable / High / Medium / Low) correctly applied, or does the agent tend to be too generous or too harsh?

**B9. Citations**
The agent cites evidence as `[file | p.N | tier X]`. Is this citation style useful for your review? Would you prefer more granular references (e.g., specific table or paragraph within a page)?

**B10. Errors of commission vs. omission**
What bothers you more — the agent saying something wrong, or the agent missing something important? This tells us whether to tune for precision (say less, be more certain) or recall (say more, risk some noise).

---

## C. Analyst Workflow — How You'll Use the Agent

**C11. Review workflow**
When you receive the agent's draft report, what's your process? Do you go topic by topic, chapter by chapter, or jump to the topics you care most about first? Which topics do you always review carefully regardless of the rating?

**C12. Follow-up and overrides**
If a topic assessment is weak or wrong, what would you want to do?
- Re-run that single topic with a different prompt?
- Provide a specific instruction ("focus on X, ignore Y") and regenerate?
- Edit the text directly and move on?
- Ask the agent a follow-up question about the evidence it found?

This tells us what interactive capabilities to prioritize.

**C13. Iterative refinement**
Do you see yourself running the agent once and then editing manually, or would you prefer to iterate with the agent (tweak prompts, re-run, compare) until the output is close enough? How many rounds of iteration would be acceptable?

**C14. Confidence signals**
The agent assigns a confidence score per topic. Is that useful to you? Would a "human review needed" flag per topic help you prioritize your review time?

---

## D. Next Use Case

**D15. What's next?**
Beyond the ODD report, what's the next task you'd want to automate or assist with? For example:
- Ongoing monitoring updates (annual re-assessment with delta from previous year)
- DDQ completion (filling out a blank DDQ from source documents)
- Comparative analysis across multiple managers
- Ad-hoc research questions against the document corpus
- Something else?

**D16. Reuse expectations**
For that next use case — would it use the same document corpus, or a different set of inputs? Same output format (Word report), or something different?

---

## Suggested Meeting Structure

| Block | Duration | Topics |
|---|---|---|
| Warm-up | 5 min | Show the agent output side by side with a real report |
| Evidence quality | 15 min | A1–A5 |
| Output review | 15 min | B6–B10 (walk through the generated report together) |
| Workflow | 10 min | C11–C14 |
| Next use case | 10 min | D15–D16 |
| Open | 5 min | Anything we missed |
