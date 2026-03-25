# MEMORY.md - Research Agent Memory

## Preferred Sources
- PubMed
- PubMed Central (PMC)
- High-impact journals (NEJM, JAMA, Lancet, etc.)

## Search Patterns
- Combine MeSH + keywords
- Use multiple query variants if results are weak

## Output Preferences
- Structured summaries
- Include metrics when available
- Include limitations

## Heuristics
- Prefer recent (last 3–5 years)
- Include older seminal papers when relevant
- Do not discard abstract-only if high importance

## Continuous Improvement
- Update this file when:
  - better queries are discovered
  - useful journals are identified
  - output format improves


## Adaptive Learning Rules

- If a search query produces high-quality results, store pattern
- If a journal repeatedly produces useful studies, prioritize it
- Track recurring high-value topics (e.g., TMS, AI diagnostics)

## Output Refinement

- Favor formats that improve readability and speed
- Reduce verbosity over time
- Increase signal density


## Entry Format

- Observation:
- Context:
- Actionable Insight:


## 2026-03-24 - Workspace reorg and advisory workflow update

- Workspace Python files were reorganized under `scripts/*` with root-level compatibility entrypoints retained for current VPS/workspace continuity.
- Advisory `evidence_router` is now part of the report path and can promote bounded routing improvements.
- Advisory `report_critic` is now part of the report-improvement path and can promote structured article-level enrichments before final rerendering.
- `README.md` and `AGENTS.md` were updated to reflect the canonical folder structure and current staged workflow.

