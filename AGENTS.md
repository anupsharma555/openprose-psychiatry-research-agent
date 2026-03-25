# AGENTS.md

This workspace powers the **prose research agent** and its deterministic research pipeline plus bounded advisory sub-agents.

## First Run

If `BOOTSTRAP.md` exists, read it first.
It is first-run setup only.

## Every Session

Before doing anything major:

1. Read:
   - `SOUL.md`
   - `USER.md`
   - `TOOLS.md`
2. Read:
   - `memory/YYYY-MM-DD.md` for today
   - recent memory notes if needed
3. In the main session, also read:
   - `MEMORY.md`

Do not assume long-term context is already loaded in Discord or other external channels.
Consult memory deliberately when needed.

## Memory Discipline

- Default to appending to `memory/YYYY-MM-DD.md`
- Update `MEMORY.md` only when the insight is reusable across tasks
- Do not create memory files for no-op heartbeat checks
- Do not overwrite existing memory casually
- Prefer compact durable lessons over verbose logs

## Workspace Structure

The canonical implementation files live under:

- `scripts/orchestration/`
- `scripts/pipeline/`
- `scripts/planner/`
- `scripts/router/`
- `scripts/reporting/`
- `scripts/diagnostics/`

Operational rule:
- treat `scripts/*` as the source of truth for code organization and future edits

Compatibility note:
- in the current VPS/workspace, root-level `prose_*.py` entrypoints may remain available for older commands and operational continuity
- those root-level entrypoints are compatibility shims, not the preferred organization model

Sub-agent contracts and schemas live under:

- `.prose/templates/subagents/`

Shared run memory lives at:

- `.prose/memory/run_memory.json`

Run artifacts live under:

- `.prose/runs/<run_id>/`

## Main Trigger Phrases

Treat the following as requests to start or continue the top-level prose research workflow:

- run the prose research
- run prose research
- start the prose research workflow
- run the prose research workflow
- equivalent phrasing asking to begin the staged research pipeline

## Preferred Entrypoints

Use top-level orchestration rather than low-level scripts one by one.

Preferred user-topic launcher:

- `prose_research_start.py --topic "<user topic>" --execute --materialize-selected --build-report`

If Discord delivery is requested:

- `prose_research_start.py --topic "<user topic>" --execute --materialize-selected --build-report --post-discord --discord-channel-id <channel_id>`

The launcher should remain thin.
It should not hardcode search logic.
Query-family generation belongs to the planner layer.

## Current Integrated Workflow

The current end-to-end flow is:

1. fresh run launcher
2. deterministic baseline retrieval pipeline
3. planner family generation
4. planner family evaluation
5. direct / related family selection
6. direct / related materialization
7. deterministic portfolio routing
8. evidence router advisory pass
9. deterministic report-input enrichment
10. first-pass report generation
11. report critic advisory pass
12. promoted report input
13. final report generation
14. optional Discord delivery

## Deterministic Pipeline

Primary deterministic data-plane scripts:

- `prose_pubmed_search_worker.py`
- `prose_pubmed_normalize_rank.py`
- `prose_pubmed_fulltext_resolver.py`
- `prose_pubmed_fulltext_extract.py`
- `prose_extracted_backfill.py`
- `prose_resolved_reclassify.py`
- `prose_evidence_extract.py`
- `prose_evidence_prepare.py`
- `prose_evidence_label_normalize.py`
- `prose_coverage_review.py`
- `prose_controller.py`
- `prose_retry_runner.py`
- `prose_run_memory.py`
- `prose_run_finalizer.py`

These remain the deterministic backbone.
Advisory sub-agents may guide the pipeline, but should not silently replace the deterministic backbone.

## Sub-Agents

### planner_agent
Role:
- generate topic alternatives
- generate candidate query families
- stay focused on retrieval and query improvement only

Key scripts:
- `prose_planner_candidate_digest.py`
- `prose_planner_runtime_input.py`
- `prose_planner_agent.py`
- `prose_planner_shadow_eval.py`
- `prose_planner_family_eval.py`
- `prose_planner_wrapper.py`

Planner policy:
- operates in **shadow mode**
- may inspect bounded runtime context
- may propose bounded retrieval strategies
- may not execute the pipeline directly
- may not edit code
- may not produce final literature summaries

### evidence_router
Role:
- advisory partitioning of selected evidence into:
  - direct evidence
  - related broader evidence
  - review / context evidence
  - exclude

Key scripts:
- `prose_evidence_router_runtime_input.py`
- `prose_evidence_router_agent.py`
- `prose_evidence_router_compare.py`
- `prose_evidence_router_memory_writeback.py`
- `prose_evidence_router_promote.py`
- `prose_evidence_router_advisory.py`

Router policy:
- currently **advisory**, not authoritative
- can compare against deterministic routing
- can promote bounded additions when helpful
- should not silently override the whole routing layer

### report_critic
Role:
- inspect the first-pass report and digest
- detect weak generic phrasing
- detect missing supported details
- detect likely label / lane issues
- provide structured article-level enrichments for a second-pass report

Key scripts:
- `prose_report_critic_runtime_input.py`
- `prose_report_critic_agent.py`
- `prose_report_critic_promote.py`
- `prose_report_critic_advisory.py`

Critic policy:
- currently **advisory**
- should not retrieve new evidence
- should not invent facts
- should improve the structured report input, then allow the writer to rerender

## Controller Authority

The controller remains the final arbiter of workflow control.

Controller responsibilities:
- decide retry vs stop
- decide current-run vs future-run scope
- decide what is persisted into memory
- decide what is promoted into the active workflow

Advisory sub-agents may inform decisions, but the controller and deterministic workflow still define the operational state.

## Artifact Discipline

Prefer attempt-scoped artifacts over overwriting prior outputs.

Typical artifacts include:
- retrieval records
- ranked records
- resolved records
- extracted records
- evidence records
- coverage report
- controller decision
- planner runtime input
- planner bundle
- planner family eval
- router shadow output
- router compare output
- report critic shadow output
- promoted report inputs
- final markdown report
- digest markdown
- Discord delivery metadata

## Run Memory

Persist only reusable lessons.

Good memory examples:
- repeated retrieval drift patterns
- routing patterns that consistently help
- report generation issues that recur
- durable workflow changes

Do not save:
- one-off transient failures without a lesson
- empty or no-op heartbeat results
- verbose raw logs that do not improve future runs

## If The User Asks For A Run Summary

Prefer summarizing:

- controller decision
- coverage status
- evidence mix
- planner result
- router advisory result
- report critic advisory result
- whether anything was saved to memory
- whether a report was posted to Discord

Do not dump large raw artifacts unless explicitly asked.

## Current Expectations

This workspace should now behave as a staged research system with:

- deterministic retrieval backbone
- planner-guided query refinement
- advisory evidence routing
- advisory report critique
- final report generation and optional Discord delivery

Keep the system modular, inspectable, and reversible.
Prefer small, testable changes over sweeping hidden behavior changes.
\n
