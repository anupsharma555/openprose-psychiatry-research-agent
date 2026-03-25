# Prose Research Workspace

This workspace powers the **prose research agent**, its deterministic literature pipeline, the planner sub-agent, the evidence router advisory path, the report critic advisory path, and report / Discord delivery.

## Current organization

Markdown and workspace identity files remain at the root.

Python implementation files are now physically organized under:

- `scripts/orchestration/`
- `scripts/pipeline/`
- `scripts/planner/`
- `scripts/router/`
- `scripts/reporting/`
- `scripts/diagnostics/`

For compatibility, root-level **symlink shims** remain in place for `prose_*.py` so older commands continue to work.

## Why this structure

The current goal is to improve maintainability without destabilizing the working pipeline.

So the workspace now uses:

- **physical organization** in folders
- **compatibility shims** at the root
- a later package-style refactor can happen once the current pipeline is more stable

## Main workflow layers

### 1. Orchestration
Location: `scripts/orchestration/`

Main files:
- `prose_research_start.py`
- `prose_research_run.py`
- `prose_controller.py`
- `prose_retry_runner.py`
- `prose_run_memory.py`
- `prose_run_finalizer.py`
- `prose_materialize_family.py`
- `prose_hybrid_materialize.py`

These scripts launch and coordinate runs, retries, family materialization, finalization, and report generation flow.

### 2. Deterministic retrieval and evidence pipeline
Location: `scripts/pipeline/`

Main files:
- `prose_pubmed_search_worker.py`
- `prose_pubmed_normalize_rank.py`
- `prose_pubmed_fulltext_resolver.py`
- `prose_pubmed_fulltext_extract.py`
- `prose_resolved_reclassify.py`
- `prose_extracted_backfill.py`
- `prose_evidence_extract.py`
- `prose_evidence_prepare.py`
- `prose_evidence_label_normalize.py`
- `prose_coverage_review.py`

These scripts handle retrieval, ranking, resolution, extraction, evidence creation, cleanup, and deterministic enrichment.

### 3. Planner sub-agent
Location: `scripts/planner/`

Main files:
- `prose_planner_candidate_digest.py`
- `prose_planner_runtime_input.py`
- `prose_planner_agent.py`
- `prose_planner_shadow_eval.py`
- `prose_planner_family_eval.py`
- `prose_planner_wrapper.py`

The planner generates topic alternatives and multiple query families, then the deterministic evaluator compares those families and selects:
- a best overall family
- a best direct family
- a best related broader family

### 4. Evidence router advisory path
Location: `scripts/router/`

Main files:
- `prose_evidence_router_runtime_input.py`
- `prose_evidence_router_agent.py`
- `prose_evidence_router_compare.py`
- `prose_evidence_router_memory_writeback.py`
- `prose_evidence_router_promote.py`
- `prose_evidence_router_advisory.py`

The evidence router is an advisory sub-agent that:
- partitions papers into:
  - direct evidence
  - related broader evidence
  - review / context evidence
  - exclude
- compares its routing to the deterministic baseline
- writes routing lessons to memory
- can advisory-promote additional papers into the report input

### 5. Reporting and report critic advisory path
Location: `scripts/reporting/`

Main files:
- `prose_run_report.py`
- `prose_run_report_input.py`
- `prose_report_input_enrich.py`
- `prose_portfolio_report_input.py`
- `prose_run_report_ai.py`
- `prose_post_discord.py`
- `prose_report_critic_runtime_input.py`
- `prose_report_critic_agent.py`
- `prose_report_critic_promote.py`
- `prose_report_critic_advisory.py`

The reporting stack now supports:
- enriched report input
- multi-lane report structure:
  - Direct evidence
  - Related broader evidence
  - Review / context evidence
- first-pass report generation with `gpt-4o-mini`
- advisory critique with `gpt-5-mini`
- promoted report input with structured improvements
- second-pass report generation from promoted input
- Discord delivery

### 6. Diagnostics
Location: `scripts/diagnostics/`

Main files:
- `prose_html_audit.py`
- `prose_html_probe.py`

These scripts are used to inspect weak HTML and publisher-access failure modes.

## Current end-to-end flow

The integrated report path now looks like:

1. baseline retrieval and evidence pipeline
2. planner family generation and evaluation
3. selection of best overall family plus best direct / related families
4. deterministic portfolio routing
5. evidence router advisory pass
6. deterministic report input enrichment
7. first-pass report generation with `gpt-4o-mini`
8. report critic advisory pass with `gpt-5-mini`
9. promoted report input
10. second-pass report generation with `gpt-4o-mini`
11. optional Discord posting

## Sub-agent lineup

Current sub-agents:

### `planner_agent`
Role:
- query planning
- topic alternatives
- multi-family retrieval strategy

### `evidence_router`
Role:
- partition selected evidence into report lanes
- advisory routing only, not yet authoritative

### `report_critic`
Role:
- critique report output
- suggest structured article-level enrichments
- advisory only, not yet authoritative

## Current strengths

The workspace now supports:

- deterministic retrieval backbone
- bounded planner-family search refinement
- direct vs related family selection
- advisory evidence routing
- advisory report critique
- enriched report input
- Discord posting
- memory writeback for router behavior

## Current limitations

Still being refined:

- upstream `paper_kind` / `document_role` labeling
- article-level detail density
- consistent use of critic enrichments by the final writer
- broader topic testing across more psychiatry topics
- eventual package-style path refactor after this folder reorg

## Important note on paths

The **real implementation files** now live under `scripts/*`.

The root-level `prose_*.py` entries are compatibility symlink shims.

This means:
- old commands should still work
- new documentation should refer to the folder structure
- future cleanup can remove root symlinks after the codebase is fully adjusted

## Recommended next steps

1. run 2 to 4 additional psychiatry topics through the updated advisory path
2. compare deterministic routing vs evidence router routing
3. compare first-pass vs critic-improved reports
4. once stable, decide whether to promote the advisory sub-agents to stronger defaults
5. later, replace root symlink shims with a true package/module invocation strategy
