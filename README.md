# OpenProse Psychiatry Research Agent v1.1

Author: Anup Sharma, MD PhD

## Overview

OpenProse Psychiatry Research Agent v1.1 is a personalized research-retrieval and report-generation workflow for psychiatry, neuroscience, artificial intelligence in medicine, and digital health. It combines a deterministic literature pipeline with bounded advisory sub-agents that improve query generation, evidence routing, and report refinement without giving model outputs direct control of the active run.

OpenProse provides the orchestration and sub-agent contract layer. OpenClaw provides chat-facing execution, scheduling, and delivery. The architecture is designed to improve retrieval quality, preserve inspectable intermediate artifacts, and progressively incorporate advisory model outputs into a clinically oriented research workflow.

## Core Capabilities

- deterministic baseline retrieval, ranking, full-text resolution, extraction, and evidence preparation
- planner sub-agent generation of multiple competing query families
- family-level evaluation and selection of direct or broader related retrieval strategies
- deterministic portfolio routing of evidence into report lanes
- advisory evidence-router sub-agent for routing comparison and promotion
- deterministic report-input enrichment before narrative generation
- advisory report-critic sub-agent for structured report refinement
- optional OpenClaw delivery of reports to chat interfaces such as Discord
- support for scheduled multi-query search workflows through OpenClaw cron jobs

## Architecture

The repository is organized under `scripts/*`, with each directory representing a functional layer in the pipeline.

### `scripts/orchestration/`

Coordinates run creation, retries, controller decisions, materialization, memory persistence, and finalization.

Representative scripts:

- `scripts/orchestration/prose_research_start.py`
- `scripts/orchestration/prose_research_run.py`
- `scripts/orchestration/prose_controller.py`
- `scripts/orchestration/prose_retry_runner.py`
- `scripts/orchestration/prose_run_memory.py`
- `scripts/orchestration/prose_run_finalizer.py`
- `scripts/orchestration/prose_materialize_family.py`
- `scripts/orchestration/prose_hybrid_materialize.py`

### `scripts/pipeline/`

Implements the deterministic retrieval and evidence-processing pipeline.

Representative scripts:

- `scripts/pipeline/prose_pubmed_search_worker.py`
- `scripts/pipeline/prose_pubmed_normalize_rank.py`
- `scripts/pipeline/prose_pubmed_fulltext_resolver.py`
- `scripts/pipeline/prose_pubmed_fulltext_extract.py`
- `scripts/pipeline/prose_resolved_reclassify.py`
- `scripts/pipeline/prose_extracted_backfill.py`
- `scripts/pipeline/prose_evidence_extract.py`
- `scripts/pipeline/prose_evidence_prepare.py`
- `scripts/pipeline/prose_evidence_label_normalize.py`
- `scripts/pipeline/prose_coverage_review.py`

### `scripts/planner/`

Implements the query-planning sub-agent and the family-level evaluation path.

Representative scripts:

- `scripts/planner/prose_planner_candidate_digest.py`
- `scripts/planner/prose_planner_runtime_input.py`
- `scripts/planner/prose_planner_agent.py`
- `scripts/planner/prose_planner_shadow_eval.py`
- `scripts/planner/prose_planner_family_eval.py`
- `scripts/planner/prose_planner_wrapper.py`

### `scripts/router/`

Implements the advisory evidence-router sub-agent, which compares model-guided routing against deterministic routing and can propose promotions into the report portfolio.

Representative scripts:

- `scripts/router/prose_evidence_router_runtime_input.py`
- `scripts/router/prose_evidence_router_agent.py`
- `scripts/router/prose_evidence_router_compare.py`
- `scripts/router/prose_evidence_router_memory_writeback.py`
- `scripts/router/prose_evidence_router_promote.py`
- `scripts/router/prose_evidence_router_advisory.py`

### `scripts/reporting/`

Implements deterministic report assembly, portfolio preparation, narrative generation, report-critic advisory logic, and delivery.

Representative scripts:

- `scripts/reporting/prose_run_report.py`
- `scripts/reporting/prose_run_report_input.py`
- `scripts/reporting/prose_report_input_enrich.py`
- `scripts/reporting/prose_portfolio_report_input.py`
- `scripts/reporting/prose_run_report_ai.py`
- `scripts/reporting/prose_post_discord.py`
- `scripts/reporting/prose_report_critic_runtime_input.py`
- `scripts/reporting/prose_report_critic_agent.py`
- `scripts/reporting/prose_report_critic_promote.py`
- `scripts/reporting/prose_report_critic_advisory.py`

### `scripts/diagnostics/`

Contains targeted diagnostics for weak HTML and publisher-access failure modes.

Representative scripts:

- `scripts/diagnostics/prose_html_audit.py`
- `scripts/diagnostics/prose_html_probe.py`

## Active Sub-Agents

The current advisory sub-agent lineup includes three specialized roles.

### `planner_agent`

Generates topic alternatives and multiple competing query families while preserving bounded output structure.

### `evidence_router`

Partitions selected evidence into direct, related broader, review/context, or excluded categories; compares advisory routing against the deterministic baseline; writes routing lessons back into memory; and can promote additional evidence into the reporting portfolio.

### `report_critic`

Critiques report output after initial generation, proposes structured article-level and section-level improvements, and supports a promoted second-pass report build without becoming the sole report authority.

## Retrieval and Selection Model

The workflow does not rely on a single static search strategy. The deterministic baseline establishes the initial reference set for a topic, after which the planner sub-agent generates multiple competing query families for bounded evaluation.

The active planner evaluation path is centered on:

- `scripts/planner/prose_planner_wrapper.py`
- `scripts/planner/prose_planner_family_eval.py`

`scripts/planner/prose_planner_shadow_eval.py` remains part of the lower-level branch evaluation path, but the principal selection logic now operates at the family level. The system can reject all planner proposals, select a single family, or construct a hybrid family merge. Only selected strategies are materialized downstream.

## End-to-End Workflow

The integrated report path follows this sequence:

1. baseline retrieval and deterministic evidence pipeline
2. planner family generation and family-level evaluation
3. selection of best overall family plus best direct and related families
4. deterministic portfolio routing
5. evidence-router advisory pass
6. deterministic report-input enrichment
7. first-pass report generation with `gpt-4o-mini`
8. report-critic advisory pass with `gpt-5-mini`
9. promoted report input
10. second-pass report generation with `gpt-4o-mini`
11. optional Discord posting through OpenClaw

## Runtime Structure

At runtime, each execution is organized under `.prose/runs/<run_id>/`. Although these directories are excluded from version control, they are central to how the workflow operates.

Typical run structure:

```text
.prose/runs/<run_id>/
├── bindings/
│   └── orchestration_plan.json
├── artifacts/
├── cache/
├── fulltext/
└── program.prose
```

The runtime model is artifact-driven. Stages emit inspectable JSON or markdown outputs for retrieval records, ranking, resolved full text, extracted records, evidence portfolios, controller decisions, planner-family evaluation, advisory outputs, and report-delivery sidecars.

## Repository Layout

```text
.
├── .prose/
│   ├── programs/
│   │   └── prose_research.prose
│   └── templates/
│       └── subagents/
├── scripts/
│   ├── orchestration/
│   ├── pipeline/
│   ├── planner/
│   ├── router/
│   ├── reporting/
│   └── diagnostics/
├── README.md
├── DEPENDENCIES.md
├── requirements.txt
└── .env.example
```

## OpenProse Integration

OpenProse provides the orchestration and sub-agent contract layer for the repository. The project includes planner, evidence-router, and report-critic templates under `.prose/templates/subagents/`, including:

- planner templates and schemas
- evidence-router runtime and output templates
- report-critic runtime, contract, and output templates

The Python scripts implement the executable data plane, while OpenProse defines the contracts and bounded advisory interfaces.

## OpenClaw Integration

OpenClaw serves as the chat-facing execution and delivery layer. In this workflow it can be used to:

- initiate or frame research queries from chat
- support scheduled multi-query search workflows through cron jobs
- deliver reports and digests into interfaces such as Discord
- support iterative human review of advisory outputs

## Requirements

- Python 3.10+, tested on Python 3.12
- `openai` Python package
- network access for PubMed, PMC, publisher endpoints, and model APIs
- a PubMed / NCBI API key for operational use of the retrieval pipeline
- an OpenAI API key for planner, evidence-router, report-critic, and report-generation requests
- optional `openclaw` installation for chat delivery workflows

## Configuration

Environment variables are defined in `.env.example`.

Primary variables:

- `OPENAI_API_KEY`
- `PROSE_PLANNER_MODEL`
- `PROSE_PLANNER_MODEL_ALIAS`
- `NCBI_EMAIL`
- `NCBI_API_KEY` or `PUBMED_API_KEY`
- `CONTACT_EMAIL`
- `UNPAYWALL_EMAIL`
- `OPENCLAW_BIN`
- `OPENCLAW_OUTBOUND_DIR`

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Example Execution

Run the integrated workflow:

```bash
python3 scripts/orchestration/prose_research_start.py \
  --topic "esketamine biomarkers major depressive disorder" \
  --execute \
  --materialize-selected \
  --build-report
```

Run with optional Discord delivery:

```bash
python3 scripts/orchestration/prose_research_start.py \
  --topic "esketamine biomarkers major depressive disorder" \
  --execute \
  --materialize-selected \
  --build-report \
  --post-discord \
  --discord-channel-id prose-research
```

## Repository Hygiene

The repository is structured to include reusable source code and stable sub-agent templates while excluding runtime artifacts and environment-specific files.

No API keys or runtime credentials are stored in the repository. Required secrets, including OpenAI and PubMed / NCBI credentials, must be provided through the runtime environment.

Do not commit:

- `.env`
- `.prose/runs/`
- `.prose/memory/`
- generated reports, digests, and delivery artifacts
- captured publisher HTML, XML, or PDF content
- personal operator files or local workspace memory
