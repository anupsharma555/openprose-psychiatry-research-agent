# OpenProse Psychiatry Research Agent v1.1

Author: Anup Sharma, MD PhD

## Overview

OpenProse Psychiatry Research Agent v1.1 is a custom research-retrieval workflow for psychiatry, neuroscience, artificial intelligence in medicine, and digital health. The system combines a deterministic literature pipeline with an LLM-guided planner sub-agent that proposes alternative query families for evaluation.

The primary design objective is to improve research retrieval quality for psychiatric research use cases while maintaining deterministic control over how model-generated suggestions are incorporated into an active run.

OpenProse provides the orchestration and planner-contract framework for the workflow. OpenClaw provides the chat-facing interface for query initiation, scheduled execution, and report delivery.

## Core Capabilities

- deterministic baseline retrieval, ranking, full-text resolution, extraction, and evidence generation
- planner sub-agent generation of multiple competing query families
- bounded shadow evaluation of planner proposals before any selected change is materialized
- controller-based stop or retry decisions with reusable future-run patch candidates
- hybrid materialization of selected query families into downstream evidence artifacts
- structured report generation with optional OpenClaw delivery to chat interfaces such as Discord
- support for scheduled multi-query search workflows through OpenClaw cron jobs

## Design Principles

- deterministic execution before model-guided adaptation
- bounded sub-agent scope rather than unconstrained workflow mutation
- explicit controller decisions for stop, retry, or future-run learning
- artifact-based workflow state so each stage produces inspectable intermediate outputs
- retrieval diversification through competing query families rather than a single static search

## System Architecture

The workflow is organized around three cooperating layers.

### Main Research Agent

The main prose-research agent is responsible for run creation, deterministic literature processing, retry execution, run finalization, and report production.

### Planner Sub-Agent

The planner sub-agent analyzes the current topic, coverage profile, controller state, and run memory to generate multiple competing retrieval strategies. These strategies are expressed as bounded query-family proposals rather than direct modifications to the active run.

### Controller and Evaluation Layer

All planner proposals are evaluated through deterministic shadow branches. The system can reject all proposals, select a single family, or construct a hybrid family merge, and only selected strategies are materialized downstream.

## Retrieval Model

The repository is designed to supplement psychiatric research retrieval by allowing an agent and a planner sub-agent to generate, compare, and evaluate competing search strategies for research report generation. This avoids reliance on a single static query and supports broader but controlled evidence discovery.

## End-to-End Workflow

1. Create a run and generate an orchestration plan.
2. Execute baseline PubMed retrieval and ranking.
3. Resolve accessible full text and extract structured content.
4. Convert extracted content into evidence records and review coverage.
5. Use the controller to determine whether the run should stop or retry.
6. Generate planner runtime input and candidate query families.
7. Evaluate each candidate family in a deterministic shadow branch.
8. Select a single family, build a hybrid merge, or reject all planner proposals.
9. Materialize the selected strategy into downstream evidence artifacts when appropriate.
10. Generate report outputs and optionally deliver them through OpenClaw.

## How Query Competition Works

The baseline retrieval path establishes the reference set for a topic. After that baseline has been ranked, resolved, extracted, and reviewed, the planner sub-agent generates multiple candidate query families rather than a single replacement query.

Each family is evaluated in its own shadow branch. The evaluation process measures the resulting candidate set against the baseline using branch-level metrics such as primary-study coverage, review pressure, tier-1 journal representation, and full-text yield. The system can then:

- reject all planner candidates
- promote a single family
- construct a hybrid merge from multiple acceptable families

This design allows the repository to use model assistance for retrieval diversification without giving the model direct control over the evidence pipeline.

## Run Directory Model

At runtime, each execution is organized under `.prose/runs/<run_id>/`. Although this directory is excluded from version control, it is central to understanding how the repository functions.

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

The major runtime directories have distinct roles:

- `bindings/`: orchestration inputs and run-scoped configuration
- `artifacts/`: JSON and markdown outputs produced by each pipeline stage
- `cache/`: reusable resolver and planner-support outputs
- `fulltext/`: downloaded or resolved full-text material used for extraction
- `program.prose`: run-specific snapshot of the OpenProse program

This structure allows a run to be audited stage by stage and supports reproducibility, debugging, and post hoc review of retrieval decisions.

## Repository Layout

```text
.
├── .prose/
│   ├── programs/
│   │   └── prose_research.prose
│   └── templates/
│       └── subagents/
├── prose_research_start.py
├── prose_research_run.py
├── prose_controller.py
├── prose_retry_runner.py
├── prose_run_finalizer.py
├── prose_run_memory.py
├── prose_pubmed_search_worker.py
├── prose_pubmed_normalize_rank.py
├── prose_pubmed_fulltext_resolver.py
├── prose_pubmed_fulltext_extract.py
├── prose_resolved_reclassify.py
├── prose_extracted_backfill.py
├── prose_evidence_extract.py
├── prose_coverage_review.py
├── prose_planner_candidate_digest.py
├── prose_planner_runtime_input.py
├── prose_planner_agent.py
├── prose_planner_shadow_eval.py
├── prose_planner_family_eval.py
├── prose_planner_wrapper.py
├── prose_hybrid_materialize.py
├── prose_run_report_input.py
├── prose_run_report_ai.py
└── prose_post_discord.py
```

## Key Components

### Orchestration

- `prose_research_start.py`: creates a run, snapshots the OpenProse program, and writes the orchestration plan
- `prose_research_run.py`: orchestrates baseline execution, retry handling, planner flow, materialization, report generation, and optional delivery

### Deterministic Literature Pipeline

- `prose_pubmed_search_worker.py`: PubMed retrieval
- `prose_pubmed_normalize_rank.py`: ranking and normalization
- `prose_pubmed_fulltext_resolver.py`: full-text resolution across PMC, publisher, and open-access routes
- `prose_pubmed_fulltext_extract.py`: structural extraction from resolved content
- `prose_html_audit.py`: inspection of weak or low-substance HTML outputs
- `prose_html_probe.py`: targeted HTML diagnostics for resolver and extraction review
- `prose_resolved_reclassify.py`: reclassification of weak full-text signals
- `prose_extracted_backfill.py`: abstract and metadata backfill
- `prose_evidence_extract.py`: evidence object construction
- `prose_coverage_review.py`: coverage quality assessment
- `prose_controller.py`: bounded retry or stop decisions
- `prose_retry_runner.py`: retry execution
- `prose_run_finalizer.py`: stop-path finalization
- `prose_run_memory.py`: future-run memory persistence

### Planner and Query-Family Evaluation

- `prose_planner_candidate_digest.py`: compressed candidate context for the planner
- `prose_planner_runtime_input.py`: planner runtime state assembly from topic, controller, coverage, and memory
- `prose_planner_agent.py`: LLM-based planner sub-agent for competing query-family generation
- `prose_planner_wrapper.py`: primary planner entrypoint coordinating digest creation, runtime-input generation, planner execution, and family evaluation
- `prose_planner_family_eval.py`: primary family-level evaluation, selection, and hybrid merge logic for the active planner flow
- `prose_planner_shadow_eval.py`: lower-level deterministic evaluation of a single planner branch used within the broader family-evaluation path

### Runtime Outputs

The repository is designed around stage-specific artifacts rather than opaque in-memory execution. In practice, this means the workflow can emit:

- retrieval records
- ranked records
- resolved records
- extracted records
- evidence records
- coverage reports
- controller decisions
- planner candidate digests
- planner runtime inputs
- planner query bundles and shadow-evaluation outputs
- report inputs, markdown reports, and delivery sidecars

This artifact model makes it possible to inspect the exact transition from search results to report-ready evidence.

### Materialization and Reporting

- `prose_hybrid_materialize.py`: selected-family or hybrid materialization into downstream evidence artifacts
- `prose_run_report_input.py`: structured report-input assembly
- `prose_run_report_ai.py`: markdown report and digest generation
- `prose_post_discord.py`: OpenClaw-based delivery workflow

## OpenProse Integration

OpenProse provides the orchestration and planner-contract layer for the project. The repository includes:

- `.prose/programs/prose_research.prose`
- `.prose/templates/subagents/planner_subagent.contract.md`
- `.prose/templates/subagents/planner_runtime_input.template.json`
- `.prose/templates/subagents/planner_query_bundle.template.json`
- `.prose/templates/subagents/planner_query_bundle.schema.json`
- `.prose/templates/subagents/planner_query_bundle.acceptance_spec.json`

The active data-plane execution remains in deterministic Python scripts.

## OpenClaw Integration

OpenClaw serves as the chat and delivery interface for the workflow. It can be used to:

- initiate or frame research queries from a chat surface
- return finished digests and report artifacts to the user
- deliver reports into chat interfaces such as Discord
- automate recurring multi-query search workflows through OpenClaw cron jobs

## Requirements

- Python 3.10+, tested on Python 3.12
- `openai` Python package
- network access for PubMed, PMC, publisher endpoints, and model APIs
- a PubMed / NCBI API key for continued operational use of the retrieval pipeline
- an OpenAI API key for planner and report-generation requests
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
python3 prose_research_start.py \
  --topic "esketamine biomarkers major depressive disorder" \
  --execute \
  --materialize-selected \
  --build-report
```

Run with optional Discord delivery:

```bash
python3 prose_research_start.py \
  --topic "esketamine biomarkers major depressive disorder" \
  --execute \
  --materialize-selected \
  --build-report \
  --post-discord \
  --discord-channel-id prose-research
```

## Version

This repository documents version 1.1 of the workflow.

## Repository Hygiene

The repository is structured to include reusable source code and stable templates while excluding runtime artifacts and environment-specific files.

No API keys or runtime credentials are stored in the repository. Required secrets, including OpenAI and PubMed / NCBI credentials, must be provided through the runtime environment.

Do not commit:

- `.env`
- `.prose/runs/`
- `.prose/memory/`
- generated reports, digests, and delivery artifacts
- captured publisher HTML, XML, or PDF content
- personal operator files or local workspace memory
