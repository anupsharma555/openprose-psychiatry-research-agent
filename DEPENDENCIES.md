# Dependencies

## Python

The repository currently requires the following Python package:

- `openai`

Install with:

```bash
python3 -m pip install -r requirements.txt
```

## Standard Library Usage

Most of the workflow relies on the Python standard library, including:

- `argparse`
- `datetime`
- `json`
- `os`
- `pathlib`
- `re`
- `shlex`
- `shutil`
- `subprocess`
- `sys`
- `time`
- `typing`
- `urllib`
- `xml.etree.ElementTree`

## External Services

The workflow depends on external services for retrieval and model execution:

- PubMed / NCBI E-utilities
- PubMed Central
- publisher and open-access endpoints used during full-text resolution
- OpenAI API for planner, evidence-router, report-critic, and report-generation stages

## Integration Components

- `OpenProse`: orchestration and planner-contract layer used to structure runs, sub-agent templates, and workflow context
- `OpenClaw`: chat and delivery layer used for user-facing query initiation, scheduled workflows, and report delivery

## Repository Structure

The implementation is organized under:

- `scripts/orchestration/`
- `scripts/pipeline/`
- `scripts/planner/`
- `scripts/router/`
- `scripts/reporting/`
- `scripts/diagnostics/`

Root-level `prose_*.py` entries are compatibility shims pointing to the corresponding implementation files under `scripts/*`.

## Credentials and Runtime Configuration

The following environment variables are relevant for operational use:

- `OPENAI_API_KEY`
- `PROSE_PLANNER_MODEL`
- `PROSE_PLANNER_MODEL_ALIAS`
- `NCBI_EMAIL`
- `NCBI_API_KEY` or `PUBMED_API_KEY`
- `CONTACT_EMAIL`
- `UNPAYWALL_EMAIL`
- `OPENCLAW_BIN`
- `OPENCLAW_OUTBOUND_DIR`

## Optional System Dependencies

The following tools are optional but supported:

- `openclaw` for chat delivery and scheduled workflows
- OpenClaw cron jobs for automated recurring multi-query searches

## Notes

- An OpenAI API key is required for planner execution, evidence-router and report-critic advisory paths, and report-generation requests.
- A PubMed / NCBI API key should be configured for continued use of the retrieval pipeline.
- No credentials are stored in this repository. Runtime secrets should be provided through the environment.
