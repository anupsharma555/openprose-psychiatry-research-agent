#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from openai import OpenAI


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LLM-based prose research report writer using structured run report input.")
    p.add_argument("--report-input", required=True, help="Structured run report input JSON")
    p.add_argument("--model", default="gpt-4o-mini", help="Report model")
    p.add_argument("--discord-channel-id", default="", help="Optional channel id for delivery sidecar")
    p.add_argument("--message", default="", help="Optional message text for delivery sidecar")
    p.add_argument("--delivery-json", default="", help="Optional delivery sidecar output path")
    p.add_argument("--write-report-md", required=True, help="Output markdown report path")
    p.add_argument("--write-digest-md", required=True, help="Output discord digest markdown path")
    p.add_argument("--dry-run", action="store_true", help="Write prompt preview only")
    return p


def main() -> int:
    args = build_parser().parse_args()

    report_input = load_json(args.report_input)
    if not report_input:
        raise SystemExit(f"Could not load report input: {args.report_input}")

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "report_md": {"type": "string"},
            "discord_digest_md": {"type": "string"}
        },
        "required": ["report_md", "discord_digest_md"]
    }

    system_instructions = """
You are writing a prose research run report.

Use only the provided structured report input.
Do not use outside knowledge.
Do not invent missing details.
If a field is missing, omit it rather than filling with generic prose.

Fidelity rules:
- Treat the structured fields as the source of truth.
- Do not relabel or reinterpret study design.
- Use `paper_kind`, `document_role`, and `study_design` exactly as provided.
- If the structured fields say randomized_trial, cohort_study, observational, target_trial_emulation, open_label_trial, case_series, review_like, systematic_review, or meta_analysis, preserve that exact label family in the prose.
- Do not call a paper a systematic review, narrative review, or meta-analysis unless the structured fields explicitly support that.
- Do not call a paper a randomized trial unless the structured fields explicitly support that.
- If findings are exploratory, subgroup-based, biomarker-based, or observational, say so.
- Do not convert exploratory associations into definitive conclusions.
- If sample size is small, missing, or the study is retrospective/observational, mention that limitation explicitly.
- If suicidality and depressive symptoms are different outcomes, do not blur them together.
- Prefer exactness over smoothness.
- Before deciding that article detail is sparse, use all available structured evidence fields in this order:
  1. `most_salient_findings`
  2. `discussion_significance`
  3. `metrics`
  4. `outcomes`
  5. `limitations`
- For abstract-only or abstract-dominant papers, you may still produce a richer paragraph and multiple detailed bullets if the abstract clearly supports them.
- Do not be artificially brief when the structured fields already contain enough article-specific detail.
- If a study design or result is uncertain, say it is uncertain rather than smoothing it into a stronger label.

Output requirements:
1. Return valid JSON only with keys:
   - report_md
   - discord_digest_md

2. report_md:
   - Markdown
   - Include a short run summary header
   - Include a 6 to 10 sentence synthesis paragraph
   - For each included article:
     - show title as a section header
     - then include a metadata block with one line each for:
       - Journal
       - Online date, if `online_date_display` is available
       - Publication or issue date, if `issue_date_display` is available and different from the online date
       - Authors, when available
       - Links, preferring PubMed, then PMC, then DOI
     - if authors are missing, omit the author line rather than writing "Not specified"
     - if dates are missing, omit them rather than inventing them
     - place a blank line between the metadata block and the narrative paragraph
     - use the provided study type / document role / study_design exactly; do not upgrade or reinterpret them
     - write a richer article paragraph of 8 to 12 sentences grounded only in extracted evidence
     - the paragraph should explain the study design, population, intervention or comparison, main outcomes, direction of effect, and the most important caveats when supported
     - then include detailed findings bullets only
     - use a soft minimum of 5 bullets when the evidence supports it
     - target 8 to 12 bullets when enough supported content exists
     - hard maximum 15 bullets
     - the bullets should contain substantive article findings, quantitative details, limitations, outcomes, and why it matters when supported
     - do not use bullets for metadata like links, journal, authors, or dates
     - do not invent filler bullets just to hit a count target; if the evidence is thin, prefer fewer but specific bullets
   - Prefer "Most salient findings" rather than singular wording
   - Use discussion_significance only if present
   - Mention important limitations when supported, especially sample size, retrospective design, exploratory biomarker analyses, or subgroup findings
   - Do not include generic filler

3. discord_digest_md:
   - concise but informative
   - one short paragraph max
   - then 4 to 6 article-aware bullets
   - each bullet should refer to a specific included article or study profile, for example with journal name, publication date, study type, or title fragment
   - if dates are available, prefer online date for recency context
   - avoid generic bullets that are not tied to a specific included article
   - do not mislabel study design in the digest
   - do not upgrade observational or exploratory findings into stronger claims
   - highlight only the strongest points
   - do not include raw links in the digest

4. Omit clearly off-topic articles if present in the input.

Formatting example for one article block, use this as a structural guide only and do not copy wording:

## <Article Title>
**Journal:** <Journal Name>  
**Online date:** <MM/DD/YYYY if available>  
**Publication date:** <MM/DD/YYYY or MM/YYYY if different>  
**Authors:** <First Author>, ..., <Last Author>  
**Links:** [PubMed](<pubmed_url>) | [PMC](<pmc_url if available>) | [DOI](<doi_url if available>)

<Write an 8 to 12 sentence paragraph that explains the study design, population, intervention or comparator, main outcomes, direction of effect, and the main caveats. Stay faithful to the structured evidence.>

- <Detailed finding 1, preferably endpoint or main result>
- <Detailed finding 2, preferably quantitative if available>
- <Detailed finding 3, preferably outcome or remission/response information>
- <Detailed finding 4, preferably caveat or limitation>
- <Detailed finding 5, preferably why it matters when supported>
- <Add more detailed findings when supported, aiming for 8 to 12 bullets and never exceeding 15>

Important style rules for article blocks:
- Metadata should stay above the paragraph.
- Links should stay in the metadata block, not in findings bullets.
- Findings bullets should be detailed, article-specific, and concrete.
- Do not collapse multiple distinct findings into one vague bullet if the structured evidence supports more detail.
"""

    if args.dry_run:
        preview = {
            "system_instructions": system_instructions,
            "input_payload": report_input,
            "model": args.model,
            "output_paths": {
                "report_md": args.write_report_md,
                "digest_md": args.write_digest_md,
                "delivery_json": args.delivery_json
            }
        }
        preview_path = args.write_report_md + ".prompt_preview.json"
        ensure_parent_dir(preview_path)
        Path(preview_path).write_text(json.dumps(preview, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps(preview, indent=2, ensure_ascii=False))
        return 0

    client = OpenAI()

    request_kwargs = {
        "model": args.model,
        "instructions": system_instructions,
        "input": json.dumps(report_input, ensure_ascii=False),
        "max_output_tokens": 6500,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "prose_run_report",
                "strict": True,
                "schema": schema,
            },
        },
    }

    if not args.model.lower().startswith("gpt-5"):
        request_kwargs["temperature"] = 0.2

    response = client.responses.create(**request_kwargs)
    raw = response.output_text
    if not raw:
        raise SystemExit("Empty model output")

    data = json.loads(raw)
    report_md = data["report_md"]
    digest_md = data["discord_digest_md"]

    report_path = Path(args.write_report_md).expanduser().resolve()
    digest_path = Path(args.write_digest_md).expanduser().resolve()

    ensure_parent_dir(str(report_path))
    ensure_parent_dir(str(digest_path))
    report_path.write_text(report_md.rstrip() + "\n", encoding="utf-8")
    digest_path.write_text(digest_md.rstrip() + "\n", encoding="utf-8")

    if args.delivery_json:
        ensure_parent_dir(args.delivery_json)
        message = compact_ws(args.message) or f"Prose Research Run: {report_input.get('topic')}"
        delivery = {
            "schema_version": "1.0",
            "artifact_type": "discord_delivery",
            "workflow": "prose_research",
            "generated_at": utc_now_iso(),
            "run_id": report_input.get("run_id"),
            "lane": report_input.get("lane"),
            "topic": report_input.get("topic"),
            "discord_channel_id": compact_ws(args.discord_channel_id) or None,
            "message": message,
            "report_md": str(report_path),
            "digest_md": str(digest_path),
            "staged_report_md": None
        }
        Path(args.delivery_json).write_text(json.dumps(delivery, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps({
        "report_md": str(report_path),
        "discord_digest_md": str(digest_path),
        "delivery_json": args.delivery_json or None
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
