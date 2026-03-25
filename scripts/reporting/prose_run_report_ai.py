#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

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
- Prefer omission over generic fallback prose.
- If key metadata or study details are missing, omit them rather than compensating with general summary language.
- When bullet_candidates are present, use them as the primary source for article-specific bullets before falling back to generic synthesis.

Output requirements:
1. Return valid JSON only with keys:
   - report_md
   - discord_digest_md

2. report_md:
   - Markdown
   - Include a short run summary header
   - Include a 6 to 10 sentence synthesis paragraph
   - If the input contains `direct_evidence_articles`, render a section titled exactly `Direct evidence`
   - If the input contains `related_evidence_articles`, render a section titled exactly `Related broader evidence`
   - If the input contains `review_context_articles`, render a section titled exactly `Review / context evidence`
   - Do not invent additional section headings beyond the section names present in the input
   - Do not create headings like `Additional context evidence` or other substitutes
   - Prefer placing strict topic-preserving empirical studies in `Direct evidence`
   - Prefer placing broader supporting empirical studies in `Related broader evidence`
   - Prefer placing review-like, systematic review, or context-framing papers in `Review / context evidence`
   - For each article in each section:
     - show title as a section header
     - then include a metadata block with one line each for:
       - Journal
       - Online date, if `online_date_display` is available
       - Publication or issue date, if `issue_date_display` is available and different from the online date
       - Authors, when available
       - Links, preferring PubMed, then PMC, then DOI
     - if authors are missing, omit the author line rather than writing "Not specified"
     - if first_author and last_author are available, use them when the full author list is short or incomplete
     - if dates are missing, omit them rather than inventing them
     - place a blank line between the metadata block and the narrative paragraph
     - use the provided study type / document role / study_design exactly; do not upgrade or reinterpret them
     - use structured evidence fields aggressively before writing any prose, in this order:
       1. critic_factual_paragraph
       2. critic_emphasis_bullets
       3. bullet_candidates
       4. most_salient_findings
       5. outcomes
       6. metrics
       7. limitations
       8. discussion_significance
     - if the article has enough concrete evidence, write a richer article paragraph of 8 to 12 sentences grounded only in extracted evidence
     - if the article does not have enough concrete evidence for a strong paragraph, do not write a generic fallback paragraph
     - in sparse cases, write at most 1 to 2 short factual sentences describing only what is explicitly supported, then rely on detailed bullets
     - do not use generic fallback language such as "more research is needed", "future studies should", "the findings should be interpreted with caution", or similar unless that limitation is directly supported by the extracted evidence
     - when `critic_emphasis_bullets` are present, treat them as high-priority supported details and surface most of them unless clearly redundant
     - if `critic_factual_paragraph` is present, use it as the narrative anchor for that article and preserve its factual content unless clearly redundant
     - if `critic_emphasis_bullets` are present, surface most of them unless truly repetitive
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
   - if both direct and related evidence sections exist, prioritize direct evidence in the digest and mention related broader evidence briefly
   - if review/context evidence exists, mention it only briefly as field framing or context, not as the main proof layer
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
