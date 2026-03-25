#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import os
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

try:
    from openai import OpenAI
except Exception:
    print("ERROR: openai package is not installed. Run: python3 -m pip install -U openai", file=sys.stderr)
    raise


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def load_text(path: str) -> str:
    p = Path(path)
    return p.read_text(encoding="utf-8") if p.exists() else ""


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def extract_text_from_response(response) -> str:
    out = getattr(response, "output_text", None)
    if out:
        return out
    try:
        pieces = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", None) == "output_text":
                    txt = getattr(content, "text", None)
                    if txt:
                        pieces.append(txt)
        if pieces:
            return "\n".join(pieces).strip()
    except Exception:
        pass
    return ""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run the report_critic sub-agent in shadow mode.")
    p.add_argument("--runtime-input", required=True)
    p.add_argument("--contract", default=".prose/templates/subagents/report_critic.contract.md")
    p.add_argument("--output-template", default=".prose/templates/subagents/report_critic_output.template.json")
    p.add_argument("--output-schema", default=".prose/templates/subagents/report_critic_output.schema.json")
    p.add_argument("--model", default=os.getenv("PROSE_REPORT_CRITIC_MODEL", "gpt-5-mini"))
    p.add_argument("--model-alias", default=os.getenv("PROSE_REPORT_CRITIC_MODEL_ALIAS", "report_critic_model"))
    p.add_argument("--max-output-tokens", type=int, default=8000)
    p.add_argument("--write", required=True)
    return p


def main() -> int:
    args = build_parser().parse_args()

    runtime = load_json(args.runtime_input)
    contract_md = load_text(args.contract)
    output_template = load_json(args.output_template)
    output_schema = load_json(args.output_schema)

    if not runtime:
        raise SystemExit(f"Could not load runtime input: {args.runtime_input}")
    if not contract_md:
        raise SystemExit(f"Could not load contract: {args.contract}")
    if not output_template:
        raise SystemExit(f"Could not load output template: {args.output_template}")
    if not output_schema:
        raise SystemExit(f"Could not load output schema: {args.output_schema}")

    system_instructions = f"""
You are the report_critic sub-agent for the prose research workflow.

Return one JSON object only.
Do not retrieve papers.
Do not invent facts.
Do not rewrite the whole report.
Do not output markdown.

# Report Critic Contract
{contract_md}

# Additional Rules
- Use the structured candidate article fields as the source of truth.
- Provide `critic_emphasis_points` for the most important article-specific details that should definitely appear in the revised report.
- Provide `suggested_bullets` for supported article-specific bullets when useful.
- Provide `suggested_factual_paragraph` only if it can be directly grounded in the structured fields and kept concise.
- If the article already looks good, use issue type `no_issue`.
- Be conservative about changing buckets.
"""

    input_payload = {
        "runtime_input": runtime,
        "output_template": output_template,
    }

    client = OpenAI()

    response = client.responses.create(
        model=args.model,
        instructions=system_instructions,
        input=json.dumps(input_payload, ensure_ascii=False),
        max_output_tokens=args.max_output_tokens,
        text={
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "report_critic_output",
                "strict": True,
                "schema": output_schema,
            },
        },
    )

    raw = extract_text_from_response(response)
    if not raw:
        raise SystemExit("Model returned empty output_text")

    payload = json.loads(raw)
    payload["template"] = False
    payload["generated_at"] = payload.get("generated_at") or utc_now_iso()
    payload["run_id"] = payload.get("run_id") or runtime.get("run_id")
    payload["topic"] = payload.get("topic") or runtime.get("topic")

    article_critiques = payload.get("article_critiques") or []
    payload["summary"] = {
        "critiqued_article_count": len(article_critiques),
        "articles_with_changes": sum(1 for x in article_critiques if "no_issue" not in (x.get("issue_types") or [])),
        "high_confidence_changes": sum(1 for x in article_critiques if x.get("confidence") == "high" and "no_issue" not in (x.get("issue_types") or [])),
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
