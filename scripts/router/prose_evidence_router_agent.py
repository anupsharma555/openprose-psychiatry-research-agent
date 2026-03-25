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


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_text(path: str) -> str:
    p = Path(path)
    return p.read_text(encoding="utf-8") if p.exists() else ""


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


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
    p = argparse.ArgumentParser(description="Run the evidence_router sub-agent in shadow mode.")
    p.add_argument("--runtime-input", required=True)
    p.add_argument("--contract", default=".prose/templates/subagents/evidence_router.contract.md")
    p.add_argument("--output-template", default=".prose/templates/subagents/evidence_router_output.template.json")
    p.add_argument("--output-schema", default=".prose/templates/subagents/evidence_router_output.schema.json")
    p.add_argument("--model", default=os.getenv("PROSE_EVIDENCE_ROUTER_MODEL", "gpt-5-mini"))
    p.add_argument("--model-alias", default=os.getenv("PROSE_EVIDENCE_ROUTER_MODEL_ALIAS", "evidence_router_model"))
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
You are the evidence_router sub-agent for the prose research workflow.

Return one JSON object only.
Do not retrieve new papers.
Do not rewrite study designs.
Do not generate final prose.
Do not output markdown.

# Evidence Router Contract
{contract_md}

# Additional Rules
- Use only the candidate records provided in the runtime input.
- Classify each candidate into exactly one bucket:
  - direct_evidence
  - related_broader_evidence
  - review_context_evidence
  - exclude
- Be conservative about direct_evidence.
- Be explicit but concise in rationale.
- Use the structured fields as the source of truth.
- Prefer review_context_evidence for clearly review-like or field-framing papers.
- If uncertain, use related_broader_evidence rather than direct_evidence.
- If clearly off-topic, use exclude.
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
                "name": "evidence_router_output",
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

    # recompute summary deterministically
    counts = {
        "direct_count": 0,
        "related_count": 0,
        "review_count": 0,
        "exclude_count": 0,
        "high_confidence_count": 0,
    }
    for rec in payload.get("routed_records", []):
        bucket = rec.get("suggested_bucket")
        conf = rec.get("bucket_confidence")
        if bucket == "direct_evidence":
            counts["direct_count"] += 1
        elif bucket == "related_broader_evidence":
            counts["related_count"] += 1
        elif bucket == "review_context_evidence":
            counts["review_count"] += 1
        elif bucket == "exclude":
            counts["exclude_count"] += 1
        if conf == "high":
            counts["high_confidence_count"] += 1
    payload["summary"] = counts

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
