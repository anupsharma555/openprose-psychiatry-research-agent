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
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8")


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def infer_tag(path: str) -> str:
    name = Path(path).name
    prefixes = [
        "planner_runtime_input.",
        "controller_decision.",
        "coverage_report.",
    ]
    for prefix in prefixes:
        if name.startswith(prefix) and name.endswith(".json"):
            return name[len(prefix):-5]
    return "latest"


def extract_text_from_response(response) -> str:
    out = getattr(response, "output_text", None)
    if out:
        return out

    try:
        pieces = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                ctype = getattr(content, "type", None)
                if ctype == "output_text":
                    txt = getattr(content, "text", None)
                    if txt:
                        pieces.append(txt)
        if pieces:
            return "\n".join(pieces).strip()
    except Exception:
        pass

    return ""


def json_safe(obj):
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(x) for x in obj]
    try:
        return obj.model_dump(mode="json")
    except Exception:
        try:
            return str(obj)
        except Exception:
            return repr(obj)


def response_debug_payload(response) -> dict:
    payload = {
        "id": getattr(response, "id", None),
        "status": getattr(response, "status", None),
        "incomplete_details": json_safe(getattr(response, "incomplete_details", None)),
        "error": json_safe(getattr(response, "error", None)),
    }
    try:
        payload["model_dump"] = json_safe(response.model_dump())
    except Exception:
        payload["model_dump"] = None
    return payload


def extract_json_object(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return raw[start:end + 1]


def write_debug_artifacts(out_path: str, raw: str, response, message: str, recovered_attempted: bool) -> tuple[str, str]:
    raw_path = out_path.replace(".json", ".raw.txt")
    debug_path = out_path.replace(".json", ".debug.json")

    ensure_parent_dir(raw_path)
    Path(raw_path).write_text(raw or "", encoding="utf-8")

    debug_payload = {
        "error": "json_decode_error",
        "message": message,
        "raw_path": raw_path,
        "recovered_attempted": recovered_attempted,
        "response_debug": response_debug_payload(response),
        "raw_preview": (raw or "")[:4000],
    }
    Path(debug_path).write_text(
        json.dumps(json_safe(debug_payload), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return raw_path, debug_path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LLM-backed planner sub-agent that produces planner_query_patch.shadow.<tag>.json from runtime input.")
    p.add_argument("--runtime-input", required=True, help="planner_runtime_input.<tag>.json")
    p.add_argument("--contract", default=".prose/templates/subagents/planner_subagent.contract.md", help="Planner contract markdown")
    p.add_argument("--patch-template", default=".prose/templates/subagents/planner_query_bundle.template.json", help="Planner patch template JSON")
    p.add_argument("--patch-schema", default=".prose/templates/subagents/planner_query_bundle.schema.json", help="Planner patch structured output schema JSON")
    p.add_argument("--acceptance-spec", default=".prose/templates/subagents/planner_query_bundle.acceptance_spec.json", help="Planner acceptance spec JSON")
    p.add_argument("--write", default="", help="Optional output path override")
    p.add_argument("--dry-run", action="store_true", help="Write prompt preview only, do not call the model")
    p.add_argument("--model", default=os.getenv("PROSE_PLANNER_MODEL", "gpt-5-mini"), help="Planner model, default from PROSE_PLANNER_MODEL or gpt-5-mini")
    p.add_argument("--model-alias", default=os.getenv("PROSE_PLANNER_MODEL_ALIAS", "planner_subagent_model"), help="Logical runtime alias for provenance")
    p.add_argument("--temperature", type=float, default=0.2, help="Sampling temperature")
    p.add_argument("--max-output-tokens", type=int, default=10000, help="Max output tokens")
    return p


def main() -> int:
    args = build_parser().parse_args()

    runtime = load_json(args.runtime_input)
    if not runtime:
        raise SystemExit(f"Could not load runtime input: {args.runtime_input}")

    contract_md = load_text(args.contract)
    patch_template = load_json(args.patch_template)
    patch_schema = load_json(args.patch_schema)
    acceptance_spec = load_json(args.acceptance_spec)

    if not contract_md:
        raise SystemExit(f"Could not load contract: {args.contract}")
    if not patch_template:
        raise SystemExit(f"Could not load patch template: {args.patch_template}")
    if not patch_schema:
        raise SystemExit(f"Could not load patch schema: {args.patch_schema}")
    if not acceptance_spec:
        raise SystemExit(f"Could not load acceptance spec: {args.acceptance_spec}")

    digest_path = ((runtime.get("artifact_paths") or {}).get("ranked_candidate_digest")) or ""
    digest = load_json(digest_path) if digest_path else {}

    tag = infer_tag(args.runtime_input)
    out_path = args.write or f".prose/runs/{runtime.get('run_id')}/artifacts/planner_query_patch.shadow.{tag}.json"

    system_instructions = f"""
You are the planner sub-agent for the prose research agent.

Return one JSON object only.
Do not execute pipeline steps.
Do not summarize literature.
Do not output commentary outside the JSON artifact.

# Planner Contract
{contract_md}

# Runtime Planning Rules
- Use the runtime input, candidate digest, and concept policy as the source of truth.
- Stay focused on retrieval and query improvement only.
- Usually propose 2 to 3 topic alternatives when the topic allows it.
- Usually propose 2 to 4 candidate query families unless the topic is unusually narrow.
- At least one, and preferably two, topic alternatives should preserve all must-have concepts.
- If the user explicitly asked for esketamine, do not let all families drift to ketamine-only evidence.
- If the user explicitly asked for biomarkers or predictors, do not let all families drop those concepts.
- Every candidate family must include a real non-empty backend_queries.pubmed_query.
- pubmed_query should use explicit Boolean grouping, quoted phrases when appropriate, OR within synonym groups, AND between concept groups, and [tiab] when appropriate.
- Use query_changes only as supporting metadata, not as the main executable search definition when a proper backend query can be written.
- Candidate families should differ meaningfully in retrieval strategy.
- Populate retained_must_have_concepts, dropped_must_have_concepts, optional_concepts_used, broadening_concepts_used, and concept_coverage_note honestly.- For broad families, you may create one comparison family without exclusions and one comparison family with bounded exclusions when repeated drift patterns justify it.
- Do not use exclusions in every family.
- Preserve at least one broad family without exclusions.
- Prefer phrase-level exclusions over broad single-word exclusions.
- Populate `negative_terms_used`, `negative_phrases_used`, and `exclusion_rationale` whenever exclusions are used.

- Preserve existing terms unless there is a clear bounded reason not to.
- If no major change is justified, still return a valid minimal bundle artifact with conservative candidates.
"""

    input_payload = {
        "runtime_input": runtime,
        "patch_template": patch_template,
        "acceptance_spec_summary": {
            "required_fields": acceptance_spec.get("required_fields"),
            "required_based_on_fields": acceptance_spec.get("required_based_on_fields"),
            "allowed_values": acceptance_spec.get("allowed_values"),
            "limits": acceptance_spec.get("limits"),
            "notes": acceptance_spec.get("notes"),
        },
        "ranked_candidate_digest": digest,
    }

    if args.dry_run:
        preview = {
            "system_instructions": system_instructions,
            "input_payload": input_payload,
            "output_path": out_path,
            "model": args.model,
            "model_alias": args.model_alias,
        }
        ensure_parent_dir(out_path)
        Path(out_path.replace(".json", ".prompt_preview.json")).write_text(
            json.dumps(preview, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(preview, indent=2, ensure_ascii=False))
        return 0

    client = OpenAI()

    request_kwargs = {
        "model": args.model,
        "instructions": system_instructions,
        "input": json.dumps(input_payload, ensure_ascii=False),
        "max_output_tokens": args.max_output_tokens,
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": "planner_query_bundle",
                "strict": True,
                "schema": patch_schema,
            },
        },
    }

    model_low = (args.model or "").lower()
    if not model_low.startswith("gpt-5"):
        request_kwargs["temperature"] = args.temperature

    response = client.responses.create(**request_kwargs)

    raw = extract_text_from_response(response)
    if not raw:
        debug_path = out_path.replace(".json", ".debug.json")
        ensure_parent_dir(debug_path)
        Path(debug_path).write_text(
            json.dumps(json_safe(response_debug_payload(response)), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        raise SystemExit(f"Model returned empty output_text, wrote debug artifact: {debug_path}")

    try:
        patch = json.loads(raw)
    except json.JSONDecodeError as e:
        recovered = extract_json_object(raw)
        if recovered:
            try:
                patch = json.loads(recovered)
            except json.JSONDecodeError:
                raw_path, debug_path = write_debug_artifacts(
                    out_path=out_path,
                    raw=raw,
                    response=response,
                    message=str(e),
                    recovered_attempted=True,
                )
                raise SystemExit(f"Model returned malformed JSON, wrote raw/debug artifacts: {raw_path} | {debug_path}")
        else:
            raw_path, debug_path = write_debug_artifacts(
                out_path=out_path,
                raw=raw,
                response=response,
                message=str(e),
                recovered_attempted=False,
            )
            raise SystemExit(f"Model returned malformed JSON, wrote raw/debug artifacts: {raw_path} | {debug_path}")

    # lightweight validation of topic alternatives and pubmed_query content
    topic_alts = patch.get("topic_alternatives") or []
    if len(topic_alts) < 2:
        raise SystemExit("Planner returned fewer than 2 topic alternatives")

    for cand in patch.get("candidate_queries", []):
        q = compact_ws(((cand.get("backend_queries") or {}).get("pubmed_query")))
        if len(q) < 25 or q in {"(", ")"}:
            raise SystemExit(f"Planner returned invalid pubmed_query for family {cand.get('family_id')}: {q!r}")

    patch["template"] = False
    patch["generated_at"] = patch.get("generated_at") or utc_now_iso()
    patch["run_id"] = patch.get("run_id") or runtime.get("run_id")
    patch["lane"] = patch.get("lane") or runtime.get("lane")

    patch.setdefault("subagent_runtime", {})
    patch["subagent_runtime"]["model_slot"] = args.model_alias
    patch["subagent_runtime"]["resolved_model"] = args.model
    patch["subagent_runtime"]["resolved_alias"] = args.model_alias
    patch["subagent_runtime"]["filled_by_runtime"] = True
    patch["subagent_runtime"]["notes"] = "Optional provenance only. Controller should not rely on these fields."

    patch.setdefault("openprose_context", {})
    patch["openprose_context"]["program_file"] = ((runtime.get("openprose_context") or {}).get("program_file"))
    patch["openprose_context"]["run_dir"] = ((runtime.get("openprose_context") or {}).get("run_dir"))
    patch["openprose_context"]["bindings_dir"] = ((runtime.get("openprose_context") or {}).get("bindings_dir"))
    patch["openprose_context"]["integration_mode"] = "openprose_subagent_artifact"
    patch["openprose_context"]["controller_contract"] = "bounded_patch_bundle_proposal"

    patch.setdefault("based_on", {})
    patch["based_on"]["orchestration_plan"] = ((runtime.get("artifact_paths") or {}).get("orchestration_plan"))
    patch["based_on"]["coverage_report"] = ((runtime.get("artifact_paths") or {}).get("coverage_report"))
    patch["based_on"]["controller_decision"] = ((runtime.get("artifact_paths") or {}).get("controller_decision"))
    patch["based_on"]["run_memory"] = ((runtime.get("artifact_paths") or {}).get("run_memory"))
    patch["based_on"]["ranked_candidate_digest"] = ((runtime.get("artifact_paths") or {}).get("ranked_candidate_digest"))

    ensure_parent_dir(out_path)
    Path(out_path).write_text(json.dumps(patch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(patch, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
