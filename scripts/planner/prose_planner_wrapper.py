#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import shlex
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

def preferred_python(default_python: str) -> str:
    venv_python = Path(__file__).resolve().parent / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return default_python



def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def latest_matching(directory: Path, pattern: str) -> Path | None:
    matches = [p for p in directory.glob(pattern) if p.is_file()]
    if not matches:
        return None
    return sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def resolve_run_dir(plan_path: str, run_id: str) -> Path:
    if plan_path:
        p = Path(plan_path)
        if p.name == "orchestration_plan.json":
            return p.parent.parent
    return Path(".prose") / "runs" / run_id


def infer_tag_from_path(path: Path, prefixes: list[str]) -> str:
    name = path.name
    for prefix in prefixes:
        if name.startswith(prefix) and name.endswith(".json"):
            return name[len(prefix):-5]
    return "latest"


def maybe_default_artifact(run_dir: Path, stem: str, tag: str) -> Path:
    return run_dir / "artifacts" / f"{stem}.{tag}.json"


def print_cmd(cmd: list[str]) -> None:
    print("$ " + shlex.join(cmd))


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print_cmd(cmd)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="OC-facing wrapper for planner flow: candidate digest -> runtime input -> planner patch -> shadow eval."
    )
    p.add_argument("--controller-input", default="", help="Optional controller_decision JSON. Defaults to latest under run artifacts.")
    p.add_argument("--coverage-input", default="", help="Optional coverage_report JSON. Defaults to latest under run artifacts.")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json override")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory JSON path")
    p.add_argument("--run-id", default="", help="Optional run identifier override")
    p.add_argument("--lane", default="", help="Optional lane override")
    p.add_argument("--planner-model", default="gpt-5-mini", help="Planner model name")
    p.add_argument("--planner-alias", default="planner_subagent_model", help="Planner logical alias for provenance")
    p.add_argument("--planner-patch", default="", help="Optional existing planner patch. If set, skip model generation and evaluate this patch.")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter to use for subprocess calls")
    p.add_argument("--dry-run", action="store_true", help="Build digest/runtime input and prompt preview only, do not run real model or shadow eval")
    p.add_argument("--write", default="", help="Optional wrapper summary output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    # Infer controller + coverage + plan
    controller = load_json(args.controller_input) if args.controller_input else {}
    coverage = load_json(args.coverage_input) if args.coverage_input else {}

    inferred_plan_path = (
        args.orchestration_plan
        or ((controller.get("orchestration_context") or {}).get("plan_path"))
        or ((coverage.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_json(inferred_plan_path)

    run_id = (
        compact_ws(args.run_id)
        or compact_ws(controller.get("run_id"))
        or compact_ws(coverage.get("run_id"))
        or compact_ws(plan.get("run_id"))
    )
    if not run_id:
        raise SystemExit("Could not infer run_id")

    run_dir = resolve_run_dir(inferred_plan_path, run_id)
    artifacts_dir = run_dir / "artifacts"

    if not controller:
        latest_controller = latest_matching(artifacts_dir, "controller_decision*.json")
        if not latest_controller:
            raise SystemExit("Could not find controller_decision artifact")
        args.controller_input = str(latest_controller)
        controller = load_json(args.controller_input)

    if not coverage:
        latest_coverage = latest_matching(artifacts_dir, "coverage_report*.json")
        if not latest_coverage:
            raise SystemExit("Could not find coverage_report artifact")
        args.coverage_input = str(latest_coverage)
        coverage = load_json(args.coverage_input)

    lane = compact_ws(args.lane) or compact_ws(controller.get("lane")) or compact_ws(coverage.get("lane")) or "default"

    tag = infer_tag_from_path(Path(args.controller_input), ["controller_decision."])
    retrieval_input = maybe_default_artifact(run_dir, "retrieval_records", tag)
    ranked_input = maybe_default_artifact(run_dir, "ranked_records", tag)
    resolved_input = maybe_default_artifact(run_dir, "resolved_records", tag)
    digest_output = maybe_default_artifact(run_dir, "planner_candidate_digest", tag)
    runtime_output = maybe_default_artifact(run_dir, "planner_runtime_input", tag)
    patch_output = maybe_default_artifact(run_dir, "planner_query_patch.shadow", tag)
    family_eval_output = maybe_default_artifact(run_dir, "planner_family_eval", tag)

    if not ranked_input.exists():
        raise SystemExit(f"Missing ranked artifact: {ranked_input}")
    if not retrieval_input.exists():
        raise SystemExit(f"Missing retrieval artifact: {retrieval_input}")
    if not resolved_input.exists():
        raise SystemExit(f"Missing resolved artifact: {resolved_input}")

    python_bin = preferred_python(args.python_bin)

    digest_cmd = [
        python_bin,
        "scripts/planner/prose_planner_candidate_digest.py",
        "--input", str(ranked_input),
        "--records-key", "kept_records",
        "--top-k", "12",
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--write", str(digest_output),
    ]

    runtime_cmd = [
        python_bin,
        "scripts/planner/prose_planner_runtime_input.py",
        "--controller-input", str(args.controller_input),
        "--coverage-input", str(args.coverage_input),
        "--orchestration-plan", str(inferred_plan_path),
        "--memory-path", str(args.memory_path),
        "--write", str(runtime_output),
    ]

    planner_cmd = [
        python_bin,
        "scripts/planner/prose_planner_agent.py",
        "--runtime-input", str(runtime_output),
        "--model", str(args.planner_model),
        "--model-alias", str(args.planner_alias),
        "--write", str(patch_output),
    ]
    if args.dry_run:
        planner_cmd.append("--dry-run")

    planner_patch_to_eval = Path(args.planner_patch) if args.planner_patch else patch_output

    family_eval_cmd = [
        python_bin,
        "scripts/planner/prose_planner_family_eval.py",
        "--planner-bundle", str(planner_patch_to_eval),
        "--baseline-retrieval-input", str(retrieval_input),
        "--baseline-ranked-input", str(ranked_input),
        "--baseline-resolved-input", str(resolved_input),
        "--controller-input", str(args.controller_input),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(family_eval_output),
    ]

    summary = {
        "schema_version": "1.0",
        "stage": "planner_wrapper",
        "generated_at": utc_now_iso(),
        "run_id": run_id,
        "lane": lane,
        "mode": "dry_run" if args.dry_run else "execute",
        "inputs": {
            "controller_input": str(args.controller_input),
            "coverage_input": str(args.coverage_input),
            "orchestration_plan": str(inferred_plan_path),
            "memory_path": str(args.memory_path),
            "retrieval_input": str(retrieval_input),
            "ranked_input": str(ranked_input),
            "resolved_input": str(resolved_input),
        },
        "outputs": {
            "planner_candidate_digest": str(digest_output),
            "planner_runtime_input": str(runtime_output),
            "planner_patch": str(patch_output),
            "planner_family_eval": str(family_eval_output),
        },
        "planner_model": args.planner_model,
        "planner_alias": args.planner_alias,
        "commands": {
            "digest": digest_cmd,
            "runtime_input": runtime_cmd,
            "planner_agent": planner_cmd,
            "family_eval": family_eval_cmd,
        },
        "shadow_eval_executed": False,
    }

    # Step 1: build digest
    print("\n## planner_candidate_digest")
    run_cmd(digest_cmd, dry_run=args.dry_run)

    # Step 2: build runtime input
    print("\n## planner_runtime_input")
    run_cmd(runtime_cmd, dry_run=args.dry_run)

    # Step 3: generate planner patch or use existing one
    if args.planner_patch:
        print("\n## planner_patch")
        print(f"Using existing planner patch: {planner_patch_to_eval}")
    else:
        print("\n## planner_patch")
        run_cmd(planner_cmd, dry_run=args.dry_run)

    # Step 4: evaluate only if not dry-run
    if not args.dry_run:
        if not planner_patch_to_eval.exists():
            raise SystemExit(f"Planner bundle not found for family eval: {planner_patch_to_eval}")
        print("\n## planner_family_eval")
        run_cmd(family_eval_cmd, dry_run=False)
        summary["shadow_eval_executed"] = True
    else:
        summary["shadow_eval_executed"] = False

    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("\nPlanner wrapper completed.")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
