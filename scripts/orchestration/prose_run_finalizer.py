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


def slugify(text: str) -> str:
    text = compact_ws(text).lower()
    out = []
    prev_us = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    return "".join(out).strip("_") or "unknown_topic"


def load_json(path: str) -> dict[str, Any]:
    if not path:
        return {}
    if path == "-":
        return json.load(sys.stdin)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_orchestration_plan(path_str: str) -> dict[str, Any]:
    if not path_str:
        return {}
    try:
        return json.loads(Path(path_str).read_text(encoding="utf-8"))
    except Exception:
        return {}


def print_cmd(cmd: list[str]) -> None:
    print("$ " + shlex.join(cmd))


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print_cmd(cmd)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def resolve_run_dir(plan_path: str, run_id: str) -> Path:
    if plan_path:
        p = Path(plan_path)
        if p.name == "orchestration_plan.json":
            return p.parent.parent
    return Path(".prose") / "runs" / run_id


def infer_tag_from_controller_path(controller_path: Path) -> str:
    name = controller_path.name
    prefix = "controller_decision."
    suffix = ".json"
    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix):-len(suffix)]
    return "latest"


def maybe_default_artifact(run_dir: Path, tag: str, stem: str) -> Path:
    return run_dir / "artifacts" / f"{stem}.{tag}.json"


def load_memory(path_str: str) -> dict[str, Any]:
    p = Path(path_str)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def memory_has_run(memory: dict[str, Any], topic: str, lane: str, run_id: str, generated_at: str) -> bool:
    topics = memory.get("topics") or {}
    topic_key = slugify(topic)
    topic_entry = topics.get(topic_key) or {}
    lane_entry = ((topic_entry.get("lanes") or {}).get(lane or "default")) or {}
    recent_runs = lane_entry.get("recent_runs") or []
    for rec in recent_runs:
        if (
            compact_ws(rec.get("run_id")) == compact_ws(run_id)
            and compact_ws(rec.get("generated_at")) == compact_ws(generated_at)
        ):
            return True
    return False


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Finalize a completed prose run by persisting memory and emitting a retention-ready summary.")
    p.add_argument("--controller-input", required=True, help="controller_decision JSON")
    p.add_argument("--coverage-input", default="", help="Optional coverage_report JSON")
    p.add_argument("--evidence-input", default="", help="Optional evidence_records JSON")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json override")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory JSON path")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter to use for subprocess stage execution")
    p.add_argument("--force", action="store_true", help="Finalize even if controller decision is not stop")
    p.add_argument("--dry-run", action="store_true", help="Print planned commands without executing them")
    p.add_argument("--write", default="", help="Optional output path for finalizer artifact")
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller_path = Path(args.controller_input)
    controller = load_json(str(controller_path))
    if compact_ws(controller.get("stage")) != "controller":
        raise SystemExit("controller input does not appear to be a controller artifact")

    inferred_plan_path = (
        args.orchestration_plan
        or ((controller.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    run_id = compact_ws(controller.get("run_id")) or compact_ws(plan.get("run_id"))
    lane = compact_ws(controller.get("lane")) or "default"
    topic = (
        compact_ws((controller.get("orchestration_context") or {}).get("topic"))
        or compact_ws(plan.get("topic"))
        or "unknown_topic"
    )
    decision = compact_ws(controller.get("decision"))
    controller_generated_at = compact_ws(controller.get("generated_at"))

    if not run_id:
        raise SystemExit("Could not infer run_id")

    run_dir = resolve_run_dir(inferred_plan_path, run_id)
    tag = infer_tag_from_controller_path(controller_path)

    coverage_path = Path(args.coverage_input) if args.coverage_input else maybe_default_artifact(run_dir, tag, "coverage_report")
    evidence_path = Path(args.evidence_input) if args.evidence_input else maybe_default_artifact(run_dir, tag, "evidence_records")
    run_memory_update_path = run_dir / "artifacts" / f"run_memory_update.{tag}.json"

    coverage_exists = coverage_path.exists()
    evidence_exists = evidence_path.exists()

    memory = load_memory(args.memory_path)
    already_persisted = False
    if memory and controller_generated_at:
        already_persisted = memory_has_run(
            memory=memory,
            topic=topic,
            lane=lane,
            run_id=run_id,
            generated_at=controller_generated_at,
        )

    finalize = args.force or (decision == "stop")
    memory_write_executed = False
    memory_write_skipped_reason = ""

    memory_cmd = [
        args.python_bin,
        "prose_run_memory.py",
        "--controller-input", str(controller_path),
        "--memory-path", str(args.memory_path),
    ]
    if coverage_exists:
        memory_cmd.extend(["--coverage-input", str(coverage_path)])
    if evidence_exists:
        memory_cmd.extend(["--evidence-input", str(evidence_path)])
    if inferred_plan_path:
        memory_cmd.extend(["--orchestration-plan", str(inferred_plan_path)])
    memory_cmd.extend(["--write", str(run_memory_update_path)])

    if not finalize:
        memory_write_skipped_reason = f"controller decision is '{decision}', not stop"
    elif already_persisted:
        memory_write_skipped_reason = "run pass already present in memory"
    else:
        run_cmd(memory_cmd, dry_run=args.dry_run)
        memory_write_executed = not args.dry_run

    retention_recommendation = {
        "state": "retain_full_recent",
        "delete_now": False,
        "safe_to_prune_heavy_artifacts_after_review": False,
    }
    if finalize:
        retention_recommendation = {
            "state": "summary_only_ready",
            "delete_now": False,
            "safe_to_prune_heavy_artifacts_after_review": True,
        }

    output = {
        "schema_version": "1.0",
        "stage": "run_finalizer",
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "topic": topic,
        "lane": lane,
        "controller_decision": decision,
        "tag": tag,
        "finalized": bool(finalize),
        "already_persisted": bool(already_persisted),
        "memory_write_executed": bool(memory_write_executed),
        "memory_write_skipped_reason": memory_write_skipped_reason or None,
        "artifacts": {
            "controller_input": str(controller_path),
            "coverage_input": str(coverage_path) if coverage_exists else None,
            "evidence_input": str(evidence_path) if evidence_exists else None,
            "memory_path": args.memory_path,
            "run_memory_update_path": str(run_memory_update_path),
        },
        "retention_recommendation": retention_recommendation,
        "next_recommended_step": (
            "ready_for_synthesis_or_retention_review" if finalize else "await_retry_or_stop_decision"
        ),
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
