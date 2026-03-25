#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

DEFAULT_PROSE_RESEARCH_DISCORD_CHANNEL_ID = "1483624740793880588"


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


def infer_latest_plan_template(explicit: str) -> Path | None:
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    runs_dir = Path(".prose/runs")
    if not runs_dir.exists():
        return None
    candidates = list(runs_dir.glob("*/bindings/orchestration_plan.json"))
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def make_run_id(prefix: str = "") -> str:
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}{ts}" if prefix else ts


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Create and optionally execute a fresh prose research run from a user-supplied topic.")
    p.add_argument("--topic", required=True, help="User-supplied research topic")
    p.add_argument("--lane", default="core_evidence", help="Initial lane")
    p.add_argument("--run-id", default="", help="Optional run id override")
    p.add_argument("--run-prefix", default="", help="Optional prefix for generated run ids")
    p.add_argument("--plan-template", default="", help="Optional orchestration_plan.json template override")
    p.add_argument("--program-template", default=".prose/programs/prose_research.prose", help="Canonical prose program path")
    p.add_argument("--execute", action="store_true", help="Immediately hand off to prose_research_run.py")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter to use for subprocess calls")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory path")
    p.add_argument("--planner-model", default="gpt-5-mini", help="Planner model")
    p.add_argument("--planner-alias", default="planner_subagent_model", help="Planner alias")
    p.add_argument("--report-model", default="gpt-4o-mini", help="Report model")
    p.add_argument("--discord-channel-id", default="", help="Optional Discord channel id")
    p.add_argument("--materialize-selected", action="store_true", help="Materialize selected planner family/hybrid into downstream artifacts")
    p.add_argument("--build-report", action="store_true", help="Build report after the run")
    p.add_argument("--post-discord", action="store_true", help="Post report to Discord after the run")
    p.add_argument("--write", default="", help="Optional output summary path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    topic = compact_ws(args.topic)
    if not topic:
        raise SystemExit("Topic cannot be empty")

    run_id = compact_ws(args.run_id) or make_run_id(args.run_prefix)
    lane = compact_ws(args.lane) or "core_evidence"

    run_dir = Path(".prose/runs") / run_id
    bindings_dir = run_dir / "bindings"
    artifacts_dir = run_dir / "artifacts"
    cache_dir = run_dir / "cache"
    fulltext_dir = run_dir / "fulltext"

    for d in [bindings_dir, artifacts_dir, cache_dir, fulltext_dir]:
        d.mkdir(parents=True, exist_ok=True)

    program_template = Path(args.program_template)
    run_program = run_dir / "program.prose"
    if program_template.exists():
        shutil.copy2(program_template, run_program)
    elif not run_program.exists():
        run_program.write_text(
            "# Prose Research Agent - Run Program Snapshot\n"
            f'# Topic: "{topic}"\n'
            f'# Run ID: {run_id}\n',
            encoding="utf-8",
        )

    plan_template = infer_latest_plan_template(args.plan_template)
    run_plan = bindings_dir / "orchestration_plan.json"

    if plan_template and plan_template.exists():
        plan = load_json(str(plan_template))
    else:
        plan = {
            "schema_version": "1.1",
            "plan_version": "v1.1",
            "topic": topic,
            "run_id": run_id,
            "lanes": [lane],
            "lane_allocations": {
                "core_evidence": 8,
                "recent_peer_reviewed": 8,
                "frontier": 4
            },
            "lane_windows": {
                "core_evidence": "no date restriction",
                "recent_peer_reviewed": "last 12 months",
                "frontier": "last 6 months"
            },
            "retry_policy": {
                "enabled": True,
                "max_additional_passes": 1
            },
            "quality_thresholds": {
                "min_core_records": 5,
                "min_recent_records": 5,
                "min_frontier_records": 2,
                "min_fulltext_rate": 0.5,
                "min_semantic_extraction_rate": 0.7,
                "max_duplicate_fraction": 0.25
            }
        }

    plan["topic"] = topic
    plan["run_id"] = run_id
    plan["workspace_root"] = str(Path.cwd().resolve())
    plan["created_at"] = utc_now_iso()

    lanes = plan.get("lanes") or []
    plan["lanes"] = [lane] + [x for x in lanes if x != lane]

    run_plan.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    summary = {
        "schema_version": "1.0",
        "stage": "prose_research_start",
        "generated_at": utc_now_iso(),
        "topic": topic,
        "run_id": run_id,
        "lane": lane,
        "run_dir": str(run_dir.resolve()),
        "program_snapshot": str(run_program.resolve()),
        "orchestration_plan": str(run_plan.resolve()),
        "execute_requested": bool(args.execute),
        "execute_result": None,
    }
    if args.execute:
        python_bin = preferred_python(args.python_bin)

        # If execute is requested with no downstream flags, default to the integrated path.
        auto_materialize = args.materialize_selected or args.build_report or args.post_discord
        auto_build = args.build_report or args.post_discord

        if not (args.materialize_selected or args.build_report or args.post_discord):
            auto_materialize = True
            auto_build = True

        cmd = [
            python_bin,
            "prose_research_run.py",
            "--orchestration-plan", str(run_plan),
            "--memory-path", args.memory_path,
            "--planner-model", args.planner_model,
            "--planner-alias", args.planner_alias,
            "--report-model", args.report_model,
            "--write", str(artifacts_dir / "prose_research_run.latest.json"),
        ]
        discord_channel_id = compact_ws(args.discord_channel_id)
        if args.post_discord and not discord_channel_id:
            discord_channel_id = DEFAULT_PROSE_RESEARCH_DISCORD_CHANNEL_ID

        if discord_channel_id:
            cmd.extend(["--discord-channel-id", discord_channel_id])
        if auto_materialize:
            cmd.append("--materialize-selected")
        if auto_build:
            cmd.append("--build-report")
        if args.post_discord:
            cmd.append("--post-discord")

        subprocess.run(cmd, check=True)
        summary["execute_result"] = "started"

    write_path = args.write or str(artifacts_dir / "prose_research_start.latest.json")
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
