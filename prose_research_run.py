#!/usr/bin/env python3
import argparse
import json
import math
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
    if not path:
        return {}
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


def infer_tag_from_path(path: Path, prefixes: list[str]) -> str:
    name = path.name
    for prefix in prefixes:
        if name.startswith(prefix) and name.endswith(".json"):
            return name[len(prefix):-5]
    return "latest"


def maybe_default_artifact(run_dir: Path, stem: str, tag: str) -> Path:
    return run_dir / "artifacts" / f"{stem}.{tag}.json"


def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def infer_latest_plan_path(explicit: str) -> str:
    if explicit:
        return explicit
    runs_dir = Path(".prose/runs")
    if not runs_dir.exists():
        return ""
    candidates = list(runs_dir.glob("*/bindings/orchestration_plan.json"))
    if not candidates:
        return ""
    return str(sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0])


def lane_defaults(lane: str, lane_allocation: int) -> dict[str, Any]:
    lane = compact_ws(lane)
    if lane == "frontier":
        return {
            "mode": "hybrid",
            "journal_set": "off",
            "journal_priority": "default",
            "max_per_journal": 2,
            "max_results": max(16, lane_allocation * 3 or 12),
            "per_query": max(6, lane_allocation * 2 or 6),
            "journal_retmax": max(10, lane_allocation * 2 or 8),
            "top_k": max(8, lane_allocation + 2 if lane_allocation else 8),
        }
    return {
        "mode": "hybrid",
        "journal_set": "tier1",
        "journal_priority": "strict",
        "max_per_journal": 1,
        "max_results": max(20, lane_allocation * 2 or 16),
        "per_query": max(8, lane_allocation or 8),
        "journal_retmax": max(12, lane_allocation * 2 or 12),
        "top_k": max(12, lane_allocation + 4 if lane_allocation else 12),
    }


def build_baseline_paths(run_dir: Path) -> dict[str, Path]:
    artifacts_dir = run_dir / "artifacts"
    fulltext_dir = run_dir / "fulltext" / "baseline"
    cache_dir = run_dir / "cache"
    return {
        "retrieval": artifacts_dir / "retrieval_records.baseline.json",
        "ranked": artifacts_dir / "ranked_records.baseline.json",
        "resolved": artifacts_dir / "resolved_records.baseline.json",
        "extracted": artifacts_dir / "extracted_records.baseline.json",
        "evidence": artifacts_dir / "evidence_records.baseline.json",
        "coverage": artifacts_dir / "coverage_report.baseline.json",
        "controller": artifacts_dir / "controller_decision.baseline.json",
        "fulltext_dir": fulltext_dir,
        "resolver_cache": cache_dir / "fulltext_resolver.baseline.json",
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Top-level orchestration entrypoint for the prose research workflow.")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json override")
    p.add_argument("--controller-input", default="", help="Optional controller decision override")
    p.add_argument("--coverage-input", default="", help="Optional coverage report override")
    p.add_argument("--run-id", default="", help="Optional run identifier override")
    p.add_argument("--lane", default="", help="Optional lane override")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory path")
    p.add_argument("--planner-model", default="gpt-5-mini", help="Planner model")
    p.add_argument("--planner-alias", default="planner_subagent_model", help="Planner alias")
    p.add_argument("--report-model", default="gpt-4o-mini", help="AI report model")
    p.add_argument("--discord-channel-id", default="", help="Optional Discord channel id for report delivery")
    p.add_argument("--max-articles", type=int, default=5, help="Max included articles in the report input")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter for subprocess calls")
    p.add_argument("--force-planner", action="store_true", help="Run planner wrapper even if planner_family_eval already exists")
    p.add_argument("--skip-planner", action="store_true", help="Skip planner wrapper / family eval")
    p.add_argument("--materialize-selected", action="store_true", help="Materialize selected planner family/hybrid into downstream artifacts")
    p.add_argument("--build-report", action="store_true", help="Build report input + AI report after selected materialization")
    p.add_argument("--post-discord", action="store_true", help="Post the generated report to Discord after report generation")
    p.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
    p.add_argument("--write", default="", help="Optional orchestration summary output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    plan_path = infer_latest_plan_path(args.orchestration_plan)
    plan = load_json(plan_path)
    run_id = compact_ws(args.run_id) or compact_ws(plan.get("run_id"))
    if not run_id:
        raise SystemExit("Could not infer run_id")

    run_dir = resolve_run_dir(plan_path, run_id)
    artifacts_dir = run_dir / "artifacts"

    controller_path = Path(args.controller_input) if args.controller_input else latest_matching(artifacts_dir, "controller_decision*.json")
    coverage_path = Path(args.coverage_input) if args.coverage_input else latest_matching(artifacts_dir, "coverage_report*.json")

    lane = compact_ws(args.lane)
    if not lane:
        if controller_path and controller_path.exists():
            lane = compact_ws(load_json(str(controller_path)).get("lane"))
        if not lane and coverage_path and coverage_path.exists():
            lane = compact_ws(load_json(str(coverage_path)).get("lane"))
        if not lane:
            lanes = plan.get("lanes") or []
            lane = compact_ws(lanes[0]) if lanes else "core_evidence"

    summary = {
        "schema_version": "1.0",
        "stage": "prose_research_run",
        "generated_at": utc_now_iso(),
        "run_id": run_id,
        "lane": lane,
        "plan_path": plan_path or None,
        "mode": "dry_run" if args.dry_run else "execute",
        "actions": [],
        "artifacts": {},
        "final_state": {},
    }

    python_bin = preferred_python(args.python_bin)

    # Baseline path if no controller exists yet
    if not controller_path or not controller_path.exists():
        lane_alloc = safe_int(((plan.get("lane_allocations") or {}).get(lane)), 8)
        defaults = lane_defaults(lane, lane_alloc)
        topic = compact_ws(plan.get("topic")) or "research topic"
        paths = build_baseline_paths(run_dir)

        search_cmd = [
            python_bin, "prose_pubmed_search_worker.py",
            "--query", topic,
            "--mode", defaults["mode"],
            "--max-results", str(defaults["max_results"]),
            "--per-query", str(defaults["per_query"]),
            "--journal-set", defaults["journal_set"],
            "--journal-retmax", str(defaults["journal_retmax"]),
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--write", str(paths["retrieval"]),
        ]
        rank_cmd = [
            python_bin, "prose_pubmed_normalize_rank.py",
            "--input", str(paths["retrieval"]),
            "--lane", lane,
            "--query", topic,
            "--journal-priority", defaults["journal_priority"],
            "--top-k", str(defaults["top_k"]),
            "--min-score", "5.0",
            "--max-per-journal", str(defaults["max_per_journal"]),
            "--run-id", run_id,
            "--orchestration-plan", plan_path,
            "--write", str(paths["ranked"]),
        ]
        resolve_cmd = [
            python_bin, "prose_pubmed_fulltext_resolver.py",
            "--input", str(paths["ranked"]),
            "--records-key", "kept_records",
            "--top-k", str(defaults["top_k"]),
            "--outdir", str(paths["fulltext_dir"]),
            "--cache", str(paths["resolver_cache"]),
            "--download-html",
            "--download-open-urls",
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--write", str(paths["resolved"]),
        ]
        extract_cmd = [
            python_bin, "prose_pubmed_fulltext_extract.py",
            "--input", str(paths["resolved"]),
            "--records-key", "resolved_records",
            "--top-k", str(defaults["top_k"]),
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--write", str(paths["extracted"]),
        ]
        evidence_cmd = [
            python_bin, "prose_evidence_extract.py",
            "--input", str(paths["extracted"]),
            "--records-key", "extracted_records",
            "--top-k", str(defaults["top_k"]),
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--write", str(paths["evidence"]),
        ]
        coverage_cmd = [
            python_bin, "prose_coverage_review.py",
            "--ranked-input", str(paths["ranked"]),
            "--resolved-input", str(paths["resolved"]),
            "--extracted-input", str(paths["extracted"]),
            "--evidence-input", str(paths["evidence"]),
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--write", str(paths["coverage"]),
        ]
        controller_cmd = [
            python_bin, "prose_controller.py",
            "--coverage-input", str(paths["coverage"]),
            "--run-id", run_id,
            "--lane", lane,
            "--orchestration-plan", plan_path,
            "--attempt-number", "0",
            "--write", str(paths["controller"]),
        ]

        for stage_name, cmd in [
            ("baseline_search", search_cmd),
            ("baseline_normalize_rank", rank_cmd),
            ("baseline_fulltext_resolve", resolve_cmd),
            ("baseline_fulltext_extract", extract_cmd),
            ("baseline_evidence_extract", evidence_cmd),
            ("baseline_coverage_review", coverage_cmd),
            ("baseline_controller", controller_cmd),
        ]:
            print(f"\n## {stage_name}")
            run_cmd(cmd, dry_run=args.dry_run)
            summary["actions"].append(stage_name)

        controller_path = paths["controller"]
        coverage_path = paths["coverage"]
        summary["artifacts"]["baseline_controller"] = str(controller_path)
        summary["artifacts"]["baseline_coverage"] = str(coverage_path)

    controller = load_json(str(controller_path))
    current_decision = compact_ws(controller.get("decision"))
    summary["final_state"]["controller_after_baseline_or_latest"] = current_decision
    summary["final_state"]["controller_path"] = str(controller_path)

    # Retry path
    if current_decision == "retry":
        next_attempt = safe_int(((controller.get("controller_policy") or {}).get("next_attempt_number")), 1)
        retry_summary = run_dir / "artifacts" / f"retry_runner.summary.attempt{next_attempt}.json"

        retry_cmd = [
            python_bin,
            "prose_retry_runner.py",
            "--controller-input", str(controller_path),
            "--orchestration-plan", plan_path,
            "--write", str(retry_summary),
        ]
        if args.dry_run:
            retry_cmd.append("--dry-run")

        print("\n## retry_runner")
        run_cmd(retry_cmd, dry_run=args.dry_run)
        summary["actions"].append("retry_runner")
        summary["artifacts"]["retry_runner_summary"] = str(retry_summary)

        if not args.dry_run:
            controller_path = run_dir / "artifacts" / f"controller_decision.attempt{next_attempt}.json"
            coverage_path = run_dir / "artifacts" / f"coverage_report.attempt{next_attempt}.json"
            controller = load_json(str(controller_path))
            current_decision = compact_ws(controller.get("decision"))

    tag = infer_tag_from_path(Path(controller_path), ["controller_decision."])
    evidence_path = maybe_default_artifact(run_dir, "evidence_records", tag)
    coverage_path = maybe_default_artifact(run_dir, "coverage_report", tag) if not coverage_path else coverage_path

    # Stop path
    if current_decision == "stop":
        finalizer_out = run_dir / "artifacts" / f"run_finalizer.{tag}.json"
        finalizer_cmd = [
            python_bin,
            "prose_run_finalizer.py",
            "--controller-input", str(controller_path),
            "--coverage-input", str(coverage_path),
            "--evidence-input", str(evidence_path),
            "--orchestration-plan", plan_path,
            "--memory-path", args.memory_path,
            "--write", str(finalizer_out),
        ]
        if args.dry_run:
            finalizer_cmd.append("--dry-run")

        print("\n## run_finalizer")
        run_cmd(finalizer_cmd, dry_run=args.dry_run)
        summary["actions"].append("run_finalizer")
        summary["artifacts"]["run_finalizer"] = str(finalizer_out)

        family_eval_path = run_dir / "artifacts" / f"planner_family_eval.{tag}.json"

        if not args.skip_planner:
            if args.force_planner or not family_eval_path.exists():
                planner_wrapper_out = run_dir / "artifacts" / f"planner_wrapper.{tag}.json"
                planner_cmd = [
                    python_bin,
                    "prose_planner_wrapper.py",
                    "--controller-input", str(controller_path),
                    "--coverage-input", str(coverage_path),
                    "--orchestration-plan", plan_path,
                    "--memory-path", args.memory_path,
                    "--planner-model", args.planner_model,
                    "--planner-alias", args.planner_alias,
                    "--write", str(planner_wrapper_out),
                ]
                if args.dry_run:
                    planner_cmd.append("--dry-run")

                print("\n## planner_wrapper")
                run_cmd(planner_cmd, dry_run=args.dry_run)
                summary["actions"].append("planner_wrapper")
                summary["artifacts"]["planner_wrapper"] = str(planner_wrapper_out)
            else:
                summary["actions"].append("planner_wrapper_skipped_existing_family_eval")

        if args.materialize_selected and not args.dry_run:
            if family_eval_path.exists():
                family_eval = load_json(str(family_eval_path))
                selected_strategy = compact_ws(family_eval.get("selected_strategy"))
                if selected_strategy in {"single_family", "hybrid_family_merge"}:
                    materialize_out = run_dir / "artifacts" / f"planner_selected_materialize.{tag}.json"
                    materialize_cmd = [
                        python_bin,
                        "prose_hybrid_materialize.py",
                        "--family-eval", str(family_eval_path),
                        "--orchestration-plan", plan_path,
                        "--write", str(materialize_out),
                    ]
                    print("\n## planner_selected_materialize")
                    run_cmd(materialize_cmd, dry_run=False)
                    summary["actions"].append("planner_selected_materialize")
                    summary["artifacts"]["planner_selected_materialize"] = str(materialize_out)

    # Build report, prefer planner_selected artifacts when present
    if args.build_report and not args.dry_run:
        selected_controller = run_dir / "artifacts" / f"planner_selected_controller.{tag}.json"
        selected_coverage = run_dir / "artifacts" / f"planner_selected_coverage.{tag}.json"
        selected_evidence = run_dir / "artifacts" / f"planner_selected_evidence.{tag}.json"

        controller_for_report = selected_controller if selected_controller.exists() else Path(controller_path)
        coverage_for_report = selected_coverage if selected_coverage.exists() else Path(coverage_path)
        evidence_for_report = selected_evidence if selected_evidence.exists() else Path(evidence_path)

        report_input_out = run_dir / "artifacts" / f"prose_run_report_input.{tag}.json"
        report_input_cmd = [
            python_bin,
            "prose_run_report_input.py",
            "--controller-input", str(controller_for_report),
            "--coverage-input", str(coverage_for_report),
            "--evidence-input", str(evidence_for_report),
            "--orchestration-plan", plan_path,
            "--max-articles", str(args.max_articles),
            "--write", str(report_input_out),
        ]
        print("\n## prose_run_report_input")
        run_cmd(report_input_cmd, dry_run=False)
        summary["actions"].append("prose_run_report_input")
        summary["artifacts"]["prose_run_report_input"] = str(report_input_out)

        report_md = run_dir / "artifacts" / f"prose_run_report.{tag}.md"
        digest_md = run_dir / "artifacts" / f"prose_run_digest.{tag}.md"
        delivery_json = run_dir / "artifacts" / f"prose_run_report.{tag}.delivery.json"

        report_ai_cmd = [
            python_bin,
            "prose_run_report_ai.py",
            "--report-input", str(report_input_out),
            "--model", args.report_model,
            "--message", f"Prose Research Run: {compact_ws(plan.get('topic')) or run_id}",
            "--delivery-json", str(delivery_json),
            "--write-report-md", str(report_md),
            "--write-digest-md", str(digest_md),
        ]
        if args.discord_channel_id:
            report_ai_cmd.extend(["--discord-channel-id", args.discord_channel_id])

        print("\n## prose_run_report_ai")
        run_cmd(report_ai_cmd, dry_run=False)
        summary["actions"].append("prose_run_report_ai")
        summary["artifacts"]["report_md"] = str(report_md)
        summary["artifacts"]["digest_md"] = str(digest_md)
        summary["artifacts"]["delivery_json"] = str(delivery_json)

        if args.post_discord and delivery_json.exists():
            post_out = run_dir / "artifacts" / f"discord_post.{tag}.json"
            post_cmd = [
                python_bin,
                "prose_post_discord.py",
                "--delivery-json", str(delivery_json),
                "--write", str(post_out),
            ]
            print("\n## prose_post_discord")
            run_cmd(post_cmd, dry_run=False)
            summary["actions"].append("prose_post_discord")
            summary["artifacts"]["discord_post"] = str(post_out)

    summary["final_state"]["controller_decision"] = current_decision
    summary["final_state"]["controller_path"] = str(controller_path)

    write_path = args.write or str(run_dir / "artifacts" / f"prose_research_run.{tag}.json")
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("\nProse research run completed.")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
