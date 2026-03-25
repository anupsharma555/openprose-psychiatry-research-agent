#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

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


def dedupe_keep_order(items: list[str]) -> list[str]:
    out = []
    seen = set()
    for item in items:
        item = compact_ws(item)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


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
    if run_id:
        return Path(".prose") / "runs" / run_id
    raise ValueError("Could not infer run directory")


def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def scaled_int(value: int, multiplier: float, minimum: int = 1) -> int:
    return max(minimum, int(math.ceil(value * multiplier)))


def build_attempt_paths(run_dir: Path, attempt_number: int) -> dict[str, Path]:
    tag = f"attempt{attempt_number}"
    artifacts_dir = run_dir / "artifacts"
    fulltext_dir = run_dir / "fulltext" / tag
    cache_dir = run_dir / "cache"

    return {
        "retrieval": artifacts_dir / f"retrieval_records.{tag}.json",
        "ranked": artifacts_dir / f"ranked_records.{tag}.json",
        "resolved": artifacts_dir / f"resolved_records.{tag}.json",
        "extracted": artifacts_dir / f"extracted_records.{tag}.json",
        "evidence": artifacts_dir / f"evidence_records.{tag}.json",
        "coverage": artifacts_dir / f"coverage_report.{tag}.json",
        "controller": artifacts_dir / f"controller_decision.{tag}.json",
        "fulltext_dir": fulltext_dir,
        "resolver_cache": cache_dir / f"fulltext_resolver.{tag}.json",
    }


def print_cmd(cmd: list[str]) -> None:
    print("$ " + shlex.join(cmd))


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print_cmd(cmd)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Actuator for controller retry decisions, reruns the bounded next pass for the active run.")
    p.add_argument("--controller-input", required=True, help="controller_decision.json from prose_controller.py")
    p.add_argument("--retrieval-input", default="", help="Optional prior retrieval artifact. Defaults to latest retrieval_records*.json in the run artifacts directory.")
    p.add_argument("--ranked-input", default="", help="Optional prior ranked artifact. Defaults to latest ranked_records*.json in the run artifacts directory.")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json override")
    p.add_argument("--run-id", default="", help="Optional run identifier override")
    p.add_argument("--lane", default="", help="Optional lane override")
    p.add_argument("--download-pdf", action="store_true", help="Also download PDFs during the resolver step. Off by default to reduce storage.")
    p.add_argument("--dry-run", action="store_true", help="Print planned commands without executing them.")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter to use for subprocess stage execution")
    p.add_argument("--write", default="", help="Optional path to write a retry runner summary JSON")
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    if not controller:
        raise SystemExit("Could not load controller input")

    if compact_ws(controller.get("stage")) != "controller":
        raise SystemExit("controller input does not appear to be a controller artifact")

    decision = compact_ws(controller.get("decision"))
    if decision != "retry":
        raise SystemExit(f"Controller decision is '{decision}', not 'retry'. Nothing to execute.")

    current_run_patch = controller.get("current_run_patch") or {}
    if not current_run_patch:
        raise SystemExit("Controller decision does not include current_run_patch")

    patch_payload = current_run_patch.get("payload") or {}
    search_overrides = patch_payload.get("search_overrides") or {}
    rank_overrides = patch_payload.get("rank_overrides") or {}
    resolver_overrides = patch_payload.get("resolver_overrides") or {}

    inferred_plan_path = (
        args.orchestration_plan
        or ((controller.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    run_id = (
        compact_ws(args.run_id)
        or compact_ws(controller.get("run_id"))
        or compact_ws(plan.get("run_id"))
    )
    lane = (
        compact_ws(args.lane)
        or compact_ws(controller.get("lane"))
        or "default"
    )

    if not run_id:
        raise SystemExit("Could not infer run_id")

    run_dir = resolve_run_dir(inferred_plan_path, run_id)
    artifacts_dir = run_dir / "artifacts"

    retrieval_input = Path(args.retrieval_input) if args.retrieval_input else latest_matching(artifacts_dir, "retrieval_records*.json")
    ranked_input = Path(args.ranked_input) if args.ranked_input else latest_matching(artifacts_dir, "ranked_records*.json")

    if not retrieval_input or not retrieval_input.exists():
        raise SystemExit("Could not find prior retrieval artifact. Pass --retrieval-input explicitly.")
    if not ranked_input or not ranked_input.exists():
        raise SystemExit("Could not find prior ranked artifact. Pass --ranked-input explicitly.")

    retrieval = load_json(str(retrieval_input))
    ranked = load_json(str(ranked_input))

    attempt_number = safe_int(((controller.get("controller_policy") or {}).get("next_attempt_number")), 1)
    paths = build_attempt_paths(run_dir, attempt_number)

    lane_allocation = safe_int(((plan.get("lane_allocations") or {}).get(lane)), 0)

    base_query = retrieval.get("query") or ""
    base_terms = retrieval.get("custom_terms") or []
    base_mode = compact_ws(search_overrides.get("mode") or retrieval.get("mode") or "hybrid")
    base_journal_set = compact_ws(search_overrides.get("journal_set") or retrieval.get("journal_set") or "tier1")
    base_max_results = safe_int(retrieval.get("max_results"), max(lane_allocation, 10))
    base_per_query = safe_int(retrieval.get("per_query"), max(lane_allocation, 5))
    base_journal_retmax = safe_int(retrieval.get("journal_retmax"), max(lane_allocation, 10))

    max_results_multiplier = safe_float(search_overrides.get("max_results_multiplier"), 1.0)
    per_query_multiplier = safe_float(search_overrides.get("per_query_multiplier"), 1.0)

    next_max_results = scaled_int(base_max_results, max_results_multiplier, minimum=1)
    next_per_query = scaled_int(base_per_query, per_query_multiplier, minimum=1)

    extra_terms = search_overrides.get("extra_terms") or []
    next_terms = dedupe_keep_order([*base_terms, *extra_terms])

    base_rank_top_k = safe_int(ranked.get("top_k"), 0)
    if base_rank_top_k <= 0:
        base_rank_top_k = max(len(ranked.get("kept_records") or []), lane_allocation, 4)

    top_k_increment = safe_int(rank_overrides.get("top_k_increment"), 0)
    next_rank_top_k = max(1, base_rank_top_k + top_k_increment)

    next_journal_priority = compact_ws(rank_overrides.get("journal_priority") or ranked.get("journal_priority") or "default")
    next_max_per_journal = safe_int(rank_overrides.get("max_per_journal"), safe_int(ranked.get("max_per_journal"), 0))
    next_min_score = safe_float(ranked.get("min_score"), 5.0)

    require_fulltext = bool(resolver_overrides.get("require_fulltext", False))

    python_bin = args.python_bin

    search_cmd = [
        python_bin,
        "prose_pubmed_search_worker.py",
        "--query", str(base_query),
        "--mode", str(base_mode),
        "--max-results", str(next_max_results),
        "--per-query", str(next_per_query),
        "--journal-set", str(base_journal_set),
        "--journal-retmax", str(base_journal_retmax),
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["retrieval"]),
    ]
    for term in next_terms:
        search_cmd.extend(["--term", str(term)])

    rank_cmd = [
        python_bin,
        "prose_pubmed_normalize_rank.py",
        "--input", str(paths["retrieval"]),
        "--lane", str(lane),
        "--query", str(base_query),
        "--journal-priority", str(next_journal_priority),
        "--top-k", str(next_rank_top_k),
        "--min-score", str(next_min_score),
        "--max-per-journal", str(next_max_per_journal),
        "--run-id", str(run_id),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["ranked"]),
    ]

    resolve_cmd = [
        python_bin,
        "prose_pubmed_fulltext_resolver.py",
        "--input", str(paths["ranked"]),
        "--records-key", "kept_records",
        "--top-k", str(next_rank_top_k),
        "--outdir", str(paths["fulltext_dir"]),
        "--cache", str(paths["resolver_cache"]),
        "--download-html",
        "--download-open-urls",
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["resolved"]),
    ]
    if args.download_pdf:
        resolve_cmd.append("--download-pdf")
    if require_fulltext:
        resolve_cmd.append("--require-fulltext")

    extract_cmd = [
        python_bin,
        "prose_pubmed_fulltext_extract.py",
        "--input", str(paths["resolved"]),
        "--records-key", "resolved_records",
        "--top-k", str(next_rank_top_k),
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["extracted"]),
    ]

    evidence_cmd = [
        python_bin,
        "prose_evidence_extract.py",
        "--input", str(paths["extracted"]),
        "--records-key", "extracted_records",
        "--top-k", str(next_rank_top_k),
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["evidence"]),
    ]

    coverage_cmd = [
        python_bin,
        "prose_coverage_review.py",
        "--ranked-input", str(paths["ranked"]),
        "--resolved-input", str(paths["resolved"]),
        "--extracted-input", str(paths["extracted"]),
        "--evidence-input", str(paths["evidence"]),
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--write", str(paths["coverage"]),
    ]

    controller_cmd = [
        python_bin,
        "prose_controller.py",
        "--coverage-input", str(paths["coverage"]),
        "--run-id", str(run_id),
        "--lane", str(lane),
        "--orchestration-plan", str(inferred_plan_path),
        "--attempt-number", str(attempt_number),
        "--write", str(paths["controller"]),
    ]

    cmds = [
        ("search", search_cmd),
        ("normalize_rank", rank_cmd),
        ("fulltext_resolve", resolve_cmd),
        ("fulltext_extract", extract_cmd),
        ("evidence_extract", evidence_cmd),
        ("coverage_review", coverage_cmd),
        ("controller", controller_cmd),
    ]

    summary = {
        "schema_version": "1.0",
        "stage": "retry_runner",
        "generated_at": utc_now_iso(),
        "run_id": run_id,
        "lane": lane,
        "attempt_number": attempt_number,
        "source_artifacts": {
            "controller_input": args.controller_input,
            "retrieval_input": str(retrieval_input),
            "ranked_input": str(ranked_input),
        },
        "applied_patch": {
            "action": current_run_patch.get("action"),
            "ttl": current_run_patch.get("ttl"),
            "payload": patch_payload,
        },
        "derived_parameters": {
            "base_query": base_query,
            "base_terms": base_terms,
            "next_terms": next_terms,
            "base_mode": base_mode,
            "base_journal_set": base_journal_set,
            "base_max_results": base_max_results,
            "next_max_results": next_max_results,
            "base_per_query": base_per_query,
            "next_per_query": next_per_query,
            "base_rank_top_k": base_rank_top_k,
            "next_rank_top_k": next_rank_top_k,
            "next_journal_priority": next_journal_priority,
            "next_max_per_journal": next_max_per_journal,
            "require_fulltext": require_fulltext,
            "download_pdf": bool(args.download_pdf),
        },
        "planned_outputs": {k: str(v) for k, v in paths.items()},
        "commands": [{"stage": name, "cmd": cmd} for name, cmd in cmds],
        "dry_run": bool(args.dry_run),
    }

    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    for stage_name, cmd in cmds:
        print(f"\n## {stage_name}")
        run_cmd(cmd, dry_run=args.dry_run)

    print("\nRetry runner completed.")
    print(f"Attempt: {attempt_number}")
    print(f"New controller artifact: {paths['controller']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
