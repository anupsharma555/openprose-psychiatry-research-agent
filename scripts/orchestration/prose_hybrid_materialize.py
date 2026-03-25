#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import subprocess
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


STATUS_PRIORITY = {
    "fulltext_xml": 7,
    "fulltext_html": 6,
    "fulltext_pdf": 5,
    "free_url_only": 4,
    "landing_page_only": 3,
    "abstract_only": 2,
    "unresolved_no_fulltext": 1,
    "resolver_error": 0,
    "unknown": -1,
}


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


def print_cmd(cmd: list[str]) -> None:
    print("$ " + " ".join(cmd))


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print_cmd(cmd)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def infer_run_dir(family_eval: dict[str, Any], family_eval_path: Path) -> Path:
    run_id = compact_ws(family_eval.get("run_id"))
    if run_id:
        return Path(".prose/runs") / run_id
    return family_eval_path.parent.parent


def infer_tag(path: Path, prefix: str) -> str:
    name = path.name
    if name.startswith(prefix) and name.endswith(".json"):
        return name[len(prefix):-5]
    return "latest"


def key_for_record(rec: dict[str, Any]) -> str:
    for k in ["pmid", "doi", "pmcid", "title"]:
        v = compact_ws(rec.get(k))
        if v:
            return f"{k}:{v.lower()}"
    return json.dumps(rec, sort_keys=True)


def better_resolved_record(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    sa = compact_ws(a.get("fulltext_status")) or "unknown"
    sb = compact_ws(b.get("fulltext_status")) or "unknown"
    pa = STATUS_PRIORITY.get(sa, -1)
    pb = STATUS_PRIORITY.get(sb, -1)

    winner = dict(b) if pb > pa else dict(a)

    fams = []
    for src in [a, b]:
        for f in (src.get("retrieved_by_query_families") or []):
            if f not in fams:
                fams.append(f)
    winner["retrieved_by_query_families"] = fams
    winner["query_agreement_count"] = len(fams)
    return winner


def merge_ranked_records(
    family_results: list[dict[str, Any]],
    selected_strategy: str,
    selected_family_id: str | None,
    per_family_quota: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    selected_family_ids = []
    if selected_strategy == "single_family" and selected_family_id:
        selected_family_ids = [selected_family_id]
    elif selected_strategy == "hybrid_family_merge":
        selected_family_ids = [
            compact_ws(item.get("family_id"))
            for item in family_results
            if compact_ws(item.get("controller_outcome")) in {"promote_to_current_run_patch", "save_for_future_run_memory"}
        ]
    else:
        return [], []

    merged: dict[str, dict[str, Any]] = {}

    for item in family_results:
        family_id = compact_ws(item.get("family_id"))
        if family_id not in selected_family_ids:
            continue

        ranked_path = compact_ws((item.get("branch_artifacts") or {}).get("ranked"))
        if not ranked_path:
            continue

        ranked = load_json(ranked_path)
        for rec in (ranked.get("kept_records") or [])[:per_family_quota]:
            k = key_for_record(rec)
            if k not in merged:
                rec_copy = dict(rec)
                rec_copy["retrieved_by_query_families"] = [family_id]
                rec_copy["query_agreement_count"] = 1
                merged[k] = rec_copy
            else:
                fams = merged[k].get("retrieved_by_query_families") or []
                if family_id not in fams:
                    fams.append(family_id)
                merged[k]["retrieved_by_query_families"] = fams
                merged[k]["query_agreement_count"] = len(fams)

    out = list(merged.values())
    out.sort(
        key=lambda r: (
            -(r.get("query_agreement_count") or 0),
            -(r.get("rank_score") or 0.0),
            compact_ws(r.get("title")),
        )
    )
    return out, selected_family_ids


def merge_resolved_records(
    family_results: list[dict[str, Any]],
    selected_family_ids: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    merged: dict[str, dict[str, Any]] = {}

    for item in family_results:
        family_id = compact_ws(item.get("family_id"))
        if family_id not in selected_family_ids:
            continue

        resolved_path = compact_ws((item.get("branch_artifacts") or {}).get("resolved"))
        if not resolved_path:
            continue

        resolved = load_json(resolved_path)
        records = (resolved.get("resolved_records") or []) + (resolved.get("unresolved_records") or [])
        for rec in records:
            k = key_for_record(rec)
            rec_copy = dict(rec)
            rec_copy["retrieved_by_query_families"] = [family_id]
            rec_copy["query_agreement_count"] = 1

            if k not in merged:
                merged[k] = rec_copy
            else:
                merged[k] = better_resolved_record(merged[k], rec_copy)

    resolved_records = []
    unresolved_records = []

    for rec in merged.values():
        status = compact_ws(rec.get("fulltext_status")) or "unknown"
        if STATUS_PRIORITY.get(status, -1) >= STATUS_PRIORITY["free_url_only"]:
            resolved_records.append(rec)
        else:
            unresolved_records.append(rec)

    resolved_records.sort(
        key=lambda r: (
            -(r.get("query_agreement_count") or 0),
            -STATUS_PRIORITY.get(compact_ws(r.get("fulltext_status")) or "unknown", -1),
            compact_ws(r.get("title")),
        )
    )
    unresolved_records.sort(
        key=lambda r: (
            -(r.get("query_agreement_count") or 0),
            compact_ws(r.get("title")),
        )
    )
    return resolved_records, unresolved_records


def build_resolved_stats(resolved_records: list[dict[str, Any]], unresolved_records: list[dict[str, Any]]) -> dict[str, Any]:
    processed = len(resolved_records) + len(unresolved_records)
    analysis_ready = sum(1 for rec in resolved_records if rec.get("analysis_ready"))
    return {
        "input_count": processed,
        "processed_count": processed,
        "analysis_ready_count": analysis_ready,
        "unresolved_count": len(unresolved_records),
        "cache_hit_count": sum(1 for rec in resolved_records if rec.get("cache_hit")),
        "analysis_ready_rate": round((analysis_ready / processed), 3) if processed else 0.0,
        "xml_count": sum(1 for rec in resolved_records if rec.get("fulltext_status") == "fulltext_xml"),
        "html_count": sum(1 for rec in resolved_records if rec.get("fulltext_status") == "fulltext_html"),
        "pdf_count": sum(1 for rec in resolved_records if rec.get("fulltext_status") == "fulltext_pdf"),
        "free_url_only_count": sum(1 for rec in resolved_records if rec.get("fulltext_status") == "free_url_only"),
        "landing_page_only_count": sum(1 for rec in resolved_records if rec.get("fulltext_status") == "landing_page_only"),
        "abstract_only_count": sum(1 for rec in unresolved_records if rec.get("fulltext_status") == "abstract_only"),
        "unresolved_no_fulltext_count": sum(1 for rec in unresolved_records if rec.get("fulltext_status") == "unresolved_no_fulltext"),
    }


def build_resolution_summary(resolved_records: list[dict[str, Any]], unresolved_records: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = {}
    best_source_counts = {}
    resolved_by_counts = {}

    for rec in resolved_records + unresolved_records:
        status = compact_ws(rec.get("fulltext_status")) or "unknown"
        best_source = compact_ws(rec.get("best_source")) or "unknown"
        resolved_by = compact_ws(rec.get("resolved_by")) or "unknown"

        status_counts[status] = status_counts.get(status, 0) + 1
        best_source_counts[best_source] = best_source_counts.get(best_source, 0) + 1
        resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

    return {
        "status_counts": dict(sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "best_source_counts": dict(sorted(best_source_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "resolved_by_counts": dict(sorted(resolved_by_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Materialize selected single-family or hybrid planner results into downstream extraction/evidence artifacts.")
    p.add_argument("--family-eval", required=True, help="planner_family_eval.<tag>.json")
    p.add_argument("--orchestration-plan", required=True, help="orchestration_plan.json")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter for subprocess calls")
    p.add_argument("--artifact-tag", default="", help="Optional explicit artifact tag override")
    p.add_argument("--per-family-quota", type=int, default=4, help="Per-family top-k quota for hybrid merge")
    p.add_argument("--finalize-on-stop", action="store_true", help="Run finalizer if controller decision is stop")
    p.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    p.add_argument("--write", default="", help="Optional summary output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    family_eval_path = Path(args.family_eval)
    family_eval = load_json(str(family_eval_path))
    if not family_eval:
        raise SystemExit(f"Could not load family eval: {family_eval_path}")

    selected_strategy = compact_ws(family_eval.get("selected_strategy"))
    selected_family_id = compact_ws(family_eval.get("selected_family_id")) or None

    if selected_strategy not in {"single_family", "hybrid_family_merge"}:
        raise SystemExit(f"Family eval did not choose a materializable strategy: {selected_strategy}")

    run_dir = infer_run_dir(family_eval, family_eval_path)
    artifacts_dir = run_dir / "artifacts"
    tag = compact_ws(args.artifact_tag) or infer_tag(family_eval_path, "planner_family_eval.")

    ranked_records, selected_family_ids = merge_ranked_records(
        family_eval.get("candidate_family_results") or [],
        selected_strategy,
        selected_family_id,
        args.per_family_quota,
    )
    resolved_records, unresolved_records = merge_resolved_records(
        family_eval.get("candidate_family_results") or [],
        selected_family_ids,
    )

    run_id = compact_ws(family_eval.get("run_id"))
    lane = compact_ws(family_eval.get("lane"))

    ranked_out = artifacts_dir / f"planner_selected_ranked.{tag}.json"
    resolved_out = artifacts_dir / f"planner_selected_resolved.{tag}.json"
    resolved_reclassified_out = artifacts_dir / f"planner_selected_resolved_reclassified.{tag}.json"
    extracted_out = artifacts_dir / f"planner_selected_extracted.{tag}.json"
    extracted_backfilled_out = artifacts_dir / f"planner_selected_extracted_backfilled.{tag}.json"
    evidence_out = artifacts_dir / f"planner_selected_evidence.{tag}.json"
    coverage_out = artifacts_dir / f"planner_selected_coverage.{tag}.json"
    controller_out = artifacts_dir / f"planner_selected_controller.{tag}.json"
    finalizer_out = artifacts_dir / f"planner_selected_finalizer.{tag}.json"

    ranked_payload = {
        "schema_version": "1.0",
        "stage": "planner_selected_ranked",
        "run_id": run_id,
        "lane": lane,
        "generated_at": utc_now_iso(),
        "selected_strategy": selected_strategy,
        "selected_family_ids": selected_family_ids,
        "kept_count": len(ranked_records),
        "dropped_count": 0,
        "top_k": len(ranked_records),
        "kept_records": ranked_records,
        "dropped_records": [],
        "source_family_eval": str(family_eval_path),
    }

    resolved_payload = {
        "schema_version": "1.0",
        "stage": "planner_selected_resolved",
        "run_id": run_id,
        "lane": lane,
        "generated_at": utc_now_iso(),
        "source_stage": "planner_selected_ranked",
        "selected_strategy": selected_strategy,
        "selected_family_ids": selected_family_ids,
        "input": str(ranked_out),
        "records_key": "kept_records",
        "resolved_count": len(resolved_records),
        "unresolved_count": len(unresolved_records),
        "stats": build_resolved_stats(resolved_records, unresolved_records),
        "resolution_summary": build_resolution_summary(resolved_records, unresolved_records),
        "resolved_records": resolved_records,
        "unresolved_records": unresolved_records,
    }

    ensure_parent_dir(str(ranked_out))
    ranked_out.write_text(json.dumps(ranked_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    resolved_out.write_text(json.dumps(resolved_payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    python_bin = preferred_python(args.python_bin)

    reclassify_cmd = [
        python_bin,
        "prose_resolved_reclassify.py",
        "--input", str(resolved_out),
        "--ranked-input", str(ranked_out),
        "--write", str(resolved_reclassified_out),
    ]

    extract_cmd = [
        python_bin,
        "prose_pubmed_fulltext_extract.py",
        "--input", str(resolved_reclassified_out),
        "--records-key", "resolved_records",
        "--top-k", str(len(resolved_records)),
        "--run-id", run_id,
        "--lane", lane,
        "--orchestration-plan", args.orchestration-plan if False else args.orchestration_plan,
        "--write", str(extracted_out),
    ]

    backfill_cmd = [
        python_bin,
        "prose_extracted_backfill.py",
        "--input", str(extracted_out),
        "--ranked-input", str(ranked_out),
        "--resolved-input", str(resolved_reclassified_out),
        "--write", str(extracted_backfilled_out),
    ]

    evidence_cmd = [
        python_bin,
        "prose_evidence_extract.py",
        "--input", str(extracted_backfilled_out),
        "--records-key", "extracted_records",
        "--run-id", run_id,
        "--lane", lane,
        "--orchestration-plan", args.orchestration_plan,
        "--write", str(evidence_out),
    ]

    coverage_cmd = [
        python_bin,
        "prose_coverage_review.py",
        "--ranked-input", str(ranked_out),
        "--resolved-input", str(resolved_out),
        "--extracted-input", str(extracted_backfilled_out),
        "--evidence-input", str(evidence_out),
        "--run-id", run_id,
        "--lane", lane,
        "--orchestration-plan", args.orchestration_plan,
        "--write", str(coverage_out),
    ]

    controller_cmd = [
        python_bin,
        "prose_controller.py",
        "--coverage-input", str(coverage_out),
        "--run-id", run_id,
        "--lane", lane,
        "--orchestration-plan", args.orchestration_plan,
        "--attempt-number", "0",
        "--write", str(controller_out),
    ]

    for stage_name, cmd in [
        ("planner_selected_reclassify", reclassify_cmd),
        ("planner_selected_extract", extract_cmd),
        ("planner_selected_backfill", backfill_cmd),
        ("planner_selected_evidence", evidence_cmd),
        ("planner_selected_coverage", coverage_cmd),
        ("planner_selected_controller", controller_cmd),
    ]:
        print(f"\n## {stage_name}")
        run_cmd(cmd, dry_run=args.dry_run)

    finalizer_executed = False
    if args.finalize_on_stop and not args.dry_run:
        controller_payload = load_json(str(controller_out))
        if compact_ws(controller_payload.get("decision")) == "stop":
            finalizer_cmd = [
                python_bin,
                "prose_run_finalizer.py",
                "--controller-input", str(controller_out),
                "--coverage-input", str(coverage_out),
                "--evidence-input", str(evidence_out),
                "--orchestration-plan", args.orchestration_plan,
                "--write", str(finalizer_out),
            ]
            print("\n## planner_selected_finalizer")
            run_cmd(finalizer_cmd, dry_run=False)
            finalizer_executed = True

    summary = {
        "schema_version": "1.0",
        "stage": "planner_selected_materialize",
        "generated_at": utc_now_iso(),
        "run_id": run_id,
        "lane": lane,
        "selected_strategy": selected_strategy,
        "selected_family_ids": selected_family_ids,
        "ranked_count": len(ranked_records),
        "resolved_count": len(resolved_records),
        "unresolved_count": len(unresolved_records),
        "artifacts": {
            "family_eval": str(family_eval_path),
            "ranked": str(ranked_out),
            "resolved": str(resolved_out),
            "resolved_reclassified": str(resolved_reclassified_out),
            "extracted": str(extracted_out),
            "extracted_backfilled": str(extracted_backfilled_out),
            "evidence": str(evidence_out),
            "coverage": str(coverage_out),
            "controller": str(controller_out),
            "finalizer": str(finalizer_out) if finalizer_executed else None,
        },
        "finalizer_executed": finalizer_executed,
    }

    write_path = args.write or str(artifacts_dir / f"planner_selected_materialize.{tag}.json")
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
