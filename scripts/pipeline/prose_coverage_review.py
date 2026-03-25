#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


REVIEW_LIKE = {"review", "systematic_review", "mechanism_review", "guideline"}
PRIMARY_LIKE = {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional", "case_report"}


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


def count_by(records: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in records:
        val = compact_ws(rec.get(key)) or "unknown"
        counts[val] = counts.get(val, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def get_stage_feedback(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    for key in ["resolver_feedback", "extraction_feedback", "semantic_feedback"]:
        if isinstance(payload.get(key), dict):
            return payload.get(key)
    return {}


def get_stage_actions(payload: dict[str, Any]) -> list[str]:
    fb = get_stage_feedback(payload)
    return [compact_ws(x) for x in (fb.get("candidate_retry_actions") or []) if compact_ws(x)]


def get_stage_missing_angles(payload: dict[str, Any]) -> list[str]:
    fb = get_stage_feedback(payload)
    return [compact_ws(x) for x in (fb.get("missing_angles") or []) if compact_ws(x)]


def lane_min_threshold(plan: dict[str, Any], lane: str) -> int | None:
    qt = plan.get("quality_thresholds") or {}
    mapping = {
        "core_evidence": "min_core_records",
        "recent_peer_reviewed": "min_recent_records",
        "frontier": "min_frontier_records",
    }
    key = mapping.get(lane)
    if key and key in qt:
        try:
            return int(qt[key])
        except Exception:
            return None
    return None


def build_counts(
    ranked: dict[str, Any],
    resolved: dict[str, Any],
    extracted: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    ranked_kept = len(ranked.get("kept_records") or [])
    ranked_dropped = len(ranked.get("dropped_records") or [])

    resolved_records = len(resolved.get("resolved_records") or [])
    unresolved_records = len(resolved.get("unresolved_records") or [])

    extracted_records = len(extracted.get("extracted_records") or [])
    extracted_skipped = len(extracted.get("skipped_records") or [])

    evidence_records = len(evidence.get("evidence_records") or [])
    partial_records = len(evidence.get("partial_records") or [])
    evidence_skipped = len(evidence.get("skipped_records") or [])

    semantic_ready = evidence_records + partial_records
    extracted_total = extracted_records + extracted_skipped

    return {
        "ranked_kept_count": ranked.get("kept_count", ranked_kept),
        "ranked_dropped_count": ranked.get("dropped_count", ranked_dropped),
        "resolved_count": resolved.get("resolved_count", resolved_records),
        "unresolved_count": resolved.get("unresolved_count", unresolved_records),
        "extracted_count": extracted.get("extracted_count", extracted_records),
        "extract_skipped_count": extracted.get("skipped_count", extracted_skipped),
        "evidence_record_count": evidence_records,
        "partial_record_count": partial_records,
        "evidence_skipped_count": evidence_skipped,
        "semantic_ready_count": semantic_ready,
        "fulltext_yield_rate": round(
            (resolved.get("stats", {}) or {}).get("analysis_ready_rate", 0.0), 3
        ) if resolved else None,
        "semantic_ready_rate": round((semantic_ready / extracted_total), 3) if extracted_total else None,
    }


def build_coverage(evidence_records: list[dict[str, Any]], partial_records: list[dict[str, Any]]) -> dict[str, Any]:
    semantic_ready_records = evidence_records + partial_records

    review_like_count = sum(
        1 for rec in semantic_ready_records
        if compact_ws(rec.get("document_role")) == "review_like"
        or (not compact_ws(rec.get("document_role")) and compact_ws(rec.get("paper_kind")) in REVIEW_LIKE)
    )
    primary_like_count = sum(
        1 for rec in semantic_ready_records
        if compact_ws(rec.get("document_role")) == "primary_empirical"
        or (not compact_ws(rec.get("document_role")) and compact_ws(rec.get("paper_kind")) in PRIMARY_LIKE)
    )
    mixed_or_unclear_count = sum(1 for rec in semantic_ready_records if compact_ws(rec.get("document_role")) == "mixed_or_unclear")
    metrics_count = sum(
        1
        for rec in semantic_ready_records
        if (rec.get("metrics") or {}).get("p_values") or (rec.get("metrics") or {}).get("score_change_snippets")
    )
    sample_size_count = sum(1 for rec in semantic_ready_records if rec.get("sample_size"))
    limitations_count = sum(1 for rec in semantic_ready_records if rec.get("limitations"))
    safety_count = sum(1 for rec in semantic_ready_records if rec.get("safety_findings"))
    high_quality_count = sum(1 for rec in semantic_ready_records if compact_ws(rec.get("extraction_quality")) == "high")

    ratio = None
    if primary_like_count > 0:
        ratio = round(review_like_count / primary_like_count, 3)

    return {
        "review_like_count": review_like_count,
        "primary_study_like_count": primary_like_count,
        "mixed_or_unclear_count": mixed_or_unclear_count,
        "review_to_primary_ratio": ratio,
        "records_with_metrics": metrics_count,
        "records_with_sample_size": sample_size_count,
        "records_with_limitations": limitations_count,
        "records_with_safety_findings": safety_count,
        "high_quality_record_count": high_quality_count,
    }


def build_retry_recommendation(
    lane: str,
    plan: dict[str, Any],
    counts: dict[str, Any],
    coverage: dict[str, Any],
    stage_missing_angles: list[str],
    stage_actions: list[str],
) -> dict[str, Any]:
    semantic_ready_count = counts.get("semantic_ready_count", 0) or 0
    partial_count = counts.get("partial_record_count", 0) or 0
    skipped_count = counts.get("evidence_skipped_count", 0) or 0
    threshold = lane_min_threshold(plan, lane)
    review_like_count = coverage.get("review_like_count", 0) or 0
    primary_like_count = coverage.get("primary_study_like_count", 0) or 0
    metrics_count = coverage.get("records_with_metrics", 0) or 0
    high_quality_count = coverage.get("high_quality_record_count", 0) or 0

    missing_angles = list(stage_missing_angles)
    actions = list(stage_actions)
    retry_suggested = False

    if threshold is not None and semantic_ready_count < threshold:
        retry_suggested = True
        missing_angles.append("semantic_ready_count_below_threshold")
        actions.append("increase_top_k")

    if high_quality_count == 0:
        retry_suggested = True
        missing_angles.append("no_high_quality_records")
        actions.append("prefer_accessible_records")

    if review_like_count > 0 and primary_like_count == 0:
        retry_suggested = True
        missing_angles.append("reviews_without_primary_studies")
        actions.append("boost_primary_study_queries")

    if metrics_count == 0:
        missing_angles.append("no_metrics_extracted")
        actions.append("increase_top_k")

    total_semantic = semantic_ready_count + skipped_count
    if total_semantic > 0 and skipped_count / total_semantic > 0.30:
        retry_suggested = True
        missing_angles.append("high_skipped_fraction")
        actions.append("swap_low_access_records")

    if semantic_ready_count > 0 and partial_count / semantic_ready_count > 0.50:
        missing_angles.append("high_partial_fraction")
        actions.append("prefer_accessible_records")

    missing_angles = dedupe_keep_order(missing_angles)
    actions = dedupe_keep_order(actions)

    priority = "low"
    if retry_suggested:
        if "reviews_without_primary_studies" in missing_angles or "semantic_ready_count_below_threshold" in missing_angles:
            priority = "high"
        else:
            priority = "medium"

    return {
        "retry_suggested": retry_suggested,
        "priority": priority,
        "missing_angles": missing_angles,
        "candidate_retry_actions": actions,
        "expected_min_semantic_ready_records": threshold,
        "observed_semantic_ready_records": semantic_ready_count,
        "threshold_met": (semantic_ready_count >= threshold) if threshold is not None else None,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Review cross-stage prose research coverage and generate a controller-ready report.")
    p.add_argument("--ranked-input", default="", help="Optional ranked_records JSON")
    p.add_argument("--resolved-input", default="", help="Optional resolved_records JSON")
    p.add_argument("--extracted-input", default="", help="Optional extracted_records JSON")
    p.add_argument("--evidence-input", required=True, help="Evidence records JSON from scripts/pipeline/prose_evidence_extract.py")
    p.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    p.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    p.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    p.add_argument("--schema-version", default="1.1", help="Schema version for coverage review artifact output.")
    p.add_argument("--write", default="", help="Optional output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    ranked = load_json(args.ranked_input)
    resolved = load_json(args.resolved_input)
    extracted = load_json(args.extracted_input)
    evidence = load_json(args.evidence_input)

    inferred_plan_path = (
        args.orchestration_plan
        or ((evidence.get("orchestration_context") or {}).get("plan_path"))
        or ((extracted.get("orchestration_context") or {}).get("plan_path"))
        or ((resolved.get("orchestration_context") or {}).get("plan_path"))
        or ((ranked.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    lane = (
        compact_ws(args.lane)
        or compact_ws(evidence.get("lane"))
        or compact_ws(extracted.get("lane"))
        or compact_ws(resolved.get("lane"))
        or compact_ws(ranked.get("lane"))
        or None
    )

    run_id = (
        args.run_id
        or evidence.get("run_id")
        or extracted.get("run_id")
        or resolved.get("run_id")
        or ranked.get("run_id")
        or plan.get("run_id")
        or None
    )

    evidence_records = evidence.get("evidence_records") or []
    partial_records = evidence.get("partial_records") or []
    skipped_records = evidence.get("skipped_records") or []

    counts = build_counts(ranked, resolved, extracted, evidence)
    coverage = build_coverage(evidence_records, partial_records)

    stage_feedback = {
        "normalize_rank": get_stage_feedback(ranked),
        "fulltext_resolve": get_stage_feedback(resolved),
        "fulltext_extract": get_stage_feedback(extracted),
        "evidence_extract": get_stage_feedback(evidence),
    }

    stage_missing_angles = dedupe_keep_order(
        get_stage_missing_angles(ranked)
        + get_stage_missing_angles(resolved)
        + get_stage_missing_angles(extracted)
        + get_stage_missing_angles(evidence)
    )
    stage_actions = dedupe_keep_order(
        get_stage_actions(ranked)
        + get_stage_actions(resolved)
        + get_stage_actions(extracted)
        + get_stage_actions(evidence)
    )

    retry_recommendation = build_retry_recommendation(
        lane=lane or "",
        plan=plan,
        counts=counts,
        coverage=coverage,
        stage_missing_angles=stage_missing_angles,
        stage_actions=stage_actions,
    )

    semantic_ready_records = evidence_records + partial_records

    output = {
        "schema_version": args.schema_version,
        "stage": "coverage_review",
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "lane": lane,
        "orchestration_context": {
            "plan_path": inferred_plan_path or None,
            "topic": plan.get("topic") or (evidence.get("orchestration_context") or {}).get("topic"),
            "lane_window": ((plan.get("lane_windows") or {}).get(lane)) if isinstance(plan.get("lane_windows"), dict) and lane else (evidence.get("orchestration_context") or {}).get("lane_window"),
            "lane_allocation": ((plan.get("lane_allocations") or {}).get(lane)) if isinstance(plan.get("lane_allocations"), dict) and lane else None,
        },
        "artifacts": {
            "ranked_input": args.ranked_input or None,
            "resolved_input": args.resolved_input or None,
            "extracted_input": args.extracted_input or None,
            "evidence_input": args.evidence_input,
        },
        "stage_inputs_available": {
            "ranked": bool(ranked),
            "resolved": bool(resolved),
            "extracted": bool(extracted),
            "evidence": bool(evidence),
        },
        "counts": counts,
        "distributions": {
            "semantic_ready_paper_kind_counts": count_by(semantic_ready_records, "paper_kind"),
            "semantic_ready_document_role_counts": count_by(semantic_ready_records, "document_role"),
            "semantic_ready_evidence_level_counts": count_by(semantic_ready_records, "evidence_level"),
            "semantic_ready_quality_counts": count_by(semantic_ready_records, "extraction_quality"),
            "semantic_ready_source_substance_counts": count_by(semantic_ready_records, "source_substance"),
        },
        "coverage": coverage,
        "stage_feedback": stage_feedback,
        "retry_recommendation": retry_recommendation,
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
