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
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


GENERALIZABLE_ACTIONS = {
    "prefer_accessible_records",
    "boost_primary_study_queries",
    "broaden_query_or_raise_top_k",
}


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


def infer_max_additional_passes(plan: dict[str, Any], cli_value: int | None) -> int:
    if cli_value is not None:
        return cli_value
    retry_policy = plan.get("retry_policy") or {}
    try:
        return int(retry_policy.get("max_additional_passes", 1))
    except Exception:
        return 1


def retry_enabled(plan: dict[str, Any]) -> bool:
    retry_policy = plan.get("retry_policy") or {}
    if "enabled" in retry_policy:
        return bool(retry_policy.get("enabled"))
    return True


def choose_action(
    retry_actions: list[str],
    missing_angles: list[str],
    counts: dict[str, Any],
    coverage: dict[str, Any],
    stage_feedback: dict[str, Any],
) -> tuple[str | None, str]:
    actions = dedupe_keep_order(retry_actions)
    missing = set(missing_angles)

    fulltext_yield = counts.get("fulltext_yield_rate")
    semantic_ready_rate = counts.get("semantic_ready_rate")
    primary_count = coverage.get("primary_study_like_count", 0) or 0
    review_count = coverage.get("review_like_count", 0) or 0

    if "reviews_without_primary_studies" in missing and "boost_primary_study_queries" in actions:
        return "boost_primary_study_queries", "No primary empirical coverage despite review-like material."

    if (
        ("no_high_quality_records" in missing or "low_fulltext_yield" in missing or "high_partial_fraction" in missing)
        and "prefer_accessible_records" in actions
    ):
        return "prefer_accessible_records", "Accessible structured full text should improve semantic yield."

    if (
        ("high_skipped_fraction" in missing or "too_many_preview_only_or_low_substance_records" in missing)
        and "swap_low_access_records" in actions
    ):
        return "swap_low_access_records", "Swap weak preview-like records for stronger accessible candidates."

    if (
        "semantic_ready_count_below_threshold" in missing
        and "increase_top_k" in actions
        and (fulltext_yield is None or fulltext_yield >= 0.8)
        and (semantic_ready_rate is None or semantic_ready_rate >= 0.6)
    ):
        return "increase_top_k", "Pipeline quality is good, but semantic-ready count is still below target."

    if "journal_concentration_detected" in missing and "broaden_query_or_raise_top_k" in actions:
        return "broaden_query_or_raise_top_k", "Current ranked pool is concentrated and may benefit from broader candidate diversity."

    if primary_count == 0 and review_count > 0 and "boost_primary_study_queries" in actions:
        return "boost_primary_study_queries", "Need more original-data papers, not just reviews."

    if actions:
        return actions[0], "Using highest-priority available retry action from coverage review."

    return None, "No retry action available."


def build_action_payload(action: str | None, lane: str | None) -> dict[str, Any]:
    lane = compact_ws(lane)

    if action == "increase_top_k":
        return {
            "search_overrides": {
                "max_results_multiplier": 1.5,
                "per_query_multiplier": 1.5,
            },
            "rank_overrides": {
                "top_k_increment": 4,
            },
            "notes": "Keep current query family and quality settings, expand candidate pool modestly.",
        }

    if action == "broaden_query_or_raise_top_k":
        return {
            "search_overrides": {
                "mode": "hybrid",
                "broaden_query": True,
                "journal_set": "tier1",
                "max_results_multiplier": 1.5,
            },
            "rank_overrides": {
                "top_k_increment": 4,
                "journal_priority": "strict",
                "max_per_journal": 1,
            },
            "notes": "Broaden candidate diversity while preserving tier-1 preference and journal diversity cap.",
        }

    if action == "prefer_accessible_records":
        return {
            "search_overrides": {
                "mode": "accessible",
                "journal_set": "tier1",
            },
            "rank_overrides": {
                "journal_priority": "strict",
                "max_per_journal": 1,
            },
            "resolver_overrides": {
                "require_fulltext": True,
            },
            "notes": "Bias toward records with better full-text accessibility and extraction potential.",
        }

    if action == "swap_low_access_records":
        return {
            "resolver_overrides": {
                "require_fulltext": True,
            },
            "selection_overrides": {
                "drop_preview_only_records": True,
                "promote_next_best_accessible_candidate": True,
            },
            "notes": "Replace low-substance preview records with stronger accessible alternates.",
        }

    if action == "boost_primary_study_queries":
        return {
            "search_overrides": {
                "mode": "hybrid",
                "extra_terms": [
                    "trial",
                    "randomized",
                    "open-label",
                    "cohort",
                    "participants",
                    "patients",
                ],
                "journal_set": "tier1",
            },
            "rank_overrides": {
                "journal_priority": "strict",
                "max_per_journal": 1,
            },
            "notes": "Bias next pass toward original-data papers and away from review-heavy retrieval.",
        }

    return {
        "notes": "No specific override payload available.",
    }


def build_future_run_patch_candidates(
    retry_actions: list[str],
    missing_angles: list[str],
    chosen_action: str | None,
    lane: str | None,
) -> list[dict[str, Any]]:
    actions = dedupe_keep_order(retry_actions)
    missing = set(missing_angles)
    candidates: list[dict[str, Any]] = []

    def add_candidate(action: str, note: str) -> None:
        if action not in GENERALIZABLE_ACTIONS:
            return
        if any(c.get("action") == action for c in candidates):
            return
        candidates.append(
            {
                "action": action,
                "ttl": "persist",
                "note": note,
                "payload": build_action_payload(action, lane),
            }
        )

    if "reviews_without_primary_studies" in missing and "boost_primary_study_queries" in actions:
        add_candidate("boost_primary_study_queries", "Repeated review-heavy retrieval suggests future planner bias toward primary empirical studies.")

    if (
        "journal_concentration_detected" in missing
        or "semantic_ready_count_below_threshold" in missing
    ) and "broaden_query_or_raise_top_k" in actions:
        add_candidate("broaden_query_or_raise_top_k", "Future runs may benefit from broader candidate diversity while preserving tier-1 focus.")

    if (
        "high_partial_fraction" in missing
        or "high_skipped_fraction" in missing
        or "too_many_preview_only_or_low_substance_records" in missing
        or "no_high_quality_records" in missing
    ) and "prefer_accessible_records" in actions:
        add_candidate("prefer_accessible_records", "Future runs may benefit from an accessibility-first bias for stronger semantic yield.")

    if chosen_action in GENERALIZABLE_ACTIONS:
        add_candidate(chosen_action, "Chosen bounded action may be worth rechecking in future scheduled runs.")

    return candidates


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Controller for prose research runs, chooses retry vs stop and one bounded next action.")
    p.add_argument("--coverage-input", required=True, help="coverage_report.json from scripts/pipeline/prose_coverage_review.py")
    p.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    p.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    p.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    p.add_argument("--attempt-number", type=int, default=0, help="Current retry attempt number, starting at 0.")
    p.add_argument("--max-additional-passes", type=int, default=None, help="Optional override for retry limit.")
    p.add_argument("--force-stop", action="store_true", help="Force a stop decision regardless of coverage review.")
    p.add_argument("--schema-version", default="1.2", help="Schema version for controller artifact output.")
    p.add_argument("--write", default="", help="Optional output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    coverage = load_json(args.coverage_input)
    inferred_plan_path = (
        args.orchestration_plan
        or ((coverage.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    lane = compact_ws(args.lane) or compact_ws(coverage.get("lane")) or None
    run_id = args.run_id or coverage.get("run_id") or plan.get("run_id") or None

    counts = coverage.get("counts") or {}
    coverage_summary = coverage.get("coverage") or {}
    retry_rec = coverage.get("retry_recommendation") or {}
    stage_feedback = coverage.get("stage_feedback") or {}

    retry_limit = infer_max_additional_passes(plan, args.max_additional_passes)
    can_retry = retry_enabled(plan) and (args.attempt_number < retry_limit)

    missing_angles = dedupe_keep_order(retry_rec.get("missing_angles") or [])
    retry_actions = dedupe_keep_order(retry_rec.get("candidate_retry_actions") or [])

    chosen_action = None
    rationale = ""
    decision = "stop"
    priority = retry_rec.get("priority") or "low"

    if args.force_stop:
        decision = "stop"
        rationale = "Forced stop requested by operator."
    elif not retry_rec.get("retry_suggested"):
        decision = "stop"
        rationale = "Coverage review did not recommend another pass."
    elif not can_retry:
        decision = "stop"
        rationale = "Retry was suggested, but max additional passes has already been reached."
    else:
        chosen_action, rationale = choose_action(
            retry_actions=retry_actions,
            missing_angles=missing_angles,
            counts=counts,
            coverage=coverage_summary,
            stage_feedback=stage_feedback,
        )
        if chosen_action:
            decision = "retry"
        else:
            decision = "stop"
            rationale = "Retry was suggested, but no actionable bounded next step could be selected."

    current_run_patch = None
    patch_scope = "none"
    patch_ttl = "none"
    if decision == "retry" and chosen_action:
        patch_scope = "current_run"
        patch_ttl = "run_only"
        current_run_patch = {
            "action": chosen_action,
            "ttl": patch_ttl,
            "payload": build_action_payload(chosen_action, lane),
        }

    future_run_patch_candidates = build_future_run_patch_candidates(
        retry_actions=retry_actions,
        missing_angles=missing_angles,
        chosen_action=chosen_action,
        lane=lane,
    )

    if patch_scope == "none" and future_run_patch_candidates:
        patch_scope = "future_run"
        patch_ttl = "persist"

    persist_for_future_runs = bool(future_run_patch_candidates)
    promote_candidate = any(c.get("action") in GENERALIZABLE_ACTIONS for c in future_run_patch_candidates)

    next_attempt_number = args.attempt_number + 1 if decision == "retry" else args.attempt_number

    output = {
        "schema_version": args.schema_version,
        "stage": "controller",
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "lane": lane,
        "orchestration_context": {
            "plan_path": inferred_plan_path or None,
            "topic": plan.get("topic") or (coverage.get("orchestration_context") or {}).get("topic"),
            "lane_window": ((plan.get("lane_windows") or {}).get(lane)) if isinstance(plan.get("lane_windows"), dict) and lane else (coverage.get("orchestration_context") or {}).get("lane_window"),
            "lane_allocation": ((plan.get("lane_allocations") or {}).get(lane)) if isinstance(plan.get("lane_allocations"), dict) and lane else (coverage.get("orchestration_context") or {}).get("lane_allocation"),
        },
        "controller_policy": {
            "retry_enabled": retry_enabled(plan),
            "attempt_number": args.attempt_number,
            "max_additional_passes": retry_limit,
            "can_retry": can_retry,
            "next_attempt_number": next_attempt_number,
        },
        "decision": decision,
        "priority": priority,
        "chosen_action": chosen_action,
        "rationale": rationale,
        "patch_scope": patch_scope,
        "patch_ttl": patch_ttl,
        "persist_for_future_runs": persist_for_future_runs,
        "promote_candidate": promote_candidate,
        "current_run_patch": current_run_patch,
        "future_run_patch_candidates": future_run_patch_candidates,
        "coverage_snapshot": {
            "counts": counts,
            "coverage": coverage_summary,
            "retry_recommendation": retry_rec,
        },
        "source_artifacts": {
            "coverage_input": args.coverage_input,
        },
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
