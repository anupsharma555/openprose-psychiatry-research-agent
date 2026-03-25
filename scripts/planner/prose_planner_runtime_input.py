#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import copy
import json
import re
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


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


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


def resolve_run_dir(plan_path: str, run_id: str) -> Path:
    if plan_path:
        p = Path(plan_path)
        if p.name == "orchestration_plan.json":
            return p.parent.parent
    return Path(".prose") / "runs" / run_id


def infer_tag_from_artifact_path(path: Path, prefixes: list[str]) -> str:
    name = path.name
    for prefix in prefixes:
        if name.startswith(prefix) and name.endswith(".json"):
            return name[len(prefix):-5]
    return "latest"


def maybe_default_artifact(run_dir: Path, stem: str, tag: str) -> Path:
    return run_dir / "artifacts" / f"{stem}.{tag}.json"



def phrase_regex(phrase: str) -> str:
    parts = [re.escape(x) for x in compact_ws(phrase).lower().split()]
    if not parts:
        return r"$a"
    return r"\b" + r"\s+".join(parts) + r"\b"


def phrase_in_text(text: str, phrase: str) -> bool:
    low = compact_ws(text).lower()
    if not low:
        return False
    return re.search(phrase_regex(phrase), low) is not None

def build_recent_notes(lane_entry: dict[str, Any], limit: int = 3) -> list[str]:
    recent_runs = lane_entry.get("recent_runs") or []
    notes = []
    for rec in recent_runs[-limit:]:
        decision = compact_ws(rec.get("decision")) or "unknown"
        chosen_action = compact_ws(rec.get("chosen_action"))
        missing = rec.get("missing_angles") or []
        missing_short = ", ".join(missing[:2]) if missing else "no major missing angles recorded"
        if chosen_action:
            notes.append(f"{decision}: {chosen_action}, {missing_short}")
        else:
            notes.append(f"{decision}: {missing_short}")
    return notes


def ordered_matches(text: str, phrases: list[str]) -> list[str]:
    low = compact_ws(text).lower()
    out = []
    seen = set()
    for phrase in phrases:
        if phrase.lower() in low and phrase not in seen:
            seen.add(phrase)
            out.append(phrase)
    return out


def derive_topic_concepts(topic: str, missing_angles: list[str] | None = None) -> dict[str, list[str]]:
    missing_angles = missing_angles or []
    low = compact_ws(topic).lower()

    intervention_catalog = [
        "intermittent theta burst stimulation",
        "continuous theta burst stimulation",
        "theta burst stimulation",
        "repetitive transcranial magnetic stimulation",
        "transcranial magnetic stimulation",
        "deep tms",
        "accelerated rtms",
        "rtms",
        "itbs",
        "ctbs",
        "tms",
        "tdcs",
        "dbs",
        "esketamine",
        "ketamine",
        "electroconvulsive therapy",
        "ect",
        "focused ultrasound",
        "ultrasound"
    ]

    condition_catalog = [
        "treatment-resistant depression",
        "major depressive disorder",
        "bipolar depression",
        "depression",
        "obsessive-compulsive disorder",
        "ocd",
        "schizophrenia",
        "anxiety",
        "ptsd",
        "post-traumatic stress disorder"
    ]

    qualifier_catalog = [
        "randomized",
        "trial",
        "cohort",
        "prospective",
        "retrospective",
        "observational",
        "real-world",
        "adolescent",
        "late-life",
        "treatment-resistant"
    ]

    intervention = ordered_matches(low, intervention_catalog)
    condition = ordered_matches(low, condition_catalog)
    optional_qualifiers = ordered_matches(low, qualifier_catalog)

    exclusion_or_noise_terms = []
    if "reviews_without_primary_studies" in missing_angles:
        exclusion_or_noise_terms.extend(["review", "systematic review", "meta-analysis"])
    if "journal_concentration_detected" in missing_angles:
        exclusion_or_noise_terms.extend(["duplicate publication"])

    return {
        "intervention": intervention,
        "condition": condition,
        "optional_qualifiers": list(dict.fromkeys(optional_qualifiers)),
        "exclusion_or_noise_terms": list(dict.fromkeys(exclusion_or_noise_terms)),
    }



def derive_concept_policy(topic: str, topic_concepts: dict[str, list[str]]) -> dict[str, Any]:
    low = compact_ws(topic).lower()

    must_have = []
    optional = []
    broadening = []

    # Intervention logic, keep esketamine strict if user asked for it.
    if phrase_in_text(low, "esketamine"):
        must_have.append("esketamine")

    # Only keep ketamine as must-have if user explicitly asked for ketamine separately.
    explicit_ketamine = phrase_in_text(low, "ketamine")
    explicit_esketamine = phrase_in_text(low, "esketamine")
    if explicit_ketamine and not explicit_esketamine:
        must_have.append("ketamine")
    elif explicit_esketamine:
        broadening.append("ketamine")

    # Conditions
    for cond in topic_concepts.get("condition", []):
        if cond not in must_have:
            must_have.append(cond)

    # Biomarker-like terms stay must-have when user asked for them
    biomarker_terms = [
        "biomarker", "biomarkers", "inflammatory biomarker", "inflammatory biomarkers",
        "inflammation", "inflammatory", "cytokine", "cytokines", "crp",
        "il-6", "il-8", "ifn", "c4", "eeg", "entropy", "connectivity",
        "predictor", "predictive", "response"
    ]
    for term in biomarker_terms:
        if phrase_in_text(low, term) and term not in must_have:
            must_have.append(term)

    # Optional qualifiers
    for x in topic_concepts.get("optional_qualifiers", []):
        if x not in optional and x not in must_have:
            optional.append(x)

    # Broadening concepts
    if "major depressive disorder" in must_have and "depression" not in must_have:
        broadening.append("depression")
    if "treatment-resistant depression" in must_have and "major depressive disorder" not in must_have:
        broadening.append("major depressive disorder")
    if "biomarker" in must_have and "predictor" not in must_have:
        broadening.append("predictor")
    if "biomarkers" in must_have and "response" not in must_have:
        broadening.append("response")

    return {
        "must_have_concepts": list(dict.fromkeys(must_have)),
        "optional_concepts": list(dict.fromkeys(optional)),
        "broadening_concepts": list(dict.fromkeys(broadening)),
        "require_at_least_one_family_to_preserve_all_must_have": True,
        "target_families_preserving_all_must_have": 2,
    }


def derive_short_note(coverage: dict[str, Any]) -> str:
    counts = coverage.get("counts") or {}
    cov = coverage.get("coverage") or {}
    retry = coverage.get("retry_recommendation") or {}

    semantic_ready = counts.get("semantic_ready_count")
    threshold_met = retry.get("threshold_met")
    review_like = cov.get("review_like_count")
    primary_like = cov.get("primary_study_like_count")

    if threshold_met is True:
        return f"Semantic-ready target met, but retrieval mix remains {review_like} review-like vs {primary_like} primary-study-like."
    if threshold_met is False:
        return f"Semantic-ready target not yet met, with {review_like} review-like vs {primary_like} primary-study-like records."
    if semantic_ready is not None:
        return f"Semantic-ready count is {semantic_ready}, but threshold status is not explicit."
    return "Coverage artifact available, but threshold status is not explicit."

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build planner sub-agent runtime input from current OpenProse run artifacts.")
    p.add_argument("--template", default=".prose/templates/subagents/planner_runtime_input.template.json", help="Planner runtime input template JSON")
    p.add_argument("--controller-input", default="", help="Optional controller_decision JSON. If omitted, use latest under the run artifacts directory.")
    p.add_argument("--coverage-input", default="", help="Optional coverage_report JSON. If omitted, use latest under the run artifacts directory.")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory JSON path")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json override")
    p.add_argument("--run-id", default="", help="Optional run identifier override")
    p.add_argument("--lane", default="", help="Optional lane override")
    p.add_argument("--topic", default="", help="Optional topic override")
    p.add_argument("--write", default="", help="Optional output path override")
    return p


def main() -> int:
    args = build_parser().parse_args()

    template = load_json(args.template)
    if not template:
        raise SystemExit(f"Could not load template: {args.template}")

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
        controller = load_json(str(latest_controller)) if latest_controller else {}
        if controller and not args.controller_input:
            args.controller_input = str(latest_controller)

    if not coverage:
        latest_coverage = latest_matching(artifacts_dir, "coverage_report*.json")
        coverage = load_json(str(latest_coverage)) if latest_coverage else {}
        if coverage and not args.coverage_input:
            args.coverage_input = str(latest_coverage)

    lane = (
        compact_ws(args.lane)
        or compact_ws(controller.get("lane"))
        or compact_ws(coverage.get("lane"))
        or "default"
    )
    topic = (
        compact_ws(args.topic)
        or compact_ws((controller.get("orchestration_context") or {}).get("topic"))
        or compact_ws((coverage.get("orchestration_context") or {}).get("topic"))
        or compact_ws(plan.get("topic"))
        or "unknown_topic"
    )

    tag = "latest"
    if args.controller_input:
        tag = infer_tag_from_artifact_path(Path(args.controller_input), ["controller_decision."])
    elif args.coverage_input:
        tag = infer_tag_from_artifact_path(Path(args.coverage_input), ["coverage_report."])

    memory = load_json(args.memory_path)
    topic_key = slugify(topic)
    lane_key = lane or "default"
    lane_entry = (((memory.get("topics") or {}).get(topic_key) or {}).get("lanes") or {}).get(lane_key) or {}

    runtime = copy.deepcopy(template)
    runtime["template"] = False
    runtime["generated_at"] = utc_now_iso()
    runtime["run_id"] = run_id
    runtime["lane"] = lane
    runtime["topic"] = topic

    retry = coverage.get("retry_recommendation") or {}
    runtime["topic_concepts"] = derive_topic_concepts(
        topic=topic,
        missing_angles=retry.get("missing_angles") or [],
    )
    runtime["concept_policy"] = derive_concept_policy(
        topic=topic,
        topic_concepts=runtime["topic_concepts"],
    )

    runtime["openprose_context"]["program_file"] = str(run_dir / "program.prose")
    runtime["openprose_context"]["run_dir"] = str(run_dir)
    runtime["openprose_context"]["bindings_dir"] = str(run_dir / "bindings")
    runtime["openprose_context"]["artifacts_dir"] = str(artifacts_dir)

    controller_policy = controller.get("controller_policy") or {}
    runtime["current_controller_state"]["decision"] = controller.get("decision")
    runtime["current_controller_state"]["priority"] = controller.get("priority")
    runtime["current_controller_state"]["can_retry"] = controller_policy.get("can_retry")
    runtime["current_controller_state"]["attempt_number"] = controller_policy.get("attempt_number")
    runtime["current_controller_state"]["max_additional_passes"] = controller_policy.get("max_additional_passes")
    runtime["current_controller_state"]["patch_scope"] = controller.get("patch_scope")
    runtime["current_controller_state"]["patch_ttl"] = controller.get("patch_ttl")

    counts = coverage.get("counts") or {}
    cov = coverage.get("coverage") or {}
    retry = coverage.get("retry_recommendation") or {}
    runtime["current_coverage_summary"]["semantic_ready_count"] = counts.get("semantic_ready_count")
    runtime["current_coverage_summary"]["review_like_count"] = cov.get("review_like_count")
    runtime["current_coverage_summary"]["primary_study_like_count"] = cov.get("primary_study_like_count")
    runtime["current_coverage_summary"]["mixed_or_unclear_count"] = cov.get("mixed_or_unclear_count")
    runtime["current_coverage_summary"]["high_quality_record_count"] = cov.get("high_quality_record_count")
    runtime["current_coverage_summary"]["records_with_metrics"] = cov.get("records_with_metrics")
    runtime["current_coverage_summary"]["fulltext_yield_rate"] = counts.get("fulltext_yield_rate")
    runtime["current_coverage_summary"]["review_to_primary_ratio"] = cov.get("review_to_primary_ratio")
    runtime["current_coverage_summary"]["missing_angles"] = retry.get("missing_angles") or []
    runtime["current_coverage_summary"]["short_note"] = derive_short_note(coverage)

    runtime["recent_memory_summary"]["run_count"] = lane_entry.get("run_count")
    runtime["recent_memory_summary"]["decision_counts"] = lane_entry.get("decision_counts") or {}
    runtime["recent_memory_summary"]["missing_angle_counts"] = lane_entry.get("missing_angle_counts") or {}
    runtime["recent_memory_summary"]["future_patch_counts"] = lane_entry.get("future_patch_counts") or {}
    runtime["recent_memory_summary"]["promotion_watchlist"] = lane_entry.get("promotion_watchlist") or []
    runtime["recent_memory_summary"]["recent_notes"] = build_recent_notes(lane_entry)

    runtime["artifact_paths"]["orchestration_plan"] = inferred_plan_path or str(run_dir / "bindings" / "orchestration_plan.json")
    runtime["artifact_paths"]["coverage_report"] = args.coverage_input or str(maybe_default_artifact(run_dir, "coverage_report", tag))
    runtime["artifact_paths"]["controller_decision"] = args.controller_input or str(maybe_default_artifact(run_dir, "controller_decision", tag))
    runtime["artifact_paths"]["run_memory"] = args.memory_path
    runtime["artifact_paths"]["ranked_candidate_digest"] = str(maybe_default_artifact(run_dir, "planner_candidate_digest", tag))

    # Prefer newer bundle files if they exist
    bundle_spec = ".prose/templates/subagents/planner_query_bundle.acceptance_spec.json"
    bundle_template = ".prose/templates/subagents/planner_query_bundle.template.json"
    if Path(bundle_spec).exists():
        runtime["artifact_paths"]["acceptance_spec"] = bundle_spec
    if Path(bundle_template).exists():
        runtime["artifact_paths"]["planner_patch_template"] = bundle_template

    acceptance_spec_path = runtime["artifact_paths"].get("acceptance_spec")
    acceptance_spec = load_json(acceptance_spec_path) if acceptance_spec_path else {}

    if acceptance_spec:
        allowed_values = acceptance_spec.get("allowed_values") or {}
        limits = acceptance_spec.get("limits") or {}

        # Support older single-patch acceptance spec shape
        allowed_action_families = ((allowed_values.get("proposal.action_family")) or [])
        allowed_search_modes = ((allowed_values.get("proposal.search_overrides.mode")) or [])
        allowed_journal_sets = ((allowed_values.get("proposal.search_overrides.journal_set")) or [])
        allowed_rank_priority = ((allowed_values.get("proposal.rank_overrides.journal_priority")) or [])

        # Support newer bundle acceptance spec shape
        if not allowed_action_families:
            allowed_action_families = acceptance_spec.get("allowed_action_families") or []
        if not allowed_search_modes:
            allowed_search_modes = acceptance_spec.get("allowed_search_modes") or []
        if not allowed_journal_sets:
            allowed_journal_sets = acceptance_spec.get("allowed_journal_sets") or []
        if not allowed_rank_priority:
            allowed_rank_priority = acceptance_spec.get("allowed_rank_journal_priority") or []

        runtime["acceptance_constraints"]["allowed_action_families"] = [x for x in allowed_action_families if x is not None]
        runtime["acceptance_constraints"]["mode"] = "shadow"
        runtime["acceptance_constraints"]["scope"] = "current_run"
        runtime["acceptance_constraints"]["max_append_terms"] = limits.get("max_append_terms", runtime["acceptance_constraints"]["max_append_terms"])
        runtime["acceptance_constraints"]["max_append_phrases"] = limits.get("max_append_phrases", runtime["acceptance_constraints"]["max_append_phrases"])
        runtime["acceptance_constraints"]["allow_remove_terms"] = bool((limits.get("max_remove_terms", 0)) > 0)
        runtime["acceptance_constraints"]["allow_remove_phrases"] = bool((limits.get("max_remove_phrases", 0)) > 0)
        runtime["acceptance_constraints"]["allowed_search_modes"] = [x for x in allowed_search_modes if x is not None]
        runtime["acceptance_constraints"]["allowed_journal_sets"] = [x for x in allowed_journal_sets if x is not None]
        runtime["acceptance_constraints"]["allowed_rank_journal_priority"] = [x for x in allowed_rank_priority if x is not None]

    write_path = args.write or str(artifacts_dir / f"planner_runtime_input.{tag}.json")
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(runtime, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(runtime, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
