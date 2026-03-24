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


def print_cmd(cmd: list[str]) -> None:
    print("$ " + shlex.join(cmd))


def run_cmd(cmd: list[str], dry_run: bool = False) -> None:
    print_cmd(cmd)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


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


def infer_tag_from_patch_path(patch_path: Path) -> str:
    name = patch_path.name
    marker = ".shadow."
    if marker in name and name.endswith(".json"):
        return name.split(marker, 1)[1][:-5]
    return "latest"


def maybe_default_artifact(run_dir: Path, stem: str, tag: str) -> Path:
    return run_dir / "artifacts" / f"{stem}.{tag}.json"


def get_nested(d: dict[str, Any], path: str) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur.get(part)
    return cur


def validate_required_fields(payload: dict[str, Any], required_fields: list[str]) -> list[str]:
    errs = []
    for field in required_fields:
        val = payload.get(field)
        if val is None:
            errs.append(f"missing_required_field:{field}")
    return errs


def validate_required_based_on(payload: dict[str, Any], required_fields: list[str]) -> list[str]:
    errs = []
    based_on = payload.get("based_on")
    if not isinstance(based_on, dict):
        return ["missing_required_field:based_on"]
    for field in required_fields:
        val = based_on.get(field)
        if val is None or compact_ws(val) == "":
            errs.append(f"missing_required_based_on_field:{field}")
    return errs


def validate_allowed_values(payload: dict[str, Any], allowed_values: dict[str, list[Any]]) -> list[str]:
    errs = []
    for path, allowed in allowed_values.items():
        val = get_nested(payload, path)
        if val not in allowed:
            errs.append(f"invalid_value:{path}={val!r}")
    return errs


def validate_limits(payload: dict[str, Any], limits: dict[str, Any]) -> list[str]:
    errs = []
    query_changes = get_nested(payload, "proposal.query_changes") or {}
    search_overrides = get_nested(payload, "proposal.search_overrides") or {}
    rank_overrides = get_nested(payload, "proposal.rank_overrides") or {}

    append_terms = query_changes.get("append_terms") or []
    append_phrases = query_changes.get("append_phrases") or []
    remove_terms = query_changes.get("remove_terms") or []
    remove_phrases = query_changes.get("remove_phrases") or []

    if len(append_terms) > int(limits.get("max_append_terms", 999)):
        errs.append("append_terms_exceed_limit")
    if len(append_phrases) > int(limits.get("max_append_phrases", 999)):
        errs.append("append_phrases_exceed_limit")
    if len(remove_terms) > int(limits.get("max_remove_terms", 0)):
        errs.append("remove_terms_nonempty")
    if len(remove_phrases) > int(limits.get("max_remove_phrases", 0)):
        errs.append("remove_phrases_nonempty")

    max_results_multiplier = search_overrides.get("max_results_multiplier")
    if max_results_multiplier is not None:
        try:
            v = float(max_results_multiplier)
            if v < float(limits.get("max_results_multiplier_min", 1.0)) or v > float(limits.get("max_results_multiplier_max", 99.0)):
                errs.append("search_override_out_of_bounds:max_results_multiplier")
        except Exception:
            errs.append("search_override_out_of_bounds:max_results_multiplier")

    per_query_multiplier = search_overrides.get("per_query_multiplier")
    if per_query_multiplier is not None:
        try:
            v = float(per_query_multiplier)
            if v < float(limits.get("per_query_multiplier_min", 1.0)) or v > float(limits.get("per_query_multiplier_max", 99.0)):
                errs.append("search_override_out_of_bounds:per_query_multiplier")
        except Exception:
            errs.append("search_override_out_of_bounds:per_query_multiplier")

    max_per_journal = rank_overrides.get("max_per_journal")
    if max_per_journal is not None:
        try:
            v = int(max_per_journal)
            if v < int(limits.get("max_per_journal_min", 1)) or v > int(limits.get("max_per_journal_max", 99)):
                errs.append("rank_override_out_of_bounds:max_per_journal")
        except Exception:
            errs.append("rank_override_out_of_bounds:max_per_journal")

    return errs


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


def extract_query_anchors(query: str) -> tuple[list[str], list[str]]:
    q = compact_ws(query).lower()

    condition_candidates = [
        "major depressive disorder",
        "treatment-resistant depression",
        "depression",
        "mdd",
        "trd",
    ]
    intervention_candidates = [
        "intermittent theta burst stimulation",
        "continuous theta burst stimulation",
        "theta burst stimulation",
        "repetitive transcranial magnetic stimulation",
        "transcranial magnetic stimulation",
        "deep tms",
        "accelerated rtms",
        "itbs",
        "ctbs",
        "rtms",
    ]

    cond = [x for x in condition_candidates if x in q]
    interv = [x for x in intervention_candidates if x in q]
    return cond, interv


def record_text(rec: dict[str, Any]) -> str:
    parts = [
        rec.get("title"),
        rec.get("abstract"),
        rec.get("abstract_text"),
        rec.get("abstract_extracted"),
    ]
    return compact_ws(" ".join(str(x or "") for x in parts)).lower()


def is_duplicate_notice(rec: dict[str, Any]) -> bool:
    title = compact_ws(rec.get("title")).lower()
    return (
        "notice of duplicate publication" in title
        or "duplicate publication" in title
        or "corrigendum" in title
        or "erratum" in title
    )


def topical_filter_records(records: list[dict[str, Any]], query_text: str) -> list[dict[str, Any]]:
    cond_terms, interv_terms = extract_query_anchors(query_text or "")
    if not cond_terms and not interv_terms:
        return [r for r in records if not is_duplicate_notice(r)]

    filtered = []
    for rec in records:
        if is_duplicate_notice(rec):
            continue
        txt = record_text(rec)
        cond_ok = True if not cond_terms else any(term in txt for term in cond_terms)
        interv_ok = True if not interv_terms else any(term in txt for term in interv_terms)
        if cond_ok and interv_ok:
            filtered.append(rec)
    return filtered


def article_type_counts(records: list[dict[str, Any]], primary_types: set[str], review_types: set[str]) -> tuple[int, int]:
    primary = 0
    review = 0
    for rec in records:
        at = compact_ws(rec.get("article_type"))
        if at in primary_types and at not in review_types:
            primary += 1
        if at in review_types:
            review += 1
    return primary, review


def tier1_count(records: list[dict[str, Any]]) -> int:
    count = 0
    for rec in records:
        tier = compact_ws(rec.get("journal_tier")).lower()
        if tier in {"tier_1", "tier1"}:
            count += 1
    return count


def build_rank_metrics(ranked: dict[str, Any], primary_types: set[str], review_types: set[str], query_text: str = "") -> dict[str, Any]:
    kept = ranked.get("kept_records") or []
    kept = topical_filter_records(kept, query_text)
    primary, review = article_type_counts(kept, primary_types, review_types)
    pressure = round(review / max(primary, 1), 3)
    return {
        "ranked_kept_count": len(kept),
        "ranked_primary_candidate_count": primary,
        "ranked_review_like_count": review,
        "review_pressure": pressure,
        "tier1_count": tier1_count(kept),
    }


def build_resolve_metrics(resolved: dict[str, Any]) -> dict[str, Any]:
    stats = resolved.get("stats") or {}
    return {
        "resolved_count": safe_int(resolved.get("resolved_count"), len(resolved.get("resolved_records") or [])),
        "fulltext_yield_rate": safe_float(stats.get("analysis_ready_rate"), 0.0),
    }


def build_attempt_paths(run_dir: Path, tag: str) -> dict[str, Path]:
    artifacts_dir = run_dir / "artifacts"
    fulltext_dir = run_dir / "fulltext" / f"planner_shadow_{tag}"
    cache_dir = run_dir / "cache"

    return {
        "retrieval": artifacts_dir / f"planner_shadow_retrieval.{tag}.json",
        "ranked": artifacts_dir / f"planner_shadow_ranked.{tag}.json",
        "resolved": artifacts_dir / f"planner_shadow_resolved.{tag}.json",
        "eval": artifacts_dir / f"planner_shadow_eval.{tag}.json",
        "fulltext_dir": fulltext_dir,
        "resolver_cache": cache_dir / f"planner_shadow_resolver.{tag}.json",
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Validate and cheaply evaluate a planner sub-agent query patch against the controller acceptance spec.")
    p.add_argument("--planner-patch", required=True, help="Planner sub-agent patch JSON, usually planner_query_patch.shadow.<tag>.json")
    p.add_argument("--acceptance-spec", default=".prose/templates/subagents/planner_query_patch.acceptance_spec.json", help="Planner patch controller acceptance spec JSON")
    p.add_argument("--baseline-retrieval-input", default="", help="Optional baseline retrieval artifact")
    p.add_argument("--baseline-ranked-input", default="", help="Optional baseline ranked artifact")
    p.add_argument("--baseline-resolved-input", default="", help="Optional baseline resolved artifact")
    p.add_argument("--controller-input", default="", help="Optional controller decision override")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration plan override")
    p.add_argument("--python-bin", default=sys.executable, help="Python interpreter to use for subprocess stage execution")
    p.add_argument("--dry-run", action="store_true", help="Print planned commands without executing them")
    p.add_argument("--write", default="", help="Optional output path override")
    return p


def main() -> int:
    args = build_parser().parse_args()

    planner_patch_path = Path(args.planner_patch)
    planner = load_json(str(planner_patch_path))
    spec = load_json(args.acceptance_spec)

    based_on = planner.get("based_on") or {}
    controller_input = args.controller_input or based_on.get("controller_decision") or ""
    controller = load_json(controller_input) if controller_input else {}

    inferred_plan_path = (
        args.orchestration_plan
        or based_on.get("orchestration_plan")
        or ((controller.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    run_id = compact_ws(planner.get("run_id")) or compact_ws(controller.get("run_id")) or compact_ws(plan.get("run_id"))
    lane = compact_ws(planner.get("lane")) or compact_ws(controller.get("lane")) or "default"
    if not run_id:
        raise SystemExit("Could not infer run_id from planner patch or context")

    run_dir = resolve_run_dir(inferred_plan_path, run_id)
    tag = infer_tag_from_patch_path(planner_patch_path)
    paths = build_attempt_paths(run_dir, tag)

    baseline_retrieval_path = Path(args.baseline_retrieval_input) if args.baseline_retrieval_input else maybe_default_artifact(run_dir, "retrieval_records", tag)
    baseline_ranked_path = Path(args.baseline_ranked_input) if args.baseline_ranked_input else maybe_default_artifact(run_dir, "ranked_records", tag)
    baseline_resolved_path = Path(args.baseline_resolved_input) if args.baseline_resolved_input else maybe_default_artifact(run_dir, "resolved_records", tag)

    if not baseline_retrieval_path.exists():
        alt = latest_matching(run_dir / "artifacts", "retrieval_records*.json")
        baseline_retrieval_path = alt or baseline_retrieval_path
    if not baseline_ranked_path.exists():
        alt = latest_matching(run_dir / "artifacts", "ranked_records*.json")
        baseline_ranked_path = alt or baseline_ranked_path
    if not baseline_resolved_path.exists():
        alt = latest_matching(run_dir / "artifacts", "resolved_records*.json")
        baseline_resolved_path = alt or baseline_resolved_path

    errors = []
    errors.extend(validate_required_fields(planner, spec.get("required_fields") or []))
    errors.extend(validate_required_based_on(planner, spec.get("required_based_on_fields") or []))
    errors.extend(validate_allowed_values(planner, spec.get("allowed_values") or {}))
    errors.extend(validate_limits(planner, spec.get("limits") or {}))

    confidence = compact_ws(planner.get("confidence"))
    if confidence not in {"medium", "high"}:
        errors.append("planner_confidence_below_shadow_eval_threshold")

    if not baseline_retrieval_path or not Path(baseline_retrieval_path).exists():
        errors.append("missing_baseline_retrieval_input")
    if not baseline_ranked_path or not Path(baseline_ranked_path).exists():
        errors.append("missing_baseline_ranked_input")
    if not baseline_resolved_path or not Path(baseline_resolved_path).exists():
        errors.append("missing_baseline_resolved_input")

    baseline_retrieval = load_json(str(baseline_retrieval_path)) if not errors else {}
    baseline_ranked = load_json(str(baseline_ranked_path)) if not errors else {}
    baseline_resolved = load_json(str(baseline_resolved_path)) if not errors else {}

    primary_types = set(spec.get("primary_candidate_article_types") or [])
    review_types = set(spec.get("review_like_article_types") or [])

    baseline_query = baseline_retrieval.get("query") if baseline_retrieval else ""
    baseline_rank_metrics = build_rank_metrics(baseline_ranked, primary_types, review_types, baseline_query) if baseline_ranked else {}
    baseline_resolve_metrics = build_resolve_metrics(baseline_resolved) if baseline_resolved else {}
    baseline_metrics = {**baseline_rank_metrics, **baseline_resolve_metrics}

    controller_can_retry = bool(((controller.get("controller_policy") or {}).get("can_retry")))
    controller_decision = compact_ws(controller.get("decision"))
    eligible_for_shadow = (not errors)

    outcome = "reject_invalid"
    rationale = ""
    promoted_current_run_patch = None
    future_run_patch_candidate = None
    branch_metrics = {}
    branch_paths = {k: str(v) for k, v in paths.items() if k in {"retrieval", "ranked", "resolved"}}

    if not eligible_for_shadow:
        rationale = "Planner patch failed validation or lacked required baseline inputs."
    elif args.dry_run:
        outcome = "accept_shadow_eval_only"
        rationale = "Planner patch validated. Dry-run requested, so shadow branch was not executed."
    else:
        proposal = planner.get("proposal") or {}
        query_changes = proposal.get("query_changes") or {}
        search_overrides = proposal.get("search_overrides") or {}
        rank_overrides = proposal.get("rank_overrides") or {}
        backend_queries = proposal.get("backend_queries") or {}

        baseline_query = baseline_retrieval.get("query") or ""
        explicit_pubmed_query = compact_ws(backend_queries.get("pubmed_query"))
        base_query = explicit_pubmed_query or baseline_query

        base_terms = baseline_retrieval.get("custom_terms") or []
        preserve_existing_terms = bool(query_changes.get("preserve_existing_terms", True))

        working_terms = list(base_terms) if preserve_existing_terms else []
        working_terms.extend(query_changes.get("append_terms") or [])
        working_terms.extend(query_changes.get("append_phrases") or [])
        remove_terms = set(compact_ws(x) for x in (query_changes.get("remove_terms") or []) if compact_ws(x))
        remove_phrases = set(compact_ws(x) for x in (query_changes.get("remove_phrases") or []) if compact_ws(x))

        # If an explicit backend query is present, use it as the executable query
        # and suppress loose term augmentation for execution.
        if explicit_pubmed_query:
            next_terms = []
        else:
            next_terms = [
                t for t in dedupe_keep_order(working_terms)
                if compact_ws(t) not in remove_terms and compact_ws(t) not in remove_phrases
            ]

        base_mode = compact_ws(search_overrides.get("mode") or baseline_retrieval.get("mode") or "hybrid")
        base_journal_set = compact_ws(search_overrides.get("journal_set") or baseline_retrieval.get("journal_set") or "tier1")
        if base_journal_set == "default":
            base_journal_set = "off"
        base_max_results = safe_int(baseline_retrieval.get("max_results"), 10)
        base_per_query = safe_int(baseline_retrieval.get("per_query"), 5)
        base_journal_retmax = safe_int(baseline_retrieval.get("journal_retmax"), 10)

        next_max_results = scaled_int(base_max_results, safe_float(search_overrides.get("max_results_multiplier"), 1.0), minimum=1)
        next_per_query = scaled_int(base_per_query, safe_float(search_overrides.get("per_query_multiplier"), 1.0), minimum=1)

        base_rank_top_k = safe_int(baseline_ranked.get("top_k"), 0)
        if base_rank_top_k <= 0:
            base_rank_top_k = max(len(baseline_ranked.get("kept_records") or []), 4)

        next_rank_top_k = base_rank_top_k
        next_journal_priority = compact_ws(rank_overrides.get("journal_priority") or baseline_ranked.get("journal_priority") or "default")
        next_max_per_journal = rank_overrides.get("max_per_journal")
        if next_max_per_journal is None:
            next_max_per_journal = safe_int(baseline_ranked.get("max_per_journal"), 0)
        next_min_score = safe_float(baseline_ranked.get("min_score"), 5.0)

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

        cmds = [
            ("search", search_cmd),
            ("normalize_rank", rank_cmd),
            ("fulltext_resolve", resolve_cmd),
        ]

        for stage_name, cmd in cmds:
            print(f"\n## {stage_name}")
            run_cmd(cmd, dry_run=False)

        branch_ranked = load_json(str(paths["ranked"]))
        branch_resolved = load_json(str(paths["resolved"]))

        branch_rank_metrics = build_rank_metrics(branch_ranked, primary_types, review_types, base_query)
        branch_resolve_metrics = build_resolve_metrics(branch_resolved)
        branch_metrics = {**branch_rank_metrics, **branch_resolve_metrics}

        eval_plan = planner.get("evaluation_plan") or {}
        success = ((eval_plan.get("success_metrics") or {}))
        target_deltas = success.get("target_deltas") or {}
        guardrails = dict(spec.get("default_guardrails") or {})
        guardrails.update((success.get("guardrails") or {}))

        baseline_primary = safe_int(baseline_metrics.get("ranked_primary_candidate_count"), 0)
        branch_primary = safe_int(branch_metrics.get("ranked_primary_candidate_count"), 0)
        baseline_review_pressure = safe_float(baseline_metrics.get("review_pressure"), 0.0)
        branch_review_pressure = safe_float(branch_metrics.get("review_pressure"), 0.0)
        baseline_tier1 = safe_int(baseline_metrics.get("tier1_count"), 0)
        branch_tier1 = safe_int(branch_metrics.get("tier1_count"), 0)
        baseline_yield = safe_float(baseline_metrics.get("fulltext_yield_rate"), 0.0)
        branch_yield = safe_float(branch_metrics.get("fulltext_yield_rate"), 0.0)

        required_primary_delta = safe_int(target_deltas.get("primary_study_like_count"), 1)
        required_semantic_delta = safe_int(target_deltas.get("semantic_ready_count"), 0)

        min_fulltext_yield_rate = safe_float(guardrails.get("min_fulltext_yield_rate"), 0.8)
        max_yield_drop = safe_float(guardrails.get("max_fulltext_yield_drop_from_baseline"), 0.10)
        min_tier1_count_delta = safe_int(guardrails.get("min_tier1_count_delta"), 0)
        max_tier1_drop = safe_int(guardrails.get("max_tier1_count_drop_from_baseline"), 3)

        primary_delta = branch_primary - baseline_primary
        tier1_delta = branch_tier1 - baseline_tier1
        yield_floor = max(min_fulltext_yield_rate, baseline_yield - max_yield_drop)
        tier1_floor = max(0, baseline_tier1 - max_tier1_drop, baseline_tier1 + min_tier1_count_delta if min_tier1_count_delta > 0 else 0)

        passes = {
            "primary_delta_ok": primary_delta >= required_primary_delta,
            "yield_ok": branch_yield >= yield_floor,
            "review_pressure_ok": branch_review_pressure <= baseline_review_pressure,
            "tier1_ok": branch_tier1 >= tier1_floor,
        }

        if all(passes.values()):
            if controller_can_retry or controller_decision == "retry":
                outcome = "promote_to_current_run_patch"
                rationale = "Shadow branch improved cheap-eval metrics and current run can still accept a bounded patch."
                promoted_current_run_patch = {
                    "source_subagent": planner.get("subagent_id"),
                    "source_artifact": str(planner_patch_path),
                    "action": get_nested(planner, "proposal.action_family"),
                    "scope": "current_run",
                    "ttl": "run_only",
                    "payload": {
                        "query_changes": get_nested(planner, "proposal.query_changes"),
                        "search_overrides": get_nested(planner, "proposal.search_overrides"),
                        "rank_overrides": get_nested(planner, "proposal.rank_overrides"),
                    },
                }
            else:
                outcome = "save_for_future_run_memory"
                rationale = "Shadow branch looked promising, but current run cannot accept another retry."
                future_run_patch_candidate = {
                    "source_subagent": planner.get("subagent_id"),
                    "source_artifact": str(planner_patch_path),
                    "action": get_nested(planner, "proposal.action_family"),
                    "scope": "future_run",
                    "ttl": "persist",
                    "payload": {
                        "query_changes": get_nested(planner, "proposal.query_changes"),
                        "search_overrides": get_nested(planner, "proposal.search_overrides"),
                        "rank_overrides": get_nested(planner, "proposal.rank_overrides"),
                    },
                }
        else:
            outcome = "reject_invalid"
            rationale = "Shadow branch failed one or more cheap-eval guardrails."

    output = {
        "schema_version": "1.0",
        "artifact_type": "subagent_eval",
        "stage": "planner_shadow_eval",
        "subagent_id": compact_ws(planner.get("subagent_id")) or "planner_agent",
        "subagent_role": compact_ws(planner.get("subagent_role")) or "query_planner",
        "parent_agent": compact_ws(planner.get("parent_agent")) or "prose_research_agent",
        "run_id": run_id,
        "lane": lane,
        "generated_at": utc_now_iso(),
        "mode": "shadow",
        "openprose_context": {
            "program_file": get_nested(planner, "openprose_context.program_file"),
            "run_dir": str(run_dir),
            "bindings_dir": get_nested(planner, "openprose_context.bindings_dir"),
            "controller_contract": get_nested(planner, "openprose_context.controller_contract"),
        },
        "source_artifacts": {
            "planner_patch": str(planner_patch_path),
            "acceptance_spec": args.acceptance_spec,
            "baseline_retrieval": str(baseline_retrieval_path) if baseline_retrieval_path else None,
            "baseline_ranked": str(baseline_ranked_path) if baseline_ranked_path else None,
            "baseline_resolved": str(baseline_resolved_path) if baseline_resolved_path else None,
            "controller_input": controller_input or None,
        },
        "validation": {
            "valid": len(errors) == 0,
            "errors": errors,
        },
        "controller_context": {
            "controller_decision": controller_decision or None,
            "controller_can_retry": controller_can_retry,
        },
        "executed_query_context": {
            "used_explicit_backend_query": bool(explicit_pubmed_query) if 'explicit_pubmed_query' in locals() else False,
            "explicit_pubmed_query": explicit_pubmed_query if 'explicit_pubmed_query' in locals() else None,
            "baseline_query": baseline_query if 'baseline_query' in locals() else None,
            "executed_query": base_query if 'base_query' in locals() else None,
            "executed_terms": next_terms if 'next_terms' in locals() else [],
        },
        "baseline_metrics": baseline_metrics,
        "branch_metrics": branch_metrics,
        "branch_artifacts": branch_paths,
        "controller_outcome": outcome,
        "rationale": rationale,
        "promoted_current_run_patch": promoted_current_run_patch,
        "future_run_patch_candidate": future_run_patch_candidate,
    }

    write_path = args.write or str(paths["eval"])
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
