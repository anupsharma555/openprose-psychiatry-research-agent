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


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
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


def infer_tag(path: str) -> str:
    name = Path(path).name
    prefix = "planner_query_patch.shadow."
    if name.startswith(prefix) and name.endswith(".json"):
        return name[len(prefix):-5]
    return "latest"


def infer_run_dir(bundle: dict[str, Any]) -> Path:
    run_dir = compact_ws((bundle.get("openprose_context") or {}).get("run_dir"))
    if run_dir:
        return Path(run_dir)
    run_id = compact_ws(bundle.get("run_id"))
    return Path(".prose/runs") / run_id


def key_for_record(rec: dict[str, Any]) -> str:
    for k in ["pmid", "doi", "pmcid", "title"]:
        v = compact_ws(rec.get(k))
        if v:
            return f"{k}:{v.lower()}"
    return json.dumps(rec, sort_keys=True)


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



def concept_fidelity_summary(candidate: dict[str, Any], concept_policy: dict[str, Any]) -> dict[str, Any]:
    must_have = [compact_ws(x) for x in (concept_policy.get("must_have_concepts") or []) if compact_ws(x)]
    retained = [compact_ws(x) for x in (candidate.get("retained_must_have_concepts") or []) if compact_ws(x)]
    dropped = [compact_ws(x) for x in (candidate.get("dropped_must_have_concepts") or []) if compact_ws(x)]
    broadening = [compact_ws(x) for x in (candidate.get("broadening_concepts_used") or []) if compact_ws(x)]

    retained_set = set(retained)
    must_have_set = set(must_have)
    dropped_set = set(dropped)

    preserves_all = must_have_set.issubset(retained_set) and not dropped_set
    return {
        "must_have_concepts": must_have,
        "retained_must_have_concepts": retained,
        "dropped_must_have_concepts": dropped,
        "broadening_concepts_used": broadening,
        "preserves_all_must_have": preserves_all,
        "retained_must_have_count": len(retained),
        "dropped_must_have_count": len(dropped),
        "broadening_used_count": len(broadening),
    }


def concept_fidelity_score(summary: dict[str, Any]) -> float:
    score = 0.0
    if summary.get("preserves_all_must_have"):
        score += 10.0
    score += 2.0 * float(summary.get("retained_must_have_count", 0))
    score -= 4.0 * float(summary.get("dropped_must_have_count", 0))
    score -= 1.0 * float(summary.get("broadening_used_count", 0))
    return score


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
    c = 0
    for rec in records:
        tier = compact_ws(rec.get("journal_tier")).lower()
        if tier in {"tier1", "tier_1"}:
            c += 1
    return c


def rank_metrics(records: list[dict[str, Any]], primary_types: set[str], review_types: set[str], query_text: str = "") -> dict[str, Any]:
    records = topical_filter_records(records, query_text)
    primary, review = article_type_counts(records, primary_types, review_types)
    return {
        "ranked_kept_count": len(records),
        "ranked_primary_candidate_count": primary,
        "ranked_review_like_count": review,
        "review_pressure": round(review / max(primary, 1), 3),
        "tier1_count": tier1_count(records),
    }


def resolve_metrics(resolved_records: list[dict[str, Any]], unresolved_records: list[dict[str, Any]]) -> dict[str, Any]:
    processed = len(resolved_records) + len(unresolved_records)
    analysis_ready = sum(1 for rec in resolved_records if rec.get("analysis_ready"))
    return {
        "resolved_count": len(resolved_records),
        "fulltext_yield_rate": round(analysis_ready / processed, 3) if processed else 0.0,
    }


def hybrid_merge(eval_payloads: list[dict[str, Any]], per_family_quota: int, primary_types: set[str], review_types: set[str], query_text: str) -> dict[str, Any]:
    merged_ranked = []
    seen = set()
    merged_resolved = []
    merged_unresolved = []
    seen_res = set()

    for payload in eval_payloads:
        branch_artifacts = payload.get("branch_artifacts") or {}
        ranked_path = branch_artifacts.get("ranked")
        resolved_path = branch_artifacts.get("resolved")
        if ranked_path:
            ranked = load_json(ranked_path)
            for rec in (ranked.get("kept_records") or [])[:per_family_quota]:
                k = key_for_record(rec)
                if k not in seen:
                    seen.add(k)
                    merged_ranked.append(rec)
        if resolved_path:
            resolved = load_json(resolved_path)
            for rec in (resolved.get("resolved_records") or []):
                k = key_for_record(rec)
                if k not in seen_res:
                    seen_res.add(k)
                    merged_resolved.append(rec)
            for rec in (resolved.get("unresolved_records") or []):
                k = key_for_record(rec)
                if k not in seen_res:
                    seen_res.add(k)
                    merged_unresolved.append(rec)

    filtered_preview = topical_filter_records(merged_ranked, query_text)[:12]
    return {
        "rank_metrics": rank_metrics(merged_ranked, primary_types, review_types, query_text),
        "resolve_metrics": resolve_metrics(merged_resolved, merged_unresolved),
        "merged_ranked_count": len(merged_ranked),
        "merged_resolved_count": len(merged_resolved),
        "merged_unresolved_count": len(merged_unresolved),
        "hybrid_preview_titles": [rec.get("title") for rec in filtered_preview],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evaluate multi-family planner query bundles by reusing single-family shadow eval and comparing single-family vs hybrid strategies.")
    p.add_argument("--planner-bundle", required=True, help="Planner bundle JSON from prose_planner_agent.py")
    p.add_argument("--baseline-retrieval-input", required=True)
    p.add_argument("--baseline-ranked-input", required=True)
    p.add_argument("--baseline-resolved-input", required=True)
    p.add_argument("--controller-input", required=True)
    p.add_argument("--orchestration-plan", required=True)
    p.add_argument("--python-bin", default=sys.executable)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write", default="")
    return p


def main() -> int:
    args = build_parser().parse_args()

    bundle = load_json(args.planner_bundle)
    if not bundle:
        raise SystemExit(f"Could not load planner bundle: {args.planner_bundle}")

    spec = load_json(".prose/templates/subagents/planner_query_bundle.acceptance_spec.json")
    primary_types = set(["randomized_trial", "cohort_study", "cross_sectional", "case_control", "diagnostic_ml", "open_label_trial"])
    review_types = set(["systematic_review", "meta_analysis", "scoping_review", "narrative_review", "review", "guideline", "mechanism_review"])

    run_dir = infer_run_dir(bundle)
    artifacts_dir = run_dir / "artifacts"
    tag = infer_tag(args.planner_bundle)

    baseline_ranked = load_json(args.baseline_ranked_input)
    baseline_resolved = load_json(args.baseline_resolved_input)

    baseline_retrieval = load_json(args.baseline_retrieval_input)
    baseline_query = baseline_retrieval.get("query") or ""
    baseline_metrics = {
        **rank_metrics(baseline_ranked.get("kept_records") or [], primary_types, review_types, baseline_query),
        **{
            "resolved_count": baseline_resolved.get("resolved_count", len(baseline_resolved.get("resolved_records") or [])),
            "fulltext_yield_rate": safe_float((baseline_resolved.get("stats") or {}).get("analysis_ready_rate"), 0.0),
        }
    }

    candidate_queries = bundle.get("candidate_queries") or []
    if not candidate_queries:
        raise SystemExit("Planner bundle has no candidate_queries")

    eval_payloads = []
    python_bin = args.python_bin

    for idx, candidate in enumerate(candidate_queries, 1):
        family_id = compact_ws(candidate.get("family_id")) or f"family{idx}"
        single_patch = {
            "schema_version": "1.0",
            "template": False,
            "artifact_type": "subagent_patch",
            "stage": "planner_subagent",
            "subagent_id": bundle.get("subagent_id"),
            "subagent_role": bundle.get("subagent_role"),
            "parent_agent": bundle.get("parent_agent"),
            "mode": bundle.get("mode"),
            "scope": bundle.get("scope"),
            "run_id": bundle.get("run_id"),
            "lane": bundle.get("lane"),
            "generated_at": utc_now_iso(),
            "subagent_runtime": bundle.get("subagent_runtime"),
            "openprose_context": bundle.get("openprose_context"),
            "based_on": bundle.get("based_on"),
            "problem_summary": bundle.get("problem_summary"),
            "proposal": {
                "action_family": candidate.get("action_family"),
                "query_changes": candidate.get("query_changes"),
                "search_overrides": candidate.get("search_overrides"),
                "rank_overrides": candidate.get("rank_overrides"),
                "backend_queries": candidate.get("backend_queries"),
            },
            "evaluation_plan": bundle.get("evaluation_plan"),
            "confidence": bundle.get("confidence"),
            "planner_note": candidate.get("why_this_candidate") or bundle.get("planner_note"),
        }

        patch_path = artifacts_dir / f"planner_query_patch.shadow.{tag}.{family_id}.json"
        eval_path = artifacts_dir / f"planner_shadow_eval.{tag}.{family_id}.json"
        ensure_parent_dir(str(patch_path))
        patch_path.write_text(json.dumps(single_patch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        cmd = [
            python_bin,
            "prose_planner_shadow_eval.py",
            "--planner-patch", str(patch_path),
            "--baseline-retrieval-input", args.baseline_retrieval_input,
            "--baseline-ranked-input", args.baseline_ranked_input,
            "--baseline-resolved-input", args.baseline_resolved_input,
            "--controller-input", args.controller_input,
            "--orchestration-plan", args.orchestration_plan,
            "--write", str(eval_path),
        ]
        if args.dry_run:
            cmd.append("--dry-run")

        print(f"\n## planner_family_eval {family_id}")
        run_cmd(cmd, dry_run=args.dry_run)

        payload = load_json(str(eval_path)) if not args.dry_run else {"controller_outcome": "accept_shadow_eval_only", "branch_artifacts": {}}
        payload["family_id"] = family_id
        payload["label"] = candidate.get("label")
        payload["why_this_candidate"] = candidate.get("why_this_candidate")
        payload["concept_fidelity"] = concept_fidelity_summary(candidate, bundle.get("concept_policy") or {})
        payload["concept_fidelity_score"] = concept_fidelity_score(payload["concept_fidelity"])
        eval_payloads.append(payload)

    successful = [p for p in eval_payloads if p.get("controller_outcome") in {"promote_to_current_run_patch", "save_for_future_run_memory"}]

    def qualifies_as_direct_family(payload: dict[str, Any]) -> bool:
        bm = payload.get("branch_metrics") or {}
        cf = payload.get("concept_fidelity") or {}
        return (
            bool(cf.get("preserves_all_must_have"))
            and safe_float(bm.get("ranked_primary_candidate_count"), 0.0) >= 2
            and safe_float(bm.get("review_pressure"), 999.0) <= 0.5
            and safe_float(bm.get("fulltext_yield_rate"), 0.0) >= 0.8
        )

    def direct_score(payload: dict[str, Any]) -> tuple:
        bm = payload.get("branch_metrics") or {}
        fidelity = safe_float(payload.get("concept_fidelity_score"), 0.0)
        return (
            fidelity,
            safe_float(bm.get("ranked_primary_candidate_count"), 0.0),
            -safe_float(bm.get("review_pressure"), 999.0),
            safe_float(bm.get("fulltext_yield_rate"), 0.0),
            safe_float(bm.get("tier1_count"), 0.0),
        )

    def qualifies_as_related_family(payload: dict[str, Any]) -> bool:
        bm = payload.get("branch_metrics") or {}
        cf = payload.get("concept_fidelity") or {}
        return (
            safe_float(bm.get("ranked_primary_candidate_count"), 0.0) >= 2
            and safe_float(bm.get("review_pressure"), 999.0) <= 0.5
            and len(cf.get("broadening_concepts_used") or []) > 0
        )

    def related_score(payload: dict[str, Any]) -> tuple:
        bm = payload.get("branch_metrics") or {}
        return (
            safe_float(bm.get("ranked_primary_candidate_count"), 0.0),
            -safe_float(bm.get("review_pressure"), 999.0),
            safe_float(bm.get("tier1_count"), 0.0),
            safe_float(bm.get("fulltext_yield_rate"), 0.0),
        )

    def score(payload: dict[str, Any]) -> tuple:
        bm = payload.get("branch_metrics") or {}
        fidelity = safe_float(payload.get("concept_fidelity_score"), 0.0)
        return (
            fidelity,
            safe_float(bm.get("ranked_primary_candidate_count"), 0.0),
            -safe_float(bm.get("review_pressure"), 999.0),
            safe_float(bm.get("tier1_count"), 0.0),
            safe_float(bm.get("fulltext_yield_rate"), 0.0),
        )

    best_single = max(successful, key=score) if successful else None

    hybrid_result = {}
    selected_strategy = "reject_all"
    selected_family_id = None
    rationale = "No candidate passed deterministic shadow evaluation."

    if successful and (bundle.get("selection_intent") or {}).get("allow_hybrid_family_merge", True):
        per_family_quota = int((spec.get("hybrid_policy") or {}).get("per_family_rank_quota", 4))
        hybrid_result = hybrid_merge(successful, per_family_quota, primary_types, review_types, baseline_query)

        h_rank = hybrid_result.get("rank_metrics") or {}
        h_res = hybrid_result.get("resolve_metrics") or {}
        hybrid_primary = h_rank.get("ranked_primary_candidate_count", 0)
        hybrid_tier1 = h_rank.get("tier1_count", 0)
        hybrid_yield = h_res.get("fulltext_yield_rate", 0.0)

        baseline_tier1 = baseline_metrics.get("tier1_count", 0)
        min_yield = float((bundle.get("evaluation_plan") or {}).get("success_metrics", {}).get("guardrails", {}).get("min_fulltext_yield_rate", 0.8))
        best_single_primary = (best_single.get("branch_metrics") or {}).get("ranked_primary_candidate_count", 0) if best_single else 0

        max_tier1_drop = int((spec.get("hybrid_policy") or {}).get("max_tier1_count_drop_from_baseline", 3))
        max_yield_drop = float((spec.get("hybrid_policy") or {}).get("max_fulltext_yield_drop_from_baseline", 0.10))
        tier1_floor = max(0, baseline_tier1 - max_tier1_drop)
        yield_floor = max(min_yield, baseline_metrics.get("fulltext_yield_rate", 0.0) - max_yield_drop)
        best_single_review = (best_single.get("branch_metrics") or {}).get("review_pressure", 999) if best_single else 999

        best_single_fidelity = safe_float(best_single.get("concept_fidelity_score"), 0.0) if best_single else 0.0

        # Hybrid should only beat a strict family if it is meaningfully better on retrieval, not just broader.
        if successful and hybrid_primary >= (best_single_primary + 2) and hybrid_tier1 >= tier1_floor and hybrid_yield >= yield_floor and h_rank.get("review_pressure", 999) <= best_single_review + 0.25:
            selected_strategy = "hybrid_family_merge"
            rationale = "Hybrid merged pool meaningfully improved primary-study candidate count beyond the best single family without violating relaxed tier-1 or yield guardrails."
        elif best_single:
            selected_strategy = "single_family"
            selected_family_id = best_single.get("family_id")
            rationale = f"A single candidate family performed best under deterministic guardrails and concept-fidelity scoring (score={best_single_fidelity})."
    elif best_single:
        selected_strategy = "single_family"
        selected_family_id = best_single.get("family_id")
        rationale = "A single candidate family performed best under deterministic guardrails."

    direct_candidates = [x for x in eval_payloads if qualifies_as_direct_family(x)]
    best_direct = max(direct_candidates, key=direct_score) if direct_candidates else None

    related_candidates = [x for x in eval_payloads if qualifies_as_related_family(x)]
    best_related = max(related_candidates, key=related_score) if related_candidates else None

    output = {
        "schema_version": "1.0",
        "artifact_type": "subagent_eval_bundle",
        "stage": "planner_family_eval",
        "run_id": bundle.get("run_id"),
        "lane": bundle.get("lane"),
        "generated_at": utc_now_iso(),
        "source_bundle": args.planner_bundle,
        "baseline_metrics": baseline_metrics,
        "candidate_family_results": eval_payloads,
        "hybrid_result": hybrid_result,
        "selected_strategy": selected_strategy,
        "concept_policy": bundle.get("concept_policy") or {},
        "selected_family_id": selected_family_id,
        "selected_direct_family_id": best_direct.get("family_id") if best_direct else None,
        "selected_related_family_id": best_related.get("family_id") if best_related else None,
        "direct_family_rationale": (
            "Best concept-faithful direct-evidence family under relaxed direct-family rules."
            if best_direct else None
        ),
        "related_family_rationale": (
            "Best broader related-evidence family under related-family rules."
            if best_related else None
        ),
        "rationale": rationale,
    }

    write_path = args.write or str(artifacts_dir / f"planner_family_eval.{tag}.json")
    ensure_parent_dir(write_path)
    Path(write_path).write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


def safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


if __name__ == "__main__":
    raise SystemExit(main())
