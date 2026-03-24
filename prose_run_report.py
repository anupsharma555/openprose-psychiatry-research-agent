#!/usr/bin/env python3
import argparse
import json
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


def fmt_list(items: list[str], limit: int = 4) -> str:
    vals = [compact_ws(x) for x in items if compact_ws(x)]
    vals = vals[:limit]
    return ", ".join(vals)


def non_generic_text(text: str) -> bool:
    t = compact_ws(text).lower()
    if not t:
        return False
    banned_prefixes = [
        "redirecting",
        "article ",
        "published ",
        "copyright",
    ]
    banned_exact = {"abstract", "introduction", "results", "discussion"}
    if t in banned_exact:
        return False
    if any(t.startswith(x) for x in banned_prefixes):
        return False
    if len(t) < 25:
        return False
    return True


def best_salient_finding(rec: dict[str, Any]) -> str:
    metrics = rec.get("metrics") or {}
    for s in metrics.get("score_change_snippets") or []:
        if non_generic_text(s):
            return compact_ws(s)
    for s in rec.get("key_findings") or []:
        if non_generic_text(s):
            return compact_ws(s)
    main_claim = rec.get("main_claim") or ""
    if non_generic_text(main_claim):
        return compact_ws(main_claim)
    return ""


def build_article_block(rec: dict[str, Any]) -> str:
    title = compact_ws(rec.get("title"))
    journal = compact_ws(rec.get("journal"))
    role = compact_ws(rec.get("document_role"))
    level = compact_ws(rec.get("evidence_level"))
    quality = compact_ws(rec.get("extraction_quality"))
    finding = best_salient_finding(rec)

    if not title or not finding:
        return ""

    lines = []
    lines.append(f"### {title}")
    meta = [x for x in [journal, role, level, quality] if x]
    if meta:
        lines.append(f"- **Profile:** {' | '.join(meta)}")

    sample_size = rec.get("sample_size")
    if sample_size:
        lines.append(f"- **Sample size:** {sample_size}")

    intervention = compact_ws(rec.get("intervention_or_exposure"))
    condition = compact_ws(rec.get("condition"))
    if condition or intervention:
        label = " / ".join([x for x in [condition, intervention] if x])
        lines.append(f"- **Context:** {label}")

    lines.append(f"- **Most salient finding:** {finding}")

    metrics = rec.get("metrics") or {}
    pvals = fmt_list(metrics.get("p_values") or [], limit=3)
    outcomes = fmt_list(metrics.get("outcome_markers") or [], limit=5)
    if pvals:
        lines.append(f"- **P values:** {pvals}")
    if outcomes:
        lines.append(f"- **Outcomes/signals:** {outcomes}")

    limitations = rec.get("limitations") or []
    if limitations:
        first_lim = next((compact_ws(x) for x in limitations if non_generic_text(x)), "")
        if first_lim:
            lines.append(f"- **Limitation:** {first_lim}")

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build a concise markdown report for a prose research run.")
    p.add_argument("--controller-input", required=True, help="controller_decision JSON")
    p.add_argument("--coverage-input", required=True, help="coverage_report JSON")
    p.add_argument("--evidence-input", required=True, help="evidence_records JSON")
    p.add_argument("--planner-shadow-eval", default="", help="Optional planner_shadow_eval JSON")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan JSON")
    p.add_argument("--max-articles", type=int, default=5, help="Maximum number of evidence/partial articles to include")
    p.add_argument("--discord-channel-id", default="", help="Optional workflow-local Discord channel id for delivery sidecar")
    p.add_argument("--message", default="", help="Optional Discord message for delivery sidecar")
    p.add_argument("--delivery-json", default="", help="Optional delivery sidecar JSON path")
    p.add_argument("--write", required=True, help="Output markdown path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    coverage = load_json(args.coverage_input)
    evidence = load_json(args.evidence_input)
    planner_eval = load_json(args.planner_shadow_eval) if args.planner_shadow_eval else {}
    plan = load_json(args.orchestration_plan) if args.orchestration_plan else {}

    run_id = compact_ws(controller.get("run_id") or coverage.get("run_id") or evidence.get("run_id") or plan.get("run_id"))
    lane = compact_ws(controller.get("lane") or coverage.get("lane") or evidence.get("lane"))
    topic = compact_ws((controller.get("orchestration_context") or {}).get("topic") or (coverage.get("orchestration_context") or {}).get("topic") or plan.get("topic"))
    decision = compact_ws(controller.get("decision"))
    priority = compact_ws(controller.get("priority"))

    counts = coverage.get("counts") or {}
    cov = coverage.get("coverage") or {}
    retry = coverage.get("retry_recommendation") or {}

    lines = []
    lines.append("# Prose Research Run Report")
    lines.append("")
    lines.append(f"- **Run ID:** {run_id}")
    if topic:
        lines.append(f"- **Topic:** {topic}")
    if lane:
        lines.append(f"- **Lane:** {lane}")
    if decision:
        lines.append(f"- **Controller decision:** {decision}")
    if priority:
        lines.append(f"- **Priority:** {priority}")
    lines.append(f"- **Generated:** {utc_now_iso()}")
    lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append(f"- **Semantic-ready records:** {counts.get('semantic_ready_count')}")
    lines.append(f"- **Evidence records:** {counts.get('evidence_record_count')}")
    lines.append(f"- **Partial records:** {counts.get('partial_record_count')}")
    lines.append(f"- **Skipped records:** {counts.get('evidence_skipped_count')}")
    lines.append(f"- **Full-text yield rate:** {counts.get('fulltext_yield_rate')}")
    lines.append(f"- **Review-like vs primary empirical:** {cov.get('review_like_count')} vs {cov.get('primary_study_like_count')}")
    lines.append(f"- **High-quality records:** {cov.get('high_quality_record_count')}")
    lines.append(f"- **Metrics present in records:** {cov.get('records_with_metrics')}")
    threshold_met = retry.get("threshold_met")
    if threshold_met is not None:
        lines.append(f"- **Threshold met:** {threshold_met}")
    lines.append("")

    missing_angles = retry.get("missing_angles") or []
    if missing_angles:
        lines.append("## Key gaps")
        lines.append("")
        for gap in missing_angles[:6]:
            lines.append(f"- {gap}")
        lines.append("")

    lines.append("## Most salient included articles")
    lines.append("")

    selected = []
    for rec in (evidence.get("evidence_records") or []):
        block = build_article_block(rec)
        if block:
            selected.append(block)
        if len(selected) >= args.max_articles:
            break

    if len(selected) < args.max_articles:
        for rec in (evidence.get("partial_records") or []):
            block = build_article_block(rec)
            if block:
                selected.append(block)
            if len(selected) >= args.max_articles:
                break

    if selected:
        lines.append("\n\n".join(selected))
        lines.append("")
    else:
        lines.append("- No article had a specific enough structured finding to include without generic fallback text.")
        lines.append("")

    if planner_eval:
        lines.append("## Planner shadow evaluation")
        lines.append("")
        lines.append(f"- **Outcome:** {planner_eval.get('controller_outcome')}")
        rationale = compact_ws(planner_eval.get("rationale"))
        if rationale:
            lines.append(f"- **Rationale:** {rationale}")

        branch_metrics = planner_eval.get("branch_metrics") or {}
        baseline_metrics = planner_eval.get("baseline_metrics") or {}
        if branch_metrics and baseline_metrics:
            lines.append(f"- **Primary candidates, baseline → branch:** {baseline_metrics.get('ranked_primary_candidate_count')} → {branch_metrics.get('ranked_primary_candidate_count')}")
            lines.append(f"- **Review pressure, baseline → branch:** {baseline_metrics.get('review_pressure')} → {branch_metrics.get('review_pressure')}")
            lines.append(f"- **Tier-1 count, baseline → branch:** {baseline_metrics.get('tier1_count')} → {branch_metrics.get('tier1_count')}")
            lines.append(f"- **Full-text yield, baseline → branch:** {baseline_metrics.get('fulltext_yield_rate')} → {branch_metrics.get('fulltext_yield_rate')}")
        lines.append("")

    future_candidates = controller.get("future_run_patch_candidates") or []
    if future_candidates:
        lines.append("## Future-run recommendations")
        lines.append("")
        for cand in future_candidates[:4]:
            action = compact_ws(cand.get("action"))
            note = compact_ws(cand.get("note"))
            if action:
                lines.append(f"- **{action}:** {note}")
        lines.append("")

    md = "\n".join(lines).rstrip() + "\n"
    ensure_parent_dir(args.write)
    report_path = Path(args.write).expanduser().resolve()
    report_path.write_text(md, encoding="utf-8")

    if args.delivery_json:
        ensure_parent_dir(args.delivery_json)
        message = compact_ws(args.message) or (f"Prose Research Run: {topic}" if topic else "Prose Research Run")
        delivery = {
            "schema_version": "1.0",
            "artifact_type": "discord_delivery",
            "workflow": "prose_research",
            "generated_at": utc_now_iso(),
            "run_id": run_id,
            "lane": lane,
            "topic": topic,
            "discord_channel_id": compact_ws(args.discord_channel_id) or None,
            "message": message,
            "report_md": str(report_path),
            "staged_report_md": None
        }
        Path(args.delivery_json).write_text(json.dumps(delivery, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
