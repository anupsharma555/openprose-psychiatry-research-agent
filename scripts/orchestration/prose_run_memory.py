#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
import re
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def slugify(text: str) -> str:
    text = compact_ws(text).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown_topic"


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


def load_memory(path_str: str) -> dict[str, Any]:
    p = Path(path_str)
    if not p.exists():
        return {
            "schema_version": "1.0",
            "stage": "run_memory_store",
            "generated_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "topics": {},
        }
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("memory is not a dict")
        data.setdefault("schema_version", "1.0")
        data.setdefault("stage", "run_memory_store")
        data.setdefault("generated_at", utc_now_iso())
        data.setdefault("updated_at", utc_now_iso())
        data.setdefault("topics", {})
        return data
    except Exception:
        return {
            "schema_version": "1.0",
            "stage": "run_memory_store",
            "generated_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "topics": {},
        }


def save_json(path_str: str, payload: dict[str, Any]) -> None:
    ensure_parent_dir(path_str)
    Path(path_str).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def dedupe_keep_order(items: list[str]) -> list[str]:
    out = []
    seen = set()
    for item in items:
        item = compact_ws(item)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def increment_counts(counter: dict[str, int], items: list[str]) -> dict[str, int]:
    for item in items:
        item = compact_ws(item)
        if not item:
            continue
        counter[item] = int(counter.get(item, 0)) + 1
    return dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))


def summarize_run(
    controller: dict[str, Any],
    coverage: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    counts = (coverage.get("counts") or {})
    cov = (coverage.get("coverage") or {})
    retry_rec = ((coverage.get("retry_recommendation") or {}))

    future_candidates = controller.get("future_run_patch_candidates") or []
    future_actions = [compact_ws(c.get("action")) for c in future_candidates if compact_ws(c.get("action"))]

    return {
        "run_id": controller.get("run_id") or coverage.get("run_id"),
        "generated_at": controller.get("generated_at") or utc_now_iso(),
        "decision": controller.get("decision"),
        "priority": controller.get("priority"),
        "chosen_action": controller.get("chosen_action"),
        "patch_scope": controller.get("patch_scope"),
        "patch_ttl": controller.get("patch_ttl"),
        "persist_for_future_runs": bool(controller.get("persist_for_future_runs")),
        "promote_candidate": bool(controller.get("promote_candidate")),
        "semantic_ready_count": counts.get("semantic_ready_count"),
        "fulltext_yield_rate": counts.get("fulltext_yield_rate"),
        "semantic_ready_rate": counts.get("semantic_ready_rate"),
        "review_like_count": cov.get("review_like_count"),
        "primary_study_like_count": cov.get("primary_study_like_count"),
        "mixed_or_unclear_count": cov.get("mixed_or_unclear_count"),
        "high_quality_record_count": cov.get("high_quality_record_count"),
        "records_with_metrics": cov.get("records_with_metrics"),
        "threshold_met": retry_rec.get("threshold_met"),
        "missing_angles": retry_rec.get("missing_angles") or [],
        "future_patch_actions": future_actions,
    }


def build_promotion_watchlist(future_patch_counts: dict[str, int], threshold: int) -> list[dict[str, Any]]:
    watchlist = []
    for action, count in sorted(future_patch_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        if count >= threshold:
            watchlist.append(
                {
                    "action": action,
                    "count": count,
                    "ready_to_promote": True,
                }
            )
    return watchlist


def update_memory(
    memory: dict[str, Any],
    topic: str,
    lane: str,
    run_summary: dict[str, Any],
    future_candidates: list[dict[str, Any]],
    max_recent_runs: int,
    promotion_threshold: int,
) -> dict[str, Any]:
    topics = memory.setdefault("topics", {})
    topic_key = slugify(topic)

    topic_entry = topics.setdefault(
        topic_key,
        {
            "topic_label": topic,
            "last_run_id": None,
            "last_updated_at": None,
            "lanes": {},
        },
    )
    topic_entry["topic_label"] = topic
    topic_entry["last_run_id"] = run_summary.get("run_id")
    topic_entry["last_updated_at"] = utc_now_iso()

    lanes = topic_entry.setdefault("lanes", {})
    lane_key = compact_ws(lane) or "default"
    lane_entry = lanes.setdefault(
        lane_key,
        {
            "run_count": 0,
            "decision_counts": {},
            "missing_angle_counts": {},
            "future_patch_counts": {},
            "recent_runs": [],
            "last_future_run_patch_candidates": [],
            "promotion_watchlist": [],
            "last_run_id": None,
            "last_updated_at": None,
        },
    )

    lane_entry["run_count"] = int(lane_entry.get("run_count", 0)) + 1
    lane_entry["last_run_id"] = run_summary.get("run_id")
    lane_entry["last_updated_at"] = utc_now_iso()

    decision = compact_ws(run_summary.get("decision")) or "unknown"
    lane_entry["decision_counts"][decision] = int(lane_entry["decision_counts"].get(decision, 0)) + 1
    lane_entry["decision_counts"] = dict(sorted(lane_entry["decision_counts"].items(), key=lambda kv: (-kv[1], kv[0])))

    lane_entry["missing_angle_counts"] = increment_counts(
        lane_entry.get("missing_angle_counts", {}),
        run_summary.get("missing_angles") or [],
    )

    future_actions = [compact_ws(c.get("action")) for c in future_candidates if compact_ws(c.get("action"))]
    lane_entry["future_patch_counts"] = increment_counts(
        lane_entry.get("future_patch_counts", {}),
        future_actions,
    )

    lane_entry["last_future_run_patch_candidates"] = future_candidates

    recent_runs = lane_entry.get("recent_runs", [])
    recent_runs.append(run_summary)
    lane_entry["recent_runs"] = recent_runs[-max_recent_runs:]

    lane_entry["promotion_watchlist"] = build_promotion_watchlist(
        lane_entry.get("future_patch_counts", {}),
        promotion_threshold,
    )

    memory["updated_at"] = utc_now_iso()

    return {
        "topic_key": topic_key,
        "lane_key": lane_key,
        "lane_run_count": lane_entry["run_count"],
        "future_actions_added": future_actions,
        "promotion_watchlist": lane_entry["promotion_watchlist"],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Persist future-run lessons from controller decisions into shared prose run memory.")
    p.add_argument("--controller-input", required=True, help="controller_decision.json from scripts/orchestration/prose_controller.py")
    p.add_argument("--coverage-input", default="", help="Optional coverage_report.json for richer summary")
    p.add_argument("--evidence-input", default="", help="Optional evidence_records.json for richer summary")
    p.add_argument("--memory-path", default=".prose/memory/run_memory.json", help="Shared run memory JSON path")
    p.add_argument("--run-id", default="", help="Optional run identifier override")
    p.add_argument("--lane", default="", help="Optional lane override")
    p.add_argument("--topic", default="", help="Optional topic override")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration_plan.json path")
    p.add_argument("--max-recent-runs", type=int, default=8, help="How many recent run summaries to keep per lane")
    p.add_argument("--promotion-threshold", type=int, default=2, help="Count needed before an action appears in the promotion watchlist")
    p.add_argument("--schema-version", default="1.0", help="Schema version for run memory update artifact output")
    p.add_argument("--write", default="", help="Optional output path for the memory update artifact")
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    coverage = load_json(args.coverage_input)
    evidence = load_json(args.evidence_input)

    inferred_plan_path = (
        args.orchestration_plan
        or ((controller.get("orchestration_context") or {}).get("plan_path"))
        or ((coverage.get("orchestration_context") or {}).get("plan_path"))
        or ""
    )
    plan = load_orchestration_plan(inferred_plan_path)

    lane = (
        compact_ws(args.lane)
        or compact_ws(controller.get("lane"))
        or compact_ws(coverage.get("lane"))
        or "default"
    )
    run_id = (
        args.run_id
        or controller.get("run_id")
        or coverage.get("run_id")
        or plan.get("run_id")
        or None
    )
    topic = (
        compact_ws(args.topic)
        or compact_ws((controller.get("orchestration_context") or {}).get("topic"))
        or compact_ws((coverage.get("orchestration_context") or {}).get("topic"))
        or compact_ws(plan.get("topic"))
        or "unknown_topic"
    )

    memory = load_memory(args.memory_path)
    memory["schema_version"] = args.schema_version

    future_candidates = controller.get("future_run_patch_candidates") or []
    run_summary = summarize_run(controller, coverage, evidence)
    if run_id and not run_summary.get("run_id"):
        run_summary["run_id"] = run_id

    update_summary = update_memory(
        memory=memory,
        topic=topic,
        lane=lane,
        run_summary=run_summary,
        future_candidates=future_candidates,
        max_recent_runs=args.max_recent_runs,
        promotion_threshold=args.promotion_threshold,
    )

    save_json(args.memory_path, memory)

    topic_entry = memory["topics"][update_summary["topic_key"]]
    lane_entry = topic_entry["lanes"][update_summary["lane_key"]]

    output = {
        "schema_version": args.schema_version,
        "stage": "run_memory",
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "topic": topic,
        "lane": lane,
        "memory_path": args.memory_path,
        "update_summary": update_summary,
        "memory_snapshot": {
            "topic_key": update_summary["topic_key"],
            "lane_key": update_summary["lane_key"],
            "run_count": lane_entry.get("run_count"),
            "decision_counts": lane_entry.get("decision_counts"),
            "missing_angle_counts": lane_entry.get("missing_angle_counts"),
            "future_patch_counts": lane_entry.get("future_patch_counts"),
            "promotion_watchlist": lane_entry.get("promotion_watchlist"),
            "last_future_run_patch_candidates": lane_entry.get("last_future_run_patch_candidates"),
            "recent_runs": lane_entry.get("recent_runs"),
        },
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        save_json(args.write, output)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
