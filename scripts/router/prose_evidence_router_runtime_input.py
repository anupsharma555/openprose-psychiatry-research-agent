#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from prose_run_report_input import (
    compact_ws,
    load_json,
    topic_tokens,
    derive_topic_groups,
    biomarker_family_terms,
)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def record_key(rec: dict[str, Any]) -> str:
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or "")


def pick_fields(rec: dict[str, Any]) -> dict[str, Any]:
    metrics = rec.get("metrics") or {}
    return {
        "source_record_key": record_key(rec),
        "pmid": rec.get("pmid"),
        "doi": rec.get("doi"),
        "title": rec.get("title"),
        "journal": rec.get("journal"),
        "publication_date": rec.get("publication_date"),
        "authors": rec.get("authors") or [],
        "first_author": rec.get("first_author"),
        "last_author": rec.get("last_author"),
        "paper_kind": rec.get("paper_kind"),
        "document_role": rec.get("document_role"),
        "study_design": rec.get("study_design"),
        "condition": rec.get("condition"),
        "intervention_or_exposure": rec.get("intervention_or_exposure"),
        "comparator": rec.get("comparator"),
        "sample_size": rec.get("sample_size"),
        "main_claim": rec.get("main_claim"),
        "most_salient_findings": rec.get("most_salient_findings") or rec.get("key_findings") or [],
        "bullet_candidates": rec.get("bullet_candidates") or [],
        "outcomes": rec.get("outcomes") or [],
        "limitations": rec.get("limitations") or [],
        "discussion_significance": rec.get("discussion_significance") or [],
        "score_change_snippets": metrics.get("score_change_snippets") or [],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build runtime input for the evidence_router sub-agent.")
    p.add_argument("--controller-input", required=True)
    p.add_argument("--coverage-input", required=True)
    p.add_argument("--primary-evidence-input", required=True)
    p.add_argument("--secondary-evidence-input", default="")
    p.add_argument("--orchestration-plan", default="")
    p.add_argument("--max-candidates", type=int, default=15)
    p.add_argument("--write", required=True)
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    coverage = load_json(args.coverage_input)
    primary = load_json(args.primary_evidence_input)
    secondary = load_json(args.secondary_evidence_input) if args.secondary_evidence_input else {}
    plan = load_json(args.orchestration_plan) if args.orchestration_plan else {}

    topic = compact_ws(
        (controller.get("orchestration_context") or {}).get("topic")
        or (coverage.get("orchestration_context") or {}).get("topic")
        or plan.get("topic")
    )

    topic_concepts = derive_topic_groups(topic)
    topic_concepts["biomarker_family_terms"] = biomarker_family_terms(topic_concepts)

    candidates = []
    seen = set()

    for source_name, payload in [("primary", primary), ("secondary", secondary)]:
        if not payload:
            continue
        for bucket in ["evidence_records", "partial_records"]:
            for rec in payload.get(bucket, []) or []:
                key = record_key(rec)
                if not key or key in seen:
                    continue
                seen.add(key)
                item = pick_fields(rec)
                item["source_name"] = source_name
                item["source_bucket"] = bucket
                candidates.append(item)

    payload = {
        "schema_version": "1.0",
        "template": False,
        "artifact_type": "evidence_router_runtime_input",
        "stage": "evidence_router_runtime_input",
        "run_id": controller.get("run_id") or coverage.get("run_id") or plan.get("run_id"),
        "topic": topic,
        "topic_tokens": topic_tokens(topic),
        "topic_concepts": topic_concepts,
        "concept_policy": {},
        "candidate_records": candidates[:args.max_candidates],
        "bucket_definitions": {
            "direct_evidence": "Highly topic-faithful, usually primary empirical, strongest answer-bearing evidence.",
            "related_broader_evidence": "Relevant and supportive, but broader in intervention, condition framing, or mechanism.",
            "review_context_evidence": "Review-like or field-framing papers useful for context.",
            "exclude": "Too far off-topic or not worth including."
        }
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "run_id": payload["run_id"],
        "topic": payload["topic"],
        "candidate_count": len(payload["candidate_records"]),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
