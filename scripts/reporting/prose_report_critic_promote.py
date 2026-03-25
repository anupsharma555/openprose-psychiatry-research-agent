#!/usr/bin/env python3

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def record_key(rec: dict[str, Any]) -> str:
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or rec.get("source_record_key") or "")


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        x = compact_ws(item)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def promote_bucket(rec: dict[str, Any], new_bucket: str) -> dict[str, Any]:
    out = dict(rec)
    out["bucket"] = new_bucket
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Promote high-confidence report_critic suggestions into a report input.")
    ap.add_argument("--report-input", required=True)
    ap.add_argument("--critic-shadow-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    report_input = load_json(args.report_input)
    critic = load_json(args.critic_shadow_input)

    if not report_input:
        raise SystemExit(f"Could not load report input: {args.report_input}")
    if not critic:
        raise SystemExit(f"Could not load critic shadow input: {args.critic_shadow_input}")

    critiques = {str(x.get("source_record_key")): x for x in critic.get("article_critiques", [])}

    sections = {
        "direct_evidence": list(report_input.get("direct_evidence_articles", [])),
        "related_broader_evidence": list(report_input.get("related_evidence_articles", [])),
        "review_context_evidence": list(report_input.get("review_context_articles", [])),
    }

    current = {}
    current_bucket = {}
    for bucket_name, records in sections.items():
        for rec in records:
            key = record_key(rec)
            current[key] = rec
            current_bucket[key] = bucket_name

    promotions = []

    for key, critique in critiques.items():
        if key not in current:
            continue
        if critique.get("confidence") not in {"high", "medium"}:
            continue
        if "no_issue" in (critique.get("issue_types") or []):
            continue

        rec = dict(current[key])

        emphasis_points = critique.get("critic_emphasis_points") or []
        if emphasis_points:
            existing_emphasis = rec.get("critic_emphasis_bullets") or []
            merged_emphasis = dedupe_keep_order(existing_emphasis + emphasis_points)
            if merged_emphasis != existing_emphasis:
                rec["critic_emphasis_bullets"] = merged_emphasis[:10]
                promotions.append({"key": key, "kind": "critic_emphasis_points", "count": len(emphasis_points)})

        suggested_bullets = critique.get("suggested_bullets") or []
        if suggested_bullets:
            existing = rec.get("bullet_candidates") or []
            merged = dedupe_keep_order(existing + suggested_bullets)
            if merged != existing:
                rec["bullet_candidates"] = merged[:15]
                promotions.append({"key": key, "kind": "suggested_bullets", "count": len(suggested_bullets)})

        factual_para = compact_ws(critique.get("suggested_factual_paragraph"))
        if factual_para:
            rec["critic_factual_paragraph"] = factual_para
            promotions.append({"key": key, "kind": "critic_factual_paragraph"})

        if critique.get("confidence") == "high":
            overrides = critique.get("suggested_label_overrides") or {}
            if overrides.get("paper_kind"):
                rec["paper_kind"] = overrides["paper_kind"]
                promotions.append({"key": key, "kind": "paper_kind_override"})
            if overrides.get("document_role"):
                rec["document_role"] = overrides["document_role"]
                promotions.append({"key": key, "kind": "document_role_override"})

            suggested_bucket = critique.get("suggested_bucket")
            if suggested_bucket in sections and suggested_bucket != current_bucket.get(key):
                current_bucket[key] = suggested_bucket
                promotions.append({"key": key, "kind": "bucket_reassignment", "to": suggested_bucket})

        current[key] = rec

    rebuilt = {
        "direct_evidence": [],
        "related_broader_evidence": [],
        "review_context_evidence": [],
    }
    for key, rec in current.items():
        rebuilt[current_bucket[key]].append(promote_bucket(rec, current_bucket[key]))

    promoted = dict(report_input)
    promoted["direct_evidence_articles"] = rebuilt["direct_evidence"]
    promoted["related_evidence_articles"] = rebuilt["related_broader_evidence"]
    promoted["review_context_articles"] = rebuilt["review_context_evidence"]
    promoted["included_articles"] = (
        promoted["direct_evidence_articles"]
        + promoted["related_evidence_articles"]
        + promoted["review_context_articles"]
    )
    promoted["report_critic_promotions"] = promotions

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(promoted, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "promotion_count": len(promotions),
        "included_articles": len(promoted.get("included_articles", [])),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
