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

from prose_run_report_input import (
    compact_ws,
    build_date_fields,
    format_publication_date,
    pubmed_url,
    pmc_url,
    doi_url,
)


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def record_key(rec: dict[str, Any]) -> str:
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or rec.get("source_record_key") or "")


def make_report_item_from_runtime(rec: dict[str, Any], bucket: str, confidence: str) -> dict[str, Any]:
    pub_date = rec.get("publication_date")
    date_fields = build_date_fields(rec)
    confidence_score = {"high": 12, "medium": 9, "low": 6}.get(confidence, 6)

    return {
        "bucket": bucket,
        "pmid": rec.get("pmid"),
        "pmcid": None,
        "doi": rec.get("doi"),
        "title": compact_ws(rec.get("title")),
        "journal": compact_ws(rec.get("journal")),
        "publication_date": pub_date,
        "display_date": format_publication_date(pub_date),
        **date_fields,
        "pubmed_url": pubmed_url(rec.get("pmid")),
        "pmc_url": None,
        "doi_url": doi_url(rec.get("doi")),
        "authors": rec.get("authors") or [],
        "first_author": rec.get("first_author"),
        "last_author": rec.get("last_author"),
        "paper_kind": rec.get("paper_kind"),
        "document_role": rec.get("document_role"),
        "classification_confidence": None,
        "study_design": rec.get("study_design"),
        "sample_size": rec.get("sample_size"),
        "condition": rec.get("condition"),
        "intervention_or_exposure": rec.get("intervention_or_exposure"),
        "comparator": rec.get("comparator"),
        "outcomes": rec.get("outcomes") or [],
        "most_salient_findings": rec.get("most_salient_findings") or [],
        "bullet_candidates": rec.get("bullet_candidates") or [],
        "metrics": {"score_change_snippets": rec.get("score_change_snippets") or []},
        "limitations": rec.get("limitations") or [],
        "discussion_significance": rec.get("discussion_significance") or [],
        "main_claim": rec.get("main_claim"),
        "evidence_level": None,
        "extraction_quality": None,
        "source_substance": None,
        "confidence": confidence,
        "relevance_score": confidence_score,
    }


def dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for rec in records:
        key = record_key(rec)
        if key not in seen:
            seen.add(key)
            out.append(rec)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Advisory promotion of evidence_router shadow output into a promoted portfolio input.")
    ap.add_argument("--portfolio-input", required=True)
    ap.add_argument("--router-shadow-input", required=True)
    ap.add_argument("--router-runtime-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    portfolio = load_json(args.portfolio_input)
    router = load_json(args.router_shadow_input)
    runtime = load_json(args.router_runtime_input)

    candidates = {record_key(rec): rec for rec in runtime.get("candidate_records", [])}

    direct = list(portfolio.get("direct_evidence_articles", []))
    related = list(portfolio.get("related_evidence_articles", []))
    review = list(portfolio.get("review_context_articles", []))

    direct_keys = {record_key(x) for x in direct}
    related_keys = {record_key(x) for x in related}
    review_keys = {record_key(x) for x in review}

    promotions = []

    for rec in router.get("routed_records", []):
        key = record_key(rec)
        src = candidates.get(key)
        if not src:
            continue

        bucket = rec.get("suggested_bucket")
        confidence = rec.get("bucket_confidence")

        # Advisory promotion rule:
        # - high confidence always eligible
        # - medium confidence only if the target lane is empty
        if confidence not in {"high", "medium"}:
            continue

        if bucket == "direct_evidence":
            if key in direct_keys:
                continue
            if confidence == "high" or len(direct) == 0:
                direct.append(make_report_item_from_runtime(src, "direct_evidence", confidence))
                direct_keys.add(key)
                promotions.append({"key": key, "bucket": bucket, "confidence": confidence})

        elif bucket == "related_broader_evidence":
            if key in direct_keys or key in related_keys:
                continue
            if confidence == "high" or len(related) == 0:
                related.append(make_report_item_from_runtime(src, "related_broader_evidence", confidence))
                related_keys.add(key)
                promotions.append({"key": key, "bucket": bucket, "confidence": confidence})

        elif bucket == "review_context_evidence":
            if key in direct_keys or key in related_keys or key in review_keys:
                continue
            if confidence == "high" or len(review) == 0:
                review.append(make_report_item_from_runtime(src, "review_context_evidence", confidence))
                review_keys.add(key)
                promotions.append({"key": key, "bucket": bucket, "confidence": confidence})

    promoted = dict(portfolio)
    promoted["direct_evidence_articles"] = dedupe(direct)
    promoted["related_evidence_articles"] = dedupe(related)
    promoted["review_context_articles"] = dedupe(review)
    promoted["included_articles"] = promoted["direct_evidence_articles"] + promoted["related_evidence_articles"] + promoted["review_context_articles"]
    promoted["router_advisory_promotions"] = promotions

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(promoted, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "direct_count": len(promoted["direct_evidence_articles"]),
        "related_count": len(promoted["related_evidence_articles"]),
        "review_context_count": len(promoted["review_context_articles"]),
        "promotion_count": len(promotions),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
