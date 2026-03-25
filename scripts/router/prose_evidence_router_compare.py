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


def key_from_record(rec: dict[str, Any]) -> str:
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or rec.get("source_record_key") or "")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare deterministic portfolio buckets with evidence_router shadow buckets.")
    ap.add_argument("--portfolio-input", required=True)
    ap.add_argument("--router-shadow-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    portfolio = load_json(args.portfolio_input)
    router = load_json(args.router_shadow_input)

    det_map = {}
    for bucket_name, records in [
        ("direct_evidence", portfolio.get("direct_evidence_articles", [])),
        ("related_broader_evidence", portfolio.get("related_evidence_articles", [])),
        ("review_context_evidence", portfolio.get("review_context_articles", [])),
    ]:
        for rec in records:
            det_map[key_from_record(rec)] = bucket_name

    router_map = {}
    router_meta = {}
    for rec in router.get("routed_records", []):
        key = key_from_record(rec)
        router_map[key] = rec.get("suggested_bucket")
        router_meta[key] = rec

    all_keys = sorted(set(det_map) | set(router_map))
    disagreements = []
    agreement_count = 0

    for key in all_keys:
        det_bucket = det_map.get(key)
        router_bucket = router_map.get(key)
        if det_bucket == router_bucket:
            agreement_count += 1
        else:
            disagreements.append({
                "source_record_key": key,
                "deterministic_bucket": det_bucket,
                "router_bucket": router_bucket,
                "router_confidence": (router_meta.get(key) or {}).get("bucket_confidence"),
                "router_rationale": (router_meta.get(key) or {}).get("rationale"),
                "needs_human_review": (router_meta.get(key) or {}).get("needs_human_review"),
            })

    out = {
        "schema_version": "1.0",
        "artifact_type": "evidence_router_compare",
        "portfolio_topic": portfolio.get("topic"),
        "router_topic": router.get("topic"),
        "summary": {
            "deterministic_total": len(det_map),
            "router_total": len(router_map),
            "agreement_count": agreement_count,
            "disagreement_count": len(disagreements),
        },
        "disagreements": disagreements,
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(out["summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
