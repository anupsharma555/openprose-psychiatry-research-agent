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


def snippet(text: str, max_chars: int = 500) -> str:
    s = compact_ws(text)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1].rstrip() + "…"


def build_digest_record(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "pmid": rec.get("pmid"),
        "title": compact_ws(rec.get("title")),
        "journal": compact_ws(rec.get("journal")),
        "publication_date": rec.get("pubdate_iso") or rec.get("epubdate_iso") or rec.get("pubdate"),
        "article_type": rec.get("article_type"),
        "journal_tier": rec.get("journal_tier"),
        "rank_score": rec.get("rank_score"),
        "has_pmcid": bool(rec.get("has_pmcid")),
        "pmcid": rec.get("pmcid"),
        "has_abstract": bool(rec.get("has_abstract")),
        "matched_variants": rec.get("matched_variants") or [],
        "filter_reasons": rec.get("filter_reasons") or [],
        "abstract_snippet": snippet(rec.get("abstract") or rec.get("abstract_text") or rec.get("abstract_extracted") or ""),
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build a compact ranked-candidate digest for the planner sub-agent.")
    p.add_argument("--input", required=True, help="ranked_records JSON")
    p.add_argument("--records-key", default="kept_records", help="Record list key to summarize")
    p.add_argument("--top-k", type=int, default=12, help="Maximum number of ranked candidates to include")
    p.add_argument("--run-id", default="", help="Optional run identifier")
    p.add_argument("--lane", default="", help="Optional lane name")
    p.add_argument("--write", default="", help="Optional output path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    payload = load_json(args.input)
    if not payload:
        raise SystemExit(f"Could not load input: {args.input}")

    records = payload.get(args.records_key) or []
    records = records[: args.top_k]

    digest = {
        "schema_version": "1.0",
        "artifact_type": "planner_candidate_digest",
        "stage": "planner_candidate_digest",
        "generated_at": utc_now_iso(),
        "run_id": args.run_id or payload.get("run_id"),
        "lane": args.lane or payload.get("lane"),
        "source_stage": payload.get("stage"),
        "source_artifact": args.input,
        "candidate_count": len(records),
        "candidates": [build_digest_record(rec) for rec in records],
    }

    text = json.dumps(digest, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
