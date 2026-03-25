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

from prose_evidence_prepare import enrich_record


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
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or "")


def enrich_articles(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    out = []
    changed = 0
    for rec in records:
        new_rec, did_change = enrich_record(rec)
        if did_change:
            changed += 1
        out.append(new_rec)
    return out, changed


def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich report input article records with deterministic label cleanup and bullet candidates.")
    ap.add_argument("--report-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    payload = load_json(args.report_input)
    if not payload:
        raise SystemExit(f"Could not load report input: {args.report_input}")

    total_changed = 0
    for key in ["included_articles", "direct_evidence_articles", "related_evidence_articles", "review_context_articles"]:
        if key in payload:
            payload[key], changed = enrich_articles(payload.get(key) or [])
            total_changed += changed

    payload["report_input_enrichment_summary"] = {
        "changed_article_records": total_changed
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload["report_input_enrichment_summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
