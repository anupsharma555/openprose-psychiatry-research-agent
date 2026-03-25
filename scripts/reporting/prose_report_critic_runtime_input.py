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


def compact_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def load_text(path: str) -> str:
    p = Path(path)
    return p.read_text(encoding="utf-8") if p.exists() else ""


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def record_key(rec: dict[str, Any]) -> str:
    return str(rec.get("pmid") or rec.get("doi") or rec.get("title") or "")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build runtime input for report_critic.")
    ap.add_argument("--report-input", required=True)
    ap.add_argument("--report-md", required=True)
    ap.add_argument("--digest-md", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    report_input = load_json(args.report_input)
    if not report_input:
        raise SystemExit(f"Could not load report input: {args.report_input}")

    report_md = load_text(args.report_md)
    digest_md = load_text(args.digest_md)

    candidate_articles = []
    for bucket_name, records in [
        ("direct_evidence", report_input.get("direct_evidence_articles", [])),
        ("related_broader_evidence", report_input.get("related_evidence_articles", [])),
        ("review_context_evidence", report_input.get("review_context_articles", [])),
    ]:
        for rec in records:
            candidate_articles.append({
                "source_record_key": record_key(rec),
                "title": rec.get("title"),
                "current_bucket": bucket_name,
                "journal": rec.get("journal"),
                "paper_kind": rec.get("paper_kind"),
                "document_role": rec.get("document_role"),
                "study_design": rec.get("study_design"),
                "sample_size": rec.get("sample_size"),
                "main_claim": rec.get("main_claim"),
                "critic_emphasis_bullets": rec.get("critic_emphasis_bullets") or [],
                "bullet_candidates": rec.get("bullet_candidates") or [],
                "most_salient_findings": rec.get("most_salient_findings") or [],
                "outcomes": rec.get("outcomes") or [],
                "limitations": rec.get("limitations") or [],
                "discussion_significance": rec.get("discussion_significance") or [],
            })

    payload = {
        "schema_version": "1.0",
        "template": False,
        "artifact_type": "report_critic_runtime_input",
        "stage": "report_critic_runtime_input",
        "run_id": report_input.get("run_id"),
        "topic": report_input.get("topic"),
        "report_goals": {
            "prefer_article_specific_detail": True,
            "avoid_generic_fallback_prose": True,
            "target_minimum_bullets_when_supported": 5,
            "prefer_supported_label_corrections": True
        },
        "candidate_articles": candidate_articles,
        "report_markdown": report_md,
        "digest_markdown": digest_md
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "run_id": payload["run_id"],
        "topic": payload["topic"],
        "candidate_count": len(payload["candidate_articles"]),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
