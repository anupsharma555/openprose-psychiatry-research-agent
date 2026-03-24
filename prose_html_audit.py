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


def record_key(rec: dict[str, Any]) -> str:
    for k in ["pmid", "doi", "pmcid", "title"]:
        v = compact_ws(rec.get(k))
        if v:
            return f"{k}:{v.lower()}"
    return json.dumps(rec, sort_keys=True)


def best_abstract_text(rec: dict[str, Any]) -> str:
    for k in ["abstract_extracted", "abstract", "abstract_text"]:
        txt = compact_ws(rec.get(k))
        if txt:
            return txt
    return ""


def text_len(rec: dict[str, Any], key: str) -> int:
    return len(compact_ws(rec.get(key)))


def bucket_maps(evidence_payload: dict[str, Any]) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    bucket_by_key = {}
    rec_by_key = {}
    for bucket in ["evidence_records", "partial_records", "skipped_records"]:
        for rec in evidence_payload.get(bucket, []) or []:
            k = record_key(rec)
            bucket_by_key[k] = bucket
            rec_by_key[k] = rec
    return bucket_by_key, rec_by_key


def classify_html_source(resolved_rec: dict[str, Any], extracted_rec: dict[str, Any], evidence_rec: dict[str, Any] | None) -> str:
    fulltext_status = compact_ws(resolved_rec.get("fulltext_status"))
    resolved_by = compact_ws(resolved_rec.get("resolved_by"))
    html_path = compact_ws(resolved_rec.get("html_path"))
    analysis_ready = bool(resolved_rec.get("analysis_ready"))

    methods_len = text_len(extracted_rec, "methods_text")
    results_len = text_len(extracted_rec, "results_text")
    discussion_len = text_len(extracted_rec, "discussion_text")
    analysis_len = text_len(extracted_rec, "analysis_text")
    abstract_len = len(best_abstract_text(extracted_rec))

    if fulltext_status == "fulltext_xml":
        return "xml_high_confidence"

    if fulltext_status != "fulltext_html":
        return "non_html_other"

    if methods_len > 500 or results_len > 500 or discussion_len > 500:
        return "html_structured"

    if abstract_len >= 250 and analysis_len >= 250:
        return "html_partial_usable"

    if resolved_by in {"doi_landing", "linkout_url"} and analysis_ready and analysis_len < 100:
        return "html_landing_or_preview"

    if html_path and analysis_len < 100:
        return "html_saved_but_thin"

    return "html_unclear"


def classify_failure_mode(
    ranked_rec: dict[str, Any] | None,
    resolved_rec: dict[str, Any],
    extracted_rec: dict[str, Any],
    evidence_rec: dict[str, Any] | None,
    evidence_bucket: str | None,
    html_class: str,
) -> tuple[str, str]:
    ranked_abs = len(best_abstract_text(ranked_rec or {}))
    extracted_abs = len(best_abstract_text(extracted_rec))
    analysis_len = text_len(extracted_rec, "analysis_text")
    section_total = sum(
        text_len(extracted_rec, k)
        for k in ["introduction_text", "methods_text", "results_text", "discussion_text", "conclusion_text"]
    )
    resolved_by = compact_ws(resolved_rec.get("resolved_by"))
    fulltext_status = compact_ws(resolved_rec.get("fulltext_status"))
    skip_reason = compact_ws((evidence_rec or {}).get("skip_reason"))
    partial_reason = compact_ws((evidence_rec or {}).get("partial_reason"))
    paper_kind = compact_ws((evidence_rec or {}).get("paper_kind"))
    trial_like = bool(((evidence_rec or {}).get("quality_signals") or {}).get("trial_like_signal"))

    if evidence_bucket == "evidence_records":
        return "usable_full_or_strong_partial", "No immediate action needed."

    if evidence_bucket == "partial_records":
        if partial_reason == "abstract_backfilled_trial_like_html":
            return "abstract_backfill_rescue", "Good rescue. Keep abstract fallback path."
        if html_class == "html_partial_usable":
            return "partial_html_but_usable", "Consider stronger HTML section parsing, but keep as partial evidence."
        return "partial_evidence", "Review whether stronger HTML parsing could upgrade this record."

    if evidence_bucket == "skipped_records":
        if ranked_abs > 100 and extracted_abs == 0:
            return "abstract_lost_between_ranked_and_extracted", "Backfill abstract from upstream ranked/resolved artifacts."
        if ranked_abs > 100 and extracted_abs > 100 and analysis_len < 100:
            return "abstract_only_but_no_body_signal", "Keep as abstract-backed partial evidence if topic-critical."
        if resolved_by in {"doi_landing", "linkout_url"} and fulltext_status == "fulltext_html" and section_total == 0:
            return "landing_or_preview_html", "Downgrade source class earlier and avoid treating as strong HTML."
        if trial_like and paper_kind in {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional"}:
            return "trial_like_record_skipped_for_weak_html", "Prefer abstract-backed partial classification over full skip."
        if skip_reason:
            return f"skip:{skip_reason}", "Inspect source HTML and parsing quality."
        return "skipped_unclear", "Manual review needed."

    return "not_classified", "Manual review needed."


def html_path_exists(resolved_rec: dict[str, Any]) -> bool | None:
    hp = compact_ws(resolved_rec.get("html_path"))
    if not hp:
        return None
    p = Path(hp)
    if p.exists():
        return True
    # also try relative to cwd
    return Path.cwd().joinpath(hp).exists()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Audit HTML/full-text handling across ranked, resolved, extracted, and evidence artifacts.")
    p.add_argument("--ranked-input", required=True, help="ranked_records JSON")
    p.add_argument("--resolved-input", required=True, help="resolved_records JSON")
    p.add_argument("--extracted-input", required=True, help="extracted_records JSON")
    p.add_argument("--evidence-input", required=True, help="evidence_records JSON")
    p.add_argument("--pmid", default="", help="Optional PMID filter")
    p.add_argument("--title-contains", default="", help="Optional title substring filter")
    p.add_argument("--write", default="", help="Optional output JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    ranked = load_json(args.ranked_input)
    resolved = load_json(args.resolved_input)
    extracted = load_json(args.extracted_input)
    evidence = load_json(args.evidence_input)

    ranked_map = {}
    for bucket in ["kept_records", "dropped_records"]:
        for rec in ranked.get(bucket, []) or []:
            ranked_map[record_key(rec)] = rec

    resolved_map = {}
    for bucket in ["resolved_records", "unresolved_records"]:
        for rec in resolved.get(bucket, []) or []:
            resolved_map[record_key(rec)] = rec

    extracted_map = {}
    for bucket in ["extracted_records", "skipped_records"]:
        for rec in extracted.get(bucket, []) or []:
            extracted_map[record_key(rec)] = rec

    evidence_bucket_map, evidence_rec_map = bucket_maps(evidence)

    records = []
    for k, rrec in resolved_map.items():
        erec = extracted_map.get(k, {})
        evrec = evidence_rec_map.get(k)
        bucket = evidence_bucket_map.get(k)
        ranked_rec = ranked_map.get(k)

        title = compact_ws(rrec.get("title") or erec.get("title") or (evrec or {}).get("title"))
        pmid = compact_ws(rrec.get("pmid") or erec.get("pmid") or (evrec or {}).get("pmid"))

        if args.pmid and pmid != compact_ws(args.pmid):
            continue
        if args.title_contains and compact_ws(args.title_contains).lower() not in title.lower():
            continue

        html_class = classify_html_source(rrec, erec, evrec)
        failure_mode, next_action = classify_failure_mode(ranked_rec, rrec, erec, evrec, bucket, html_class)

        item = {
            "pmid": pmid or None,
            "title": title or None,
            "journal": compact_ws(rrec.get("journal") or erec.get("journal") or (evrec or {}).get("journal")) or None,
            "evidence_bucket": bucket,
            "html_source_class": html_class,
            "likely_failure_mode": failure_mode,
            "suggested_next_action": next_action,
            "resolved_by": compact_ws(rrec.get("resolved_by")) or None,
            "best_source": compact_ws(rrec.get("best_source")) or None,
            "fulltext_status": compact_ws(rrec.get("fulltext_status")) or None,
            "analysis_ready": rrec.get("analysis_ready"),
            "html_path_exists": html_path_exists(rrec),
            "ranked_abstract_length": len(best_abstract_text(ranked_rec or {})),
            "extracted_abstract_length": len(best_abstract_text(erec)),
            "analysis_text_length": text_len(erec, "analysis_text"),
            "section_lengths": {
                "introduction": text_len(erec, "introduction_text"),
                "methods": text_len(erec, "methods_text"),
                "results": text_len(erec, "results_text"),
                "discussion": text_len(erec, "discussion_text"),
                "conclusion": text_len(erec, "conclusion_text"),
            },
            "paper_kind": compact_ws((evrec or {}).get("paper_kind")) or None,
            "document_role": compact_ws((evrec or {}).get("document_role")) or None,
            "skip_reason": compact_ws((evrec or {}).get("skip_reason")) or None,
            "partial_reason": compact_ws((evrec or {}).get("partial_reason")) or None,
            "quality_signals": (evrec or {}).get("quality_signals") or {},
            "retrieved_by_query_families": (rrec.get("retrieved_by_query_families") or (evrec or {}).get("retrieved_by_query_families") or []),
        }
        records.append(item)

    failure_counts = {}
    html_class_counts = {}
    bucket_counts = {}
    for rec in records:
        failure_counts[rec["likely_failure_mode"]] = failure_counts.get(rec["likely_failure_mode"], 0) + 1
        html_class_counts[rec["html_source_class"]] = html_class_counts.get(rec["html_source_class"], 0) + 1
        bucket = rec["evidence_bucket"] or "unbucketed"
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    output = {
        "schema_version": "1.0",
        "artifact_type": "html_audit",
        "stage": "html_audit",
        "generated_at": utc_now_iso(),
        "summary": {
            "record_count": len(records),
            "failure_mode_counts": dict(sorted(failure_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
            "html_source_class_counts": dict(sorted(html_class_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
            "evidence_bucket_counts": dict(sorted(bucket_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        },
        "records": records,
    }

    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(output["summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
