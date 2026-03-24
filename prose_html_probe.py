#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, UTC
from html import unescape
from pathlib import Path
from typing import Any


SECTION_KEYWORDS = [
    "abstract",
    "introduction",
    "background",
    "methods",
    "methodology",
    "materials and methods",
    "results",
    "discussion",
    "conclusion",
    "limitations",
]

ARTICLE_CONTAINER_PATTERNS = [
    r"<article\b",
    r'article-body',
    r'article-content',
    r'full-text',
    r'main-content',
    r'content-body',
    r'abstract',
]


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


def strip_html(html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return compact_ws(unescape(text))


def extract_title(html: str) -> str:
    m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    return compact_ws(unescape(m.group(1))) if m else ""


def extract_meta_value(html: str, key_patterns: list[str]) -> str:
    meta_tags = re.findall(r'(?is)<meta\s+[^>]*?(?:name|property)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']*)["\'][^>]*?>', html)
    for name, content in meta_tags:
        low = name.lower()
        for pat in key_patterns:
            if pat in low:
                return compact_ws(unescape(content))
    return ""


def extract_jsonld_blocks(html: str) -> list[str]:
    return re.findall(r'(?is)<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html)


def parse_jsonld_signal(html: str) -> dict[str, Any]:
    blocks = extract_jsonld_blocks(html)
    abstract_found = False
    body_found = False
    author_found = False

    for block in blocks:
        low = block.lower()
        if '"abstract"' in low:
            abstract_found = True
        if '"articlebody"' in low:
            body_found = True
        if '"author"' in low:
            author_found = True

    return {
        "jsonld_block_count": len(blocks),
        "jsonld_has_abstract": abstract_found,
        "jsonld_has_article_body": body_found,
        "jsonld_has_author": author_found,
    }


def extract_headings(html: str) -> list[str]:
    headings = re.findall(r'(?is)<h[1-3][^>]*>(.*?)</h[1-3]>', html)
    out = []
    for h in headings:
        h = re.sub(r"(?s)<[^>]+>", " ", h)
        h = compact_ws(unescape(h))
        if h:
            out.append(h)
    return out


def section_keyword_hits(headings: list[str]) -> dict[str, int]:
    hits = {}
    for key in SECTION_KEYWORDS:
        hits[key] = sum(1 for h in headings if key in h.lower())
    return hits


def article_container_hits(html: str) -> list[str]:
    hits = []
    low = html.lower()
    for pat in ARTICLE_CONTAINER_PATTERNS:
        if re.search(pat, low):
            hits.append(pat)
    return hits


def classify_probe(meta_abstract_len: int, stripped_len: int, headings: list[str], section_hits: dict[str, int], container_hits: list[str]) -> str:
    if stripped_len > 10000 and (section_hits.get("methods", 0) or section_hits.get("results", 0) or section_hits.get("discussion", 0)):
        return "article_body_likely_present"
    if meta_abstract_len > 200 and stripped_len > 1500 and not any(section_hits.values()):
        return "abstract_or_partial_html"
    if meta_abstract_len > 200 and stripped_len < 1500:
        return "abstract_only_or_preview"
    if not container_hits and stripped_len < 1000:
        return "landing_or_preview_page"
    return "unclear_html"


def suggested_action(probe_class: str) -> str:
    mapping = {
        "article_body_likely_present": "Patch extractor to use article container and heading-based section parsing.",
        "abstract_or_partial_html": "Harvest metadata and preserve as abstract-backed partial evidence; consider light body extraction fallback.",
        "abstract_only_or_preview": "Preserve abstract only and classify as partial evidence, not full HTML.",
        "landing_or_preview_page": "Downgrade earlier in resolver and avoid treating as analysis-ready full HTML.",
        "unclear_html": "Manual inspection or targeted parser refinement needed.",
    }
    return mapping.get(probe_class, "Manual inspection needed.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Probe saved HTML for current-run records to determine whether failures are access-limited or parser-limited.")
    p.add_argument("--resolved-input", required=True, help="resolved_records JSON")
    p.add_argument("--ranked-input", required=True, help="ranked_records JSON")
    p.add_argument("--extracted-input", required=True, help="extracted_records JSON")
    p.add_argument("--evidence-input", required=True, help="evidence_records JSON")
    p.add_argument("--pmid", default="", help="Optional PMID filter")
    p.add_argument("--title-contains", default="", help="Optional title substring filter")
    p.add_argument("--write", default="", help="Optional output JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    resolved = load_json(args.resolved_input)
    ranked = load_json(args.ranked_input)
    extracted = load_json(args.extracted_input)
    evidence = load_json(args.evidence_input)

    ranked_map = {}
    for bucket in ["kept_records", "dropped_records"]:
        for rec in ranked.get(bucket, []) or []:
            ranked_map[record_key(rec)] = rec

    extracted_map = {}
    for bucket in ["extracted_records", "skipped_records"]:
        for rec in extracted.get(bucket, []) or []:
            extracted_map[record_key(rec)] = rec

    evidence_bucket_map, evidence_rec_map = bucket_maps(evidence)

    records = []
    for bucket in ["resolved_records", "unresolved_records"]:
        for rec in resolved.get(bucket, []) or []:
            fulltext_status = compact_ws(rec.get("fulltext_status"))
            if fulltext_status != "fulltext_html":
                continue

            pmid = compact_ws(rec.get("pmid"))
            title = compact_ws(rec.get("title"))

            if args.pmid and pmid != compact_ws(args.pmid):
                continue
            if args.title_contains and compact_ws(args.title_contains).lower() not in title.lower():
                continue

            k = record_key(rec)
            ranked_rec = ranked_map.get(k, {})
            extracted_rec = extracted_map.get(k, {})
            evidence_rec = evidence_rec_map.get(k, {})
            evidence_bucket = evidence_bucket_map.get(k)

            html_path = compact_ws(rec.get("html_path"))
            html_exists = False
            html_text = ""
            if html_path:
                hp = Path(html_path)
                if not hp.exists():
                    hp = Path.cwd() / html_path
                if hp.exists():
                    html_exists = True
                    html_text = hp.read_text(encoding="utf-8", errors="ignore")

            title_tag = extract_title(html_text) if html_text else ""
            meta_abstract = extract_meta_value(html_text, ["citation_abstract", "description", "dc.description"]) if html_text else ""
            meta_title = extract_meta_value(html_text, ["citation_title", "og:title", "dc.title"]) if html_text else ""
            meta_journal = extract_meta_value(html_text, ["citation_journal_title", "prism.publicationname", "dc.source"]) if html_text else ""

            jsonld = parse_jsonld_signal(html_text) if html_text else {
                "jsonld_block_count": 0,
                "jsonld_has_abstract": False,
                "jsonld_has_article_body": False,
                "jsonld_has_author": False,
            }

            headings = extract_headings(html_text) if html_text else []
            section_hits = section_keyword_hits(headings)
            container_hits = article_container_hits(html_text) if html_text else []
            stripped = strip_html(html_text) if html_text else ""

            probe_class = classify_probe(
                meta_abstract_len=len(meta_abstract),
                stripped_len=len(stripped),
                headings=headings,
                section_hits=section_hits,
                container_hits=container_hits,
            )

            item = {
                "pmid": pmid or None,
                "title": title or None,
                "journal": compact_ws(rec.get("journal") or extracted_rec.get("journal") or evidence_rec.get("journal")) or None,
                "evidence_bucket": evidence_bucket,
                "resolved_by": compact_ws(rec.get("resolved_by")) or None,
                "best_source": compact_ws(rec.get("best_source")) or None,
                "html_path": html_path or None,
                "html_exists": html_exists,
                "title_tag": title_tag or None,
                "meta_title": meta_title or None,
                "meta_journal": meta_journal or None,
                "meta_abstract_length": len(meta_abstract),
                "ranked_abstract_length": len(best_abstract_text(ranked_rec)),
                "extracted_abstract_length": len(best_abstract_text(extracted_rec)),
                "analysis_text_length": text_len(extracted_rec, "analysis_text"),
                "stripped_text_length": len(stripped),
                "heading_count": len(headings),
                "headings_preview": headings[:12],
                "section_keyword_hits": section_hits,
                "article_container_hits": container_hits,
                "jsonld": jsonld,
                "probe_class": probe_class,
                "suggested_next_action": suggested_action(probe_class),
                "skip_reason": compact_ws(evidence_rec.get("skip_reason")) or None,
                "partial_reason": compact_ws(evidence_rec.get("partial_reason")) or None,
                "paper_kind": compact_ws(evidence_rec.get("paper_kind")) or None,
                "document_role": compact_ws(evidence_rec.get("document_role")) or None,
            }
            records.append(item)

    summary = {
        "record_count": len(records),
        "probe_class_counts": {},
        "evidence_bucket_counts": {},
    }

    for rec in records:
        summary["probe_class_counts"][rec["probe_class"]] = summary["probe_class_counts"].get(rec["probe_class"], 0) + 1
        bucket = rec["evidence_bucket"] or "unbucketed"
        summary["evidence_bucket_counts"][bucket] = summary["evidence_bucket_counts"].get(bucket, 0) + 1

    summary["probe_class_counts"] = dict(sorted(summary["probe_class_counts"].items(), key=lambda kv: (-kv[1], kv[0])))
    summary["evidence_bucket_counts"] = dict(sorted(summary["evidence_bucket_counts"].items(), key=lambda kv: (-kv[1], kv[0])))

    output = {
        "schema_version": "1.0",
        "artifact_type": "html_probe",
        "stage": "html_probe",
        "generated_at": utc_now_iso(),
        "summary": summary,
        "records": records,
    }

    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
