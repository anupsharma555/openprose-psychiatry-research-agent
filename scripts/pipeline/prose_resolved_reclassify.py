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


def strip_html(html: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return compact_ws(unescape(text))


def extract_meta_value(html: str, key_patterns: list[str]) -> str:
    meta_tags = re.findall(r'(?is)<meta\s+[^>]*?(?:name|property)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']*)["\'][^>]*?>', html)
    for name, content in meta_tags:
        low = name.lower()
        for pat in key_patterns:
            if pat in low:
                return compact_ws(unescape(content))
    return ""


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


def best_abstract_text(rec: dict[str, Any]) -> str:
    for k in ["abstract_extracted", "abstract", "abstract_text"]:
        txt = compact_ws(rec.get(k))
        if txt:
            return txt
    return ""


def classify_html(html_text: str, ranked_abstract_len: int) -> tuple[str, dict[str, Any]]:
    if not html_text:
        if ranked_abstract_len >= 250:
            return "landing_page_only", {
                "probe_reason": "no_saved_html_but_abstract_available",
                "stripped_text_length": 0,
                "heading_count": 0,
                "meta_abstract_length": 0,
                "container_hits": [],
            }
        return "landing_page_only", {
            "probe_reason": "no_saved_html",
            "stripped_text_length": 0,
            "heading_count": 0,
            "meta_abstract_length": 0,
            "container_hits": [],
        }

    stripped = strip_html(html_text)
    headings = extract_headings(html_text)
    section_hits = section_keyword_hits(headings)
    container_hits = article_container_hits(html_text)
    meta_abstract = extract_meta_value(html_text, ["citation_abstract", "description", "dc.description"])
    jsonld = parse_jsonld_signal(html_text)

    stripped_len = len(stripped)
    meta_abstract_len = len(meta_abstract)

    if stripped_len > 10000 and (section_hits.get("methods", 0) or section_hits.get("results", 0) or section_hits.get("discussion", 0)):
        html_class = "fulltext_html_structured"
        reason = "long_html_with_section_signals"
    elif stripped_len > 1500 and (meta_abstract_len > 200 or jsonld["jsonld_has_abstract"] or container_hits):
        html_class = "partial_html_usable"
        reason = "substantive_html_but_weak_section_structure"
    elif ranked_abstract_len >= 250:
        html_class = "landing_page_only"
        reason = "thin_html_with_upstream_abstract_only"
    else:
        html_class = "landing_page_only"
        reason = "thin_preview_html"

    return html_class, {
        "probe_reason": reason,
        "stripped_text_length": stripped_len,
        "heading_count": len(headings),
        "meta_abstract_length": meta_abstract_len,
        "container_hits": container_hits,
        "section_hits": section_hits,
        "jsonld": jsonld,
    }


def should_carry_forward(rec: dict[str, Any], html_class: str, ranked_abstract_len: int) -> bool:
    status = compact_ws(rec.get("fulltext_status"))

    if status == "fulltext_xml":
        return True
    if status == "fulltext_pdf":
        return True
    if status == "free_url_only":
        return True
    if html_class == "fulltext_html_structured":
        return True
    if html_class == "partial_html_usable":
        return True
    if html_class == "landing_page_only" and ranked_abstract_len >= 250:
        return True
    return False


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Reclassify resolved HTML records more honestly while preserving abstract-backed fallback for relevant records.")
    p.add_argument("--input", required=True, help="Resolved JSON")
    p.add_argument("--ranked-input", default="", help="Optional ranked JSON for abstract fallback signal")
    p.add_argument("--write", required=True, help="Output reclassified resolved JSON")
    return p


def main() -> int:
    args = build_parser().parse_args()

    resolved = load_json(args.input)
    ranked = load_json(args.ranked_input) if args.ranked_input else {}

    ranked_map = {}
    for bucket in ["kept_records", "dropped_records"]:
        for rec in ranked.get(bucket, []) or []:
            key = compact_ws(rec.get("pmid")) or compact_ws(rec.get("title"))
            if key:
                ranked_map[key] = rec

    new_resolved = []
    new_unresolved = []
    changed = 0

    for bucket in ["resolved_records", "unresolved_records"]:
        for rec in resolved.get(bucket, []) or []:
            item = dict(rec)
            status = compact_ws(item.get("fulltext_status"))

            key = compact_ws(item.get("pmid")) or compact_ws(item.get("title"))
            ranked_rec = ranked_map.get(key, {})
            ranked_abstract_len = len(best_abstract_text(ranked_rec))

            html_path = compact_ws(item.get("html_path"))
            html_text = ""
            if html_path:
                hp = Path(html_path)
                if not hp.exists():
                    hp = Path.cwd() / html_path
                if hp.exists():
                    html_text = hp.read_text(encoding="utf-8", errors="ignore")

            if status == "fulltext_html":
                html_class, probe = classify_html(html_text, ranked_abstract_len)
                item["html_source_class"] = html_class
                item["html_probe"] = probe
                item["abstract_backfill_candidate"] = ranked_abstract_len >= 250 and html_class in {"landing_page_only", "partial_html_usable"}

                if html_class == "landing_page_only":
                    item["fulltext_status"] = "landing_page_only"
                    item["analysis_ready"] = False
                    changed += 1
                elif html_class == "partial_html_usable":
                    item["fulltext_status"] = "partial_html_usable"
                    item["analysis_ready"] = False
                    changed += 1
                elif html_class == "fulltext_html_structured":
                    item["fulltext_status"] = "fulltext_html_structured"
                    item["analysis_ready"] = True
                    changed += 1

            carry = should_carry_forward(item, compact_ws(item.get("html_source_class")), ranked_abstract_len)
            if carry:
                new_resolved.append(item)
            else:
                new_unresolved.append(item)

    out = dict(resolved)
    out["stage"] = "resolved_reclassified"
    out["generated_at"] = utc_now_iso()
    out["resolved_records"] = new_resolved
    out["unresolved_records"] = new_unresolved
    out["resolved_count"] = len(new_resolved)
    out["unresolved_count"] = len(new_unresolved)
    out["reclassify_stats"] = {
        "changed_record_count": changed,
        "carry_forward_count": len(new_resolved),
        "downgraded_to_unresolved_count": len(new_unresolved),
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(out["reclassify_stats"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
