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


def best_abstract(rec: dict[str, Any]) -> str:
    for k in ["abstract_extracted", "abstract", "abstract_text"]:
        txt = compact_ws(rec.get(k))
        if txt:
            return txt
    return ""


def normalize_author_entry(item: Any) -> str:
    if isinstance(item, str):
        return compact_ws(item)
    if isinstance(item, dict):
        collective = compact_ws(item.get("name") or item.get("full_name"))
        given = compact_ws(item.get("given") or item.get("firstname") or item.get("first_name"))
        family = compact_ws(item.get("family") or item.get("lastname") or item.get("last_name"))
        return collective or " ".join(x for x in [given, family] if x).strip()
    return ""


def author_list_from_field(raw_authors: Any) -> list[str]:
    out = []
    if isinstance(raw_authors, list):
        for item in raw_authors:
            name = normalize_author_entry(item)
            if name and name not in out:
                out.append(name)
    elif isinstance(raw_authors, str):
        # simple fallback if comma-separated author string
        parts = [compact_ws(x) for x in re.split(r';|,', raw_authors) if compact_ws(x)]
        for part in parts:
            if part not in out:
                out.append(part)
    return out


def extract_meta_authors(html: str) -> list[str]:
    out = []
    meta_tags = re.findall(
        r'(?is)<meta\s+[^>]*?(?:name|property)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']*)["\'][^>]*?>',
        html,
    )
    for name, content in meta_tags:
        low = name.lower()
        if any(k in low for k in ["citation_author", "dc.creator", "author"]):
            val = compact_ws(unescape(content))
            if val and val not in out:
                out.append(val)
    return out


def collect_jsonld_authors(obj: Any, out: list[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "author":
                if isinstance(v, list):
                    for item in v:
                        name = normalize_author_entry(item)
                        if name and name not in out:
                            out.append(name)
                else:
                    name = normalize_author_entry(v)
                    if name and name not in out:
                        out.append(name)
            else:
                collect_jsonld_authors(v, out)
    elif isinstance(obj, list):
        for item in obj:
            collect_jsonld_authors(item, out)


def extract_jsonld_authors(html: str) -> list[str]:
    out = []
    blocks = re.findall(r'(?is)<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html)
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        try:
            obj = json.loads(block)
            collect_jsonld_authors(obj, out)
        except Exception:
            continue
    return out


def extract_html_authors(html: str) -> list[str]:
    out = []
    for name in extract_meta_authors(html) + extract_jsonld_authors(html):
        if name and name not in out:
            out.append(name)
    return out


def best_publication_date(rec: dict[str, Any], source: dict[str, Any]) -> str:
    for obj in [rec, source]:
        for k in ["publication_date", "pubdate_iso", "epubdate_iso", "pubdate"]:
            val = compact_ws(obj.get(k))
            if val:
                return val
    return ""


def best_metadata_value(rec: dict[str, Any], source: dict[str, Any], key: str) -> str:
    for obj in [rec, source]:
        val = compact_ws(obj.get(key))
        if val:
            return val
    return ""


def best_authors(rec: dict[str, Any], source: dict[str, Any]) -> list[str]:
    # 1. direct structured fields
    for candidate in [
        author_list_from_field(rec.get("authors")),
        author_list_from_field(source.get("authors")),
    ]:
        if candidate:
            return candidate

    # 2. reconstruct from first/last if present
    for obj in [rec, source]:
        first = compact_ws(obj.get("first_author"))
        last = compact_ws(obj.get("last_author"))
        if first and last:
            if first == last:
                return [first]
            return [first, last]

    # 3. parse saved HTML
    html_path = compact_ws(rec.get("html_path") or source.get("html_path"))
    if html_path:
        hp = Path(html_path)
        if not hp.exists():
            hp = Path.cwd() / html_path
        if hp.exists():
            try:
                html = hp.read_text(encoding="utf-8", errors="ignore")
                names = extract_html_authors(html)
                if names:
                    return names
            except Exception:
                pass

    return []


def build_source_map(*payloads: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for payload in payloads:
        for bucket in [
            "kept_records", "dropped_records",
            "resolved_records", "unresolved_records",
            "extracted_records", "skipped_records"
        ]:
            for rec in payload.get(bucket, []) or []:
                k = record_key(rec)
                if k not in out:
                    out[k] = rec
    return out


def patch_record(rec: dict[str, Any], source_map: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], bool]:
    item = dict(rec)
    k = record_key(item)
    source = source_map.get(k, {})

    original_abs = compact_ws(item.get("abstract_extracted"))
    original_analysis = compact_ws(item.get("analysis_text"))
    original_authors = author_list_from_field(item.get("authors"))

    fallback_abs = best_abstract(item) or best_abstract(source)
    changed = False

    if not original_abs and fallback_abs:
        item["abstract_extracted"] = fallback_abs
        item["abstract_backfilled"] = True
        changed = True

    if len(original_analysis) < 50 and fallback_abs:
        item["analysis_text"] = fallback_abs
        item["analysis_backfilled_from_abstract"] = True
        changed = True

    if not original_authors:
        fallback_authors = best_authors(item, source)
        if fallback_authors:
            item["authors"] = fallback_authors
            item["author_backfilled"] = True
            item["first_author"] = fallback_authors[0]
            item["last_author"] = fallback_authors[-1]
            changed = True

    original_pubdate = compact_ws(item.get("publication_date") or item.get("pubdate_iso") or item.get("epubdate_iso") or item.get("pubdate"))
    if not original_pubdate:
        fallback_pubdate = best_publication_date(item, source)
        if fallback_pubdate:
            item["publication_date"] = fallback_pubdate
            changed = True

    for key in ["pmid", "pmcid", "doi", "pubdate_iso", "epubdate_iso", "pubdate"]:
        if not compact_ws(item.get(key)):
            fallback_val = best_metadata_value(item, source, key)
            if fallback_val:
                item[key] = fallback_val
                changed = True

    return item, changed


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill missing abstract/analysis_text/authors into extracted records from upstream ranked/resolved artifacts.")
    p.add_argument("--input", required=True, help="Extracted JSON")
    p.add_argument("--ranked-input", default="", help="Optional ranked JSON")
    p.add_argument("--resolved-input", default="", help="Optional resolved JSON")
    p.add_argument("--write", required=True, help="Output JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    extracted = load_json(args.input)
    ranked = load_json(args.ranked_input) if args.ranked_input else {}
    resolved = load_json(args.resolved_input) if args.resolved_input else {}

    source_map = build_source_map(ranked, resolved, extracted)

    changed_count = 0
    out = dict(extracted)

    for bucket in ["extracted_records", "skipped_records"]:
        new_records = []
        for rec in extracted.get(bucket, []) or []:
            patched, changed = patch_record(rec, source_map)
            if changed:
                changed_count += 1
            new_records.append(patched)
        out[bucket] = new_records

    out["stage"] = "fulltext_extract_backfilled"
    out["generated_at"] = utc_now_iso()
    out["backfill_stats"] = {
        "changed_record_count": changed_count
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(out["backfill_stats"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
