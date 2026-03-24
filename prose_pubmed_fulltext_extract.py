#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Any

SECTION_ORDER = [
    "abstract",
    "introduction",
    "methods",
    "results",
    "discussion",
    "conclusion",
    "body",
]

DISPLAY_SECTION_ORDER = [
    "abstract",
    "introduction",
    "methods",
    "results",
    "conclusion",
    "discussion",
    "body",
]

STOP_SECTION_PATTERNS = [
    r"^references?$",
    r"^acknowledg",
    r"^author contributions?$",
    r"^conflicts? of interest$",
    r"^ethics",
    r"^funding$",
    r"^supplement",
    r"^data availability$",
    r"^code availability$",
    r"^footnotes?$",
]

HTML_STRIP_BLOCKS = [
    "script", "style", "noscript", "svg", "header", "footer",
    "nav", "aside", "form", "button",
]

def compact_ws(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def normalize_ws_multiline(text: str | None) -> str:
    text = text or ""
    lines = [compact_ws(x) for x in text.splitlines()]
    lines = [x for x in lines if x]
    return "\n".join(lines)

def load_json(path: str) -> dict[str, Any]:
    if path == "-":
        return json.load(sys.stdin)
    return json.loads(Path(path).read_text(encoding="utf-8"))

def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

def load_orchestration_plan(path_str: str) -> dict[str, Any]:
    if not path_str:
        return {}
    try:
        return json.loads(Path(path_str).read_text(encoding="utf-8"))
    except Exception:
        return {}

def utc_now_iso() -> str:
    return __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def build_extract_stats(
    input_records: list[dict[str, Any]],
    extracted_records: list[dict[str, Any]],
    skipped_records: list[dict[str, Any]],
) -> dict[str, Any]:
    all_processed = extracted_records + skipped_records

    def has_text(rec: dict[str, Any], key: str) -> bool:
        return bool(compact_ws(rec.get(key)))

    analysis_lengths = [len(rec.get("analysis_text") or "") for rec in extracted_records]
    analysis_lengths_sorted = sorted(analysis_lengths)

    if analysis_lengths_sorted:
        mid = len(analysis_lengths_sorted) // 2
        if len(analysis_lengths_sorted) % 2 == 1:
            median_len = analysis_lengths_sorted[mid]
        else:
            median_len = int((analysis_lengths_sorted[mid - 1] + analysis_lengths_sorted[mid]) / 2)
        max_len = max(analysis_lengths_sorted)
    else:
        median_len = 0
        max_len = 0

    return {
        "input_count": len(input_records),
        "processed_count": len(all_processed),
        "extracted_count": len(extracted_records),
        "skipped_count": len(skipped_records),
        "xml_input_count": sum(1 for rec in all_processed if rec.get("fulltext_status") == "fulltext_xml"),
        "html_input_count": sum(1 for rec in all_processed if rec.get("fulltext_status") == "fulltext_html"),
        "pdf_input_count": sum(1 for rec in all_processed if rec.get("fulltext_status") == "fulltext_pdf"),
        "analysis_text_nonempty_count": sum(1 for rec in extracted_records if has_text(rec, "analysis_text")),
        "methods_present_count": sum(1 for rec in extracted_records if has_text(rec, "methods_text")),
        "results_present_count": sum(1 for rec in extracted_records if has_text(rec, "results_text")),
        "discussion_present_count": sum(1 for rec in extracted_records if has_text(rec, "discussion_text")),
        "conclusion_present_count": sum(1 for rec in extracted_records if has_text(rec, "conclusion_text")),
        "median_analysis_text_length": median_len,
        "max_analysis_text_length": max_len,
    }

def build_extraction_feedback(extracted_records: list[dict[str, Any]], skipped_records: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(extracted_records)
    if n == 0:
        return {
            "retry_suggested": True,
            "missing_angles": ["no_records_extracted"],
            "candidate_retry_actions": ["prefer_xml_or_html_fulltext", "increase_top_k"],
        }

    def rate(key: str) -> float:
        count = sum(1 for rec in extracted_records if compact_ws(rec.get(key)))
        return count / n if n else 0.0

    analysis_rate = rate("analysis_text")
    methods_rate = rate("methods_text")
    results_rate = rate("results_text")
    discussion_rate = rate("discussion_text")
    conclusion_rate = rate("conclusion_text")
    skipped_rate = (len(skipped_records) / (len(extracted_records) + len(skipped_records))) if (len(extracted_records) + len(skipped_records)) else 0.0

    missing_angles: list[str] = []
    candidate_retry_actions: list[str] = []
    retry_suggested = False

    if analysis_rate < 0.8:
        retry_suggested = True
        missing_angles.append("low_analysis_text_coverage")
        candidate_retry_actions.append("prefer_xml_or_html_fulltext")

    if methods_rate < 0.5:
        missing_angles.append("low_methods_section_coverage")

    if results_rate < 0.5:
        missing_angles.append("low_results_section_coverage")

    if discussion_rate < 0.5:
        missing_angles.append("low_discussion_section_coverage")

    if conclusion_rate < 0.4:
        missing_angles.append("low_conclusion_section_coverage")

    if skipped_rate > 0.25:
        missing_angles.append("high_skip_rate")
        candidate_retry_actions.append("swap_low_access_records")

    deduped_actions = []
    seen = set()
    for action in candidate_retry_actions:
        if action not in seen:
            seen.add(action)
            deduped_actions.append(action)

    return {
        "retry_suggested": retry_suggested,
        "missing_angles": missing_angles,
        "candidate_retry_actions": deduped_actions,
        "section_coverage": {
            "analysis_text_rate": round(analysis_rate, 3),
            "methods_text_rate": round(methods_rate, 3),
            "results_text_rate": round(results_rate, 3),
            "discussion_text_rate": round(discussion_rate, 3),
            "conclusion_text_rate": round(conclusion_rate, 3),
            "skipped_rate": round(skipped_rate, 3),
        },
    }

def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag

def iter_elems_by_localname(root: ET.Element, wanted: set[str]):
    for elem in root.iter():
        if local_name(elem.tag) in wanted:
            yield elem

def elem_text(elem: ET.Element) -> str:
    return compact_ws(" ".join(t for t in elem.itertext()))

def canonical_section_name(title: str) -> str | None:
    t = compact_ws(title).lower()
    t = re.sub(r"[^a-z0-9\s/-]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    if not t:
        return None

    if re.search(r"^(introduction|background|overview)$", t):
        return "introduction"

    if re.search(
        r"(materials and methods|methods and materials|methods|methodology|study design|design|participants?|sample|cohort|setting|measures|assessment|assessments|procedures?|statistical analysis|analysis plan|data and preprocessing|preprocessing|data preprocessing|image processing|feature extraction)",
        t,
    ):
        return "methods"

    if re.search(
        r"(results|findings|main findings|outcomes?|performance|model performance|classification performance|predictive performance|experimental results|experiment results|empirical results|primary outcome|secondary outcome|validation|external validation|internal validation|test performance|prediction performance|classification results)",
        t,
    ):
        return "results"

    if re.search(r"(discussion|general discussion|interpretation|implications)", t):
        return "discussion"

    if re.search(r"(conclusion|conclusions|summary and conclusions?)", t):
        return "conclusion"

    return None

def should_stop_section(title: str) -> bool:
    t = compact_ws(title).lower()
    return any(re.search(pat, t) for pat in STOP_SECTION_PATTERNS)

def merge_text(dest: dict[str, str], key: str, text: str) -> None:
    text = normalize_ws_multiline(text)
    if not text:
        return
    if key in dest and dest[key]:
        dest[key] = normalize_ws_multiline(dest[key] + "\n\n" + text)
    else:
        dest[key] = text

def classify_article_structure(title: str, sections: dict[str, str]) -> str:
    tl = compact_ws(title).lower()
    keys = set(sections.keys())

    if "systematic" in tl or "scoping review" in tl or "review" in tl:
        return "review"
    if "commentary" in tl or "cautionary tale" in tl or "perspective" in tl:
        return "commentary"
    if "methods" in keys and ("results" in keys or "discussion" in keys):
        return "empirical"
    if "results" in keys:
        return "empirical"
    if "discussion" in keys and "conclusion" in keys and "methods" not in keys:
        return "commentary"
    return "unknown"

def ordered_sections_from_map(sections: dict[str, str], abstract_text: str) -> list[dict[str, str]]:
    ordered = []
    if compact_ws(abstract_text):
        ordered.append({"name": "abstract", "text": normalize_ws_multiline(abstract_text)})
    for name in SECTION_ORDER[1:]:
        text = sections.get(name) or ""
        if compact_ws(text):
            ordered.append({"name": name, "text": normalize_ws_multiline(text)})
    extras = [k for k in sections.keys() if k not in SECTION_ORDER]
    for name in sorted(extras):
        text = sections.get(name) or ""
        if compact_ws(text):
            ordered.append({"name": name, "text": normalize_ws_multiline(text)})
    return ordered


def ordered_sections_from_map_with_order(
    sections: dict[str, str],
    abstract_text: str,
    order: list[str],
) -> list[dict[str, str]]:
    ordered = []
    if compact_ws(abstract_text):
        ordered.append({"name": "abstract", "text": normalize_ws_multiline(abstract_text)})
    for name in [x for x in order if x != "abstract"]:
        text = sections.get(name) or ""
        if compact_ws(text):
            ordered.append({"name": name, "text": normalize_ws_multiline(text)})
    extras = [k for k in sections.keys() if k not in order]
    for name in sorted(extras):
        text = sections.get(name) or ""
        if compact_ws(text):
            ordered.append({"name": name, "text": normalize_ws_multiline(text)})
    return ordered


def split_paragraphs(text: str) -> list[str]:
    text = normalize_ws_multiline(text)
    if not text:
        return []
    return [normalize_ws_multiline(x) for x in re.split(r"\n{2,}", text) if compact_ws(x)]


def results_paragraph_score(text: str) -> int:
    t = compact_ws(text).lower()
    score = 0

    # Strong result anchors
    if re.search(r"\b(table|fig(?:ure)?)\s*\.?\s*\d+\b", t):
        score += 3
    if re.search(r"\b(balanced accuracy|auc|sensitivity|specificity|f1|accuracy|odds ratio|hazard ratio|confidence interval|ci\b|p\s*[<=>])", t):
        score += 2
    if re.search(r"\b(we found|we observed|yielded|achieved|performed better|performed worse|predicted|classified|demonstrated|showed)\b", t):
        score += 1
    if re.search(r"\b(classification performance|model performance|predictive performance|validation|external validation|internal validation|subgroup analysis|post-hoc)\b", t):
        score += 2

    # Discussion-style negatives
    if re.search(r"\b(in this study|in this work|our findings|these findings|this suggests|this indicates|to the best of our knowledge|future studies|future research|limitation|limitations|in conclusion)\b", t):
        score -= 2

    return score


def rescue_results_from_body(
    article_structure: str,
    sections: dict[str, str],
    body_text: str,
) -> tuple[dict[str, str], str]:
    if article_structure != "empirical":
        return sections, body_text
    if compact_ws(sections.get("results")):
        return sections, body_text

    paras = split_paragraphs(body_text)
    if not paras:
        return sections, body_text

    result_paras = []
    remaining_paras = []

    for para in paras:
        score = results_paragraph_score(para)
        if score >= 3:
            result_paras.append(para)
        else:
            remaining_paras.append(para)

    if result_paras:
        merge_text(sections, "results", "\n\n".join(result_paras))
        body_text = normalize_ws_multiline("\n\n".join(remaining_paras))
        if body_text:
            sections["body"] = body_text
        elif "body" in sections:
            del sections["body"]

    return sections, body_text


def finalize_extracted_record(
    title: str,
    abstract_text: str,
    sections: dict[str, str],
    body_text: str,
) -> dict[str, Any]:
    article_structure = classify_article_structure(title, sections)
    sections, body_text = rescue_results_from_body(article_structure, sections, body_text)
    article_structure = classify_article_structure(title, sections)

    ordered_sections_source = ordered_sections_from_map_with_order(sections, abstract_text, SECTION_ORDER)
    ordered_sections_display = ordered_sections_from_map_with_order(sections, abstract_text, DISPLAY_SECTION_ORDER)

    analysis_text = normalize_ws_multiline(
        "\n\n".join(
            part["text"]
            for part in ordered_sections_display
            if part["name"] in {"abstract", "introduction", "methods", "results", "discussion", "conclusion", "body"}
        )
    )

    return {
        "sections": sections,
        "ordered_sections": ordered_sections_display,
        "ordered_sections_source": ordered_sections_source,
        "ordered_sections_display": ordered_sections_display,
        "body_text": body_text,
        "analysis_text": analysis_text,
        "article_structure": article_structure,
        "introduction_text": sections.get("introduction", ""),
        "methods_text": sections.get("methods", ""),
        "results_text": sections.get("results", ""),
        "discussion_text": sections.get("discussion", ""),
        "conclusion_text": sections.get("conclusion", ""),
    }

def rescue_sections_from_body(body_text: str, sections: dict[str, str]) -> dict[str, str]:
    body = normalize_ws_multiline(body_text)
    if not body:
        return sections

    heading_patterns = {
        "introduction": r"(?im)^(introduction|background)\s*$",
        "methods": r"(?im)^(materials and methods|methods and materials|methods|methodology|study design|participants|sample|procedures|statistical analysis|data and preprocessing|preprocessing)\s*$",
        "results": r"(?im)^(results|findings|main findings|outcomes?|performance|model performance|classification performance|predictive performance|experimental results|validation)\s*$",
        "discussion": r"(?im)^(discussion|general discussion|interpretation|implications)\s*$",
        "conclusion": r"(?im)^(conclusion|conclusions|summary and conclusions?)\s*$",
    }

    hits = []
    for name, pat in heading_patterns.items():
        for m in re.finditer(pat, body):
            hits.append((m.start(), m.end(), name))
    hits.sort()

    if not hits:
        return sections

    for i, (start, end, name) in enumerate(hits):
        seg_start = end
        seg_end = hits[i + 1][0] if i + 1 < len(hits) else len(body)
        seg = normalize_ws_multiline(body[seg_start:seg_end])
        if seg and name not in sections:
            sections[name] = seg
    return sections

def parse_jats_xml(xml_path: str) -> dict[str, Any]:
    root = ET.parse(xml_path).getroot()

    title = ""
    for elem in iter_elems_by_localname(root, {"article-title"}):
        txt = elem_text(elem)
        if txt:
            title = txt
            break

    abstract_parts = []
    for abs_elem in iter_elems_by_localname(root, {"abstract"}):
        txt = elem_text(abs_elem)
        if txt:
            abstract_parts.append(txt)
    abstract_text = normalize_ws_multiline("\n\n".join(abstract_parts))

    sections: dict[str, str] = {}
    uncategorized_chunks: list[str] = []

    # Find body first
    body_elem = None
    for elem in iter_elems_by_localname(root, {"body"}):
        body_elem = elem
        break

    def walk_sec(sec: ET.Element):
        sec_title = ""
        sec_chunks: list[str] = []
        for child in list(sec):
            lname = local_name(child.tag)
            if lname == "title":
                sec_title = elem_text(child)
            elif lname == "p":
                txt = elem_text(child)
                if txt:
                    sec_chunks.append(txt)
            elif lname in {"sec"}:
                walk_sec(child)
            elif lname in {"fig", "table-wrap"}:
                # ignore for now
                pass
            else:
                txt = elem_text(child)
                if txt and len(txt) < 5000:
                    sec_chunks.append(txt)

        joined = normalize_ws_multiline("\n\n".join(sec_chunks))
        canonical = canonical_section_name(sec_title)
        if sec_title and should_stop_section(sec_title):
            return
        if canonical and joined:
            merge_text(sections, canonical, joined)
        elif joined:
            uncategorized_chunks.append(joined)

    if body_elem is not None:
        for child in list(body_elem):
            lname = local_name(child.tag)
            if lname == "sec":
                walk_sec(child)
            elif lname == "p":
                txt = elem_text(child)
                if txt:
                    uncategorized_chunks.append(txt)

    body_text = normalize_ws_multiline("\n\n".join(uncategorized_chunks))
    if body_text:
        sections["body"] = body_text

    sections = rescue_sections_from_body(body_text, sections)

    finalized = finalize_extracted_record(
        title=title,
        abstract_text=abstract_text,
        sections=sections,
        body_text=body_text,
    )

    return {
        "title_extracted": title,
        "abstract_extracted": abstract_text,
        **finalized,
    }

def strip_html_blocks(html: str) -> str:
    out = html
    for tag in HTML_STRIP_BLOCKS:
        out = re.sub(
            rf"(?is)<{tag}\b[^>]*>.*?</{tag}>",
            " ",
            out,
        )
    return out

def html_meta_dict(html: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in re.finditer(
        r'(?is)<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']+)["\']',
        html,
    ):
        out[m.group(1).strip().lower()] = unescape(compact_ws(m.group(2)))
    return out

def strip_tags(html: str) -> str:
    text = re.sub(r"(?is)<br\s*/?>", "\n", html)
    text = re.sub(r"(?is)</p\s*>", "\n\n", text)
    text = re.sub(r"(?is)</h[1-6]\s*>", "\n", text)
    text = re.sub(r"(?is)<li\b[^>]*>", "\n- ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    return normalize_ws_multiline(text)

def parse_html_sections(html_text: str) -> dict[str, str]:
    cleaned = strip_html_blocks(html_text)

    # Preserve headings to help section splitting
    heading_html = re.sub(
        r"(?is)<h([1-6])\b[^>]*>(.*?)</h\1>",
        lambda m: f"\n\nSECTION_HEADING: {strip_tags(m.group(2))}\n\n",
        cleaned,
    )
    text = strip_tags(heading_html)

    # Trim off obvious preamble/site chrome if article markers exist
    start_markers = [
        r"(?im)^abstract$",
        r"(?im)^introduction$",
        r"(?im)^background$",
    ]
    starts = [m.start() for pat in start_markers for m in re.finditer(pat, text)]
    if starts:
        text = text[min(starts):]

    # Stop at references / acknowledgments / boilerplate
    stop_pat = re.compile(
        r"(?im)^(references?|acknowledg(?:e)?ments?|author information|ethics declarations|additional information|supplementary information)\s*$"
    )
    mstop = stop_pat.search(text)
    if mstop:
        text = text[:mstop.start()]

    sections: dict[str, str] = {}
    body_chunks: list[str] = []
    current_name = "body"
    current_chunks: list[str] = []

    for raw_line in text.splitlines():
        line = compact_ws(raw_line)
        if not line:
            continue
        if line.startswith("SECTION_HEADING:"):
            if current_chunks:
                merge_text(sections, current_name, "\n".join(current_chunks))
                current_chunks = []
            heading = compact_ws(line.replace("SECTION_HEADING:", "", 1))
            canonical = canonical_section_name(heading)
            if heading and should_stop_section(heading):
                break
            current_name = canonical or "body"
            continue
        current_chunks.append(line)

    if current_chunks:
        merge_text(sections, current_name, "\n".join(current_chunks))

    body_text = sections.get("body", "")
    sections = rescue_sections_from_body(body_text, sections)

    return sections

def parse_html_file(html_path: str) -> dict[str, Any]:
    html_text = Path(html_path).read_text(encoding="utf-8", errors="replace")
    meta = html_meta_dict(html_text)

    title = (
        meta.get("citation_title")
        or meta.get("og:title")
        or ""
    )
    if not title:
        m = re.search(r"(?is)<title>(.*?)</title>", html_text)
        if m:
            title = compact_ws(strip_tags(m.group(1)))

    abstract_text = (
        meta.get("citation_abstract")
        or meta.get("description")
        or ""
    )
    sections = parse_html_sections(html_text)
    body_text = sections.get("body", "")
    finalized = finalize_extracted_record(
        title=title,
        abstract_text=normalize_ws_multiline(abstract_text),
        sections=sections,
        body_text=body_text,
    )

    return {
        "title_extracted": title,
        "abstract_extracted": normalize_ws_multiline(abstract_text),
        **finalized,
    }

def extract_record(rec: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "pmid": rec.get("pmid"),
        "pmcid": rec.get("pmcid"),
        "doi": rec.get("doi"),
        "title": rec.get("title"),
        "journal": rec.get("journal"),
        "fulltext_status": rec.get("fulltext_status"),
        "resolved_by": rec.get("resolved_by"),
        "best_source": rec.get("best_source"),
        "source_type": None,
        "source_path": None,
        "extraction_status": None,
        "title_extracted": "",
        "abstract_extracted": "",
        "sections": {},
        "ordered_sections": [],
        "ordered_sections_source": [],
        "ordered_sections_display": [],
        "body_text": "",
        "analysis_text": "",
        "article_structure": "unknown",
        "introduction_text": "",
        "methods_text": "",
        "results_text": "",
        "discussion_text": "",
        "conclusion_text": "",
    }

    if rec.get("xml_path") and Path(rec["xml_path"]).exists():
        out["source_type"] = "xml"
        out["source_path"] = rec["xml_path"]
        try:
            parsed = parse_jats_xml(rec["xml_path"])
            out.update(parsed)
            out["extraction_status"] = "parsed_xml"
        except Exception as exc:
            out["extraction_status"] = f"xml_error: {exc}"
        return out

    if rec.get("html_path") and Path(rec["html_path"]).exists():
        out["source_type"] = "html"
        out["source_path"] = rec["html_path"]
        try:
            parsed = parse_html_file(rec["html_path"])
            out.update(parsed)
            out["extraction_status"] = "parsed_html"
        except Exception as exc:
            out["extraction_status"] = f"html_error: {exc}"
        return out

    if rec.get("pdf_path") and Path(rec["pdf_path"]).exists():
        out["source_type"] = "pdf"
        out["source_path"] = rec["pdf_path"]
        out["extraction_status"] = "pdf_not_parsed"
        return out

    out["extraction_status"] = "no_supported_source"
    return out

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Extract structured full text from resolver output.")
    p.add_argument("--input", required=True, help="Resolver JSON or - for stdin")
    p.add_argument("--records-key", default="resolved_records", help="Key containing full-text-ready records")
    p.add_argument("--top-k", type=int, default=0, help="Optional cap on number of records to extract")
    p.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    p.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    p.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    p.add_argument("--schema-version", default="1.1", help="Schema version for extractor artifact output.")
    p.add_argument("--write", default="", help="Optional output path")
    return p

def main() -> int:
    args = build_parser().parse_args()
    payload = load_json(args.input)
    plan_path = args.orchestration_plan or ((payload.get("orchestration_context") or {}).get("plan_path")) or ""
    plan = load_orchestration_plan(plan_path)
    lane = compact_ws(args.lane) or compact_ws(payload.get("lane")) or None

    records = payload.get(args.records_key) or []
    if args.top_k and args.top_k > 0:
        records = records[: args.top_k]

    extracted = []
    skipped = []

    for rec in records:
        item = extract_record(rec)
        if item["extraction_status"] in {"parsed_xml", "parsed_html", "pdf_not_parsed"}:
            extracted.append(item)
        else:
            skipped.append(item)

    stats = build_extract_stats(records, extracted, skipped)
    extraction_feedback = build_extraction_feedback(extracted, skipped)

    output = {
        "schema_version": args.schema_version,
        "stage": "fulltext_extract",
        "run_id": args.run_id or payload.get("run_id") or plan.get("run_id") or None,
        "generated_at": utc_now_iso(),
        "input": args.input,
        "source_stage": payload.get("stage"),
        "records_key": args.records_key,
        "lane": lane,
        "orchestration_context": {
            "plan_path": plan_path or None,
            "topic": plan.get("topic") or (payload.get("orchestration_context") or {}).get("topic"),
            "lane_window": ((plan.get("lane_windows") or {}).get(lane)) if isinstance(plan.get("lane_windows"), dict) and lane else (payload.get("orchestration_context") or {}).get("lane_window"),
        },
        "input_stats": payload.get("stats"),
        "stats": stats,
        "extraction_feedback": extraction_feedback,
        "extracted_count": len(extracted),
        "skipped_count": len(skipped),
        "extracted_records": extracted,
        "skipped_records": skipped,
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
