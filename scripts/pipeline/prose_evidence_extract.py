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
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


PREVIEW_MARKERS = [
    "access through your institution",
    "buy or subscribe",
    "this is a preview of subscription content",
    "subscribe to this journal",
    "purchase on springerlink",
    "skip to main content",
    "thank you for visiting nature.com",
    "instant access to the full article pdf",
    "access via your institution",
]

OUTCOME_MARKERS = [
    "hamd", "hamd-17", "madrs", "hama", "qids", "phq-9", "bdi", "ymrs",
    "response", "remission", "cognition", "executive function", "memory",
    "fmri", "resting-state", "reward", "symptom", "depressive", "anxiety",
]

CONDITION_PATTERNS = [
    "major depressive disorder",
    "bipolar depression",
    "treatment resistant depression",
    "treatment-resistant depression",
    "depression",
    "obsessive-compulsive disorder",
    "ocd",
    "anxiety",
    "substance use disorder",
    "schizophrenia",
    "bipolar disorder",
]

INTERVENTION_PATTERNS = [
    "transcranial magnetic stimulation",
    "repetitive transcranial magnetic stimulation",
    "theta burst stimulation",
    "intermittent theta burst stimulation",
    "deep tms",
    "transcranial direct current stimulation",
    "electroconvulsive therapy",
    "ect",
    "deep brain stimulation",
    "dbs",
    "tms",
    "tdcs",
]

SAFETY_MARKERS = [
    "adverse event", "adverse events", "well tolerated", "tolerated",
    "safe", "safety", "scalp discomfort", "no serious adverse", "serious adverse event",
]

LIMITATION_MARKERS = [
    "limitation", "limitations", "future research", "further research", "challenge", "challenges",
    "small sample", "preliminary", "open-label", "single-arm", "inconsistent", "heterogeneity",
]

PAPER_KIND_RULES = [
    ("systematic_review", [r"\bsystematic review\b", r"\bmeta-analysis\b", r"\bmeta analysis\b"]),
    ("review", [r"\breview\b", r"\bfocused review\b", r"\bnarrative review\b"]),
    ("guideline", [r"\bguideline\b", r"\bconsensus\b", r"\brecommendation\b"]),
    ("randomized_trial", [r"\brandomized\b", r"\brandomised\b", r"\bsham-controlled\b", r"\bdouble-blind\b"]),
    ("open_label_trial", [r"\bopen-label\b", r"\bsingle-arm\b", r"\bfeasibility study\b", r"\bpilot trial\b"]),
    ("cohort_study", [r"\bcohort\b", r"\blongitudinal\b", r"\bretrospective\b"]),
    ("cross_sectional", [r"\bcross-sectional\b"]),
    ("case_report", [r"\bcase report\b", r"\bcase series\b"]),
    ("mechanism_review", [r"\bmechanism\b", r"\bmodels\b", r"\bmissing links\b"]),
]


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


def compact_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def html_unescape_basic(text: str) -> str:
    return (
        text.replace("&nbsp;", " ")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
    )


def normalize_for_compare(text: str) -> str:
    text = html_unescape_basic(compact_ws(text)).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def first_sentences(text: str, max_sentences: int = 2, max_chars: int = 600) -> str:
    text = compact_ws(html_unescape_basic(text))
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = []
    total = 0
    for part in parts:
        part = part.strip()
        if not part:
            continue
        out.append(part)
        total += len(part)
        if len(out) >= max_sentences or total >= max_chars:
            break
    return compact_ws(" ".join(out))[:max_chars]


def count_preview_markers(text: str) -> int:
    low = normalize_for_compare(text)
    return sum(1 for marker in PREVIEW_MARKERS if marker in low)


def section_nonempty_count(rec: dict[str, Any]) -> int:
    fields = ["introduction_text", "methods_text", "results_text", "discussion_text", "conclusion_text"]
    return sum(1 for key in fields if compact_ws(rec.get(key)))


def detect_abstract_duplication(rec: dict[str, Any]) -> bool:
    abstract = normalize_for_compare(rec.get("abstract_extracted") or "")
    body = normalize_for_compare(rec.get("body_text") or rec.get("analysis_text") or "")
    if not abstract or not body:
        return False
    if len(abstract) < 250:
        return False
    return abstract[:600] in body or body[:600] in abstract


def infer_paper_kind(title: str, combined_text: str) -> str:
    hay = f"{title}\n{combined_text}".lower()
    for label, patterns in PAPER_KIND_RULES:
        for pat in patterns:
            if re.search(pat, hay, flags=re.I):
                return label
    return "unclear"


def infer_condition(text: str) -> str:
    low = text.lower()
    for pat in CONDITION_PATTERNS:
        if pat in low:
            return pat
    return ""


def infer_intervention(text: str) -> str:
    low = text.lower()
    for pat in INTERVENTION_PATTERNS:
        if pat in low:
            return pat
    return ""


def infer_study_design(text: str, paper_kind: str) -> str:
    low = text.lower()
    if paper_kind in {"systematic_review", "review", "guideline", "mechanism_review"}:
        return paper_kind
    if re.search(r"\brandomized\b|\brandomised\b|\bdouble-blind\b|\bsham", low):
        return "randomized_interventional"
    if re.search(r"\bopen-label\b|\bsingle-arm\b|\bfeasibility study\b|\bpilot", low):
        return "open_label_interventional"
    if re.search(r"\bcohort\b|\blongitudinal\b|\bretrospective\b", low):
        return "observational_cohort"
    if re.search(r"\bcross-sectional\b", low):
        return "cross_sectional"
    if re.search(r"\bcase report\b|\bcase series\b", low):
        return "case_series"
    return "unclear"


def extract_sample_size(text: str):
    patterns = [
        r"\b(\d{1,4})\s+(patients|participants|subjects)\b",
        r"\bn\s*=\s*(\d{1,4})\b",
        r"\b(\d{1,4})\s+completed\b",
        r"\b(\d{1,4})\s+were recruited\b",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def extract_p_values(text: str, limit: int = 8) -> list[str]:
    vals = re.findall(r"p\s*[<=>]\s*0?\.\d+|p\s*[<=>]\s*0+", text, flags=re.I)
    out = []
    seen = set()
    for val in vals:
        val = compact_ws(val.lower())
        if val not in seen:
            seen.add(val)
            out.append(val)
        if len(out) >= limit:
            break
    return out


def extract_outcome_markers(text: str, limit: int = 10) -> list[str]:
    low = text.lower()
    found = []
    for marker in OUTCOME_MARKERS:
        if marker in low:
            found.append(marker)
        if len(found) >= limit:
            break
    return found


def extract_score_change_snippets(text: str, limit: int = 6) -> list[str]:
    snippets = []
    patterns = [
        r"[A-Z]{2,10}(?:-\d+)?\s+scores?\s+(?:decreas\w+|improv\w+|from)\s+[^.]{0,120}\.",
        r"\bresponse\b[^.]{0,120}\.",
        r"\bremission\b[^.]{0,120}\.",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            snippet = compact_ws(html_unescape_basic(m.group(0)))
            if snippet and snippet not in snippets:
                snippets.append(snippet)
            if len(snippets) >= limit:
                return snippets
    return snippets


def extract_safety_snippets(text: str, limit: int = 4) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", compact_ws(html_unescape_basic(text)))
    out = []
    for sent in sentences:
        low = sent.lower()
        if any(marker in low for marker in SAFETY_MARKERS):
            out.append(sent[:260])
        if len(out) >= limit:
            break
    return out


def extract_limitation_snippets(text: str, limit: int = 5) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", compact_ws(html_unescape_basic(text)))
    out = []
    for sent in sentences:
        low = sent.lower()
        if any(marker in low for marker in LIMITATION_MARKERS):
            out.append(sent[:280])
        if len(out) >= limit:
            break
    return out


def extract_metrics_blob(text: str) -> dict[str, Any]:
    return {
        "p_values": extract_p_values(text),
        "outcome_markers": extract_outcome_markers(text),
        "score_change_snippets": extract_score_change_snippets(text),
    }


def classify_record(rec: dict[str, Any]) -> dict[str, Any]:
    fulltext_status = compact_ws(rec.get("fulltext_status"))
    title = compact_ws(rec.get("title"))
    abstract = html_unescape_basic(compact_ws(rec.get("abstract_extracted")))
    analysis_text = html_unescape_basic(compact_ws(rec.get("analysis_text")))
    body_text = html_unescape_basic(compact_ws(rec.get("body_text")))
    discussion = html_unescape_basic(compact_ws(rec.get("discussion_text")))
    combined_text = "\n".join(x for x in [abstract, analysis_text, discussion] if x)

    section_count = section_nonempty_count(rec)
    preview_marker_count = count_preview_markers("\n".join([body_text, analysis_text, title]))
    duplicated_abstract = detect_abstract_duplication(rec)
    paper_kind = infer_paper_kind(title, combined_text)

    analysis_len = len(compact_ws(analysis_text))
    abstract_len = len(compact_ws(abstract))
    has_methods = bool(compact_ws(rec.get("methods_text")))
    has_results = bool(compact_ws(rec.get("results_text")))
    has_discussion = bool(compact_ws(rec.get("discussion_text")))
    has_intro = bool(compact_ws(rec.get("introduction_text")))

    study_design_guess = infer_study_design(combined_text, paper_kind)
    sample_size_hint = extract_sample_size(f"{abstract}\n{analysis_text}\n{body_text}")
    metrics_hint = extract_metrics_blob(f"{abstract}\n{analysis_text}\n{body_text}")
    outcome_marker_count = len(metrics_hint.get("outcome_markers") or [])
    p_value_count = len(metrics_hint.get("p_values") or [])
    score_snippet_count = len(metrics_hint.get("score_change_snippets") or [])

    trial_like_signal = (
        paper_kind in {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional", "case_report"}
        or study_design_guess in {"randomized_interventional", "open_label_interventional", "observational_cohort", "cross_sectional", "case_series"}
    )

    evidence_signal_count = sum([
        1 if sample_size_hint else 0,
        1 if p_value_count > 0 else 0,
        1 if score_snippet_count > 0 else 0,
        1 if outcome_marker_count >= 2 else 0,
        1 if trial_like_signal else 0,
    ])

    source_substance = "partial_fulltext"
    extraction_quality = "medium"
    semantic_ready = True
    partial_reason = ""
    skip_reason = ""

    if fulltext_status == "fulltext_xml":
        if section_count >= 3 or (paper_kind in {"review", "mechanism_review", "systematic_review"} and (has_intro or has_discussion)):
            source_substance = "fulltext_structured"
            extraction_quality = "high"
            semantic_ready = True
        else:
            source_substance = "partial_fulltext"
            extraction_quality = "medium"
            semantic_ready = True
            partial_reason = "xml_but_limited_section_structure"

    elif fulltext_status == "fulltext_html":
        if section_count >= 2 and analysis_len >= 1200:
            source_substance = "fulltext_structured"
            extraction_quality = "high"
            semantic_ready = True

        elif preview_marker_count >= 2 and not (has_methods or has_results or has_discussion):
            strong_preview_signal = (
                abstract_len >= 350
                and analysis_len >= 700
                and evidence_signal_count >= 2
                and (
                    sample_size_hint
                    or p_value_count > 0
                    or score_snippet_count > 0
                    or outcome_marker_count >= 4
                )
            )

            if strong_preview_signal:
                source_substance = "partial_fulltext"
                extraction_quality = "low" if duplicated_abstract else "medium"
                semantic_ready = True
                partial_reason = "html_preview_with_meaningful_trial_like_signals"
            else:
                source_substance = "preview_only"
                extraction_quality = "low"
                semantic_ready = False
                skip_reason = "preview_only_html_with_no_structured_sections"

        elif (
            abstract_len >= 350
            and analysis_len >= 700
            and (section_count >= 1 or evidence_signal_count >= 2)
        ):
            source_substance = "partial_fulltext"
            extraction_quality = "medium" if section_count >= 1 else "low"
            semantic_ready = True
            partial_reason = "html_partial_text_with_evidence_signals"

        elif (
            abstract_len >= 250
            and trial_like_signal
            and paper_kind in {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional", "case_report"}
        ):
            source_substance = "partial_fulltext"
            extraction_quality = "low"
            semantic_ready = True
            partial_reason = "abstract_backfilled_trial_like_html"

        else:
            source_substance = "preview_only"
            extraction_quality = "low"
            semantic_ready = False
            skip_reason = "insufficient_html_substance"

    elif fulltext_status == "fulltext_pdf":
        source_substance = "partial_fulltext"
        extraction_quality = "low"
        semantic_ready = abstract_len >= 350
        partial_reason = "pdf_not_parsed"
        if not semantic_ready:
            skip_reason = "pdf_not_parsed_and_abstract_insufficient"

    elif fulltext_status in {"landing_page_only", "partial_html_usable"}:
        abstract_backfill_candidate = bool(rec.get("abstract_backfill_candidate"))
        if (
            abstract_len >= 250
            and (
                trial_like_signal
                or evidence_signal_count >= 2
                or paper_kind in {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional"}
            )
        ):
            source_substance = "partial_fulltext"
            extraction_quality = "low"
            semantic_ready = True
            partial_reason = (
                "abstract_backfilled_landing_page"
                if fulltext_status == "landing_page_only" or abstract_backfill_candidate
                else "partial_html_usable"
            )
        elif abstract_len >= 250:
            source_substance = "partial_fulltext"
            extraction_quality = "low"
            semantic_ready = True
            partial_reason = "abstract_backfilled_partial_html"
        else:
            source_substance = "preview_only"
            extraction_quality = "low"
            semantic_ready = False
            skip_reason = "insufficient_landing_page_substance"

    else:
        source_substance = "preview_only"
        extraction_quality = "low"
        semantic_ready = False
        skip_reason = "no_meaningful_fulltext_content"

    if paper_kind in {"review", "systematic_review", "mechanism_review", "guideline"} and semantic_ready:
        if extraction_quality == "low":
            extraction_quality = "medium"

    evidence_level = "moderate"
    if paper_kind in {"systematic_review", "guideline"} and source_substance == "fulltext_structured":
        evidence_level = "high"
    elif paper_kind in {"randomized_trial"} and source_substance in {"fulltext_structured", "partial_fulltext"}:
        evidence_level = "high" if extraction_quality == "high" else "moderate"
    elif paper_kind in {"open_label_trial", "cohort_study", "cross_sectional", "case_report"}:
        evidence_level = "low"
    elif paper_kind in {"review", "mechanism_review"}:
        evidence_level = "moderate" if source_substance != "preview_only" else "low"
    elif source_substance == "preview_only":
        evidence_level = "provisional"
    else:
        evidence_level = "provisional"

    confidence = "high" if extraction_quality == "high" else ("medium" if extraction_quality == "medium" else "low")

    return {
        "paper_kind": paper_kind,
        "extraction_quality": extraction_quality,
        "source_substance": source_substance,
        "semantic_ready": semantic_ready,
        "evidence_level": evidence_level,
        "confidence": confidence,
        "partial_reason": partial_reason or None,
        "skip_reason": skip_reason or None,
        "quality_signals": {
            "section_count": section_count,
            "preview_marker_count": preview_marker_count,
            "duplicated_abstract": duplicated_abstract,
            "analysis_text_length": analysis_len,
            "abstract_length": abstract_len,
            "has_introduction": has_intro,
            "has_methods": has_methods,
            "has_results": has_results,
            "has_discussion": has_discussion,
            "study_design_guess": study_design_guess,
            "sample_size_hint": sample_size_hint,
            "trial_like_signal": trial_like_signal,
            "evidence_signal_count": evidence_signal_count,
            "outcome_marker_count": outcome_marker_count,
            "p_value_count": p_value_count,
            "score_snippet_count": score_snippet_count,
            "abstract_backfill_candidate": bool(rec.get("abstract_backfill_candidate")),
        },
    }


def classify_document_role(title: str, paper_kind: str, quality_signals: dict[str, Any]) -> tuple[str, str, str | None]:
    title_low = compact_ws(title).lower()

    review_title_marker = any(
        marker in title_low
        for marker in [
            "systematic review",
            "meta-analysis",
            "meta analysis",
            "narrative review",
            "review",
            "guideline",
            "consensus",
        ]
    )
    review_like_kind = paper_kind in {"review", "systematic_review", "mechanism_review", "guideline"}

    primary_signal_count = sum([
        1 if quality_signals.get("sample_size_hint") else 0,
        1 if quality_signals.get("trial_like_signal") else 0,
        1 if quality_signals.get("has_methods") else 0,
        1 if quality_signals.get("has_results") else 0,
        1 if (quality_signals.get("p_value_count") or 0) > 0 else 0,
        1 if (quality_signals.get("score_snippet_count") or 0) > 0 else 0,
    ])

    if review_title_marker or review_like_kind:
        note = "review marker in title or metadata" if review_title_marker else "review-style content pattern"
        confidence = "high" if review_title_marker or paper_kind in {"systematic_review", "guideline"} else "medium"
        return "review_like", confidence, note

    if primary_signal_count >= 4:
        return "primary_empirical", "high", "sample, methods, or result signals suggest original study"

    if primary_signal_count >= 2:
        return "primary_empirical", "medium", "multiple empirical study signals present"

    return "mixed_or_unclear", "low", "limited signals for clear review vs primary classification"


def extract_author_names(raw_authors: Any) -> list[str]:
    names: list[str] = []
    if isinstance(raw_authors, list):
        for item in raw_authors:
            if isinstance(item, str):
                name = compact_ws(item)
            elif isinstance(item, dict):
                given = compact_ws(item.get("given") or item.get("firstname") or item.get("first_name"))
                family = compact_ws(item.get("family") or item.get("lastname") or item.get("last_name"))
                collective = compact_ws(item.get("name") or item.get("full_name"))
                name = collective or " ".join(x for x in [given, family] if x).strip()
            else:
                name = ""
            if name:
                names.append(name)
    return names


def extract_discussion_significance(text: str, limit: int = 3) -> list[str]:
    sentences = re.split(r"(?<=[.!?])\s+", compact_ws(html_unescape_basic(text)))
    out = []
    keywords = [
        "suggest", "indicate", "support", "implication", "important",
        "may provide", "may improve", "clinical", "relevant", "highlight"
    ]
    for sent in sentences:
        low = sent.lower()
        if any(k in low for k in keywords):
            out.append(sent[:320])
        if len(out) >= limit:
            break
    return out


def phrase_regex(phrase: str) -> str:
    parts = [re.escape(x) for x in compact_ws(phrase).lower().split()]
    if not parts:
        return r"$a"
    return r"\b" + r"\s+".join(parts) + r"\b"


def phrase_in_text(text: str, phrase: str) -> bool:
    low = compact_ws(text).lower()
    if not low:
        return False
    return re.search(phrase_regex(phrase), low) is not None


def infer_intervention_from_text(title: str, combined: str) -> str | None:
    txt = compact_ws(f"{title}\n{combined}").lower()

    patterns = [
        ("intranasal racemic ketamine", ["intranasal racemic ketamine"]),
        ("racemic ketamine", ["racemic ketamine"]),
        ("intranasal esketamine", ["intranasal esketamine"]),
        ("esketamine", ["esketamine", "s-ketamine"]),
        ("ketamine", ["ketamine"]),
        ("electroconvulsive therapy", ["electroconvulsive therapy", "ect"]),
        ("repetitive transcranial magnetic stimulation", ["repetitive transcranial magnetic stimulation", "rtms"]),
        ("theta burst stimulation", ["theta burst stimulation", "intermittent theta burst stimulation", "continuous theta burst stimulation", "itbs", "ctbs"]),
        ("deep tms", ["deep tms"]),
        ("tdcs", ["tdcs", "transcranial direct current stimulation"]),
        ("dbs", ["dbs", "deep brain stimulation"]),
        ("vagus nerve stimulation", ["vagus nerve stimulation", "vns"]),
        ("psilocybin", ["psilocybin"]),
    ]

    for label, variants in patterns:
        for variant in variants:
            if phrase_in_text(txt, variant):
                return label
    return None



def corrected_paper_kind_from_title(title: str, paper_kind: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)

    # Strong review signals first
    if any(x in t for x in ["systematic review", "meta-analysis", "meta analysis", "scoping review", "narrative review", "review of"]):
        return "systematic_review" if "systematic review" in t or "meta-analysis" in t or "meta analysis" in t else "review"

    # Strong empirical signals
    if "randomized clinical trial" in t or "randomised clinical trial" in t:
        return "randomized_trial"
    if "randomized controlled trial" in t or "randomised controlled trial" in t:
        return "randomized_trial"
    if "double-blind randomized" in t or "double blind randomized" in t:
        return "randomized_trial"
    if "open-label" in t or "open label" in t:
        return "open_label_trial"
    if "longitudinal study" in t:
        return "cohort_study"
    if "cohort study" in t or "retrospective cohort" in t or "prospective cohort" in t:
        return "cohort_study"
    if "observational study" in t or "real-world" in t or "real world" in t:
        return "observational_study"
    if "validation study" in t or "pilot study" in t:
        return "observational_study"
    if "case series" in t:
        return "case_series"

    return pk or None


def corrected_document_role_from_kind_and_title(title: str, paper_kind: str | None, document_role: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)
    dr = compact_ws(document_role)

    if pk in {"randomized_trial", "open_label_trial", "cohort_study", "observational_study", "case_series"}:
        return "primary_empirical"
    if pk in {"systematic_review", "meta_analysis", "review", "narrative_review", "scoping_review"}:
        return "review_like"

    # Title-based backstop
    if any(x in t for x in [
        "randomized clinical trial",
        "randomized controlled trial",
        "randomised controlled trial",
        "double-blind randomized",
        "open-label",
        "observational study",
        "cohort study",
        "longitudinal study",
        "validation study",
        "pilot study",
        "real-world",
        "real world",
    ]):
        return "primary_empirical"

    return dr or None



def corrected_paper_kind_from_title_and_design(title: str, paper_kind: str | None, study_design: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)
    sd = compact_ws(study_design)

    # Strong review signals
    if "systematic review" in t or "meta-analysis" in t or "meta analysis" in t:
        return "systematic_review"
    if "scoping review" in t:
        return "scoping_review"
    if "narrative review" in t:
        return "review"
    if re.search(r"\breview\b", t):
        if any(x in t for x in ["systematic review", "meta-analysis", "meta analysis", "narrative review", "scoping review"]) or t.endswith("review"):
            return "review"

    # Strong empirical title signals
    if "randomized clinical trial" in t or "randomised clinical trial" in t:
        return "randomized_trial"
    if "randomized controlled trial" in t or "randomised controlled trial" in t:
        return "randomized_trial"
    if "double-blind randomized" in t or "double blind randomized" in t:
        return "randomized_trial"
    if "open-label" in t or "open label" in t:
        return "open_label_trial"
    if "longitudinal study" in t:
        return "cohort_study"
    if "cohort study" in t or "retrospective cohort" in t or "prospective cohort" in t:
        return "cohort_study"
    if "observational study" in t or "real-world" in t or "real world" in t:
        return "observational_study"
    if "validation study" in t or "pilot study" in t:
        return "observational_study"
    if "case series" in t:
        return "case_series"

    # Fall back to inferred study design when title is less explicit
    if sd in {"randomized_trial", "open_label_trial", "cohort_study", "observational_study", "case_series"}:
        return sd
    if sd in {"systematic_review", "meta_analysis", "review", "narrative_review", "scoping_review"}:
        return sd

    return pk or None


def corrected_document_role_from_kind_and_title(title: str, paper_kind: str | None, document_role: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)
    dr = compact_ws(document_role)

    if pk in {"randomized_trial", "open_label_trial", "cohort_study", "observational_study", "case_series"}:
        return "primary_empirical"
    if pk in {"systematic_review", "meta_analysis", "review", "narrative_review", "scoping_review"}:
        return "review_like"

    if any(x in t for x in [
        "randomized clinical trial",
        "randomized controlled trial",
        "randomised controlled trial",
        "double-blind randomized",
        "open-label",
        "observational study",
        "cohort study",
        "longitudinal study",
        "validation study",
        "pilot study",
        "real-world",
        "real world",
    ]):
        return "primary_empirical"

    return dr or None


def build_evidence_object(rec: dict[str, Any], cls: dict[str, Any]) -> dict[str, Any]:
    title = compact_ws(rec.get("title"))
    abstract = html_unescape_basic(compact_ws(rec.get("abstract_extracted")))
    intro = html_unescape_basic(compact_ws(rec.get("introduction_text")))
    methods = html_unescape_basic(compact_ws(rec.get("methods_text")))
    results = html_unescape_basic(compact_ws(rec.get("results_text")))
    discussion = html_unescape_basic(compact_ws(rec.get("discussion_text")))
    conclusion = html_unescape_basic(compact_ws(rec.get("conclusion_text")))
    analysis = html_unescape_basic(compact_ws(rec.get("analysis_text")))
    body = html_unescape_basic(compact_ws(rec.get("body_text")))

    combined = "\n".join(x for x in [abstract, intro, methods, results, discussion, conclusion, analysis, body] if x)
    authors = extract_author_names(rec.get("authors") or [])
    first_author = authors[0] if authors else None
    last_author = authors[-1] if authors else None
    discussion_significance = extract_discussion_significance(f"{discussion}\n{conclusion}\n{analysis}")
    inferred_intervention = infer_intervention_from_text(title, combined)
    initial_paper_kind = compact_ws(cls["paper_kind"])
    study_design = infer_study_design(combined, initial_paper_kind)
    corrected_paper_kind = corrected_paper_kind_from_title_and_design(title, initial_paper_kind, study_design)
    corrected_document_role = corrected_document_role_from_kind_and_title(title, corrected_paper_kind, cls.get("document_role"))
    condition = infer_condition(f"{title}\n{combined}")
    intervention = infer_intervention(f"{title}\n{combined}")
    sample_size = extract_sample_size(f"{abstract}\n{methods}\n{results}\n{analysis}\n{body}")
    metrics = extract_metrics_blob(f"{abstract}\n{results}\n{analysis}\n{body}")
    safety = extract_safety_snippets(f"{abstract}\n{results}\n{discussion}\n{analysis}\n{body}")
    limitations = extract_limitation_snippets(f"{discussion}\n{conclusion}\n{analysis}\n{body}")

    main_claim_source = abstract or results or discussion or analysis or body
    main_claim = first_sentences(main_claim_source, max_sentences=2, max_chars=480)

    key_findings = []
    for candidate in [
        first_sentences(results, max_sentences=2, max_chars=360),
        first_sentences(discussion, max_sentences=2, max_chars=360),
        first_sentences(abstract, max_sentences=2, max_chars=360),
        first_sentences(analysis, max_sentences=2, max_chars=360),
    ]:
        candidate = compact_ws(candidate)
        if candidate and candidate not in key_findings:
            key_findings.append(candidate)
    key_findings = key_findings[:3]

    outcomes = metrics["outcome_markers"]
    peer_review_status = "peer_reviewed"
    journal_low = compact_ws(rec.get("journal")).lower()
    if journal_low == "biorxiv" or "preprint" in journal_low:
        peer_review_status = "preprint_or_unclear"

    clinical_relevance = ""
    if condition and intervention:
        clinical_relevance = f"Relevant to {condition} and {intervention} literature."
    elif intervention:
        clinical_relevance = f"Relevant to neuromodulation literature involving {intervention}."

    document_role, classification_confidence, classification_note = classify_document_role(
        title=title,
        paper_kind=cls["paper_kind"],
        quality_signals=cls["quality_signals"],
    )

    return {
        "pmid": rec.get("pmid"),
        "doi": rec.get("doi"),
        "pmcid": rec.get("pmcid"),
        "pmid": rec.get("pmid"),
        "pmcid": rec.get("pmcid"),
        "doi": rec.get("doi"),
        "title": title,
        "journal": compact_ws(rec.get("journal")),
        "publication_date": rec.get("publication_date") or rec.get("pubdate_iso") or rec.get("epubdate_iso") or rec.get("pubdate"),
        "pubdate_iso": rec.get("pubdate_iso"),
        "epubdate_iso": rec.get("epubdate_iso"),
        "pubdate": rec.get("pubdate"),
        "authors": authors,
        "first_author": first_author,
        "last_author": last_author,
        "lane": rec.get("lane"),
        "paper_kind": cls["paper_kind"],
        "document_role": document_role,
        "classification_confidence": classification_confidence,
        "classification_note": classification_note,
        "peer_review_status": peer_review_status,
        "population": None,
        "sample_size": sample_size,
        "condition": condition or None,
        "intervention_or_exposure": inferred_intervention,
        "target_or_mechanism": None,
        "comparator": None,
        "study_design": study_design,
        "outcomes": outcomes,
        "key_findings": key_findings,
        "metrics": metrics,
        "limitations": limitations,
        "safety_findings": safety,
        "discussion_significance": discussion_significance,
        "main_claim": main_claim,
        "clinical_relevance": clinical_relevance or None,
        "evidence_level": cls["evidence_level"],
        "extraction_quality": cls["extraction_quality"],
        "semantic_ready": cls["semantic_ready"],
        "source_substance": cls["source_substance"],
        "confidence": cls["confidence"],
        "partial_reason": cls["partial_reason"],
        "skip_reason": cls["skip_reason"],
        "provenance": {
            "fulltext_status": rec.get("fulltext_status"),
            "extraction_status": rec.get("extraction_status"),
            "resolved_by": rec.get("resolved_by"),
            "best_source": rec.get("best_source"),
            "xml_path": rec.get("xml_path"),
            "html_path": rec.get("html_path"),
            "pdf_path": rec.get("pdf_path"),
        },
        "quality_signals": cls["quality_signals"],
    }


def build_stats(evidence_records: list[dict[str, Any]], partial_records: list[dict[str, Any]], skipped_records: list[dict[str, Any]]) -> dict[str, Any]:
    all_records = evidence_records + partial_records + skipped_records

    def count_by(key: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for rec in all_records:
            val = compact_ws(rec.get(key)) or "unknown"
            counts[val] = counts.get(val, 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))

    return {
        "input_count": len(all_records),
        "semantic_ready_count": len(evidence_records) + len(partial_records),
        "evidence_record_count": len(evidence_records),
        "partial_record_count": len(partial_records),
        "skipped_record_count": len(skipped_records),
        "quality_counts": count_by("extraction_quality"),
        "source_substance_counts": count_by("source_substance"),
        "paper_kind_counts": count_by("paper_kind"),
        "records_with_sample_size": sum(1 for rec in evidence_records + partial_records if rec.get("sample_size")),
        "records_with_metrics": sum(1 for rec in evidence_records + partial_records if (rec.get("metrics") or {}).get("p_values") or (rec.get("metrics") or {}).get("score_change_snippets")),
        "records_with_limitations": sum(1 for rec in evidence_records + partial_records if rec.get("limitations")),
    }


def build_semantic_feedback(evidence_records: list[dict[str, Any]], partial_records: list[dict[str, Any]], skipped_records: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(evidence_records) + len(partial_records) + len(skipped_records)
    if total == 0:
        return {
            "retry_suggested": True,
            "missing_angles": ["no_records_available_for_evidence_extraction"],
            "candidate_retry_actions": ["increase_top_k"],
        }

    partial_rate = len(partial_records) / total
    skipped_rate = len(skipped_records) / total
    high_quality_count = sum(1 for rec in evidence_records if rec.get("extraction_quality") == "high")
    review_count = sum(1 for rec in evidence_records + partial_records if rec.get("paper_kind") in {"review", "systematic_review", "mechanism_review", "guideline"})
    primary_count = sum(1 for rec in evidence_records + partial_records if rec.get("paper_kind") in {"randomized_trial", "open_label_trial", "cohort_study", "cross_sectional", "case_report"})
    with_metrics = sum(1 for rec in evidence_records + partial_records if ((rec.get("metrics") or {}).get("p_values") or (rec.get("metrics") or {}).get("score_change_snippets")))

    missing_angles = []
    candidate_retry_actions = []
    retry_suggested = False

    if high_quality_count == 0:
        missing_angles.append("no_high_quality_fulltext_records")
        candidate_retry_actions.append("prefer_accessible_records")
        retry_suggested = True
    if partial_rate > 0.4:
        missing_angles.append("high_partial_record_fraction")
    if skipped_rate > 0.3:
        missing_angles.append("too_many_preview_only_or_low_substance_records")
        candidate_retry_actions.append("swap_low_access_records")
        retry_suggested = True
    if primary_count == 0 and review_count > 0:
        missing_angles.append("reviews_without_primary_studies")
        candidate_retry_actions.append("boost_primary_study_queries")
    if with_metrics == 0:
        missing_angles.append("no_metrics_extracted")
        candidate_retry_actions.append("increase_top_k")

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
        "coverage": {
            "partial_rate": round(partial_rate, 3),
            "skipped_rate": round(skipped_rate, 3),
            "high_quality_count": high_quality_count,
            "review_like_count": review_count,
            "primary_study_like_count": primary_count,
            "records_with_metrics": with_metrics,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Classify extracted prose records and produce evidence-ready objects.")
    p.add_argument("--input", required=True, help="Input JSON from scripts/pipeline/prose_pubmed_fulltext_extract.py, or - for stdin")
    p.add_argument("--records-key", default="extracted_records", help="List key to read from input JSON")
    p.add_argument("--top-k", type=int, default=0, help="Optional cap on number of records to process")
    p.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    p.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    p.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    p.add_argument("--schema-version", default="1.1", help="Schema version for evidence extraction artifact output.")
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

    evidence_records: list[dict[str, Any]] = []
    partial_records: list[dict[str, Any]] = []
    skipped_records: list[dict[str, Any]] = []

    for rec in records:
        rec = dict(rec)
        rec["lane"] = lane or rec.get("lane")
        cls = classify_record(rec)
        obj = build_evidence_object(rec, cls)
        if obj["semantic_ready"] and obj["source_substance"] == "fulltext_structured":
            evidence_records.append(obj)
        elif obj["semantic_ready"]:
            partial_records.append(obj)
        else:
            skipped_records.append(obj)

    stats = build_stats(evidence_records, partial_records, skipped_records)
    semantic_feedback = build_semantic_feedback(evidence_records, partial_records, skipped_records)

    output = {
        "schema_version": args.schema_version,
        "stage": "evidence_extract",
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
        "semantic_feedback": semantic_feedback,
        "evidence_records": evidence_records,
        "partial_records": partial_records,
        "skipped_records": skipped_records,
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
