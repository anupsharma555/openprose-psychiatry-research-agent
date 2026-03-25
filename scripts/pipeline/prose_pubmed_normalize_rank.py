#!/usr/bin/env python3

from __future__ import annotations

# WORKSPACE_ROOT_BOOTSTRAP
from pathlib import Path as _WorkspacePath
import sys as _workspace_sys
_WORKSPACE_ROOT = _WorkspacePath(__file__).resolve().parents[2]
if str(_WORKSPACE_ROOT) not in _workspace_sys.path:
    _workspace_sys.path.insert(0, str(_WORKSPACE_ROOT))

import argparse
import datetime as dt
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "how",
    "in", "into", "is", "it", "its", "of", "on", "or", "s", "such", "that",
    "the", "their", "this", "to", "was", "were", "will", "with", "within",
    "using", "use", "used", "via", "after", "before", "during", "over",
    "among", "across", "between", "through", "than", "we", "our",
    "title", "abstract", "preprint", "pt",
}

AI_TERMS = {
    "artificial intelligence", "machine learning", "deep learning",
    "large language model", "large language models", "llm", "llms",
    "natural language processing", "nlp", "random forest",
    "gradient boosting", "neural network", "neural networks",
    "classifier", "prediction model", "predictive model", "algorithm",
    "algorithms", "transformer", "foundation model", "foundation models",
}

MH_TERMS = {
    "psychiatry", "mental health", "psychiatric", "depression", "anxiety",
    "schizophrenia", "bipolar", "suicide", "suicidal", "autism", "adhd",
    "substance use", "addiction", "ptsd", "post-traumatic stress",
    "eating disorder", "psychosis", "ocd", "mood disorder", "self-harm",
    "behavioral health", "behavioural health", "cognitive impairment",
}

CORE_PSYCH_TERMS = {
    "psychiatry", "psychiatric", "mental health", "depression", "anxiety",
    "schizophrenia", "bipolar", "suicide", "suicidal", "psychosis", "ptsd",
    "obsessive-compulsive", "ocd", "mood disorder", "addiction",
    "substance use", "autism", "adhd", "cognition", "cognitive",
    "emotional", "affective", "anhedonia",
}

NONCORE_BIOMED_TERMS = {
    "allergic", "allergy", "asthma", "urticaria", "rhinitis", "food allergy",
    "neuroradiology", "stroke", "intracranial", "aneurysm", "fracture",
    "spine", "vascular", "hemorrhage", "cord compression", "dermatitis",
    "pediatric imaging", "acute ischemic", "arteriovenous malformation",
}

EXCLUDE_PATTERNS = {
    "editorial": [r"\beditorial\b"],
    "comment": [r"\bcomment\b", r"\bletter\b", r"\breply\b"],
    "protocol": [r"\bprotocol\b", r"study protocol"],
    "news": [r"\bnews\b"],
}

ARTICLE_TYPE_PATTERNS = [
    ("systematic_review", [r"systematic review", r"review of reviews"]),
    ("meta_analysis", [r"meta-analysis", r"meta analysis", r"individual participant data meta-analysis"]),
    ("scoping_review", [r"scoping review"]),
    ("narrative_review", [r"\breview\b", r"research agenda"]),
    ("randomized_trial", [r"randomized", r"randomised", r"\btrial\b"]),
    ("cohort_study", [r"\bcohort\b", r"longitudinal"]),
    ("case_control", [r"case-control", r"case control"]),
    ("cross_sectional", [r"cross-sectional", r"cross sectional"]),
    ("diagnostic_ml", [r"classifier", r"roc-auc", r"roc auc", r"machine learning", r"transformer"]),
]

DATE_PATTERNS = [
    "%Y/%m/%d %H:%M",
    "%Y %b %d",
    "%Y %b",
    "%Y",
]

JOURNAL_PATTERNS = {
    "tier_1": [
        "american journal of psychiatry",
        "jama psychiatry",
        "biological psychiatry",
        "molecular psychiatry",
        "world psychiatry",
        "the lancet psychiatry",
        "translational psychiatry",
        "nature mental health",
        "nature medicine",
        "nature",
        "science",
        "jama",
        "new england journal of medicine",
        "bmj",
    ],
    "tier_2": [
        "journal of clinical psychiatry",
        "journal of affective disorders",
        "journal of psychiatric research",
        "psychiatry research",
        "psychiatry research neuroimaging",
        "neuropsychopharmacology",
        "schizophrenia bulletin",
        "psychological medicine",
        "depression and anxiety",
        "american journal of geriatric psychiatry",
        "bipolar disorders",
        "addiction",
        "journal of the american academy of child and adolescent psychiatry",
        "journal of child psychology and psychiatry",
        "journal of psychiatric and mental health nursing",
    ],
}

PSYCH_JOURNAL_HINTS = {
    "psychiatry", "psychiatric", "psychology", "mental health",
    "schizophrenia", "affective disorders", "bipolar", "neuropsychopharmacology",
    "depression and anxiety",
}


def compact_ws(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_journal_name(text: str | None) -> str:
    text = compact_ws(text).lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return text


def journal_matches_pattern(journal_norm: str, pattern: str) -> bool:
    return (
        journal_norm == pattern
        or journal_norm.startswith(pattern + " ")
        or f" {pattern} " in f" {journal_norm} "
    )


def infer_journal_tier(journal: str) -> str:
    norm = normalize_journal_name(journal)
    for pat in JOURNAL_PATTERNS["tier_1"]:
        if journal_matches_pattern(norm, pat):
            return "tier_1"
    for pat in JOURNAL_PATTERNS["tier_2"]:
        if journal_matches_pattern(norm, pat):
            return "tier_2"
    return "other"


def tokenize(text: str) -> list[str]:
    return re.findall(r"[A-Za-z][A-Za-z0-9\-]+", text.lower())


def extract_query_terms(query: str, min_len: int = 3) -> list[str]:
    terms = []
    for tok in tokenize(query):
        if len(tok) < min_len or tok in STOPWORDS:
            continue
        terms.append(tok)
    out = []
    seen = set()
    for term in terms:
        if term not in seen:
            seen.add(term)
            out.append(term)
    return out


def text_has_any(text: str, phrases: set[str]) -> tuple[bool, list[str]]:
    hay = text.lower()
    hits = [phrase for phrase in phrases if phrase in hay]
    return bool(hits), sorted(hits)


def infer_article_type(title: str, abstract: str) -> str:
    hay = f"{title} {abstract}".lower()
    for label, patterns in ARTICLE_TYPE_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, hay):
                return label
    return "unknown"


def exclusion_reasons(title: str, abstract: str) -> list[str]:
    hay = f"{title} {abstract}".lower()
    reasons: list[str] = []
    for label, patterns in EXCLUDE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, hay):
                reasons.append(label)
                break
    return reasons


def parse_best_date(rec: dict[str, Any]) -> dt.date | None:
    for key in ("sortpubdate", "epubdate", "pubdate"):
        raw = compact_ws(rec.get(key))
        if not raw:
            continue
        for fmt in DATE_PATTERNS:
            try:
                if fmt == "%Y":
                    return dt.datetime.strptime(raw, fmt).date().replace(month=1, day=1)
                if fmt == "%Y %b":
                    return dt.datetime.strptime(raw, fmt).date().replace(day=1)
                return dt.datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    return None


def recency_score(best_date: dt.date | None, today: dt.date | None = None) -> tuple[float, int | None]:
    if best_date is None:
        return 0.0, None
    today = today or dt.date.today()
    days_old = max((today - best_date).days, 0)
    score = max(0.0, 3.0 - math.log10(days_old + 1))
    return round(score, 3), days_old


def overlap_score(text: str, query_terms: list[str]) -> tuple[float, list[str]]:
    hay = set(tokenize(text))
    hits = [term for term in query_terms if term in hay]
    if not query_terms:
        return 0.0, []
    ratio = len(hits) / max(len(query_terms), 1)
    return round(4.0 * ratio, 3), hits


def lane_type_score(article_type: str, lane: str) -> float:
    lane = canonicalize_lane(lane)
    review_like = {"systematic_review", "meta_analysis", "scoping_review", "narrative_review"}
    primary_like = {"randomized_trial", "cohort_study", "cross_sectional", "case_control", "diagnostic_ml"}

    if lane == "reviews":
        return 2.5 if article_type in review_like else 0.4

    if lane == "core_evidence":
        if article_type in {"systematic_review", "meta_analysis"}:
            return 2.5
        if article_type in {"randomized_trial", "cohort_study"}:
            return 1.8
        if article_type in review_like:
            return 1.5
        return 0.75

    if lane == "recent_peer_reviewed":
        if article_type in primary_like:
            return 1.9
        if article_type in review_like:
            return 1.25
        return 0.8

    if lane == "frontier":
        if article_type in primary_like:
            return 2.0
        if article_type in review_like:
            return 0.8
        return 0.5

    return 1.0


def journal_quality_score(journal: str, priority: str) -> tuple[str, float]:
    tier = infer_journal_tier(journal)
    if priority == "off":
        return tier, 0.0

    if priority == "strict":
        base = {
            "tier_1": 4.0,
            "tier_2": 2.0,
            "other": -0.25,
        }.get(tier, 0.0)
        return tier, round(base, 3)

    base = {
        "tier_1": 1.75,
        "tier_2": 0.9,
        "other": 0.0,
    }.get(tier, 0.0)
    return tier, round(base, 3)


def variant_bonus(matched_variants: list[str], journal_tier: str, priority: str) -> float:
    bonus = 0.0
    if "top_journals" in (matched_variants or []):
        bonus += 2.5 if priority == "strict" else 1.75
    if priority == "strict" and journal_tier == "tier_1":
        bonus += 0.75
    return round(bonus, 3)


def classify_core_psychiatry(title: str, abstract: str, journal: str, journal_tier: str) -> tuple[bool, list[str], list[str], int]:
    text_blob = f"{title} {abstract}".lower()
    core_hits = sorted([term for term in CORE_PSYCH_TERMS if term in text_blob])
    noncore_hits = sorted([term for term in NONCORE_BIOMED_TERMS if term in text_blob])

    jnorm = normalize_journal_name(journal)
    psych_journalish = any(hint in jnorm for hint in PSYCH_JOURNAL_HINTS)

    score = 0
    score += min(len(core_hits), 3)
    if psych_journalish:
        score += 1
    if journal_tier in {"tier_1", "tier_2"} and psych_journalish:
        score += 2
    score -= min(len(noncore_hits), 2)

    is_core = score >= 1
    return is_core, core_hits, noncore_hits, score


def load_seen_cache(path: str) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "seen_pmids" in payload and isinstance(payload["seen_pmids"], dict):
            return {str(k): str(v) for k, v in payload["seen_pmids"].items()}
        if isinstance(payload, dict):
            return {str(k): str(v) for k, v in payload.items()}
    except Exception:
        return {}
    return {}


def save_seen_cache(path: str, seen_pmids: dict[str, str]) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": utc_now_iso(),
        "seen_pmids": seen_pmids,
    }
    p.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def normalize_record(raw: dict[str, Any], query_terms: list[str], lane: str, journal_priority: str) -> dict[str, Any]:
    title = compact_ws(raw.get("title"))
    abstract = compact_ws(raw.get("abstract"))
    journal = compact_ws(raw.get("journal"))
    text_blob = f"{title} {abstract}".strip()

    ai_signal, ai_hits = text_has_any(text_blob, AI_TERMS)
    mh_signal, mh_hits = text_has_any(text_blob, MH_TERMS)
    topic_score, topic_hits = overlap_score(text_blob, query_terms)
    article_type = infer_article_type(title, abstract)
    exclude = exclusion_reasons(title, abstract)
    recency, days_old = recency_score(parse_best_date(raw))
    retrieval_score = float(raw.get("score", 0.0) or 0.0)
    retrieval_component = round(min(retrieval_score / 2.5, 4.0), 3)
    access_component = 0.9 if raw.get("has_pmcid") else (0.35 if raw.get("has_abstract") else 0.0)
    signal_component = (1.2 if ai_signal else 0.0) + (1.2 if mh_signal else 0.0)
    type_component = lane_type_score(article_type, lane)
    journal_tier, journal_component = journal_quality_score(journal, journal_priority)
    variant_component = variant_bonus(raw.get("matched_variants") or [], journal_tier, journal_priority)

    is_core_psych, core_hits, noncore_hits, core_score = classify_core_psychiatry(
        title=title,
        abstract=abstract,
        journal=journal,
        journal_tier=journal_tier,
    )
    core_component = 1.25 if is_core_psych else (-0.5 if noncore_hits else 0.0)

    if lane == "frontier" and article_type in {"systematic_review", "meta_analysis", "scoping_review", "narrative_review"}:
        if journal_tier == "tier_1":
            type_component = max(type_component, 1.8)
        elif journal_tier == "tier_2":
            type_component = max(type_component, 1.35)

    rank_score = round(
        retrieval_component
        + recency
        + topic_score
        + access_component
        + signal_component
        + type_component
        + journal_component
        + variant_component
        + core_component,
        3,
    )

    return {
        "pmid": str(raw.get("pmid", "")).strip(),
        "title": title,
        "journal": journal,
        "journal_norm": normalize_journal_name(journal),
        "journal_tier": journal_tier,
        "pubdate": raw.get("pubdate"),
        "sortpubdate": raw.get("sortpubdate"),
        "epubdate": raw.get("epubdate"),
        "doi": raw.get("doi"),
        "pmcid": raw.get("pmcid"),
        "url": raw.get("url"),
        "authors": raw.get("authors") or [],
        "n_authors": len(raw.get("authors") or []),
        "abstract": abstract,
        "has_abstract": bool(raw.get("has_abstract")),
        "has_pmcid": bool(raw.get("has_pmcid")),
        "matched_variants": raw.get("matched_variants") or [],
        "matched_terms": raw.get("matched_terms") or [],
        "article_type": article_type,
        "ai_signal": ai_signal,
        "ai_hits": ai_hits,
        "mental_health_signal": mh_signal,
        "mental_health_hits": mh_hits,
        "core_psychiatry": is_core_psych,
        "core_psychiatry_hits": core_hits,
        "noncore_biomed_hits": noncore_hits,
        "core_psychiatry_score": core_score,
        "topic_hits": topic_hits,
        "days_old": days_old,
        "retrieval_score": retrieval_score,
        "rank_score": rank_score,
        "score_components": {
            "retrieval": retrieval_component,
            "recency": recency,
            "topic_overlap": topic_score,
            "accessibility": access_component,
            "domain_signals": round(signal_component, 3),
            "article_type_fit": type_component,
            "journal_quality": journal_component,
            "journal_variant": variant_component,
            "core_psychiatry": core_component,
        },
        "exclusion_reasons": exclude,
        "seen_before": False,
    }


def keep_record(
    rec: dict[str, Any],
    lane: str,
    require_ai: bool,
    require_mh: bool,
    require_core_psychiatry: bool,
    novel_only: bool,
    min_score: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    excl = rec.get("exclusion_reasons", [])
    if excl:
        reasons.extend(excl)

    if require_ai and not rec.get("ai_signal", False):
        reasons.append("missing_ai_signal")

    if require_mh and not rec.get("mental_health_signal", False):
        reasons.append("missing_mental_health_signal")

    if require_core_psychiatry and not rec.get("core_psychiatry", False):
        reasons.append("not_core_psychiatry")

    if novel_only and rec.get("seen_before", False):
        reasons.append("seen_before")

    if rec.get("rank_score", 0.0) < min_score:
        reasons.append("below_min_score")

    if lane == "reviews" and rec.get("article_type") not in {
        "systematic_review", "meta_analysis", "scoping_review", "narrative_review"
    }:
        reasons.append("not_review_like")

    return len(reasons) == 0, reasons


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


def canonicalize_lane(lane: str) -> str:
    lane = compact_ws(lane).lower().replace("-", "_").replace(" ", "_")
    if lane in {"core_evidence", "recent_peer_reviewed", "frontier", "reviews"}:
        return lane
    return "general"


def summarize_filter_reasons(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in records:
        for reason in rec.get("filter_reasons", []) or []:
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def build_normalize_stats(
    input_records: list[dict[str, Any]],
    normalized_records: list[dict[str, Any]],
    kept_records: list[dict[str, Any]],
    dropped_records: list[dict[str, Any]],
) -> dict[str, Any]:
    rank_scores = [float(rec.get("rank_score", 0.0) or 0.0) for rec in normalized_records]
    kept_scores = [float(rec.get("rank_score", 0.0) or 0.0) for rec in kept_records]
    return {
        "input_count": len(input_records),
        "normalized_count": len(normalized_records),
        "kept_count": len(kept_records),
        "dropped_count": len(dropped_records),
        "kept_with_pmcid": sum(1 for rec in kept_records if rec.get("has_pmcid")),
        "kept_with_abstract": sum(1 for rec in kept_records if rec.get("has_abstract")),
        "kept_seen_before": sum(1 for rec in kept_records if rec.get("seen_before")),
        "article_type_counts_kept": dict(sorted({
            article_type: sum(1 for rec in kept_records if rec.get("article_type") == article_type)
            for article_type in {rec.get("article_type") for rec in kept_records}
            if article_type
        }.items())),
        "journal_tier_counts_kept": dict(sorted({
            tier: sum(1 for rec in kept_records if rec.get("journal_tier") == tier)
            for tier in {rec.get("journal_tier") for rec in kept_records}
            if tier
        }.items())),
        "rank_score_max": round(max(rank_scores), 3) if rank_scores else None,
        "rank_score_min": round(min(rank_scores), 3) if rank_scores else None,
        "kept_rank_score_max": round(max(kept_scores), 3) if kept_scores else None,
        "kept_rank_score_min": round(min(kept_scores), 3) if kept_scores else None,
    }


def build_resolver_feedback(
    input_records: list[dict[str, Any]],
    kept_records: list[dict[str, Any]],
    dropped_records: list[dict[str, Any]],
) -> dict[str, Any]:
    input_count = len(input_records) or 0
    kept_count = len(kept_records) or 0
    pmcid_rate = (sum(1 for rec in kept_records if rec.get("has_pmcid")) / kept_count) if kept_count else 0.0
    abstract_rate = (sum(1 for rec in kept_records if rec.get("has_abstract")) / kept_count) if kept_count else 0.0
    duplicate_like = sum(1 for rec in dropped_records if "max_per_journal" in (rec.get("filter_reasons") or []))
    feedback = {
        "retry_suggested": False,
        "missing_angles": [],
        "candidate_retry_actions": [],
        "filter_reason_counts": summarize_filter_reasons(dropped_records),
        "access_signals": {
            "kept_pmcid_rate": round(pmcid_rate, 3),
            "kept_abstract_rate": round(abstract_rate, 3),
        },
    }
    if kept_count < min(5, input_count):
        feedback["retry_suggested"] = True
        feedback["candidate_retry_actions"].append("broaden_query_or_raise_top_k")
    if pmcid_rate < 0.3:
        feedback["missing_angles"].append("low_accessible_fulltext_candidates")
        feedback["candidate_retry_actions"].append("prefer_accessible_records")
    if abstract_rate < 0.7:
        feedback["missing_angles"].append("low_abstract_coverage_after_ranking")
    if duplicate_like > 0:
        feedback["missing_angles"].append("journal_concentration_detected")
    return feedback


def apply_per_journal_cap(records: list[dict[str, Any]], max_per_journal: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if max_per_journal <= 0:
        return records, []

    kept: list[dict[str, Any]] = []
    overflow: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    for rec in records:
        key = rec.get("journal_norm") or "__unknown__"
        counts[key] = counts.get(key, 0)
        if counts[key] < max_per_journal:
            kept.append(rec)
            counts[key] += 1
        else:
            rec = dict(rec)
            rec["keep"] = False
            rec["filter_reasons"] = list(rec.get("filter_reasons", [])) + ["max_per_journal"]
            overflow.append(rec)

    return kept, overflow


def preview_lines(records: list[dict[str, Any]], top_k: int) -> list[str]:
    lines = []
    for i, rec in enumerate(records[:top_k], start=1):
        seen = "seen" if rec.get("seen_before") else "new"
        lines.append(
            f"{i}. {rec['pmid']} | {rec['article_type']} | {rec['journal_tier']} | {seen} | {rec['rank_score']} | {rec['journal']} | {rec['title']}"
        )
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize and rank PubMed retrieval output for the prose pipeline.")
    parser.add_argument("--input", required=True, help="Input JSON from scripts/pipeline/prose_pubmed_search_worker.py, or - for stdin")
    parser.add_argument("--write", default="", help="Optional path to write normalized JSON")
    parser.add_argument("--preview", action="store_true", help="Print a compact human preview")
    parser.add_argument("--lane", choices=["core_evidence", "recent_peer_reviewed", "frontier", "reviews", "general"], default="general")
    parser.add_argument("--query", default="", help="Optional override query for normalization scoring")
    parser.add_argument("--top-k", type=int, default=15)
    parser.add_argument("--min-score", type=float, default=5.0)
    parser.add_argument("--require-ai", action="store_true")
    parser.add_argument("--require-mental-health", action="store_true")
    parser.add_argument("--require-core-psychiatry", action="store_true")
    parser.add_argument("--journal-priority", choices=["off", "default", "strict"], default="default")
    parser.add_argument("--max-per-journal", type=int, default=0)
    parser.add_argument("--seen-cache", default="", help="Optional JSON cache of previously surfaced PMIDs")
    parser.add_argument("--novel-only", action="store_true", help="Drop PMIDs that already exist in --seen-cache")
    parser.add_argument("--update-seen-cache", action="store_true", help="Update seen cache with final kept_records")
    parser.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    parser.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    parser.add_argument("--schema-version", default="1.1", help="Schema version for normalize/rank artifact output.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    payload = load_json(args.input)
    plan_path = args.orchestration_plan or ((payload.get("orchestration_context") or {}).get("plan_path")) or ""
    plan = load_orchestration_plan(plan_path)
    lane = canonicalize_lane(payload.get("lane") if args.lane == "general" and payload.get("lane") else args.lane)
    query = args.query or payload.get("query") or ""
    query_terms = extract_query_terms(query)
    input_records = payload.get("results", []) or []

    seen_pmids = load_seen_cache(args.seen_cache)

    normalized = [
        normalize_record(
            rec,
            query_terms=query_terms,
            lane=lane,
            journal_priority=args.journal_priority,
        )
        for rec in input_records
    ]

    for rec in normalized:
        rec["seen_before"] = rec["pmid"] in seen_pmids

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for rec in normalized:
        keep, reasons = keep_record(
            rec,
            lane=lane,
            require_ai=args.require_ai,
            require_mh=args.require_mental_health,
            require_core_psychiatry=args.require_core_psychiatry,
            novel_only=args.novel_only,
            min_score=args.min_score,
        )
        rec["keep"] = keep
        if reasons:
            rec["filter_reasons"] = reasons
        if keep:
            kept.append(rec)
        else:
            dropped.append(rec)

    kept.sort(
        key=lambda rec: (
            rec["rank_score"],
            1 if rec["has_pmcid"] else 0,
            rec["retrieval_score"],
            -(rec["days_old"] if rec["days_old"] is not None else 10**9),
        ),
        reverse=True,
    )

    kept, overflow = apply_per_journal_cap(kept, args.max_per_journal)
    dropped.extend(overflow)
    dropped.sort(key=lambda rec: rec.get("rank_score", 0.0), reverse=True)

    kept_records = kept[: args.top_k]

    if args.update_seen_cache and args.seen_cache:
        now = utc_now_iso()
        for rec in kept_records:
            seen_pmids[rec["pmid"]] = now
        save_seen_cache(args.seen_cache, seen_pmids)

    stats = build_normalize_stats(
        input_records=input_records,
        normalized_records=normalized,
        kept_records=kept_records,
        dropped_records=dropped,
    )
    resolver_feedback = build_resolver_feedback(
        input_records=input_records,
        kept_records=kept_records,
        dropped_records=dropped,
    )

    output = {
        "schema_version": args.schema_version,
        "stage": "normalize_rank",
        "run_id": args.run_id or payload.get("run_id") or plan.get("run_id") or None,
        "generated_at": utc_now_iso(),
        "source_backend": payload.get("backend"),
        "source_stage": payload.get("stage"),
        "lane": lane,
        "journal_priority": args.journal_priority,
        "query": query,
        "query_terms": query_terms,
        "orchestration_context": {
            "plan_path": plan_path or None,
            "topic": plan.get("topic") or (payload.get("orchestration_context") or {}).get("topic"),
            "lane_window": ((plan.get("lane_windows") or {}).get(lane)) if isinstance(plan.get("lane_windows"), dict) else (payload.get("orchestration_context") or {}).get("lane_window"),
        },
        "input_stats": payload.get("stats"),
        "stats": stats,
        "resolver_feedback": resolver_feedback,
        "input_count": len(input_records),
        "kept_count": len(kept_records),
        "dropped_count": len(dropped),
        "max_per_journal": args.max_per_journal,
        "seen_cache": args.seen_cache or None,
        "novel_only": args.novel_only,
        "kept_records": kept_records,
        "dropped_records": dropped,
    }

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")

    if args.preview:
        print("\n".join(preview_lines(kept_records, args.top_k)))
    else:
        print(text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
