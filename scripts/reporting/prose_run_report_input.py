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
from pathlib import Path
from typing import Any


STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "among",
    "major", "disorder", "study", "research", "effect", "effects", "using"
}


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


def topic_tokens(topic: str) -> list[str]:
    toks = re.findall(r"[A-Za-z0-9\-]+", topic.lower())
    out = []
    for t in toks:
        if len(t) >= 3 and t not in STOPWORDS:
            out.append(t)
    return list(dict.fromkeys(out))


def format_publication_date(raw: Any) -> str | None:
    s = compact_ws(raw)
    if not s:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, m, d = s.split("-")
        return f"{m}/{d}/{y}"

    if re.fullmatch(r"\d{4}-\d{2}", s):
        y, m = s.split("-")
        return f"{m}/{y}"

    if re.fullmatch(r"\d{4}", s):
        return s

    return s


def build_date_fields(rec: dict[str, Any]) -> dict[str, Any]:
    online_raw = rec.get("epubdate_iso")
    issue_raw = rec.get("publication_date") or rec.get("pubdate_iso") or rec.get("pubdate")

    online_display = format_publication_date(online_raw)
    issue_display = format_publication_date(issue_raw)

    if online_display:
        date_display = online_display
        date_display_label = "Online date"
    elif issue_display:
        date_display = issue_display
        date_display_label = "Publication date"
    else:
        date_display = None
        date_display_label = None

    return {
        "online_date": online_raw,
        "online_date_display": online_display,
        "issue_date": issue_raw,
        "issue_date_display": issue_display,
        "date_display": date_display,
        "date_display_label": date_display_label,
    }


def pubmed_url(pmid: Any) -> str | None:
    p = compact_ws(pmid)
    return f"https://pubmed.ncbi.nlm.nih.gov/{p}/" if p else None


def pmc_url(pmcid: Any) -> str | None:
    p = compact_ws(pmcid)
    return f"https://pmc.ncbi.nlm.nih.gov/articles/{p}/" if p else None


def doi_url(doi: Any) -> str | None:
    d = compact_ws(doi)
    return f"https://doi.org/{d}" if d else None


def contains_topic_signal(text: str, tokens: list[str]) -> bool:
    low = compact_ws(text).lower()
    if not low:
        return False
    for tok in tokens:
        if re.search(rf"\b{re.escape(tok)}\b", low):
            return True
    return False


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


def contains_any_phrase(text: str, phrases: list[str]) -> bool:
    low = compact_ws(text).lower()
    if not low:
        return False
    return any(phrase_in_text(low, p) for p in phrases if compact_ws(p))


def derive_topic_groups(topic: str) -> dict[str, list[str]]:
    low = compact_ws(topic).lower()

    intervention_catalog = [
        "intranasal esketamine",
        "esketamine",
        "intranasal racemic ketamine",
        "racemic ketamine",
        "ketamine"
    ]
    condition_catalog = [
        "treatment-resistant depression",
        "major depressive disorder",
        "depression",
        "mdd",
        "trd"
    ]
    biomarker_catalog = [
        "inflammatory biomarkers",
        "inflammatory biomarker",
        "biomarkers",
        "biomarker",
        "inflammation",
        "inflammatory",
        "cytokines",
        "cytokine",
        "crp",
        "il-6",
        "il-8",
        "ifn",
        "c4",
        "eeg",
        "entropy",
        "connectivity",
        "predictor",
        "predictive"
    ]

    intervention = [x for x in intervention_catalog if phrase_in_text(low, x)]
    condition = [x for x in condition_catalog if phrase_in_text(low, x)]
    biomarker = [x for x in biomarker_catalog if phrase_in_text(low, x)]

    return {
        "intervention": list(dict.fromkeys(intervention)),
        "condition": list(dict.fromkeys(condition)),
        "biomarker": list(dict.fromkeys(biomarker)),
    }


def biomarker_family_terms(topic_groups: dict[str, list[str]]) -> list[str]:
    biomarker_terms = set(topic_groups.get("biomarker", []))
    if biomarker_terms:
        biomarker_terms.update([
            "biomarker", "biomarkers", "marker", "markers",
            "inflammatory marker", "inflammatory markers",
            "cytokine", "cytokines", "crp", "bdnf",
            "eeg", "entropy", "connectivity",
            "methylation", "epigenetic", "dna methylation",
            "predictor", "predictive", "predictive marker",
            "response marker", "treatment response",
            "treatment-predictive", "predictive signature",
        ])
    return list(dict.fromkeys(sorted(biomarker_terms)))


def article_text_blob(rec: dict[str, Any]) -> str:
    parts = [
        rec.get("title"),
        rec.get("condition"),
        rec.get("intervention_or_exposure"),
        rec.get("comparator"),
        rec.get("main_claim"),
        " ".join(rec.get("outcomes") or []),
        " ".join(rec.get("most_salient_findings") or []),
        " ".join(rec.get("discussion_significance") or []),
        " ".join(rec.get("limitations") or []),
    ]
    return compact_ws(" ".join(str(x or "") for x in parts)).lower()


def best_salient_findings(rec: dict[str, Any], limit: int = 3) -> list[str]:
    vals = []
    metrics = rec.get("metrics") or {}

    for s in metrics.get("score_change_snippets") or []:
        s = compact_ws(s)
        if s and s not in vals:
            vals.append(s)

    for s in rec.get("key_findings") or []:
        s = compact_ws(s)
        if s and s not in vals:
            vals.append(s)

    mc = compact_ws(rec.get("main_claim"))
    if mc and mc not in vals:
        vals.append(mc)

    return vals[:limit]


def relevance_score(rec: dict[str, Any], topic: str, tokens: list[str], groups: dict[str, list[str]]) -> int:
    blob = article_text_blob(rec)
    low_topic = compact_ws(topic).lower()
    score = 0

    intervention_ok = not groups["intervention"] or contains_any_phrase(blob, groups["intervention"])
    condition_ok = not groups["condition"] or contains_any_phrase(blob, groups["condition"])
    biomarker_ok = not groups["biomarker"] or contains_any_phrase(blob, biomarker_family_terms(groups))

    if intervention_ok:
        score += 5
    if condition_ok:
        score += 4
    if biomarker_ok:
        score += 4
    elif groups["biomarker"]:
        score -= 4

    findings = best_salient_findings(rec)
    score += min(3, len(findings))

    if rec.get("sample_size"):
        score += 2
    if compact_ws(rec.get("document_role")) == "primary_empirical":
        score += 3
    if compact_ws(rec.get("document_role")) == "review_like":
        score -= 1

    off_topic_penalties = [
        ("ptsd", 5),
        ("post-traumatic stress", 5),
        ("bipolar", 5),
        ("schizophrenia", 5),
        ("mouse", 6),
        ("mice", 6),
        ("rat", 6),
        ("rats", 6),
        ("murine", 6),
        ("preclinical", 5),
        ("animal model", 5),
        ("lps", 4),
        ("breast cancer", 5),
        ("postpartum", 5),
        ("cesarean", 5),
        ("perioperative", 5),
    ]
    for term, penalty in off_topic_penalties:
        if term in blob and term not in low_topic:
            score -= penalty

    return score


def select_articles(evidence_payload: dict[str, Any], topic: str, tokens: list[str], max_articles: int) -> list[dict[str, Any]]:
    groups = derive_topic_groups(topic)
    candidates = []

    for bucket_priority, bucket in enumerate(["evidence_records", "partial_records"]):
        for rec in evidence_payload.get(bucket, []) or []:
            findings = best_salient_findings(rec)
            if not findings:
                continue

            blob = article_text_blob(rec)

            if groups["intervention"] and not contains_any_phrase(blob, groups["intervention"]):
                continue
            if groups["condition"] and not contains_any_phrase(blob, groups["condition"]):
                continue

            biomarker_terms = biomarker_family_terms(groups)
            if biomarker_terms and not contains_any_phrase(blob, biomarker_terms):
                continue

            rel = relevance_score(rec, topic, tokens, groups)
            if rel < 6:
                continue

            pub_date = rec.get("publication_date")
            date_fields = build_date_fields(rec)

            item = {
                "bucket": bucket,
                "pmid": rec.get("pmid"),
                "pmcid": rec.get("pmcid"),
                "doi": rec.get("doi"),
                "title": compact_ws(rec.get("title")),
                "journal": compact_ws(rec.get("journal")),
                "publication_date": pub_date,
                "display_date": format_publication_date(pub_date),
                **date_fields,
                "pubmed_url": pubmed_url(rec.get("pmid")),
                "pmc_url": pmc_url(rec.get("pmcid")),
                "doi_url": doi_url(rec.get("doi")),
                "authors": rec.get("authors") or [],
                "first_author": rec.get("first_author"),
                "last_author": rec.get("last_author"),
                "paper_kind": rec.get("paper_kind"),
                "document_role": rec.get("document_role"),
                "classification_confidence": rec.get("classification_confidence"),
                "study_design": rec.get("study_design"),
                "sample_size": rec.get("sample_size"),
                "condition": rec.get("condition"),
                "intervention_or_exposure": rec.get("intervention_or_exposure"),
                "comparator": rec.get("comparator"),
                "outcomes": rec.get("outcomes") or [],
                "most_salient_findings": findings,
                "metrics": rec.get("metrics") or {},
                "limitations": rec.get("limitations") or [],
                "discussion_significance": rec.get("discussion_significance") or [],
                "main_claim": rec.get("main_claim"),
                "evidence_level": rec.get("evidence_level"),
                "extraction_quality": rec.get("extraction_quality"),
                "source_substance": rec.get("source_substance"),
                "confidence": rec.get("confidence"),
                "relevance_score": rel,
                "_bucket_priority": bucket_priority,
            }
            candidates.append(item)

    candidates.sort(
        key=lambda r: (
            -r["relevance_score"],
            r["_bucket_priority"],
            -(1 if r.get("sample_size") else 0),
            r.get("title") or "",
        )
    )

    out = candidates[:max_articles]
    for rec in out:
        rec.pop("_bucket_priority", None)
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build a structured report input payload for LLM-based prose research reporting.")
    p.add_argument("--controller-input", required=True, help="controller_decision JSON")
    p.add_argument("--coverage-input", required=True, help="coverage_report JSON")
    p.add_argument("--evidence-input", required=True, help="evidence_records JSON")
    p.add_argument("--planner-shadow-eval", default="", help="Optional planner_shadow_eval JSON")
    p.add_argument("--orchestration-plan", default="", help="Optional orchestration plan JSON")
    p.add_argument("--max-articles", type=int, default=5, help="Max included articles")
    p.add_argument("--write", required=True, help="Output JSON path")
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    coverage = load_json(args.coverage_input)
    evidence = load_json(args.evidence_input)
    planner_eval = load_json(args.planner_shadow_eval) if args.planner_shadow_eval else {}
    plan = load_json(args.orchestration_plan) if args.orchestration_plan else {}

    topic = compact_ws(
        (controller.get("orchestration_context") or {}).get("topic")
        or (coverage.get("orchestration_context") or {}).get("topic")
        or plan.get("topic")
    )
    tokens = topic_tokens(topic)

    counts = coverage.get("counts") or {}
    cov = coverage.get("coverage") or {}
    retry = coverage.get("retry_recommendation") or {}

    payload = {
        "schema_version": "1.0",
        "artifact_type": "run_report_input",
        "stage": "run_report_input",
        "generated_at": utc_now_iso(),
        "run_id": controller.get("run_id") or coverage.get("run_id") or evidence.get("run_id") or plan.get("run_id"),
        "topic": topic,
        "topic_tokens": tokens,
        "lane": controller.get("lane") or coverage.get("lane") or evidence.get("lane"),
        "controller": {
            "decision": controller.get("decision"),
            "priority": controller.get("priority"),
        },
        "coverage": {
            "semantic_ready_count": counts.get("semantic_ready_count"),
            "evidence_record_count": counts.get("evidence_record_count"),
            "partial_record_count": counts.get("partial_record_count"),
            "skipped_record_count": counts.get("evidence_skipped_count"),
            "fulltext_yield_rate": counts.get("fulltext_yield_rate"),
            "review_like_count": cov.get("review_like_count"),
            "primary_study_like_count": cov.get("primary_study_like_count"),
            "mixed_or_unclear_count": cov.get("mixed_or_unclear_count"),
            "high_quality_record_count": cov.get("high_quality_record_count"),
            "records_with_metrics": cov.get("records_with_metrics"),
            "review_to_primary_ratio": cov.get("review_to_primary_ratio"),
            "threshold_met": retry.get("threshold_met"),
            "missing_angles": retry.get("missing_angles") or [],
        },
        "included_articles": select_articles(evidence, topic, tokens, args.max_articles),
        "future_run_recommendations": controller.get("future_run_patch_candidates") or [],
        "planner_shadow_eval": {
            "controller_outcome": planner_eval.get("controller_outcome"),
            "rationale": planner_eval.get("rationale"),
            "baseline_metrics": planner_eval.get("baseline_metrics") or {},
            "branch_metrics": planner_eval.get("branch_metrics") or {},
        } if planner_eval else {},
        "report_requirements": {
            "summary_paragraph_sentences": [5, 8],
            "per_article_bullets_target": 10,
            "grounding_rule": "Use only included article evidence and extracted fields. Do not invent unsupported claims.",
            "omit_generic_filler": True
        }
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
