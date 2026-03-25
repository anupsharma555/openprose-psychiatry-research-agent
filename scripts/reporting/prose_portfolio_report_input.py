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

from prose_run_report_input import (
    utc_now_iso,
    compact_ws,
    ensure_parent_dir,
    load_json,
    topic_tokens,
    derive_topic_groups,
    article_text_blob,
    contains_any_phrase,
    phrase_in_text,
    best_salient_findings,
    build_date_fields,
    format_publication_date,
    pubmed_url,
    pmc_url,
    doi_url,
    biomarker_family_terms,
)


def dedupe_articles(articles: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for rec in articles:
        key = str(rec.get("pmid") or rec.get("doi") or rec.get("title"))
        if key not in seen:
            seen.add(key)
            out.append(rec)
    return out


def broader_intervention_terms(topic_groups: dict[str, list[str]]) -> list[str]:
    terms = []
    interventions = set(topic_groups.get("intervention", []))

    if "esketamine" in interventions or "intranasal esketamine" in interventions:
        terms.extend(["ketamine", "racemic ketamine", "intranasal racemic ketamine"])

    if any(x in interventions for x in ["ssri", "ssris", "sertraline", "escitalopram", "fluoxetine", "paroxetine", "citalopram", "fluvoxamine"]):
        terms.extend(["antidepressant", "antidepressants", "pharmacotherapy"])

    return list(dict.fromkeys(terms))


def treatment_context_terms(topic_groups: dict[str, list[str]]) -> list[str]:
    interventions = set(topic_groups.get("intervention", []))
    terms = [
        "treatment response",
        "response prediction",
        "predictive signature",
        "treatment-predictive",
        "predictor of response",
        "remission",
        "antidepressant selection",
        "antidepressant efficacy",
        "pharmacotherapy response",
        "treatment response rate",
        "response rates",
    ]

    if any(x in interventions for x in ["ssri", "ssris", "sertraline", "escitalopram", "fluoxetine", "paroxetine", "citalopram", "fluvoxamine"]):
        terms.extend([
            "ssri response",
            "ssri treatment",
            "antidepressant response",
            "antidepressant treatment",
        ])

    if "esketamine" in interventions or "intranasal esketamine" in interventions:
        terms.extend([
            "esketamine response",
            "ketamine response",
        ])

    return list(dict.fromkeys(terms))


def context_exclusion_reason(rec: dict, topic: str) -> str | None:
    low_topic = compact_ws(topic).lower()

    title = compact_ws(rec.get("title")).lower()
    condition = compact_ws(rec.get("condition")).lower()
    intervention = compact_ws(rec.get("intervention_or_exposure")).lower()
    comparator = compact_ws(rec.get("comparator")).lower()

    # Use only focused metadata fields here, not the full article blob.
    focused_text = " | ".join(x for x in [title, condition, intervention, comparator] if x)

    # Keep these as hard exclusions only when they appear in focused fields.
    hard_exclusions = [
        ("breast cancer", "breast_cancer"),
        ("postpartum", "postpartum"),
        ("cesarean", "cesarean"),
        ("perioperative", "perioperative"),
        ("substance use", "substance_use"),
        ("alcohol use disorder", "substance_use"),
        ("mouse", "animal"),
        ("mice", "animal"),
        ("rat", "animal"),
        ("rats", "animal"),
        ("murine", "animal"),
        ("preclinical", "animal"),
        ("animal model", "animal"),
    ]

    for term, label in hard_exclusions:
        if phrase_in_text(focused_text, term) and not phrase_in_text(low_topic, term):
            return label
    return None


def is_review_context(rec: dict) -> bool:
    role = compact_ws(rec.get("document_role"))
    kind = compact_ws(rec.get("paper_kind"))
    return role == "review_like" or kind in {
        "review", "systematic_review", "meta_analysis", "narrative_review", "scoping_review"
    }


def make_report_item(rec: dict, bucket: str, rel: int) -> dict:
    pub_date = rec.get("publication_date")
    date_fields = build_date_fields(rec)
    findings = best_salient_findings(rec)

    return {
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
    }


def direct_relevance_score(rec: dict, topic: str, groups: dict[str, list[str]]) -> int:
    blob = article_text_blob(rec)
    low_topic = compact_ws(topic).lower()
    score = 0

    if groups["condition"] and contains_any_phrase(blob, groups["condition"]):
        score += 5
    if groups["biomarker"] and contains_any_phrase(blob, biomarker_family_terms(groups)):
        score += 5
    if groups["intervention"] and contains_any_phrase(blob, groups["intervention"]):
        score += 5
    if compact_ws(rec.get("document_role")) == "primary_empirical":
        score += 4
    if rec.get("sample_size"):
        score += 1
    score += min(3, len(best_salient_findings(rec)))

    # Soft penalty for other psychiatric conditions outside the topic
    soft_psych_penalties = [
        ("ptsd", 4),
        ("post-traumatic stress", 4),
        ("bipolar", 4),
        ("schizophrenia", 4),
    ]
    for term, penalty in soft_psych_penalties:
        if term in blob and term not in low_topic:
            score -= penalty

    return score


def related_relevance_score(rec: dict, topic: str, topic_groups: dict[str, list[str]]) -> int:
    blob = article_text_blob(rec)
    low_topic = compact_ws(topic).lower()
    score = 0

    condition_ok = not topic_groups["condition"] or contains_any_phrase(blob, topic_groups["condition"])
    biomarker_ok = not topic_groups["biomarker"] or contains_any_phrase(blob, biomarker_family_terms(topic_groups))
    direct_intervention_ok = not topic_groups["intervention"] or contains_any_phrase(blob, topic_groups["intervention"])
    broader_intervention_ok = contains_any_phrase(blob, broader_intervention_terms(topic_groups))
    treatment_context_ok = contains_any_phrase(blob, treatment_context_terms(topic_groups))

    if condition_ok:
        score += 5
    if biomarker_ok:
        score += 5
    elif topic_groups["biomarker"]:
        score -= 4

    if direct_intervention_ok:
        score += 4
    elif broader_intervention_ok:
        score += 2
    elif treatment_context_ok:
        score += 2
    else:
        score -= 4

    if compact_ws(rec.get("document_role")) == "primary_empirical":
        score += 3
    if rec.get("sample_size"):
        score += 1

    score += min(3, len(best_salient_findings(rec)))

    soft_psych_penalties = [
        ("ptsd", 4),
        ("post-traumatic stress", 4),
        ("bipolar", 4),
        ("schizophrenia", 4),
    ]
    for term, penalty in soft_psych_penalties:
        if term in blob and term not in low_topic:
            score -= penalty

    if direct_intervention_ok:
        score += 1

    return score


def review_relevance_score(rec: dict, topic: str, topic_groups: dict[str, list[str]]) -> int:
    blob = article_text_blob(rec)
    low_topic = compact_ws(topic).lower()
    score = 0

    condition_ok = not topic_groups["condition"] or contains_any_phrase(blob, topic_groups["condition"])
    intervention_ok = (
        contains_any_phrase(blob, topic_groups["intervention"])
        or contains_any_phrase(blob, broader_intervention_terms(topic_groups))
        or contains_any_phrase(blob, treatment_context_terms(topic_groups))
    )
    biomarker_or_treatment_context_ok = (
        contains_any_phrase(blob, biomarker_family_terms(topic_groups))
        or contains_any_phrase(blob, treatment_context_terms(topic_groups))
    )

    if condition_ok:
        score += 5
    if intervention_ok:
        score += 3
    if biomarker_or_treatment_context_ok:
        score += 3

    score += min(2, len(best_salient_findings(rec)))

    soft_psych_penalties = [
        ("ptsd", 4),
        ("post-traumatic stress", 4),
        ("bipolar", 4),
        ("schizophrenia", 4),
    ]
    for term, penalty in soft_psych_penalties:
        if term in blob and term not in low_topic:
            score -= penalty

    return score


def diagnose_direct_candidate(rec: dict, topic: str, groups: dict[str, list[str]]) -> tuple[bool, int, list[str]]:
    blob = article_text_blob(rec)
    reasons = []

    if compact_ws(rec.get("document_role")) != "primary_empirical":
        reasons.append("not_primary_empirical")
    if groups["condition"] and not contains_any_phrase(blob, groups["condition"]):
        reasons.append("missing_condition_match")
    if groups["biomarker"] and not contains_any_phrase(blob, biomarker_family_terms(groups)):
        reasons.append("missing_biomarker_match")
    if groups["intervention"] and not contains_any_phrase(blob, groups["intervention"]):
        reasons.append("missing_intervention_match")
    if context_exclusion_reason(rec, topic) is not None:
        reasons.append("hard_context_exclusion")
    if not best_salient_findings(rec):
        reasons.append("no_salient_findings")

    score = direct_relevance_score(rec, topic, groups)
    if score < 10:
        reasons.append("score_below_direct_threshold")

    return len(reasons) == 0, score, reasons


def diagnose_related_candidate(rec: dict, topic: str, groups: dict[str, list[str]]) -> tuple[bool, int, list[str]]:
    blob = article_text_blob(rec)
    reasons = []

    if is_review_context(rec):
        reasons.append("review_context_goes_to_review_lane")
    if groups["condition"] and not contains_any_phrase(blob, groups["condition"]):
        reasons.append("missing_condition_match")

    biomarker_related = contains_any_phrase(blob, biomarker_family_terms(groups)) or contains_any_phrase(blob, treatment_context_terms(groups))
    if groups["biomarker"] and not biomarker_related:
        reasons.append("missing_biomarker_or_treatment_context_match")

    intervention_ok = False
    if groups["intervention"] and contains_any_phrase(blob, groups["intervention"]):
        intervention_ok = True
    elif contains_any_phrase(blob, broader_intervention_terms(groups)):
        intervention_ok = True
    elif contains_any_phrase(blob, treatment_context_terms(groups)):
        intervention_ok = True

    if not intervention_ok:
        reasons.append("missing_intervention_or_broad_context_match")

    if context_exclusion_reason(rec, topic) is not None:
        reasons.append("hard_context_exclusion")
    if not best_salient_findings(rec):
        reasons.append("no_salient_findings")

    score = related_relevance_score(rec, topic, groups)
    if score < 7:
        reasons.append("score_below_related_threshold")

    return len(reasons) == 0, score, reasons


def diagnose_review_candidate(rec: dict, topic: str, groups: dict[str, list[str]]) -> tuple[bool, int, list[str]]:
    blob = article_text_blob(rec)
    reasons = []

    if not is_review_context(rec):
        reasons.append("not_review_context")
    if groups["condition"] and not contains_any_phrase(blob, groups["condition"]):
        reasons.append("missing_condition_match")

    intervention_ok = (
        contains_any_phrase(blob, groups["intervention"])
        or contains_any_phrase(blob, broader_intervention_terms(groups))
        or contains_any_phrase(blob, treatment_context_terms(groups))
    )
    if not intervention_ok:
        reasons.append("missing_intervention_or_treatment_context_match")

    if context_exclusion_reason(rec, topic) is not None:
        reasons.append("hard_context_exclusion")

    score = review_relevance_score(rec, topic, groups)
    if score < 6:
        reasons.append("score_below_review_threshold")

    return len(reasons) == 0, score, reasons


def filter_direct_articles(articles: list[dict]) -> list[dict]:
    primary = [x for x in articles if compact_ws(x.get("document_role")) == "primary_empirical"]
    if primary:
        return primary
    return articles


def select_direct_articles(evidence_payload: dict, topic: str, max_articles: int) -> tuple[list[dict], list[dict]]:
    groups = derive_topic_groups(topic)
    candidates = []
    diagnostics = []

    for bucket_priority, bucket in enumerate(["evidence_records", "partial_records"]):
        for rec in evidence_payload.get(bucket, []) or []:
            include, score, reasons = diagnose_direct_candidate(rec, topic, groups)
            diagnostics.append({
                "lane": "direct_evidence",
                "title": rec.get("title"),
                "paper_kind": rec.get("paper_kind"),
                "document_role": rec.get("document_role"),
                "condition": rec.get("condition"),
                "intervention_or_exposure": rec.get("intervention_or_exposure"),
                "score": score,
                "included": include,
                "reasons": reasons,
            })
            if not include:
                continue

            item = make_report_item(rec, bucket, score)
            item["_bucket_priority"] = bucket_priority
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

    out = filter_direct_articles(out)
    return out, diagnostics


def select_related_articles(evidence_payload: dict, topic: str, max_articles: int) -> tuple[list[dict], list[dict]]:
    groups = derive_topic_groups(topic)
    candidates = []
    diagnostics = []

    for bucket_priority, bucket in enumerate(["evidence_records", "partial_records"]):
        for rec in evidence_payload.get(bucket, []) or []:
            include, score, reasons = diagnose_related_candidate(rec, topic, groups)
            diagnostics.append({
                "lane": "related_evidence",
                "title": rec.get("title"),
                "paper_kind": rec.get("paper_kind"),
                "document_role": rec.get("document_role"),
                "condition": rec.get("condition"),
                "intervention_or_exposure": rec.get("intervention_or_exposure"),
                "score": score,
                "included": include,
                "reasons": reasons,
            })
            if not include:
                continue

            item = make_report_item(rec, bucket, score)
            item["_bucket_priority"] = bucket_priority
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
    return out, diagnostics


def select_review_context_articles(direct_payload: dict, related_payload: dict, topic: str, max_articles: int) -> tuple[list[dict], list[dict]]:
    groups = derive_topic_groups(topic)
    candidates = []
    diagnostics = []

    for source_name, evidence_payload in [("direct", direct_payload), ("related", related_payload)]:
        for bucket in ["evidence_records", "partial_records"]:
            for rec in evidence_payload.get(bucket, []) or []:
                include, score, reasons = diagnose_review_candidate(rec, topic, groups)
                diagnostics.append({
                    "lane": "review_context",
                    "source": source_name,
                    "title": rec.get("title"),
                    "paper_kind": rec.get("paper_kind"),
                    "document_role": rec.get("document_role"),
                    "condition": rec.get("condition"),
                    "intervention_or_exposure": rec.get("intervention_or_exposure"),
                    "score": score,
                    "included": include,
                    "reasons": reasons,
                })
                if not include:
                    continue

                item = make_report_item(rec, bucket, score)
                item["_source_name"] = source_name
                candidates.append(item)

    candidates.sort(
        key=lambda r: (
            -r["relevance_score"],
            r.get("title") or "",
        )
    )

    out = dedupe_articles(candidates)[:max_articles]
    for rec in out:
        rec.pop("_source_name", None)
    return out, diagnostics


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build a three-section report input with direct evidence, related broader evidence, and review/context evidence.")
    p.add_argument("--controller-input", required=True)
    p.add_argument("--coverage-input", required=True)
    p.add_argument("--direct-evidence-input", required=True)
    p.add_argument("--related-evidence-input", required=True)
    p.add_argument("--orchestration-plan", default="")
    p.add_argument("--max-direct", type=int, default=5)
    p.add_argument("--max-related", type=int, default=4)
    p.add_argument("--max-review-context", type=int, default=3)
    p.add_argument("--write", required=True)
    return p


def main() -> int:
    args = build_parser().parse_args()

    controller = load_json(args.controller_input)
    coverage = load_json(args.coverage_input)
    direct_evidence = load_json(args.direct_evidence_input)
    related_evidence = load_json(args.related_evidence_input)
    plan = load_json(args.orchestration_plan) if args.orchestration_plan else {}

    topic = compact_ws(
        (controller.get("orchestration_context") or {}).get("topic")
        or (coverage.get("orchestration_context") or {}).get("topic")
        or plan.get("topic")
    )
    tokens = topic_tokens(topic)

    direct_articles, direct_diagnostics = select_direct_articles(direct_evidence, topic, args.max_direct)
    related_articles, related_diagnostics = select_related_articles(related_evidence, topic, args.max_related)

    direct_keys = {str(x.get("pmid") or x.get("doi") or x.get("title")) for x in direct_articles}
    related_articles = [
        x for x in related_articles
        if str(x.get("pmid") or x.get("doi") or x.get("title")) not in direct_keys
    ]

    review_context_articles, review_diagnostics = select_review_context_articles(
        direct_payload=direct_evidence,
        related_payload=related_evidence,
        topic=topic,
        max_articles=args.max_review_context,
    )

    used_keys = direct_keys | {str(x.get("pmid") or x.get("doi") or x.get("title")) for x in related_articles}
    review_context_articles = [
        x for x in review_context_articles
        if str(x.get("pmid") or x.get("doi") or x.get("title")) not in used_keys
    ]

    direct_articles = dedupe_articles(direct_articles)
    related_articles = dedupe_articles(related_articles)
    review_context_articles = dedupe_articles(review_context_articles)

    counts = coverage.get("counts") or {}
    cov = coverage.get("coverage") or {}
    retry = coverage.get("retry_recommendation") or {}

    payload = {
        "schema_version": "1.0",
        "artifact_type": "portfolio_run_report_input",
        "stage": "portfolio_run_report_input",
        "generated_at": utc_now_iso(),
        "run_id": controller.get("run_id") or coverage.get("run_id") or plan.get("run_id"),
        "topic": topic,
        "topic_tokens": tokens,
        "lane": controller.get("lane") or coverage.get("lane"),
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
        "direct_evidence_articles": direct_articles,
        "related_evidence_articles": related_articles,
        "review_context_articles": review_context_articles,
        "included_articles": direct_articles + related_articles + review_context_articles,
        "selection_diagnostics": {
            "direct_evidence": direct_diagnostics,
            "related_evidence": related_diagnostics,
            "review_context": review_diagnostics,
        },
        "report_requirements": {
            "summary_paragraph_sentences": [5, 8],
            "per_article_bullets_target": 10,
            "grounding_rule": "Use only included article evidence and extracted fields. Do not invent unsupported claims.",
            "omit_generic_filler": True
        }
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "direct_count": len(direct_articles),
        "related_count": len(related_articles),
        "review_context_count": len(review_context_articles),
        "total_count": len(payload["included_articles"]),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
