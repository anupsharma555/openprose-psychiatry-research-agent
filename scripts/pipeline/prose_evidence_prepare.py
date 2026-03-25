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
from pathlib import Path
from typing import Any


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
    return json.loads(p.read_text(encoding="utf-8"))


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        x = compact_ws(item)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def corrected_paper_kind(title: str, paper_kind: str | None, study_design: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)
    sd = compact_ws(study_design)

    # strong empirical cues first
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

    # review cues after empirical cues
    if "systematic review" in t or "meta-analysis" in t or "meta analysis" in t:
        return "systematic_review"
    if "scoping review" in t:
        return "scoping_review"
    if "narrative review" in t:
        return "review"
    if re.search(r"\breview\b", t):
        if any(x in t for x in ["systematic review", "meta-analysis", "meta analysis", "narrative review", "scoping review"]) or t.endswith("review"):
            return "review"

    if sd in {"randomized_trial", "open_label_trial", "cohort_study", "observational_study", "case_series"}:
        return sd
    if sd in {"systematic_review", "meta_analysis", "review", "narrative_review", "scoping_review"}:
        return sd

    return pk or None


def corrected_document_role(title: str, paper_kind: str | None, document_role: str | None) -> str | None:
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


def normalize_authors(rec: dict[str, Any]) -> tuple[list[str], str | None, str | None]:
    authors = rec.get("authors") or []
    if not isinstance(authors, list):
        authors = []

    first_author = rec.get("first_author")
    last_author = rec.get("last_author")

    norm_authors = [compact_ws(x) for x in authors if compact_ws(x)]
    if not norm_authors:
        fa = compact_ws(first_author)
        la = compact_ws(last_author)
        if fa and la and fa != la:
            norm_authors = [fa, la]
        elif fa:
            norm_authors = [fa]

    if not first_author and norm_authors:
        first_author = norm_authors[0]
    if not last_author and norm_authors:
        last_author = norm_authors[-1]

    return norm_authors, first_author, last_author


def infer_sample_size(rec: dict[str, Any]) -> int | None:
    existing = rec.get("sample_size")
    if isinstance(existing, int) and existing > 0:
        return existing

    texts = []
    for key in [
        "title",
        "main_claim",
        "condition",
        "intervention_or_exposure",
        "comparator",
    ]:
        texts.append(compact_ws(rec.get(key)))

    for key in ["most_salient_findings", "limitations", "discussion_significance", "outcomes"]:
        vals = rec.get(key) or []
        if isinstance(vals, list):
            texts.extend([compact_ws(x) for x in vals])

    metrics = rec.get("metrics") or {}
    if isinstance(metrics, dict):
        for v in metrics.values():
            if isinstance(v, list):
                texts.extend([compact_ws(x) for x in v])
            else:
                texts.append(compact_ws(v))

    blob = " | ".join([x for x in texts if x])

    patterns = [
        r"\bn\s*=\s*(\d{1,4})\b",
        r"\b(\d{1,4})\s+(?:patients|participants|subjects|adolescents|adults)\b",
        r"\bincluded\s+(\d{1,4})\b",
        r"\bsample size\s*(?:of)?\s*(\d{1,4})\b",
    ]
    for pat in patterns:
        m = re.search(pat, blob, flags=re.I)
        if m:
            try:
                n = int(m.group(1))
                if n > 0:
                    return n
            except Exception:
                pass
    return None


def build_bullet_candidates(rec: dict[str, Any]) -> list[str]:
    bullets = []

    if rec.get("main_claim"):
        bullets.append(str(rec.get("main_claim")))

    for key in ["most_salient_findings", "outcomes", "limitations", "discussion_significance"]:
        vals = rec.get(key) or []
        if isinstance(vals, list):
            bullets.extend([str(x) for x in vals])

    metrics = rec.get("metrics") or {}
    if isinstance(metrics, dict):
        for v in metrics.values():
            if isinstance(v, list):
                bullets.extend([str(x) for x in v])

    bullets = dedupe_keep_order(bullets)
    return bullets[:12]


def enrich_record(rec: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    out = dict(rec)
    changed = False

    title = compact_ws(rec.get("title"))
    old_pk = compact_ws(rec.get("paper_kind"))
    old_dr = compact_ws(rec.get("document_role"))
    sd = compact_ws(rec.get("study_design"))

    new_pk = corrected_paper_kind(title, old_pk, sd)
    new_dr = corrected_document_role(title, new_pk, old_dr)

    notes = out.get("label_normalization_notes") or []

    if new_pk and new_pk != old_pk:
        out["original_paper_kind"] = rec.get("paper_kind")
        out["paper_kind"] = new_pk
        notes.append(f"paper_kind:{old_pk}->{new_pk}")
        changed = True

    if new_dr and new_dr != old_dr:
        out["original_document_role"] = rec.get("document_role")
        out["document_role"] = new_dr
        notes.append(f"document_role:{old_dr}->{new_dr}")
        changed = True

    if notes:
        out["label_normalization_notes"] = dedupe_keep_order(notes)

    authors, first_author, last_author = normalize_authors(out)
    if authors != (rec.get("authors") or []):
        out["authors"] = authors
        changed = True
    if first_author and first_author != rec.get("first_author"):
        out["first_author"] = first_author
        changed = True
    if last_author and last_author != rec.get("last_author"):
        out["last_author"] = last_author
        changed = True

    sample_size = infer_sample_size(out)
    if sample_size and sample_size != rec.get("sample_size"):
        out["sample_size"] = sample_size
        changed = True

    bullet_candidates = build_bullet_candidates(out)
    if bullet_candidates:
        out["bullet_candidates"] = bullet_candidates
        changed = True

    richness = 0
    richness += len(out.get("bullet_candidates") or [])
    richness += 1 if out.get("sample_size") else 0
    richness += 1 if out.get("authors") else 0
    richness += 1 if out.get("main_claim") else 0
    out["detail_richness_score"] = richness

    return out, changed


def main() -> int:
    ap = argparse.ArgumentParser(description="Deterministic evidence cleanup and detail enrichment.")
    ap.add_argument("--evidence-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    payload = load_json(args.evidence_input)
    if not payload:
        raise SystemExit(f"Could not load evidence input: {args.evidence_input}")

    changed_count = 0
    for bucket in ["evidence_records", "partial_records", "skipped_records"]:
        out_records = []
        for rec in payload.get(bucket, []) or []:
            new_rec, changed = enrich_record(rec)
            if changed:
                changed_count += 1
            out_records.append(new_rec)
        payload[bucket] = out_records

    payload["evidence_prepare_summary"] = {
        "changed_count": changed_count,
    }

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload["evidence_prepare_summary"], indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
