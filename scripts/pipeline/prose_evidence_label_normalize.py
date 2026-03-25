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


def load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def corrected_paper_kind(title: str, paper_kind: str | None, study_design: str | None) -> str | None:
    t = compact_ws(title).lower()
    pk = compact_ws(paper_kind)
    sd = compact_ws(study_design)

    # strong empirical signals first
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

    # review signals after strong empirical rules
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


def normalize_record(rec: dict[str, Any]) -> dict[str, Any]:
    out = dict(rec)
    title = compact_ws(rec.get("title"))
    old_pk = compact_ws(rec.get("paper_kind"))
    old_dr = compact_ws(rec.get("document_role"))
    sd = compact_ws(rec.get("study_design"))

    new_pk = corrected_paper_kind(title, old_pk, sd)
    new_dr = corrected_document_role(title, new_pk, old_dr)

    changed = False
    notes = []

    if new_pk and new_pk != old_pk:
        out["original_paper_kind"] = rec.get("paper_kind")
        out["paper_kind"] = new_pk
        changed = True
        notes.append(f"paper_kind:{old_pk}->{new_pk}")

    if new_dr and new_dr != old_dr:
        out["original_document_role"] = rec.get("document_role")
        out["document_role"] = new_dr
        changed = True
        notes.append(f"document_role:{old_dr}->{new_dr}")

    if changed:
        out["label_normalization_notes"] = notes

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize evidence labels deterministically after extraction.")
    ap.add_argument("--evidence-input", required=True)
    ap.add_argument("--write", required=True)
    args = ap.parse_args()

    payload = load_json(args.evidence_input)
    if not payload:
        raise SystemExit(f"Could not load evidence input: {args.evidence_input}")

    summary = {"changed_count": 0}

    for bucket in ["evidence_records", "partial_records", "skipped_records"]:
        new_records = []
        for rec in payload.get(bucket, []) or []:
            new_rec = normalize_record(rec)
            if new_rec != rec:
                summary["changed_count"] += 1
            new_records.append(new_rec)
        payload[bucket] = new_records

    payload["label_normalization_summary"] = summary

    ensure_parent_dir(args.write)
    Path(args.write).write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
