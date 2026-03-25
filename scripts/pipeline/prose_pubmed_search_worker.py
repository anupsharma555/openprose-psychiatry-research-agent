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
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
TOOL = "openprose-pubmed-worker"
PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

TIER_1_JOURNALS = [
    "American Journal of Psychiatry",
    "JAMA Psychiatry",
    "Biological Psychiatry",
    "Molecular Psychiatry",
    "World Psychiatry",
    "The Lancet Psychiatry",
    "Translational Psychiatry",
    "Nature Mental Health",
    "Nature Medicine",
    "Nature",
    "Science",
    "JAMA",
    "New England Journal of Medicine",
    "BMJ",
]

TIER_2_JOURNALS = [
    "Journal of Clinical Psychiatry",
    "Journal of Affective Disorders",
    "Journal of Psychiatric Research",
    "Psychiatry Research",
    "Psychiatry Research. Neuroimaging",
    "Neuropsychopharmacology",
    "Schizophrenia Bulletin",
    "Psychological Medicine",
    "Depression and Anxiety",
    "American Journal of Geriatric Psychiatry",
    "Bipolar Disorders",
    "Addiction",
    "Journal of the American Academy of Child and Adolescent Psychiatry",
    "Journal of Child Psychology and Psychiatry and Allied Disciplines",
]


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


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


def build_search_stats(results: list[dict[str, Any]], variants: list[dict[str, Any]]) -> dict[str, Any]:
    scores = [float(r.get("score", 0.0) or 0.0) for r in results]
    journals = {compact_ws(r.get("journal") or "") for r in results if compact_ws(r.get("journal") or "")}
    return {
        "variant_count": len(variants),
        "variant_names": [v.get("name") for v in variants],
        "returned_result_count": len(results),
        "results_with_abstract": sum(1 for r in results if r.get("has_abstract")),
        "results_with_pmcid": sum(1 for r in results if r.get("has_pmcid")),
        "unique_journal_count": len(journals),
        "score_max": round(max(scores), 3) if scores else None,
        "score_min": round(min(scores), 3) if scores else None,
        "variant_total_hits": sum(int(v.get("count", 0) or 0) for v in variants),
    }


def build_search_warnings(results: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    if not results:
        return ["no_results_returned"]
    abstract_rate = sum(1 for r in results if r.get("has_abstract")) / len(results)
    pmcid_rate = sum(1 for r in results if r.get("has_pmcid")) / len(results)
    if abstract_rate < 0.5:
        warnings.append("low_abstract_coverage")
    if pmcid_rate < 0.25:
        warnings.append("low_pmc_fulltext_candidate_coverage")
    return warnings


@dataclass
class SearchVariant:
    name: str
    term: str
    sort: str = "relevance"
    retmax: int = 15


@dataclass
class PaperHit:
    pmid: str
    score: float = 0.0
    matched_variants: list[str] = field(default_factory=list)
    matched_terms: list[str] = field(default_factory=list)
    title: str | None = None
    journal: str | None = None
    pubdate: str | None = None
    sortpubdate: str | None = None
    epubdate: str | None = None
    authors: list[str] = field(default_factory=list)
    doi: str | None = None
    pmcid: str | None = None
    abstract: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pmid": self.pmid,
            "title": self.title,
            "journal": self.journal,
            "pubdate": self.pubdate,
            "sortpubdate": self.sortpubdate,
            "epubdate": self.epubdate,
            "authors": self.authors,
            "doi": self.doi,
            "pmcid": self.pmcid,
            "has_abstract": bool(self.abstract),
            "has_pmcid": bool(self.pmcid),
            "score": round(self.score, 3),
            "matched_variants": self.matched_variants,
            "matched_terms": self.matched_terms,
            "url": PUBMED_URL.format(pmid=self.pmid),
            "abstract": self.abstract,
        }


class EUtilsClient:
    def __init__(self, email: str | None, api_key: str | None, tool: str = TOOL, timeout: int = 30) -> None:
        self.email = email or os.getenv("NCBI_EMAIL") or ""
        self.api_key = api_key or os.getenv("NCBI_API_KEY") or os.getenv("PUBMED_API_KEY") or ""
        self.tool = tool
        self.timeout = timeout
        self.min_interval = 0.12 if self.api_key else 0.34
        self._last_call = 0.0

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call
        wait = self.min_interval - elapsed
        if wait > 0:
            time.sleep(wait)

    def _call(self, endpoint: str, params: dict[str, Any]) -> bytes:
        query = {k: v for k, v in params.items() if v not in (None, "")}
        query["tool"] = self.tool
        if self.email:
            query["email"] = self.email
        if self.api_key:
            query["api_key"] = self.api_key
        url = f"{BASE}/{endpoint}?{urllib.parse.urlencode(query, doseq=True)}"
        self._rate_limit()
        req = urllib.request.Request(url, headers={"User-Agent": f"{self.tool}/1.0"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = resp.read()
        self._last_call = time.monotonic()
        return data

    def esearch(self, term: str, sort: str, retmax: int, reldate: int | None, datetype: str) -> dict[str, Any]:
        raw = self._call(
            "esearch.fcgi",
            {
                "db": "pubmed",
                "term": term,
                "sort": sort,
                "retmax": retmax,
                "retmode": "json",
                "usehistory": "y",
                "datetype": datetype,
                "reldate": reldate,
            },
        )
        return json.loads(raw.decode("utf-8"))

    def esummary(self, ids: list[str]) -> dict[str, Any]:
        raw = self._call(
            "esummary.fcgi",
            {
                "db": "pubmed",
                "id": ",".join(ids),
                "retmode": "json",
            },
        )
        return json.loads(raw.decode("utf-8"))

    def efetch_xml(self, ids: list[str]) -> ET.Element:
        raw = self._call(
            "efetch.fcgi",
            {
                "db": "pubmed",
                "id": ",".join(ids),
                "retmode": "xml",
            },
        )
        return ET.fromstring(raw)


def build_journal_clause(journal_set: str) -> str:
    journals: list[str] = []
    if journal_set == "tier1":
        journals = TIER_1_JOURNALS
    elif journal_set == "tier1_tier2":
        journals = TIER_1_JOURNALS + TIER_2_JOURNALS
    elif journal_set == "tier2":
        journals = TIER_2_JOURNALS

    if not journals:
        return ""

    return " OR ".join([f'"{j}"[ta]' for j in journals])


def build_default_variants(
    query: str,
    mode: str,
    per_query: int,
    include_preprints: bool,
    journal_set: str,
    journal_retmax: int,
) -> list[SearchVariant]:
    base = f"({query})"
    abstract_guard = "hasabstract"
    if not include_preprints:
        base = f"{base} NOT preprint[pt]"

    variants: list[SearchVariant] = []

    if mode in {"frontier", "hybrid"}:
        variants.extend(
            [
                SearchVariant(
                    name="recent_relevance",
                    term=f"({base}) AND {abstract_guard}",
                    sort="relevance",
                    retmax=per_query,
                ),
                SearchVariant(
                    name="recent_pubdate",
                    term=f"({base}) AND {abstract_guard}",
                    sort="pub date",
                    retmax=per_query,
                ),
            ]
        )

    if mode in {"accessible", "hybrid", "frontier"}:
        variants.append(
            SearchVariant(
                name="recent_accessible",
                term=f"({base}) AND ({abstract_guard} OR free full text[sb] OR pubmed pmc[sb])",
                sort="pub date",
                retmax=per_query,
            )
        )

    if mode == "reviews":
        variants.extend(
            [
                SearchVariant(
                    name="systematic_reviews",
                    term=f"({base}) AND systematic[sb]",
                    sort="pub date",
                    retmax=per_query,
                ),
                SearchVariant(
                    name="reviews",
                    term=f"({base}) AND review[pt]",
                    sort="relevance",
                    retmax=per_query,
                ),
            ]
        )

    journal_clause = build_journal_clause(journal_set)
    if journal_clause:
        variants.append(
            SearchVariant(
                name="top_journals",
                term=f"({base}) AND ({journal_clause})",
                sort="pub date",
                retmax=journal_retmax,
            )
        )

    if not variants:
        variants.append(SearchVariant(name="default", term=base, sort="relevance", retmax=per_query))

    return variants


def score_for_variant(name: str, rank: int) -> float:
    base = {
        "recent_relevance": 4.0,
        "recent_pubdate": 3.2,
        "recent_accessible": 2.4,
        "systematic_reviews": 3.5,
        "reviews": 3.0,
        "top_journals": 4.6,
        "default": 2.0,
    }.get(name, 2.0)
    return base + max(0.0, 1.0 - 0.03 * rank)


def extract_article_ids(summary_item: dict[str, Any]) -> tuple[str | None, str | None]:
    doi = None
    pmcid = None
    for article_id in summary_item.get("articleids", []) or []:
        idtype = (article_id.get("idtype") or "").lower()
        value = article_id.get("value")
        if not value:
            continue
        if idtype == "doi" and not doi:
            doi = value
        elif idtype == "pmc" and not pmcid:
            pmcid = value
    return doi, pmcid


def extract_authors(summary_item: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for author in summary_item.get("authors", []) or []:
        name = author.get("name")
        if name:
            out.append(name)
    return out


def attach_summaries(hits: dict[str, PaperHit], summary_json: dict[str, Any]) -> None:
    result = summary_json.get("result", {})
    for uid in result.get("uids", []) or []:
        if uid not in hits:
            continue
        item = result.get(uid, {}) or {}
        doi, pmcid = extract_article_ids(item)
        hit = hits[uid]
        hit.title = item.get("title") or hit.title
        hit.journal = item.get("fulljournalname") or item.get("source") or hit.journal
        hit.pubdate = item.get("pubdate") or hit.pubdate
        hit.sortpubdate = item.get("sortpubdate") or hit.sortpubdate
        hit.epubdate = item.get("epubdate") or hit.epubdate
        hit.authors = extract_authors(item) or hit.authors
        hit.doi = doi or hit.doi
        hit.pmcid = pmcid or hit.pmcid


def attach_abstracts(hits: dict[str, PaperHit], xml_root: ET.Element) -> None:
    for article in xml_root.findall(".//PubmedArticle"):
        pmid = article.findtext(".//MedlineCitation/PMID")
        if not pmid or pmid not in hits:
            continue
        abstract_nodes = article.findall(".//Abstract/AbstractText")
        abstract_parts: list[str] = []
        for node in abstract_nodes:
            label = (node.attrib.get("Label") or "").strip()
            text = compact_ws("".join(node.itertext()))
            if not text:
                continue
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        hits[pmid].abstract = "\n".join(abstract_parts).strip() or None


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def run_search(args: argparse.Namespace) -> dict[str, Any]:
    client = EUtilsClient(email=args.email, api_key=args.api_key, tool=args.tool)

    variants = [
        SearchVariant(name=f"custom_{i+1}", term=t, sort=args.sort, retmax=args.per_query)
        for i, t in enumerate(args.term)
    ]
    if not variants:
        variants = build_default_variants(
            query=args.query,
            mode=args.mode,
            per_query=args.per_query,
            include_preprints=args.include_preprints,
            journal_set=args.journal_set,
            journal_retmax=args.journal_retmax,
        )

    hits: dict[str, PaperHit] = {}
    search_meta: list[dict[str, Any]] = []

    for variant in variants:
        payload = client.esearch(
            term=variant.term,
            sort=variant.sort,
            retmax=variant.retmax,
            reldate=args.reldate,
            datetype=args.datetype,
        )
        esearchresult = payload.get("esearchresult", {}) or {}
        ids = esearchresult.get("idlist", []) or []
        count = int(esearchresult.get("count", 0) or 0)

        search_meta.append(
            {
                "name": variant.name,
                "term": variant.term,
                "sort": variant.sort,
                "retmax": variant.retmax,
                "count": count,
                "returned_ids": ids,
            }
        )

        for rank, pmid in enumerate(ids):
            hit = hits.setdefault(pmid, PaperHit(pmid=pmid))
            hit.score += score_for_variant(variant.name, rank)
            if variant.name not in hit.matched_variants:
                hit.matched_variants.append(variant.name)
            if variant.term not in hit.matched_terms:
                hit.matched_terms.append(variant.term)

    ordered_pmids = sorted(hits, key=lambda pmid: (-hits[pmid].score, pmid))
    ordered_pmids = ordered_pmids[: args.max_results]
    trimmed_hits = {pmid: hits[pmid] for pmid in ordered_pmids}

    for batch in chunked(ordered_pmids, 100):
        attach_summaries(trimmed_hits, client.esummary(batch))
    for batch in chunked(ordered_pmids, 100):
        attach_abstracts(trimmed_hits, client.efetch_xml(batch))

    def recency_key(hit: PaperHit) -> str:
        return hit.sortpubdate or hit.epubdate or hit.pubdate or ""

    results = sorted(
        trimmed_hits.values(),
        key=lambda h: (h.score, recency_key(h), h.pmid),
        reverse=True,
    )

    plan = load_orchestration_plan(args.orchestration_plan)
    result_dicts = [hit.to_dict() for hit in results]
    stats = build_search_stats(result_dicts, search_meta)
    warnings = build_search_warnings(result_dicts)
    lane_window = None
    if args.lane and isinstance(plan.get("lane_windows"), dict):
        lane_window = plan.get("lane_windows", {}).get(args.lane)

    return {
        "schema_version": args.schema_version,
        "stage": "search",
        "run_id": args.run_id or plan.get("run_id") or None,
        "lane": args.lane or None,
        "generated_at": utc_now_iso(),
        "backend": "ncbi_eutils_pubmed",
        "mode": args.mode,
        "journal_set": args.journal_set,
        "query": args.query,
        "custom_terms": args.term,
        "datetype": args.datetype,
        "reldate": args.reldate,
        "max_results": args.max_results,
        "per_query": args.per_query,
        "journal_retmax": args.journal_retmax,
        "orchestration_context": {
            "plan_path": args.orchestration_plan or None,
            "topic": plan.get("topic") or None,
            "lane_window": lane_window,
        },
        "stats": stats,
        "warnings": warnings,
        "variants": search_meta,
        "results": result_dicts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search PubMed with one or more query variants and return merged JSON."
    )
    parser.add_argument(
        "--query",
        default="",
        help="Human topic string used for built-in modes when --term is not supplied.",
    )
    parser.add_argument(
        "--term",
        action="append",
        default=[],
        help="Explicit PubMed term. Repeat for multi-query search.",
    )
    parser.add_argument(
        "--mode",
        choices=["frontier", "accessible", "hybrid", "reviews"],
        default="hybrid",
    )
    parser.add_argument("--sort", default="relevance", help="Sort to use for explicit --term queries.")
    parser.add_argument("--max-results", type=int, default=20)
    parser.add_argument("--per-query", type=int, default=12)
    parser.add_argument("--reldate", type=int, default=365, help="Limit search to the last N days.")
    parser.add_argument("--datetype", default="pdat", choices=["pdat", "edat", "mdat"])
    parser.add_argument("--include-preprints", action="store_true")
    parser.add_argument("--journal-set", choices=["off", "tier1", "tier2", "tier1_tier2"], default="off")
    parser.add_argument("--journal-retmax", type=int, default=10)
    parser.add_argument("--email", default=os.getenv("NCBI_EMAIL", ""))
    parser.add_argument("--api-key", default=(os.getenv("NCBI_API_KEY") or os.getenv("PUBMED_API_KEY") or ""))
    parser.add_argument("--tool", default=TOOL)
    parser.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    parser.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    parser.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    parser.add_argument("--schema-version", default="1.1", help="Schema version for search artifact output.")
    parser.add_argument("--write", default="", help="Optional path to write JSON output.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.query and not args.term:
        parser.error("Provide --query or at least one --term.")

    try:
        payload = run_search(args)
    except Exception as exc:
        err = {
            "generated_at": utc_now_iso(),
            "backend": "ncbi_eutils_pubmed",
            "error": str(exc),
        }
        print(json.dumps(err, indent=2))
        return 1

    text = json.dumps(payload, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        with open(args.write, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
