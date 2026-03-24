#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from html import unescape
from pathlib import Path
from typing import Any

NCBI_EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
PMC_OAI = "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/"
PMC_OA = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"
PMC_IDCONV = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
EUROPE_PMC = "https://www.ebi.ac.uk/europepmc/webservices/rest"
CROSSREF_WORKS = "https://api.crossref.org/works/"
UNPAYWALL = "https://api.unpaywall.org/v2/"
DOI_ROOT = "https://doi.org/"

DEFAULT_TOOL = "openprose-fulltext-resolver"
TIMEOUT = 30

FULLTEXT_READY = {"fulltext_xml", "fulltext_html", "fulltext_pdf"}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_ws(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def safe_component(text: str, max_len: int = 80) -> str:
    text = compact_ws(text)
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return (text or "item")[:max_len]


def rec_key(rec: dict[str, Any]) -> str:
    return (
        compact_ws(rec.get("pmcid"))
        or compact_ws(rec.get("doi"))
        or compact_ws(rec.get("pmid"))
        or safe_component(rec.get("title", "item"))
    )


def default_headers(tool: str, contact_email: str | None) -> dict[str, str]:
    ua = tool if not contact_email else f"{tool} ({contact_email})"
    return {
        "User-Agent": ua,
    }


def http_request(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = TIMEOUT,
) -> tuple[int, dict[str, str], bytes, str]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        final_url = resp.geturl()
        body = resp.read()
        header_map = {k.lower(): v for k, v in resp.headers.items()}

        content_encoding = (header_map.get("content-encoding") or "").lower()
        if "gzip" in content_encoding:
            body = gzip.decompress(body)
        elif "deflate" in content_encoding:
            try:
                body = zlib.decompress(body)
            except zlib.error:
                body = zlib.decompress(body, -zlib.MAX_WBITS)
        elif body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)

        return status, header_map, body, final_url


def http_json(url: str, *, headers: dict[str, str] | None = None, timeout: int = TIMEOUT) -> tuple[dict[str, Any], str]:
    status, _, body, final_url = http_request(url, headers=headers, timeout=timeout)
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status} for {url}")
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body.decode("utf-8")), final_url


def write_bytes(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)


def write_text(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def load_json(path: str) -> dict[str, Any]:
    if path == "-":
        return json.load(sys.stdin)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_cache(path: str) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(path: str, cache: dict[str, Any]) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def ensure_parent_dir(path_str: str) -> None:
    if not path_str:
        return
    Path(path_str).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def load_orchestration_plan(path: str) -> dict[str, Any]:
    if not path:
        return {}
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_resolution_summary(processed_records: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    best_source_counts: dict[str, int] = {}
    resolved_by_counts: dict[str, int] = {}

    for rec in processed_records:
        status = compact_ws(rec.get("fulltext_status")) or "unknown"
        best_source = compact_ws(rec.get("best_source")) or "unknown"
        resolved_by = compact_ws(rec.get("resolved_by")) or "unknown"

        status_counts[status] = status_counts.get(status, 0) + 1
        best_source_counts[best_source] = best_source_counts.get(best_source, 0) + 1
        resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

    return {
        "status_counts": dict(sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "best_source_counts": dict(sorted(best_source_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "resolved_by_counts": dict(sorted(resolved_by_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
    }


def build_resolver_stats(
    input_records: list[dict[str, Any]],
    processed_records: list[dict[str, Any]],
    unresolved_records: list[dict[str, Any]],
) -> dict[str, Any]:
    processed_count = len(processed_records)
    analysis_ready_count = sum(1 for rec in processed_records if rec.get("analysis_ready"))
    cache_hit_count = sum(1 for rec in processed_records if rec.get("cache_hit"))

    def status_count(name: str) -> int:
        return sum(1 for rec in processed_records if rec.get("fulltext_status") == name)

    return {
        "input_count": len(input_records),
        "processed_count": processed_count,
        "analysis_ready_count": analysis_ready_count,
        "unresolved_count": len(unresolved_records),
        "cache_hit_count": cache_hit_count,
        "analysis_ready_rate": round((analysis_ready_count / processed_count), 3) if processed_count else 0.0,
        "cache_hit_rate": round((cache_hit_count / processed_count), 3) if processed_count else 0.0,
        "xml_count": status_count("fulltext_xml"),
        "html_count": status_count("fulltext_html"),
        "pdf_count": status_count("fulltext_pdf"),
        "free_url_only_count": status_count("free_url_only"),
        "landing_page_only_count": status_count("landing_page_only"),
        "abstract_only_count": status_count("abstract_only"),
        "unresolved_no_fulltext_count": status_count("unresolved_no_fulltext"),
    }


def build_resolver_feedback(processed_records: list[dict[str, Any]]) -> dict[str, Any]:
    processed_count = len(processed_records)
    if not processed_count:
        return {
            "retry_suggested": True,
            "missing_angles": ["no_records_processed"],
            "candidate_retry_actions": ["increase_top_k"],
        }

    analysis_ready_count = sum(1 for rec in processed_records if rec.get("analysis_ready"))
    xml_count = sum(1 for rec in processed_records if rec.get("fulltext_status") == "fulltext_xml")
    landing_only_count = sum(1 for rec in processed_records if rec.get("fulltext_status") == "landing_page_only")
    abstract_only_count = sum(1 for rec in processed_records if rec.get("fulltext_status") in {"abstract_only", "unresolved_no_fulltext"})

    ready_rate = analysis_ready_count / processed_count
    landing_rate = landing_only_count / processed_count
    abstract_rate = abstract_only_count / processed_count

    missing_angles: list[str] = []
    candidate_retry_actions: list[str] = []
    retry_suggested = False

    if ready_rate < 0.5:
        retry_suggested = True
        missing_angles.append("low_fulltext_yield")
        candidate_retry_actions.append("prefer_accessible_records")

    if landing_rate > 0.25:
        missing_angles.append("too_many_landing_page_only_records")
        candidate_retry_actions.append("swap_low_access_records")

    if abstract_rate > 0.35:
        missing_angles.append("too_many_abstract_only_records")
        candidate_retry_actions.append("increase_top_k")

    if xml_count == 0:
        missing_angles.append("no_xml_fulltext_records")

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
        "access_signals": {
            "analysis_ready_rate": round(ready_rate, 3),
            "landing_page_only_rate": round(landing_rate, 3),
            "abstract_only_rate": round(abstract_rate, 3),
            "xml_count": xml_count,
        },
    }


def select_records(payload: dict[str, Any], records_key: str, top_k: int) -> list[dict[str, Any]]:
    if records_key in payload and isinstance(payload[records_key], list):
        recs = payload[records_key]
    elif "results" in payload and isinstance(payload["results"], list):
        recs = payload["results"]
    else:
        raise ValueError(f"Could not find a list of records under '{records_key}' or 'results'")
    return recs[:top_k] if top_k > 0 else recs


def parse_meta_tags(html_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in re.finditer(
        r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.I,
    ):
        key = m.group(1).strip().lower()
        val = unescape(m.group(2).strip())
        out[key] = val
    return out


def normalize_doi(doi: str | None) -> str:
    doi = compact_ws(doi)
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I)
    return doi


def pmc_numeric(pmcid: str | None) -> str:
    pmcid = compact_ws(pmcid)
    return re.sub(r"^PMC", "", pmcid, flags=re.I)


def add_unique_url(bucket: list[dict[str, str]], url: str, source: str, kind: str) -> None:
    url = compact_ws(url)
    if not url:
        return
    if any(item["url"] == url for item in bucket):
        return
    bucket.append({"url": url, "source": source, "kind": kind})


def discover_pmcid(rec: dict[str, Any], tool: str, contact_email: str, sleep_s: float) -> dict[str, Any]:
    pmcid = compact_ws(rec.get("pmcid"))
    if pmcid:
        return {"pmcid": pmcid, "pmid": compact_ws(rec.get("pmid")), "doi": normalize_doi(rec.get("doi")), "source": "input"}

    ids = normalize_doi(rec.get("doi")) or compact_ws(rec.get("pmid"))
    if not ids:
        return {}

    params = {
        "ids": ids,
        "format": "json",
        "tool": tool,
        "email": contact_email,
    }
    url = PMC_IDCONV + "?" + urllib.parse.urlencode(params)
    data, _ = http_json(url, headers=default_headers(tool, contact_email))
    time.sleep(sleep_s)

    records = data.get("records") or []
    if not records:
        return {}
    hit = records[0]
    return {
        "pmcid": compact_ws(hit.get("pmcid")),
        "pmid": compact_ws(hit.get("pmid")) or compact_ws(rec.get("pmid")),
        "doi": normalize_doi(hit.get("doi")) or normalize_doi(rec.get("doi")),
        "source": "pmc_idconv",
    }


def resolve_pmc_oai(rec: dict[str, Any], outdir: Path, tool: str, contact_email: str, sleep_s: float) -> dict[str, Any]:
    pmcid = compact_ws(rec.get("pmcid"))
    if not pmcid:
        return {}

    out: dict[str, Any] = {"route": "pmc_oai", "pmcid": pmcid}
    num = pmc_numeric(pmcid)

    oa_url = PMC_OA + "?" + urllib.parse.urlencode({"id": pmcid})
    try:
        status, _, body, _ = http_request(oa_url, headers=default_headers(tool, contact_email))
        if status == 200:
            try:
                root = ET.fromstring(body)
                rec_node = root.find(".//record")
                if rec_node is not None:
                    out["oa_license"] = rec_node.attrib.get("license")
                    out["oa_retracted"] = rec_node.attrib.get("retracted")
                    links = []
                    for link in rec_node.findall("./link"):
                        href = link.attrib.get("href")
                        fmt = link.attrib.get("format", "")
                        if href:
                            links.append({"url": href, "format": fmt})
                    if links:
                        out["oa_links"] = links
            except ET.ParseError:
                pass
    except urllib.error.HTTPError as e:
        out["oa_http_status"] = e.code
    except Exception as e:
        out["oa_error"] = compact_ws(str(e))
    time.sleep(sleep_s)

    oai_params = {
        "verb": "GetRecord",
        "identifier": f"oai:pubmedcentral.nih.gov:{num}",
        "metadataPrefix": "pmc",
    }
    oai_url = PMC_OAI + "?" + urllib.parse.urlencode(oai_params)

    try:
        status, headers, body, _ = http_request(oai_url, headers=default_headers(tool, contact_email))
        content_type = headers.get("content-type", "")
        if status == 200 and (b"<GetRecord" in body or b"<record" in body):
            xml_path = outdir / "xml" / f"{safe_component(pmcid)}.pmc.xml"
            out["xml_path"] = write_bytes(xml_path, body)
            out["xml_source"] = "pmc_oai"
            out["content_type"] = content_type
            out["status"] = "fulltext_xml"
    except urllib.error.HTTPError as e:
        out["oai_http_status"] = e.code
        out["status"] = out.get("status") or "pmc_oai_miss"
    except Exception as e:
        out["oai_error"] = compact_ws(str(e))
        out["status"] = out.get("status") or "pmc_oai_miss"

    return out


def europe_pmc_article_url(rec: dict[str, Any], contact_email: str | None) -> tuple[str, str] | tuple[None, None]:
    pmcid = compact_ws(rec.get("pmcid"))
    pmid = compact_ws(rec.get("pmid"))
    doi = normalize_doi(rec.get("doi"))

    if pmcid:
        src, ident = "PMC", pmcid
    elif pmid:
        src, ident = "MED", pmid
    elif doi:
        src, ident = "DOI", doi
    else:
        return None, None

    params = {"resultType": "core", "format": "json"}
    if contact_email:
        params["email"] = contact_email
    url = f"{EUROPE_PMC}/article/{src}/{urllib.parse.quote(ident, safe='') }?" + urllib.parse.urlencode(params)
    return url, src


def resolve_europe_pmc(rec: dict[str, Any], outdir: Path, tool: str, contact_email: str, sleep_s: float) -> dict[str, Any]:
    article_url, src = europe_pmc_article_url(rec, contact_email)
    if not article_url:
        return {}

    out: dict[str, Any] = {"route": "europe_pmc", "source_type": src}
    try:
        data, _ = http_json(article_url, headers=default_headers(tool, contact_email))
    except Exception:
        return out
    time.sleep(sleep_s)

    result = data if isinstance(data, dict) else {}
    out["is_open_access"] = result.get("isOpenAccess")
    if result.get("pmcid") and not rec.get("pmcid"):
        out["pmcid"] = result.get("pmcid")

    urls: list[dict[str, str]] = []
    ft_list = ((result.get("fullTextUrlList") or {}).get("fullTextUrl") or [])
    if isinstance(ft_list, dict):
        ft_list = [ft_list]
    for item in ft_list:
        url = item.get("url")
        style = item.get("documentStyle") or item.get("availability") or "full_text"
        site = item.get("site") or "europe_pmc"
        if url:
            add_unique_url(urls, url, site, style)
    if urls:
        out["fulltext_urls"] = urls

    pmcid = compact_ws(result.get("pmcid") or rec.get("pmcid"))
    if pmcid:
        xml_url = f"{EUROPE_PMC}/{urllib.parse.quote(pmcid, safe='')}/fullTextXML"
        try:
            status, headers, body, _ = http_request(xml_url, headers=default_headers(tool, contact_email))
            ct = headers.get("content-type", "")
            if status == 200 and (b"<" in body and b"article" in body[:5000]):
                xml_path = outdir / "xml" / f"{safe_component(pmcid)}.europepmc.xml"
                out["xml_path"] = write_bytes(xml_path, body)
                out["xml_source"] = "europe_pmc"
                out["content_type"] = ct
                out["status"] = "fulltext_xml"
        except Exception:
            pass
        time.sleep(sleep_s)

    return out


def resolve_pubmed_linkout(rec: dict[str, Any], tool: str, contact_email: str, api_key: str | None, sleep_s: float) -> dict[str, Any]:
    pmid = compact_ws(rec.get("pmid"))
    if not pmid:
        return {}

    params = {"dbfrom": "pubmed", "id": pmid, "cmd": "llinks", "retmode": "xml", "tool": tool, "email": contact_email}
    if api_key:
        params["api_key"] = api_key
    url = f"{NCBI_EUTILS}/elink.fcgi?" + urllib.parse.urlencode(params)

    out: dict[str, Any] = {"route": "pubmed_linkout", "pmid": pmid}
    try:
        status, _, body, _ = http_request(url, headers=default_headers(tool, contact_email))
    except Exception:
        return out
    time.sleep(sleep_s)
    if status != 200:
        return out

    urls: list[dict[str, str]] = []
    try:
        root = ET.fromstring(body)
        for node in root.findall(".//ObjUrl"):
            target = node.findtext("./Url")
            provider = compact_ws(node.findtext("./Provider/Name")) or "linkout"
            subj = compact_ws(node.findtext("./SubjectType")) or "provider"
            if target:
                add_unique_url(urls, target, provider, subj)
    except ET.ParseError:
        pass
    if urls:
        out["provider_urls"] = urls
        out["status"] = "linkout_urls"
    return out


def resolve_crossref(rec: dict[str, Any], tool: str, contact_email: str, sleep_s: float) -> dict[str, Any]:
    doi = normalize_doi(rec.get("doi"))
    if not doi:
        return {}
    url = CROSSREF_WORKS + urllib.parse.quote(doi, safe="")
    out: dict[str, Any] = {"route": "crossref", "doi": doi}
    try:
        data, _ = http_json(url, headers=default_headers(tool, contact_email))
    except Exception:
        return out
    time.sleep(sleep_s)
    msg = (data or {}).get("message") or {}
    out["landing_url"] = msg.get("URL")
    out["license_urls"] = [x.get("URL") for x in (msg.get("license") or []) if x.get("URL")]
    links = []
    for x in (msg.get("link") or []):
        href = x.get("URL") or x.get("url")
        ctype = x.get("content-type") or x.get("content_type") or "link"
        if href:
            links.append({"url": href, "content_type": ctype, "intended_application": x.get("intended-application")})
    if links:
        out["links"] = links
    if out.get("landing_url") or links:
        out["status"] = "metadata_only"
    return out


def resolve_unpaywall(rec: dict[str, Any], unpaywall_email: str | None, tool: str, contact_email: str, sleep_s: float) -> dict[str, Any]:
    doi = normalize_doi(rec.get("doi"))
    if not doi or not unpaywall_email:
        return {}
    url = UNPAYWALL + urllib.parse.quote(doi, safe="") + "?" + urllib.parse.urlencode({"email": unpaywall_email})
    out: dict[str, Any] = {"route": "unpaywall", "doi": doi}
    try:
        data, _ = http_json(url, headers=default_headers(tool, contact_email))
    except Exception:
        return out
    time.sleep(sleep_s)
    best = data.get("best_oa_location") or {}
    out["is_oa"] = data.get("is_oa")
    out["oa_status"] = data.get("oa_status")
    out["best_oa_location"] = {
        "url": best.get("url"),
        "url_for_pdf": best.get("url_for_pdf"),
        "url_for_landing_page": best.get("url_for_landing_page"),
        "host_type": best.get("host_type"),
        "version": best.get("version"),
        "license": best.get("license"),
    }
    oa_urls: list[dict[str, str]] = []
    for key in ("url_for_pdf", "url_for_landing_page", "url"):
        if best.get(key):
            kind = "pdf" if key == "url_for_pdf" else "oa_location"
            add_unique_url(oa_urls, best.get(key), "unpaywall", kind)
    for item in data.get("oa_locations") or []:
        for key in ("url_for_pdf", "url_for_landing_page", "url"):
            if item.get(key):
                kind = "pdf" if key == "url_for_pdf" else "oa_location"
                add_unique_url(oa_urls, item.get(key), "unpaywall", kind)
    if oa_urls:
        out["oa_urls"] = oa_urls
        out["status"] = "free_url_only"
    return out


def resolve_doi_landing(rec: dict[str, Any], outdir: Path, tool: str, contact_email: str, download_html: bool, download_pdf: bool, sleep_s: float) -> dict[str, Any]:
    doi = normalize_doi(rec.get("doi"))
    if not doi:
        return {}
    url = DOI_ROOT + urllib.parse.quote(doi, safe="")
    headers = default_headers(tool, contact_email)
    headers["Accept"] = "text/html,application/pdf,application/xhtml+xml;q=0.9,*/*;q=0.8"

    out: dict[str, Any] = {"route": "doi_landing", "doi": doi}
    try:
        status, resp_headers, body, final_url = http_request(url, headers=headers)
    except Exception:
        return out
    time.sleep(sleep_s)
    if status != 200:
        return out

    ctype = resp_headers.get("content-type", "")
    out["final_url"] = final_url
    out["content_type"] = ctype

    stem = safe_component(rec_key(rec))
    if "pdf" in ctype.lower() and download_pdf:
        path = outdir / "pdf" / f"{stem}.doi.pdf"
        out["pdf_path"] = write_bytes(path, body)
        out["status"] = "fulltext_pdf"
        return out

    if "html" in ctype.lower() or body.lstrip().startswith(b"<"):
        html_text = body.decode("utf-8", errors="replace")
        meta = parse_meta_tags(html_text)
        out["meta_urls"] = {k: v for k, v in meta.items() if k in {"citation_pdf_url", "citation_fulltext_html_url", "citation_abstract_html_url", "og:url", "citation_public_url"}}
        if download_html:
            path = outdir / "html" / f"{stem}.landing.html"
            out["html_path"] = write_text(path, html_text)
        out["status"] = out.get("status") or "landing_page"
    return out


def maybe_download_candidate(
    url: str,
    *,
    outdir: Path,
    stem: str,
    label: str,
    tool: str,
    contact_email: str,
    download_html: bool,
    download_pdf: bool,
    sleep_s: float,
) -> dict[str, Any]:
    headers = default_headers(tool, contact_email)
    headers["Accept"] = "text/html,application/pdf,application/xhtml+xml;q=0.9,*/*;q=0.8"
    out: dict[str, Any] = {"url": url, "label": label}
    try:
        status, resp_headers, body, final_url = http_request(url, headers=headers)
    except Exception as exc:
        out["error"] = str(exc)
        return out
    time.sleep(sleep_s)
    if status != 200:
        out["error"] = f"HTTP {status}"
        return out

    ctype = resp_headers.get("content-type", "")
    out["final_url"] = final_url
    out["content_type"] = ctype
    if "pdf" in ctype.lower() and download_pdf:
        out["pdf_path"] = write_bytes(outdir / "pdf" / f"{stem}.{label}.pdf", body)
        out["status"] = "fulltext_pdf"
        return out
    if ("html" in ctype.lower() or body.lstrip().startswith(b"<")) and download_html:
        text = body.decode("utf-8", errors="replace")
        out["html_path"] = write_text(outdir / "html" / f"{stem}.{label}.html", text)
        out["status"] = "fulltext_html"
        return out
    return out


def choose_best_status(result: dict[str, Any]) -> tuple[str, str | None]:
    if result.get("xml_path"):
        return "fulltext_xml", result.get("xml_source") or result.get("resolved_by")
    if result.get("pdf_path"):
        return "fulltext_pdf", result.get("resolved_by")
    if result.get("html_path"):
        return "fulltext_html", result.get("resolved_by")

    for section in ("unpaywall", "europe_pmc", "pubmed_linkout", "crossref", "doi_landing"):
        blob = result.get(section) or {}
        if blob.get("oa_urls") or blob.get("fulltext_urls") or blob.get("provider_urls"):
            return "free_url_only", section
        if blob.get("landing_url") or blob.get("final_url"):
            return "landing_page_only", section
    return "abstract_only", None


def resolve_record(rec: dict[str, Any], cfg: argparse.Namespace) -> dict[str, Any]:
    outdir = Path(cfg.outdir)
    stem = safe_component(rec_key(rec))
    result: dict[str, Any] = {
        "pmid": compact_ws(rec.get("pmid")),
        "pmcid": compact_ws(rec.get("pmcid")),
        "doi": normalize_doi(rec.get("doi")),
        "title": compact_ws(rec.get("title")),
        "journal": compact_ws(rec.get("journal")),
        "input_url": compact_ws(rec.get("url")),
        "resolved_at": utc_now_iso(),
    }

    # Step 0: discover PMCID if possible.
    discovered = discover_pmcid(rec, cfg.tool, cfg.contact_email, cfg.sleep_seconds)
    if discovered.get("pmcid") and not result.get("pmcid"):
        result["pmcid"] = discovered["pmcid"]
    if discovered.get("pmid") and not result.get("pmid"):
        result["pmid"] = discovered["pmid"]
    if discovered.get("doi") and not result.get("doi"):
        result["doi"] = discovered["doi"]
    if discovered:
        result["pmc_id_discovery"] = discovered

    pmc_blob = resolve_pmc_oai(result, outdir, cfg.tool, cfg.contact_email, cfg.sleep_seconds)
    if pmc_blob:
        result["pmc_oai"] = pmc_blob
        if pmc_blob.get("xml_path"):
            result["xml_path"] = pmc_blob["xml_path"]
            result["resolved_by"] = "pmc_oai"
            status, best_source = choose_best_status(result)
            result["fulltext_status"] = status
            result["best_source"] = best_source or "pmc_oai"
            return result

    epmc_blob = resolve_europe_pmc(result, outdir, cfg.tool, cfg.contact_email, cfg.sleep_seconds)
    if epmc_blob:
        result["europe_pmc"] = epmc_blob
        if epmc_blob.get("pmcid") and not result.get("pmcid"):
            result["pmcid"] = epmc_blob["pmcid"]
        if epmc_blob.get("xml_path"):
            result["xml_path"] = epmc_blob["xml_path"]
            result["resolved_by"] = "europe_pmc"
            status, best_source = choose_best_status(result)
            result["fulltext_status"] = status
            result["best_source"] = best_source or "europe_pmc"
            return result

    linkout_blob = resolve_pubmed_linkout(result, cfg.tool, cfg.contact_email, cfg.api_key, cfg.sleep_seconds)
    if linkout_blob:
        result["pubmed_linkout"] = linkout_blob

    crossref_blob = resolve_crossref(result, cfg.tool, cfg.contact_email, cfg.sleep_seconds)
    if crossref_blob:
        result["crossref"] = crossref_blob

    unpaywall_blob = resolve_unpaywall(result, cfg.unpaywall_email, cfg.tool, cfg.contact_email, cfg.sleep_seconds)
    if unpaywall_blob:
        result["unpaywall"] = unpaywall_blob

    doi_blob = resolve_doi_landing(
        result,
        outdir,
        cfg.tool,
        cfg.contact_email,
        cfg.download_html,
        cfg.download_pdf,
        cfg.sleep_seconds,
    )
    if doi_blob:
        result["doi_landing"] = doi_blob
        if doi_blob.get("html_path"):
            result["html_path"] = doi_blob["html_path"]
            result["resolved_by"] = "doi_landing"
        if doi_blob.get("pdf_path"):
            result["pdf_path"] = doi_blob["pdf_path"]
            result["resolved_by"] = "doi_landing"

    # Optional fetch of best open candidate URL only.
    if cfg.download_open_urls:
        candidates: list[tuple[str, str]] = []
        up_best = ((result.get("unpaywall") or {}).get("best_oa_location") or {})
        for key, label in (("url_for_pdf", "unpaywall_pdf"), ("url_for_landing_page", "unpaywall_html"), ("url", "unpaywall_url")):
            if up_best.get(key):
                candidates.append((up_best[key], label))
        for item in ((result.get("europe_pmc") or {}).get("fulltext_urls") or []):
            if item.get("url"):
                candidates.append((item["url"], "europepmc_url"))
        for item in ((result.get("pubmed_linkout") or {}).get("provider_urls") or []):
            if item.get("url"):
                candidates.append((item["url"], "linkout_url"))

        seen = set()
        fetched = []
        for url, label in candidates:
            if url in seen:
                continue
            seen.add(url)
            blob = maybe_download_candidate(
                url,
                outdir=outdir,
                stem=stem,
                label=label,
                tool=cfg.tool,
                contact_email=cfg.contact_email,
                download_html=cfg.download_html,
                download_pdf=cfg.download_pdf,
                sleep_s=cfg.sleep_seconds,
            )
            fetched.append(blob)
            if blob.get("pdf_path") and not result.get("pdf_path"):
                result["pdf_path"] = blob["pdf_path"]
                result["resolved_by"] = label
                break
            if blob.get("html_path") and not result.get("html_path"):
                result["html_path"] = blob["html_path"]
                result["resolved_by"] = label
        if fetched:
            result["download_attempts"] = fetched

    status, best_source = choose_best_status(result)
    result["fulltext_status"] = status
    result["best_source"] = best_source
    return result



def normalize_analysis_status(item: dict[str, Any]) -> dict[str, Any]:
    status = item.get("fulltext_status")
    item["analysis_ready"] = status in FULLTEXT_READY
    if not item["analysis_ready"]:
        item["fulltext_status"] = "unresolved_no_fulltext"
    return item


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Resolve full-text paths for ranked PubMed records using PMCID, Europe PMC, DOI, LinkOut, and web routes.")
    p.add_argument("--input", required=True, help="Input JSON from ranking stage, or - for stdin")
    p.add_argument("--records-key", default="kept_records", help="List key to read from input JSON")
    p.add_argument("--top-k", type=int, default=10, help="How many records to resolve")
    p.add_argument("--outdir", default="/tmp/prose_fulltext", help="Directory for downloaded artifacts")
    p.add_argument("--write", default="", help="Optional path for output JSON")
    p.add_argument("--cache", default="", help="Optional JSON cache file")
    p.add_argument("--refresh", action="store_true", help="Ignore cached resolver outputs")
    p.add_argument("--download-html", action="store_true", help="Save HTML when accessible")
    p.add_argument("--download-pdf", action="store_true", help="Save PDF when accessible")
    p.add_argument("--download-open-urls", action="store_true", help="Try downloading only candidate OA/LinkOut URLs after identifier-native routes")
    p.add_argument("--tool", default=DEFAULT_TOOL)
    p.add_argument("--contact-email", default=os.getenv("CONTACT_EMAIL") or os.getenv("NCBI_EMAIL") or os.getenv("CROSSREF_EMAIL") or "")
    p.add_argument("--unpaywall-email", default=os.getenv("UNPAYWALL_EMAIL") or os.getenv("CONTACT_EMAIL") or "")
    p.add_argument("--api-key", default=os.getenv("NCBI_API_KEY") or os.getenv("PUBMED_API_KEY") or "")
    p.add_argument("--sleep-seconds", type=float, default=0.2, help="Polite delay between network requests")
    p.add_argument("--run-id", default="", help="Optional run identifier for artifact metadata.")
    p.add_argument("--lane", default="", help="Optional lane name, for example core_evidence or frontier.")
    p.add_argument("--orchestration-plan", default="", help="Optional path to orchestration_plan.json for context metadata.")
    p.add_argument("--schema-version", default="1.1", help="Schema version for resolver artifact output.")
    p.add_argument("--require-fulltext", action="store_true", help="Only keep XML/HTML/PDF records in resolved_records")
    return p


def main() -> int:
    args = build_parser().parse_args()
    payload = load_json(args.input)
    plan_path = args.orchestration_plan or ((payload.get("orchestration_context") or {}).get("plan_path")) or ""
    plan = load_orchestration_plan(plan_path)
    lane = compact_ws(args.lane) or compact_ws(payload.get("lane")) or None
    records = select_records(payload, args.records_key, args.top_k)

    cache = load_cache(args.cache)
    cache_records = cache.get("records", {}) if isinstance(cache, dict) else {}
    processed: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    for rec in records:
        key = rec_key(rec)
        if not args.refresh and key in cache_records:
            item = dict(cache_records[key])
            item["cache_hit"] = True
        else:
            try:
                item = resolve_record(rec, args)
            except Exception as e:
                item = dict(rec)
                item["route_error"] = compact_ws(str(e))
                item["fulltext_status"] = "resolver_error"
            item["cache_hit"] = False
            cache_records[key] = item

        item = normalize_analysis_status(item)
        processed.append(item)

        if args.require_fulltext:
            if item["analysis_ready"]:
                resolved.append(item)
            else:
                unresolved.append(item)
        else:
            resolved.append(item)
            if not item["analysis_ready"]:
                unresolved.append(item)

    resolution_summary = build_resolution_summary(processed)
    stats = build_resolver_stats(
        input_records=records,
        processed_records=processed,
        unresolved_records=unresolved,
    )
    resolver_feedback = build_resolver_feedback(processed)

    output = {
        "schema_version": args.schema_version,
        "stage": "fulltext_resolve",
        "run_id": args.run_id or payload.get("run_id") or plan.get("run_id") or None,
        "generated_at": utc_now_iso(),
        "resolver": args.tool,
        "source_stage": payload.get("stage"),
        "input": args.input,
        "records_key": args.records_key,
        "lane": lane,
        "outdir": args.outdir,
        "orchestration_context": {
            "plan_path": plan_path or None,
            "topic": plan.get("topic") or (payload.get("orchestration_context") or {}).get("topic"),
            "lane_window": ((plan.get("lane_windows") or {}).get(lane)) if isinstance(plan.get("lane_windows"), dict) and lane else (payload.get("orchestration_context") or {}).get("lane_window"),
        },
        "input_stats": payload.get("stats"),
        "resolution_summary": resolution_summary,
        "stats": stats,
        "resolver_feedback": resolver_feedback,
        "resolved_count": len(resolved),
        "unresolved_count": len(unresolved),
        "resolved_records": resolved,
        "unresolved_records": unresolved,
    }

    if args.cache:
        cache_payload = {
            "updated_at": utc_now_iso(),
            "records": cache_records,
        }
        save_cache(args.cache, cache_payload)

    text = json.dumps(output, indent=2, ensure_ascii=False)
    if args.write:
        ensure_parent_dir(args.write)
        Path(args.write).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
