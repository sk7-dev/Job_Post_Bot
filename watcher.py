from __future__ import annotations
import json
import os
import sys
import time
from typing import List, Optional
from urllib.parse import urlparse, quote
import re
from html import unescape
from urllib.parse import urlparse
from typing import List, Tuple
import time


import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

import requests

CONFIG_PATH = "config.json"
STATE_PATH = "state_seen.json"
REQUEST_TIMEOUT = 30


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def matches_filters(job: dict, filters: dict) -> bool:
    title = normalize_text(job.get("title"))
    location = normalize_text(job.get("location"))
    department = normalize_text(job.get("department"))
    combined = " | ".join([title, location, department])

    title_keywords_any = [
        normalize_text(x) for x in filters.get("title_keywords_any", []) if str(x).strip()
    ]
    locations_any = [
        normalize_text(x) for x in filters.get("locations_any", []) if str(x).strip()
    ]
    excluded_keywords_any = [
        normalize_text(x) for x in filters.get("excluded_keywords_any", []) if str(x).strip()
    ]

    title_ok = True
    if title_keywords_any:
        title_ok = any(k in title for k in title_keywords_any)

    location_ok = True
    if locations_any:
        location_ok = any(k in combined for k in locations_any)

    excluded_ok = True
    if excluded_keywords_any:
        excluded_ok = not any(k in combined for k in excluded_keywords_any)

    return title_ok and location_ok and excluded_ok


def safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


def safe_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    return safe_get(url, params=params, headers=headers).json()


def safe_post_json(url: str, body: dict, headers: Optional[dict] = None) -> dict:
    resp = requests.post(url, json=body, headers=headers or {}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object from {url}, got {type(data).__name__}")
    return data


def safe_get_text(url: str, headers: Optional[dict] = None) -> str:
    resp = requests.get(url, headers=headers or {}, timeout=30)
    resp.raise_for_status()
    return resp.text


def fetch_greenhouse(source: dict) -> List[dict]:
    token = source["board_token"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    data = safe_get_json(url)

    jobs = []
    for item in data.get("jobs", []):
        location = ""
        if isinstance(item.get("location"), dict):
            location = item["location"].get("name", "")

        departments = item.get("departments") or []
        department = ", ".join(d.get("name", "") for d in departments if d.get("name"))

        offices = item.get("offices") or []
        office = ", ".join(o.get("name", "") for o in offices if o.get("name"))

        jobs.append({
            "source_name": source["name"],
            "source_type": "greenhouse",
            "external_id": str(item.get("id")),
            "title": item.get("title", ""),
            "location": location or office,
            "department": department,
            "url": item.get("absolute_url", ""),
            "posted_at": "",
        })
    return jobs


def fetch_lever(source: dict) -> List[dict]:
    company = source["company"]
    url = f"https://api.lever.co/v0/postings/{company}"
    data = safe_get_json(url, params={"mode": "json"})

    jobs = []
    for item in data:
        categories = item.get("categories") or {}
        jobs.append({
            "source_name": source["name"],
            "source_type": "lever",
            "external_id": str(item.get("id")),
            "title": item.get("text", ""),
            "location": categories.get("location", ""),
            "department": categories.get("team", ""),
            "url": item.get("hostedUrl") or item.get("applyUrl") or "",
            "posted_at": str(item.get("createdAt", "")),
        })
    return jobs


def fetch_ashby(source: dict) -> List[dict]:
    url = source.get("api_url", "https://api.ashbyhq.com/jobPosting.list")
    body = {
        "organizationHostedJobsPageName": source["organization_key"],
        "listedOnly": True,
    }
    data = safe_post_json(url, body)

    jobs = []
    for item in data.get("results", []):
        location = ""
        loc = item.get("location")
        if isinstance(loc, dict):
            location = (
                loc.get("locationSummary")
                or loc.get("city")
                or loc.get("region")
                or loc.get("country")
                or ""
            )

        department = ""
        departments = item.get("department") or item.get("departments") or []
        if isinstance(departments, list):
            if departments and isinstance(departments[0], dict):
                department = ", ".join(d.get("name", "") for d in departments if d.get("name"))
            else:
                department = ", ".join(str(x) for x in departments if x)
        elif isinstance(departments, dict):
            department = departments.get("name", "")

        jobs.append({
            "source_name": source["name"],
            "source_type": "ashby",
            "external_id": str(item.get("id") or item.get("jobPostingId") or item.get("title", "")),
            "title": item.get("title", ""),
            "location": location,
            "department": department,
            "url": item.get("jobUrl") or item.get("applicationUrl") or "",
            "posted_at": item.get("publishedAt", ""),
        })
    return jobs


def extract_json_object(text: str, marker: str) -> Optional[dict]:
    start = text.find(marker)
    if start == -1:
        return None

    start = text.find("{", start)
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start:i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    return None
    return None


def fetch_phenom_embedded(source: dict) -> List[dict]:
    url = source["url"]
    resp = safe_get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
        },
    )

    ddo = extract_json_object(resp.text, "phApp.ddo =")
    if not ddo:
        raise ValueError("Could not locate phApp.ddo JSON in page source")

    jobs = (
        ddo.get("eagerLoadRefineSearch", {})
           .get("data", {})
           .get("jobs", [])
    )

    out = []
    for item in jobs:
        title = item.get("title", "")
        location = (
            item.get("location")
            or item.get("cityStateCountry")
            or item.get("locationName")
            or ""
        )
        department = item.get("category", "")
        external_id = item.get("jobId") or item.get("jobSeqNo") or title

        apply_url = item.get("applyUrl", "")
        if source.get("strip_apply_suffix", True) and apply_url.endswith("/apply"):
            job_url = apply_url[:-6]
        else:
            job_url = apply_url

        out.append({
            "source_name": source["name"],
            "source_type": "phenom_embedded",
            "external_id": str(external_id),
            "title": title,
            "location": location,
            "department": department,
            "url": job_url,
            "posted_at": item.get("postedDate", "") or item.get("dateCreated", ""),
        })

    return out

def parse_workday_source(source: dict):
    url = source["url"].rstrip("/")
    parsed = urlparse(url)

    base_url = f"{parsed.scheme}://{parsed.netloc}"
    tenant = parsed.netloc.split(".")[0]

    path = parsed.path.strip("/")
    parts = path.split("/") if path else []

    if not parts:
        raise ValueError(f"Invalid Workday URL: {url}")

    if "recruiting" in parts:
        idx = parts.index("recruiting")
        public_base_path = "/".join(parts)
        api_site_path = "/".join(parts[idx:])
        return base_url, tenant, public_base_path, api_site_path

    public_base_path = "/".join(parts)
    api_site_path = parts[-1]
    return base_url, tenant, public_base_path, api_site_path


def workday_extract_location(item: dict) -> str:
    locations = item.get("locationsText")
    if locations:
        return str(locations)

    locations = item.get("locations")
    if isinstance(locations, list) and locations:
        values = []
        for loc in locations:
            if isinstance(loc, dict):
                text = loc.get("displayName") or loc.get("name") or loc.get("value")
                if text:
                    values.append(str(text))
            elif loc:
                values.append(str(loc))
        if values:
            return ", ".join(values)

    bullet_fields = item.get("bulletFields") or []
    for field in bullet_fields:
        if isinstance(field, str) and "," in field:
            return field

    return ""


def workday_extract_posted(item: dict) -> str:
    for key in ("postedOn", "postedDate", "startDate", "timePosted", "publicationDate"):
        value = item.get(key)
        if value:
            return str(value)

    bullet_fields = item.get("bulletFields") or []
    for field in bullet_fields:
        if isinstance(field, str) and re.search(r"\b(day|days|hour|hours|week|weeks|month|months)\b", field, re.I):
            return field

    return ""


def normalize_text(s: str) -> str:
    return " ".join((s or "").split()).strip()


def extract_wells_fargo_req_id(text: str, url: str) -> str:
    patterns = [
        r"\bR[- ]?\d+\b",
        r"\bReq(?:uisition)?[: ]+([A-Za-z0-9-]+)\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return m.group(0).strip()

    tail = url.rstrip("/").split("/")[-1]
    return tail or text[:80]


def parse_wells_fargo_job_detail(job_url: str, session: requests.Session) -> Dict[str, str]:
    details = {
        "location": "",
        "department": "",
        "posted_at": "",
        "external_id": "",
    }

    try:
        resp = session.get(job_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(" ", strip=True)

        details["external_id"] = extract_wells_fargo_req_id(text, job_url)

        for sel in ['[data-automation-id="locations"]', '[data-automation-id="location"]']:
            node = soup.select_one(sel)
            if node:
                details["location"] = normalize_text(node.get_text(" ", strip=True))
                break

        for sel in [
            '[data-automation-id="postedOn"]',
            '[data-automation-id="timePosted"]',
            '[data-automation-id="jobPostingHeader"]',
        ]:
            node = soup.select_one(sel)
            if node:
                txt = normalize_text(node.get_text(" ", strip=True))
                if re.search(r"\b(day|days|hour|hours|week|weeks|month|months|posted)\b", txt, re.I):
                    details["posted_at"] = txt
                    break

        dept_labels = ["job family", "job category", "department", "business division"]
        for label in dept_labels:
            m = re.search(rf"{re.escape(label)}\s*[:\-]?\s*([A-Za-z0-9 ,&/\-]+)", text, re.I)
            if m:
                details["department"] = normalize_text(m.group(1))
                break

    except Exception:
        pass

    return details


def fetch_wells_fargo_workday(source: dict) -> List[dict]:
    """
    Wells Fargo-specific Workday fetcher using Playwright.
    Loads all visible job cards by repeatedly clicking pagination / load-more controls.
    """
    base_url = source["url"].rstrip("/")
    enrich_details = bool(source.get("enrich_details", True))
    max_rounds = int(source.get("max_rounds", 100))  # safety cap only

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    })

    jobs: List[dict] = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(base_url, wait_until="networkidle", timeout=90000)
        page.wait_for_timeout(5000)

        rounds = 0
        previous_seen_count = -1

        while rounds < max_rounds:
            rounds += 1
            page.wait_for_timeout(2500)

            anchors = page.locator("a").all()

            for a in anchors:
                try:
                    href = (a.get_attribute("href") or "").strip()
                    title = normalize_text(a.inner_text())
                except Exception:
                    continue

                if not href or "/job/" not in href:
                    continue
                if not title:
                    continue

                job_url = urljoin(base_url + "/", href)

                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)

                location = ""
                department = ""
                posted_at = ""
                external_id = job_url.rstrip("/").split("/")[-1]

                if enrich_details:
                    details = parse_wells_fargo_job_detail(job_url, session)
                    location = details.get("location", "")
                    department = details.get("department", "")
                    posted_at = details.get("posted_at", "")
                    external_id = details.get("external_id") or external_id

                jobs.append({
                    "source_name": source["name"],
                    "source_type": "workday",
                    "external_id": str(external_id),
                    "title": title,
                    "location": location,
                    "department": department,
                    "url": job_url,
                    "posted_at": posted_at,
                })

            # If no new jobs appeared since last round, try to paginate/load more once more.
            current_seen_count = len(seen_urls)

            next_button = None
            next_selectors = [
                'button[aria-label*="Next"]',
                'button[aria-label*="next"]',
                'button[data-automation-id="pagination-next"]',
                'button:has-text("Next")',
                'button:has-text("Load More")',
                'button:has-text("Load more")',
                'button:has-text("Show More")',
                'button:has-text("Show more")',
                'a[aria-label*="Next"]',
                'a:has-text("Next")',
                'a:has-text("Load More")',
                'a:has-text("Load more")',
            ]

            for selector in next_selectors:
                loc = page.locator(selector)
                if loc.count() > 0:
                    candidate = loc.first
                    try:
                        if candidate.is_visible():
                            next_button = candidate
                            break
                    except Exception:
                        continue

            if next_button is None:
                # no way to advance, so we're done
                break

            try:
                disabled = False
                try:
                    disabled = next_button.is_disabled()
                except Exception:
                    disabled_attr = next_button.get_attribute("disabled")
                    aria_disabled = next_button.get_attribute("aria-disabled")
                    disabled = (disabled_attr is not None) or (aria_disabled == "true")

                if disabled:
                    break

                next_button.click()
                page.wait_for_load_state("networkidle", timeout=60000)
                page.wait_for_timeout(3000)

            except Exception:
                break

            # If clicking didn't reveal anything new after a full round, stop.
            if current_seen_count == previous_seen_count:
                break

            previous_seen_count = current_seen_count

        browser.close()

    return jobs


def fetch_workday(source: dict) -> List[dict]:
    url = (source.get("url") or "").lower()

    if "wd1.myworkdaysite.com" in url and "wellsfargojobs" in url:
        return fetch_wells_fargo_workday(source)

    base_url, tenant, public_base_path, api_site_path = parse_workday_source(source)
    endpoint = f"{base_url}/wday/cxs/{tenant}/{api_site_path}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }

    limit = int(source.get("limit", 20))
    offset = 0
    jobs: List[dict] = []

    while True:
        body = {
            "limit": limit,
            "offset": offset,
            "searchText": source.get("search_text", ""),
        }

        data = safe_post_json(endpoint, body, headers=headers)

        postings = (
            data.get("jobPostings")
            or data.get("job_postings")
            or data.get("jobs")
            or []
        )

        if not postings:
            break

        for item in postings:
            title = item.get("title", "")
            external_path = (item.get("externalPath") or "").strip()

            if external_path:
                external_path = external_path.lstrip("/")
                job_url = f"{base_url}/{public_base_path}/job/{external_path}"
            else:
                job_url = source.get("url", "")

            department = ""
            if item.get("jobFamily"):
                department = str(item.get("jobFamily"))
            elif item.get("jobFamilyGroup"):
                department = str(item.get("jobFamilyGroup"))

            bullet_fields = item.get("bulletFields") or []
            external_id = (
                (bullet_fields[-1] if bullet_fields else None)
                or item.get("jobReqId")
                or item.get("id")
                or item.get("title")
            )

            jobs.append({
                "source_name": source["name"],
                "source_type": "workday",
                "external_id": str(external_id),
                "title": title,
                "location": workday_extract_location(item),
                "department": department,
                "url": job_url,
                "posted_at": workday_extract_posted(item),
            })

        total = data.get("total")
        offset += len(postings)

        if total is not None and offset >= total:
            break
        if len(postings) < limit:
            break

        time.sleep(0.2)

    return jobs


def entertime_extract_list(data: dict) -> List[dict]:
    for key in ("job_requisitions", "items", "data", "results", "jobs", "requisitions", "jobRequisitions"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    if isinstance(data, list):
        return data
    return []


def entertime_pick(item: dict, keys: List[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value:
            return str(value)
    return ""


def entertime_location(item: dict) -> str:
    location = item.get("location")
    if isinstance(location, dict):
        parts = [
            location.get("city"),
            location.get("state"),
            location.get("country"),
        ]
        parts = [str(x).strip() for x in parts if x]
        if parts:
            return ", ".join(parts)

        line_parts = [
            location.get("address_line_1"),
            location.get("city"),
            location.get("state"),
            location.get("zip"),
            location.get("country"),
        ]
        line_parts = [str(x).strip() for x in line_parts if x]
        if line_parts:
            return ", ".join(line_parts)

    return entertime_pick(item, ["locationName", "jobLocation", "cityState", "location"])


def fetch_entertime(source: dict) -> List[dict]:
    base_url = source["base_url"].rstrip("/")
    company_id = source["company_id"]
    lang = source.get("lang", "en-US")
    size = int(source.get("size", 20))
    sort = source.get("sort", "desc")

    endpoint = f"{base_url}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
        "Referer": f"{base_url}/ta/{company_id}.careers?CareersSearch=&lang={lang}",
    }

    offset = 0
    jobs = []

    while True:
        params = {
            "_": str(int(time.time() * 1000)),
            "offset": offset,
            "size": size,
            "lang": lang,
        }

        ein_id = source.get("ein_id")
        if ein_id is not None:
            params["ein_id"] = ein_id

        if sort:
            params["sort"] = sort

        data = safe_get_json(endpoint, params=params, headers=headers)
        items = entertime_extract_list(data)

        if not items:
            break

        for item in items:
            title = entertime_pick(item, ["job_title", "title", "jobTitle", "requisitionTitle", "name"])
            location = entertime_location(item)
            department = ""

            employee_type = item.get("employee_type")
            if isinstance(employee_type, dict):
                department = employee_type.get("name", "")

            req_id = entertime_pick(item, ["id", "jobId", "requisitionId", "jobReqId", "reqId"])
            external_id = req_id or title

            if req_id:
                detail_url = f"{base_url}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions/{req_id}"
            else:
                detail_url = f"{base_url}/ta/{company_id}.careers?CareersSearch=&lang={quote(lang)}"

            posted_at = entertime_pick(item, ["postedDate", "datePosted", "createdDate", "updateDate"])

            jobs.append({
                "source_name": source["name"],
                "source_type": "entertime",
                "external_id": str(external_id),
                "title": title,
                "location": location,
                "department": department,
                "url": detail_url,
                "posted_at": posted_at,
            })

        if len(items) < size:
            break

        offset += size
        time.sleep(0.2)

    return jobs


def strip_html_tags(value: str) -> str:
    value = re.sub(r"<script[\s\S]*?</script>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"<style[\s\S]*?</style>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def extract_between(text: str, start_pattern: str, end_pattern: str) -> List[str]:
    pattern = re.compile(start_pattern + r"([\s\S]*?)" + end_pattern, re.IGNORECASE)
    return [m.group(1) for m in pattern.finditer(text)]


def first_match(text: str, patterns: List[str]) -> str:
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return strip_html_tags(m.group(1))
    return ""


def fetch_custom_html(source: dict) -> List[dict]:
    url = source["url"]
    resp = safe_get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
        },
    )
    html = resp.text

    site = source.get("site", "").lower()
    if site == "petco":
        return fetch_petco_html(source, html)

    raise ValueError(f"Unsupported custom_html site: {site}")


def fetch_petco_html(source: dict, html: str) -> List[dict]:
    jobs = []

    blocks = extract_between(
        html,
        r'<section[^>]*class="[^"]*jobs-list-item[^"]*"[^>]*>',
        r'</section>'
    )

    if not blocks:
        blocks = extract_between(
            html,
            r'<div[^>]*class="[^"]*jobs-list-item[^"]*"[^>]*>',
            r'</div>\s*</div>'
        )

    for block in blocks:
        title = first_match(block, [
            r'<a[^>]*class="[^"]*job-title[^"]*"[^>]*>(.*?)</a>',
            r'<h2[^>]*>(.*?)</h2>',
            r'<h3[^>]*>(.*?)</h3>',
        ])

        job_url = first_match(block, [
            r'<a[^>]+href="([^"]+)"[^>]*class="[^"]*job-title[^"]*"',
            r'<a[^>]+href="([^"]+)"[^>]*>',
        ])

        location = first_match(block, [
            r'<span[^>]*class="[^"]*job-location[^"]*"[^>]*>(.*?)</span>',
            r'<li[^>]*class="[^"]*job-location[^"]*"[^>]*>(.*?)</li>',
        ])

        department = first_match(block, [
            r'<span[^>]*class="[^"]*job-category[^"]*"[^>]*>(.*?)</span>',
            r'<li[^>]*class="[^"]*job-category[^"]*"[^>]*>(.*?)</li>',
        ])

        req_id = first_match(block, [
            r'Job\s*ID[:\s#-]*([A-Za-z0-9_-]+)',
            r'Req(?:uisition)?\s*ID[:\s#-]*([A-Za-z0-9_-]+)',
        ])

        if not title and not job_url:
            continue

        if job_url and job_url.startswith("/"):
            parsed = urlparse(source["url"])
            job_url = f"{parsed.scheme}://{parsed.netloc}{job_url}"

        external_id = req_id or job_url or title

        jobs.append({
            "source_name": source["name"],
            "source_type": "custom_html",
            "external_id": str(external_id),
            "title": title,
            "location": location,
            "department": department,
            "url": job_url or source["url"],
            "posted_at": "",
        })

    if not jobs:
        raise ValueError("Could not parse Petco jobs from HTML page")

    return jobs


def fetch_jobs_for_source(source: dict) -> List[dict]:
    stype = source["type"].lower()
    if stype == "greenhouse":
        return fetch_greenhouse(source)
    if stype == "lever":
        return fetch_lever(source)
    if stype == "ashby":
        return fetch_ashby(source)
    if stype == "phenom_embedded":
        return fetch_phenom_embedded(source)
    if stype == "workday":
        return fetch_workday(source)
    if stype == "entertime":
        return fetch_entertime(source)
    if stype == "custom_html":
        return fetch_custom_html(source)
    raise ValueError(f"Unsupported source type: {stype}")


def stable_job_key(job: dict) -> str:
    return "||".join([
        job.get("source_type", ""),
        job.get("source_name", ""),
        job.get("external_id", ""),
        job.get("url", ""),
    ])


def format_discord_text(new_jobs: List[dict]) -> str:
    lines = [f"{len(new_jobs)} new matching job(s) found:"]
    for job in new_jobs:
        lines.append(
            f"- {job['title']} | {job['source_name']} | {job.get('location', '')} | {job['url']}"
        )

    text = "\n".join(lines)
    if len(text) <= 1900:
        return text

    trimmed = [f"{len(new_jobs)} new matching job(s) found:"]
    count = 0

    for job in new_jobs:
        line = f"- {job['title']} | {job['source_name']} | {job.get('location', '')} | {job['url']}"
        candidate = "\n".join(trimmed + [line])
        if len(candidate) > 1900:
            break
        trimmed.append(line)
        count += 1

    remaining = len(new_jobs) - count
    if remaining > 0:
        trimmed.append(f"...and {remaining} more.")

    return "\n".join(trimmed)


def send_discord(webhook_url: str, text: str) -> bool:
    last_error = None

    for attempt in range(3):
        try:
            resp = requests.post(
                webhook_url,
                json={"content": text},
                timeout=REQUEST_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )

            if 200 <= resp.status_code < 300:
                return True

            if resp.status_code in (429, 500, 502, 503, 504):
                last_error = f"{resp.status_code} {resp.text[:200]}"
                time.sleep(2 * (attempt + 1))
                continue

            resp.raise_for_status()

        except requests.RequestException as e:
            last_error = str(e)
            time.sleep(2 * (attempt + 1))

    print(f"ERROR - Discord webhook failed: {last_error}", file=sys.stderr)
    return False


def main() -> int:
    config = load_json(CONFIG_PATH)
    state = load_json(STATE_PATH)

    filters = config.get("filters", {})
    sources = config.get("sources", [])
    seen_keys = set(state.get("seen_keys", []))

    all_jobs = []
    errors = []

    for source in sources:
        try:
            jobs = fetch_jobs_for_source(source)
            all_jobs.extend(jobs)
            print(f"{source.get('name', 'unknown source')}: fetched {len(jobs)} job(s)")
            time.sleep(0.5)
        except Exception as e:
            err = f"{source.get('name', 'unknown source')}: {e}"
            errors.append(err)
            print(f"ERROR - {err}", file=sys.stderr)

    matching_jobs = [job for job in all_jobs if matches_filters(job, filters)]

    new_jobs = []
    new_keys = []
    for job in matching_jobs:
        key = stable_job_key(job)
        if key not in seen_keys:
            new_jobs.append(job)
            new_keys.append(key)

    print(f"Fetched total jobs: {len(all_jobs)}")
    print(f"Matching jobs: {len(matching_jobs)}")
    print(f"New jobs: {len(new_jobs)}")

    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    delivered = False

    if new_jobs and webhook:
        delivered = send_discord(webhook, format_discord_text(new_jobs))
        if delivered:
            print("Discord alert sent.")
        else:
            print("New jobs found, but Discord webhook delivery failed.", file=sys.stderr)
    elif new_jobs:
        print("New jobs found, but DISCORD_WEBHOOK_URL is not configured.")
    else:
        print("No new matching jobs to send.")

    if not new_jobs or delivered or not webhook:
        for key in new_keys:
            seen_keys.add(key)

    state["seen_keys"] = sorted(seen_keys)
    save_json(STATE_PATH, state)

    if errors:
        print("Completed with source errors:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())