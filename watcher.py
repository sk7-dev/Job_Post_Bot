import json
import os
import sys
import time
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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


def safe_post_json(url: str, json_body: dict, headers: Optional[dict] = None):
    resp = requests.post(url, json=json_body, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


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


def fetch_html_search(source: dict) -> List[dict]:
    url = source["url"]
    resp = safe_get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
        },
    )

    soup = BeautifulSoup(resp.text, "html.parser")
    jobs = []

    link_prefix = source.get("link_prefix", "")
    match_contains = [x.lower() for x in source.get("match_contains", ["/job/", "/jobs/"])]
    seen_urls = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = " ".join(a.get_text(" ", strip=True).split())

        if not href:
            continue

        full_url = urljoin(link_prefix or url, href)
        full_url_lc = full_url.lower()

        if not any(token in full_url_lc for token in match_contains):
            continue

        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        title = text or full_url.rstrip("/").rsplit("/", 1)[-1]
        jobs.append({
            "source_name": source["name"],
            "source_type": "html_search",
            "external_id": full_url,
            "title": title,
            "location": "",
            "department": "",
            "url": full_url,
            "posted_at": "",
        })

    return jobs


def fetch_jobs_for_source(source: dict) -> List[dict]:
    stype = source["type"].lower()
    if stype == "greenhouse":
        return fetch_greenhouse(source)
    if stype == "lever":
        return fetch_lever(source)
    if stype == "ashby":
        return fetch_ashby(source)
    if stype == "html_search":
        return fetch_html_search(source)
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
    for job in new_jobs[:10]:
        lines.append(
            f"- {job['title']} | {job['source_name']} | {job.get('location', '')} | {job['url']}"
        )
    if len(new_jobs) > 10:
        lines.append(f"...and {len(new_jobs) - 10} more.")
    return "\n".join(lines)


def send_discord(webhook_url: str, text: str) -> None:
    resp = requests.post(
        webhook_url,
        json={"content": text},
        timeout=REQUEST_TIMEOUT,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()


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
    for job in matching_jobs:
        key = stable_job_key(job)
        if key not in seen_keys:
            new_jobs.append(job)
            seen_keys.add(key)

    state["seen_keys"] = sorted(seen_keys)
    save_json(STATE_PATH, state)

    print(f"Fetched total jobs: {len(all_jobs)}")
    print(f"Matching jobs: {len(matching_jobs)}")
    print(f"New jobs: {len(new_jobs)}")

    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if new_jobs and webhook:
        send_discord(webhook, format_discord_text(new_jobs))
        print("Discord alert sent.")
    elif new_jobs:
        print("New jobs found, but DISCORD_WEBHOOK_URL is not configured.")
    else:
        print("No new matching jobs to send.")

    if errors:
        print("Completed with source errors:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())