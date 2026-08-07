"""Generates the read-only JSON snapshots consumed by the Next.js dashboard.

This module never touches state_seen.json or the scraping/filtering logic in
watcher.py. It only reads what watcher.py already computed each run and
writes small, bounded JSON files under dashboard_data/. Any failure here is
caught by the caller in watcher.py so it can never break the core bot.
"""
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

DASHBOARD_DIR = "dashboard_data"
OVERVIEW_PATH = os.path.join(DASHBOARD_DIR, "overview.json")
JOBS_PATH = os.path.join(DASHBOARD_DIR, "jobs.json")
SOURCES_PATH = os.path.join(DASHBOARD_DIR, "sources.json")
ACTIVITY_PATH = os.path.join(DASHBOARD_DIR, "activity.json")

JOBS_RETENTION_DAYS = 90
JOBS_MAX_COUNT = 2000
ACTIVITY_MAX_RUNS = 200
ERROR_MESSAGE_MAX_LEN = 300

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FORMAT)


def _parse_iso(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _atomic_write_json(path: str, data) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-dashboard-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _load_json_list(path: str) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    return data if isinstance(data, list) else []


def _truncate_error(exc: BaseException) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    text = " ".join(text.split())
    if len(text) > ERROR_MESSAGE_MAX_LEN:
        text = text[:ERROR_MESSAGE_MAX_LEN].rstrip() + "..."
    return text


# fetch_results: name -> (jobs_or_None, exception_or_None, duration_seconds)
FetchResults = Dict[str, Tuple[Optional[List[dict]], Optional[BaseException], float]]


def build_sources_snapshot(
    sources: Iterable[dict],
    fetch_results: FetchResults,
    matched_counts: Dict[str, int],
    now: str,
) -> List[dict]:
    snapshot = []
    for source in sources:
        name = source.get("name", "unknown source")
        result = fetch_results.get(name)
        jobs, exc, duration = result if result else (None, None, 0.0)

        if exc is not None:
            status = "failed"
            error = _truncate_error(exc)
            jobs_fetched = 0
        else:
            jobs_fetched = len(jobs or [])
            error = None
            status = "healthy" if jobs_fetched > 0 else "warning"

        snapshot.append({
            "name": name,
            "type": source.get("type", ""),
            "status": status,
            "jobs_fetched": jobs_fetched,
            "jobs_matched": matched_counts.get(name, 0),
            "last_scan": now,
            "duration_ms": int(round((duration or 0.0) * 1000)),
            "error": error,
        })
    return snapshot


def update_jobs_store(
    existing_jobs: List[dict],
    matching_jobs: List[dict],
    new_keys: Set[str],
    key_fn: Callable[[dict], str],
    now: str,
) -> List[dict]:
    by_key: Dict[str, dict] = {
        item["key"]: item
        for item in existing_jobs
        if isinstance(item, dict) and item.get("key")
    }

    for job in matching_jobs:
        key = key_fn(job)
        existing = by_key.get(key)
        first_seen = existing["first_seen"] if existing else now

        by_key[key] = {
            "key": key,
            "title": job.get("title", ""),
            "company": job.get("source_name", ""),
            "location": job.get("location", ""),
            "department": job.get("department", ""),
            "source_name": job.get("source_name", ""),
            "source_type": job.get("source_type", ""),
            "url": job.get("url", ""),
            "external_id": job.get("external_id", ""),
            "matched_keywords": job.get("matched_keywords", []),
            "first_seen": first_seen,
            "last_seen": now,
            "is_new": key in new_keys,
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=JOBS_RETENTION_DAYS)
    items = []
    for item in by_key.values():
        last_seen = _parse_iso(item.get("last_seen")) or datetime.now(timezone.utc)
        if last_seen >= cutoff:
            items.append(item)

    items.sort(key=lambda item: item.get("last_seen", ""), reverse=True)
    return items[:JOBS_MAX_COUNT]


def append_activity(existing_runs: List[dict], run_entry: dict) -> List[dict]:
    runs = [r for r in existing_runs if isinstance(r, dict)] + [run_entry]
    return runs[-ACTIVITY_MAX_RUNS:]


def build_overview(
    now: str,
    scan_status: str,
    jobs_fetched_last_scan: int,
    jobs_matched_last_scan: int,
    new_jobs_last_scan: int,
    jobs_store: List[dict],
    sources_snapshot: List[dict],
) -> dict:
    today = datetime.now(timezone.utc).date()
    week_cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    jobs_today = 0
    jobs_last_7_days = 0
    for item in jobs_store:
        first_seen = _parse_iso(item.get("first_seen"))
        if first_seen is None:
            continue
        if first_seen.date() == today:
            jobs_today += 1
        if first_seen >= week_cutoff:
            jobs_last_7_days += 1

    healthy_sources = sum(1 for s in sources_snapshot if s["status"] == "healthy")
    failed_sources = sum(1 for s in sources_snapshot if s["status"] == "failed")

    return {
        "last_scan": now,
        "scan_status": scan_status,
        "jobs_fetched_last_scan": jobs_fetched_last_scan,
        "jobs_matched_last_scan": jobs_matched_last_scan,
        "new_jobs_last_scan": new_jobs_last_scan,
        "jobs_today": jobs_today,
        "jobs_last_7_days": jobs_last_7_days,
        "total_sources": len(sources_snapshot),
        "healthy_sources": healthy_sources,
        "failed_sources": failed_sources,
    }


def export_dashboard_data(
    *,
    sources: List[dict],
    fetch_results: FetchResults,
    matching_jobs: List[dict],
    new_keys: Set[str],
    key_fn: Callable[[dict], str],
    jobs_fetched_total: int,
    scan_duration_seconds: float,
    delivered: bool,
) -> None:
    now = now_iso()

    matched_counts: Dict[str, int] = {}
    for job in matching_jobs:
        source_name = job.get("source_name", "")
        matched_counts[source_name] = matched_counts.get(source_name, 0) + 1

    sources_snapshot = build_sources_snapshot(sources, fetch_results, matched_counts, now)

    existing_jobs = _load_json_list(JOBS_PATH)
    jobs_store = update_jobs_store(existing_jobs, matching_jobs, new_keys, key_fn, now)

    sources_total = len(sources_snapshot)
    sources_failed = sum(1 for s in sources_snapshot if s["status"] == "failed")
    sources_succeeded = sources_total - sources_failed

    if sources_total > 0 and sources_succeeded == 0:
        scan_status = "failed"
    elif sources_failed > 0:
        scan_status = "partial"
    else:
        scan_status = "success"

    jobs_matched_total = len(matching_jobs)
    new_jobs_total = len(new_keys)
    alerts_sent = new_jobs_total if delivered else 0

    run_entry = {
        "timestamp": now,
        "status": scan_status,
        "duration_seconds": round(scan_duration_seconds, 2),
        "sources_total": sources_total,
        "sources_succeeded": sources_succeeded,
        "sources_failed": sources_failed,
        "jobs_fetched": jobs_fetched_total,
        "jobs_matched": jobs_matched_total,
        "new_jobs": new_jobs_total,
        "alerts_sent": alerts_sent,
        "sources": [
            {
                "name": s["name"],
                "status": s["status"],
                "jobs_fetched": s["jobs_fetched"],
                "jobs_matched": s["jobs_matched"],
                "error": s["error"],
            }
            for s in sources_snapshot
        ],
    }

    existing_activity = _load_json_list(ACTIVITY_PATH)
    activity = append_activity(existing_activity, run_entry)

    overview = build_overview(
        now,
        scan_status,
        jobs_fetched_total,
        jobs_matched_total,
        new_jobs_total,
        jobs_store,
        sources_snapshot,
    )

    _atomic_write_json(SOURCES_PATH, sources_snapshot)
    _atomic_write_json(JOBS_PATH, jobs_store)
    _atomic_write_json(ACTIVITY_PATH, activity)
    _atomic_write_json(OVERVIEW_PATH, overview)
