import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import dashboard_export as de


def make_source(name, stype="greenhouse"):
    return {"name": name, "type": stype}


def make_job(source_name, external_id, title="Data Analyst"):
    return {
        "source_name": source_name,
        "source_type": "greenhouse",
        "external_id": external_id,
        "title": title,
        "location": "Remote",
        "department": "Data",
        "url": f"https://example.com/{source_name}/{external_id}",
        "posted_at": "",
        "matched_keywords": ["analyst"],
    }


def key_fn(job):
    return "||".join([
        job.get("source_type", ""),
        job.get("source_name", ""),
        job.get("external_id", ""),
        job.get("url", ""),
    ])


class DashboardExportTestCase(unittest.TestCase):
    def setUp(self):
        self._old_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)

    def tearDown(self):
        os.chdir(self._old_cwd)
        self._tmpdir.cleanup()

    def _read(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_produces_valid_json_for_all_four_files(self):
        sources = [make_source("A"), make_source("B")]
        job = make_job("A", "1")
        fetch_results = {
            "A": ([job], None, 0.1),
            "B": ([], None, 0.05),
        }
        de.export_dashboard_data(
            sources=sources,
            fetch_results=fetch_results,
            matching_jobs=[job],
            new_keys={key_fn(job)},
            key_fn=key_fn,
            jobs_fetched_total=1,
            scan_duration_seconds=0.2,
            delivered=True,
        )

        for path in (de.OVERVIEW_PATH, de.JOBS_PATH, de.SOURCES_PATH, de.ACTIVITY_PATH):
            self.assertTrue(os.path.exists(path))
            data = self._read(path)  # raises if not valid JSON
            self.assertIsNotNone(data)

        overview = self._read(de.OVERVIEW_PATH)
        self.assertEqual(overview["scan_status"], "success")
        self.assertEqual(overview["new_jobs_last_scan"], 1)
        self.assertEqual(overview["total_sources"], 2)
        self.assertEqual(overview["healthy_sources"], 1)
        self.assertEqual(overview["failed_sources"], 0)

        jobs = self._read(de.JOBS_PATH)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["key"], key_fn(job))
        self.assertTrue(jobs[0]["is_new"])
        self.assertEqual(jobs[0]["matched_keywords"], ["analyst"])

    def test_handles_zero_matches(self):
        sources = [make_source("A")]
        fetch_results = {"A": ([], None, 0.05)}
        de.export_dashboard_data(
            sources=sources,
            fetch_results=fetch_results,
            matching_jobs=[],
            new_keys=set(),
            key_fn=key_fn,
            jobs_fetched_total=0,
            scan_duration_seconds=0.05,
            delivered=False,
        )

        overview = self._read(de.OVERVIEW_PATH)
        self.assertEqual(overview["new_jobs_last_scan"], 0)
        self.assertEqual(overview["jobs_matched_last_scan"], 0)

        jobs = self._read(de.JOBS_PATH)
        self.assertEqual(jobs, [])

        sources_snapshot = self._read(de.SOURCES_PATH)
        self.assertEqual(sources_snapshot[0]["status"], "warning")

    def test_handles_failed_source_without_stopping_others(self):
        sources = [make_source("Good"), make_source("Bad")]
        good_job = make_job("Good", "1")
        fetch_results = {
            "Good": ([good_job], None, 0.1),
            "Bad": (None, RuntimeError("HTTP 500 boom"), 0.2),
        }
        de.export_dashboard_data(
            sources=sources,
            fetch_results=fetch_results,
            matching_jobs=[good_job],
            new_keys={key_fn(good_job)},
            key_fn=key_fn,
            jobs_fetched_total=1,
            scan_duration_seconds=0.3,
            delivered=False,
        )

        sources_snapshot = self._read(de.SOURCES_PATH)
        by_name = {s["name"]: s for s in sources_snapshot}
        self.assertEqual(by_name["Good"]["status"], "healthy")
        self.assertEqual(by_name["Bad"]["status"], "failed")
        self.assertIn("boom", by_name["Bad"]["error"])
        self.assertNotIn("webhook", by_name["Bad"]["error"].lower())

        overview = self._read(de.OVERVIEW_PATH)
        self.assertEqual(overview["scan_status"], "partial")
        self.assertEqual(overview["healthy_sources"], 1)
        self.assertEqual(overview["failed_sources"], 1)

        activity = self._read(de.ACTIVITY_PATH)
        self.assertEqual(activity[-1]["status"], "partial")
        self.assertEqual(activity[-1]["sources_failed"], 1)

    def test_error_message_is_truncated(self):
        sources = [make_source("Bad")]
        long_error = RuntimeError("x" * 1000)
        fetch_results = {"Bad": (None, long_error, 0.1)}
        de.export_dashboard_data(
            sources=sources,
            fetch_results=fetch_results,
            matching_jobs=[],
            new_keys=set(),
            key_fn=key_fn,
            jobs_fetched_total=0,
            scan_duration_seconds=0.1,
            delivered=False,
        )
        sources_snapshot = self._read(de.SOURCES_PATH)
        self.assertLessEqual(len(sources_snapshot[0]["error"]), de.ERROR_MESSAGE_MAX_LEN + 3)

    def test_activity_history_is_capped(self):
        existing_runs = [{"timestamp": f"run-{i}", "status": "success"} for i in range(de.ACTIVITY_MAX_RUNS)]
        new_run = {"timestamp": "newest", "status": "success"}
        result = de.append_activity(existing_runs, new_run)
        self.assertEqual(len(result), de.ACTIVITY_MAX_RUNS)
        self.assertEqual(result[-1], new_run)
        self.assertEqual(result[0]["timestamp"], "run-1")

    def test_jobs_store_is_capped_at_max_count(self):
        existing_jobs = []
        for i in range(de.JOBS_MAX_COUNT + 50):
            existing_jobs.append({
                "key": f"key-{i}",
                "title": "t",
                "first_seen": de.now_iso(),
                "last_seen": de.now_iso(),
                "is_new": False,
            })
        result = de.update_jobs_store(existing_jobs, [], set(), key_fn, de.now_iso())
        self.assertLessEqual(len(result), de.JOBS_MAX_COUNT)

    def test_jobs_older_than_retention_are_pruned(self):
        from datetime import datetime, timedelta, timezone
        stale_ts = (datetime.now(timezone.utc) - timedelta(days=de.JOBS_RETENTION_DAYS + 5)).strftime(de.ISO_FORMAT)
        existing_jobs = [{
            "key": "stale",
            "title": "old job",
            "first_seen": stale_ts,
            "last_seen": stale_ts,
            "is_new": False,
        }]
        result = de.update_jobs_store(existing_jobs, [], set(), key_fn, de.now_iso())
        self.assertEqual(result, [])

    def test_atomic_write_never_leaves_partial_file_on_failure(self):
        class Unserializable:
            pass

        path = os.path.join(de.DASHBOARD_DIR, "broken.json")
        with self.assertRaises(TypeError):
            de._atomic_write_json(path, {"bad": Unserializable()})
        self.assertFalse(os.path.exists(path))
        leftover_tmp = [f for f in os.listdir(de.DASHBOARD_DIR) if f.startswith(".tmp-dashboard-")]
        self.assertEqual(leftover_tmp, [])


if __name__ == "__main__":
    unittest.main()
