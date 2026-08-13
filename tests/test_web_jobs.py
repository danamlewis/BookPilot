import io
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.models import Book, get_session, init_db
from web.jobs import JobAlreadyRunning, JobManager
import web.app as web_app


class JobManagerTests(unittest.TestCase):
    def test_runs_one_job_and_reports_progress(self):
        manager = JobManager()
        started = threading.Event()
        release = threading.Event()

        def target(progress):
            progress(message="Halfway", current=1, total=2)
            started.set()
            release.wait(2)
            return {"message": "Finished", "value": 42}

        first = manager.start("test", "Test Job", target)
        self.assertTrue(started.wait(1))
        running = manager.snapshot()
        self.assertEqual(running["id"], first["id"])
        self.assertEqual(running["state"], "running")
        self.assertEqual(running["percent"], 50)

        with self.assertRaises(JobAlreadyRunning):
            manager.start("second", "Second Job", lambda _progress: {})

        release.set()
        manager.wait(2)
        finished = manager.snapshot()
        self.assertEqual(finished["state"], "succeeded")
        self.assertEqual(finished["message"], "Finished")
        self.assertEqual(finished["result"]["value"], 42)

    def test_captures_job_failure(self):
        manager = JobManager()

        def target(_progress):
            raise RuntimeError("sample failure")

        manager.start("test", "Test Job", target)
        manager.wait(2)
        job = manager.snapshot()
        self.assertEqual(job["state"], "failed")
        self.assertEqual(job["error"], "sample failure")


class WebJobApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.original_db_path = web_app.DB_PATH
        self.original_job_manager = web_app.JOB_MANAGER
        web_app.DB_PATH = Path(self.temp_directory.name) / "bookpilot.db"
        web_app.JOB_MANAGER = JobManager()
        web_app.app.config.update(TESTING=True)
        self.client = web_app.app.test_client()

    def tearDown(self):
        web_app.JOB_MANAGER.wait(2)
        web_app.DB_PATH = self.original_db_path
        web_app.JOB_MANAGER = self.original_job_manager
        self.temp_directory.cleanup()

    def wait_for_job(self):
        web_app.JOB_MANAGER.wait(3)
        job = web_app.JOB_MANAGER.snapshot()
        self.assertIsNotNone(job)
        self.assertIn(job["state"], {"succeeded", "failed"})
        return job

    def test_status_has_no_job_initially(self):
        response = self.client.get("/api/jobs/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"active": False, "job": None})

    def test_home_page_exposes_update_menu_and_status_row(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        page = response.get_data(as_text=True)
        self.assertIn("Update Library", page)
        self.assertIn("Import CSV", page)
        self.assertIn("Check for New Books", page)
        self.assertIn('id="jobStatusRow"', page)
        self.assertIn('id="libraryResultReview"', page)
        self.assertIn('class="sort-btn active" data-sort="count"', page)
        self.assertRegex(page, r"refreshHeaderStatus\(\);\s*pollLibraryJob\(\);")
        self.assertNotIn("action will be saved when available", page)
        self.assertIn("The failed action was not persisted", page)
        self.assertIn("function suppressRecommendationInClientState", page)
        self.assertEqual(
            page.count("suppressRecommendationInClientState(formatType, title, author);"),
            4,
        )
        self.assertIn("container.rankedHigh = null", page)

    def test_ebook_recommendations_are_generated_once_per_request(self):
        generated = {
            "Fiction": [{
                "catalog_book_id": 1,
                "title": "A New Book",
                "author": "Example Author",
            }],
            "General": [{
                "catalog_book_id": 1,
                "title": "A New Book",
                "author": "Example Author",
            }],
        }
        with patch.object(web_app, "recommend_new_books", return_value=generated) as recommend:
            response = self.client.get("/api/recommendations/ebook")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["total"], 1)
        recommend.assert_called_once()

    def test_imports_uploaded_libby_csv(self):
        csv_bytes = (
            b"title,author,publisher,isbn,timestamp\n"
            b"A Sample Book,Example Author,Sample Publisher,9780000000002,\n"
        )
        response = self.client.post(
            "/api/jobs/import",
            data={"file": (io.BytesIO(csv_bytes), "libby-history.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 202)

        job = self.wait_for_job()
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["result"]["books_added"], 1)
        self.assertEqual(job["result"]["authors_added"], 1)

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            book = session.query(Book).one()
            self.assertEqual(book.title, "A Sample Book")
            self.assertEqual(book.author, "Example Author")
        finally:
            session.close()
            engine.dispose()

    def test_rejects_non_libby_csv(self):
        response = self.client.post(
            "/api/jobs/import",
            data={"file": (io.BytesIO(b"name,value\nsample,1\n"), "other.csv")},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("title and author columns", response.get_json()["error"])
        self.assertIsNone(web_app.JOB_MANAGER.snapshot())

    def test_rejects_non_csv_extension(self):
        response = self.client.post(
            "/api/jobs/import",
            data={"file": (io.BytesIO(b"title,author\nA,B\n"), "history.txt")},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn(".csv", response.get_json()["error"])

    def test_rejects_second_update_while_one_is_running(self):
        started = threading.Event()
        release = threading.Event()

        def blocking_target(_progress):
            started.set()
            release.wait(2)
            return {"message": "Done"}

        web_app.JOB_MANAGER.start("blocking", "Blocking Job", blocking_target)
        self.assertTrue(started.wait(1))
        response = self.client.post("/api/jobs/catalog/recent")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["job"]["action"], "blocking")
        release.set()

    def test_recent_catalog_job_uses_safe_allowlisted_options(self):
        calls = []

        def fake_fetch(_session, **kwargs):
            calls.append(kwargs)
            kwargs["progress_callback"](
                message="Checking Example Author — 1 of 1 authors",
                current=1,
                total=1,
            )
            return {
                "total_authors": 1,
                "catalogs_fetched": 1,
                "catalogs_skipped": 0,
                "total_books_added": 2,
                "total_books_updated": 0,
                "added_books": [
                    {"title": "First New Book", "author": "Example Author"},
                    {"title": "Second New Book", "author": "Example Author"},
                ],
                "errors": ["Missing Author: Author not found in Open Library"],
                "stopped_early": False,
            }

        with patch.object(web_app, "fetch_all_author_catalogs", side_effect=fake_fetch):
            response = self.client.post("/api/jobs/catalog/recent")
            self.assertEqual(response.status_code, 202)
            job = self.wait_for_job()

        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["result"]["total_books_added"], 2)
        self.assertEqual(len(job["result"]["added_books"]), 2)
        self.assertEqual(len(job["result"]["errors"]), 1)
        self.assertEqual(len(calls), 1)
        self.assertFalse(calls[0]["force_refresh"])
        self.assertTrue(calls[0]["only_recent"])
        self.assertEqual(calls[0]["recent_years"], 1)
        self.assertTrue(calls[0]["auto_cleanup"])

    def test_full_catalog_job_forces_complete_refresh(self):
        calls = []

        def fake_fetch(_session, **kwargs):
            calls.append(kwargs)
            return {
                "total_authors": 0,
                "catalogs_fetched": 0,
                "catalogs_skipped": 0,
                "total_books_added": 0,
                "total_books_updated": 0,
                "errors": [],
                "stopped_early": False,
            }

        with patch.object(web_app, "fetch_all_author_catalogs", side_effect=fake_fetch):
            response = self.client.post("/api/jobs/catalog/full")
            self.assertEqual(response.status_code, 202)
            job = self.wait_for_job()

        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0]["force_refresh"])
        self.assertFalse(calls[0]["only_recent"])
        self.assertTrue(calls[0]["auto_cleanup"])


if __name__ == "__main__":
    unittest.main()
