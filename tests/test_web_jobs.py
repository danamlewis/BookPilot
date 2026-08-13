import io
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.models import Author, AuthorCatalogBook, Book, Recommendation, SystemMetadata, get_session, init_db
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
        self.original_series_job_manager = web_app.SERIES_RECONCILIATION_JOB_MANAGER
        self.original_enrichment_job_manager = web_app.SERIES_ENRICHMENT_JOB_MANAGER
        self.original_hardcover_token = web_app.app.config.get("HARDCOVER_API_TOKEN")
        web_app.DB_PATH = Path(self.temp_directory.name) / "bookpilot.db"
        web_app.JOB_MANAGER = JobManager()
        web_app.SERIES_RECONCILIATION_JOB_MANAGER = JobManager()
        web_app.SERIES_ENRICHMENT_JOB_MANAGER = JobManager()
        web_app.app.config["HARDCOVER_API_TOKEN"] = ""
        web_app.app.config.update(TESTING=True)
        self.client = web_app.app.test_client()

    def tearDown(self):
        web_app.JOB_MANAGER.wait(2)
        web_app.SERIES_RECONCILIATION_JOB_MANAGER.wait(2)
        web_app.SERIES_ENRICHMENT_JOB_MANAGER.wait(2)
        web_app.DB_PATH = self.original_db_path
        web_app.JOB_MANAGER = self.original_job_manager
        web_app.SERIES_RECONCILIATION_JOB_MANAGER = self.original_series_job_manager
        web_app.SERIES_ENRICHMENT_JOB_MANAGER = self.original_enrichment_job_manager
        web_app.app.config["HARDCOVER_API_TOKEN"] = self.original_hardcover_token
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
        self.assertNotIn('data-tab="series-review"', page)
        self.assertIn('id="seriesReconciliationTitle"', page)
        self.assertIn('id="seriesViewTabs"', page)
        self.assertIn('data-series-view="progress"', page)
        self.assertIn('data-series-view="reconciliation"', page)
        self.assertIn("function showSeriesView", page)
        self.assertIn("loadActiveSeriesView();", page)
        self.assertIn('id="runSeriesReconciliation"', page)
        self.assertIn('id="seriesReviewContent"', page)
        self.assertIn("function loadSeriesReview()", page)
        self.assertIn("function readingProgressBookHTML", page)
        self.assertIn('reading-progress-series', page)
        self.assertIn("books.some(book => book.status === 'recommendation')", page)
        self.assertIn('class="official-recommendation-action official-pass"', page)
        self.assertIn('class="official-recommendation-action official-duplicate"', page)
        self.assertLess(page.index('class="official-recommendation-action official-duplicate"'), page.index('class="official-mark-read"'))
        self.assertIn('class="action-btn duplicate-btn"', page)
        self.assertIn('class="official-ignore-series progress-ignore-series"', page)
        self.assertIn("Mark matched recommendations read", page)
        self.assertIn('class="official-mark-series-read"', page)
        self.assertIn("Ignore for now", page)
        self.assertIn("Ignored series", page)
        self.assertIn("function installIgnoredSeriesControls", page)
        self.assertIn("pollSeriesReconciliationJob();", page)
        self.assertIn('id="runSeriesEnrichment"', page)
        self.assertIn("function renderSeriesEnrichmentStatus", page)
        self.assertIn("pollSeriesEnrichmentJob();", page)
        self.assertIn("Update Hardcover catalog", page)
        self.assertIn("language and duplicate cleanup", page)
        self.assertIn('class="reading-progress-actions hardcover-only-actions"', page)
        self.assertIn('data-action="non-english"', page)
        self.assertIn("handleNonEnglish('ebook', title, author)", page)
        self.assertIn("data-hardcover-book-id", page)
        self.assertIn("function updateHardcoverSeriesBook", page)
        self.assertIn("/api/series/hardcover-book-action", page)
        self.assertIn("recommendationAction.dataset.hardcoverBookId", page)
        self.assertIn('class="official-recommendation-action official-not-english"', page)
        self.assertIn("updateHardcoverSeriesBook(recommendationAction, 'non_english')", page)
        self.assertIn("function removeReconciliationBookFromClientState", page)
        self.assertIn("if (!isReconciliation)", page)

    def test_series_enrichment_job_requires_token(self):
        response = self.client.post("/api/jobs/series-enrichment", json={})
        self.assertEqual(response.status_code, 400)
        self.assertIn("Hardcover", response.get_json()["error"])

    def test_hardcover_book_action_is_scoped_to_stable_membership_ids(self):
        response = self.client.post(
            "/api/series/hardcover-book-action",
            json={
                "action": "duplicate",
                "series_id": 101,
                "book_id": 202,
                "position": 1,
                "title": "Same Title",
                "author": "Example Author",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["hidden_membership"]["book_id"], 202)

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            row = session.query(SystemMetadata).filter_by(
                key="hardcover_series_book_actions",
            ).one()
            self.assertIn('"book_id":202', row.value)
            self.assertEqual(session.query(Recommendation).count(), 0)
        finally:
            session.close()
            engine.dispose()

    def test_series_enrichment_job_completes_without_calls_when_no_series_exist(self):
        web_app.app.config["HARDCOVER_API_TOKEN"] = "secret"
        response = self.client.post(
            "/api/jobs/series-enrichment", json={"force_refresh": False},
        )
        self.assertEqual(response.status_code, 202)
        web_app.SERIES_ENRICHMENT_JOB_MANAGER.wait(3)
        job = web_app.SERIES_ENRICHMENT_JOB_MANAGER.snapshot()
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["result"]["api_calls"], 0)

    def test_series_reconciliation_status_reports_eligibility_and_configuration(self):
        from src.series_review import ReviewCandidate
        candidates = [
            ReviewCandidate("Example Author", f"Book {number}", {"ebook"}, None)
            for number in range(6)
        ]
        web_app.app.config["HARDCOVER_API_TOKEN"] = "secret"
        with patch.object(web_app, "load_visible_recommendation_candidates", return_value=candidates):
            response = self.client.get("/api/series-reconciliation")

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertTrue(data["configured"])
        self.assertEqual(data["threshold"], 5)
        self.assertEqual(data["eligible_authors"], [{"author": "Example Author", "recommendations": 6}])
        self.assertIsNone(data["result"])

    def test_series_reconciliation_job_requires_server_side_token(self):
        response = self.client.post("/api/jobs/series-reconciliation", json={"batch_size": 10})
        self.assertEqual(response.status_code, 400)
        self.assertIn("HARDCOVER_API_TOKEN", response.get_json()["error"])

    def test_series_can_be_ignored_and_restored(self):
        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            web_app.save_reconciliation_result(session, {
                "authors": [{
                    "author": "Example Author",
                    "series": [{
                        "name": "Example Series",
                        "hardcover_series_id": 101,
                        "recommended_matches": 1,
                        "books": [{
                            "book": "Example Book", "local_title": "Example Book",
                            "status": "recommendation", "formats": ["ebook"],
                        }],
                    }],
                }],
            })
        finally:
            session.close()
            engine.dispose()

        ignored = self.client.post(
            "/api/series-reconciliation/ignored",
            json={
                "action": "ignore",
                "series_id": 101,
                "name": "Example Series",
                "author": "Example Author",
            },
        )
        self.assertEqual(ignored.status_code, 200)
        self.assertEqual(ignored.get_json()["ignored_series"][0]["series_id"], 101)
        self.assertEqual(ignored.get_json()["passed_count"], 1)

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            passed = session.query(Recommendation).order_by(Recommendation.format).all()
            self.assertEqual([row.format for row in passed], ["audiobook", "ebook"])
            self.assertTrue(all(row.thumbs_down and not row.thumbs_up for row in passed))
        finally:
            session.close()
            engine.dispose()

        restored = self.client.post(
            "/api/series-reconciliation/ignored",
            json={"action": "restore", "series_id": 101},
        )
        self.assertEqual(restored.status_code, 200)
        self.assertEqual(restored.get_json()["ignored_series"], [])

    def test_progress_series_can_be_ignored_and_restored(self):
        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            author = Author(name="Local Author", normalized_name="Local Author")
            session.add(author)
            session.flush()
            session.add(AuthorCatalogBook(
                author_id=author.id, title="Local Book", series_name="Local Series",
                series_position=1, is_read=False, format_available="both",
            ))
            session.commit()
        finally:
            session.close()
            engine.dispose()

        ignored = self.client.post(
            "/api/series/ignored",
            json={"action": "ignore", "name": "Local Series", "author": "Local Author"},
        )
        self.assertEqual(ignored.status_code, 200)
        self.assertEqual(ignored.get_json()["ignored_series"][0]["name"], "Local Series")
        self.assertEqual(ignored.get_json()["passed_titles"], ["Local Book"])

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            passed = session.query(Recommendation).order_by(Recommendation.format).all()
            self.assertEqual([row.format for row in passed], ["audiobook", "ebook"])
            self.assertTrue(all(row.thumbs_down for row in passed))
        finally:
            session.close()
            engine.dispose()

        status = self.client.get("/api/series").get_json()
        self.assertEqual(status["ignored_series"][0]["author"], "Local Author")

        restored = self.client.post(
            "/api/series/ignored",
            json={"action": "restore", "name": "Local Series", "author": "Local Author"},
        )
        self.assertEqual(restored.status_code, 200)
        self.assertEqual(restored.get_json()["ignored_series"], [])

    def test_duplicate_feedback_removes_saved_reconciliation_match(self):
        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            web_app.save_reconciliation_result(session, {
                "authors": [{
                    "author": "Example Author",
                    "series": [{
                        "name": "Example Series",
                        "books": [{
                            "book": "Example Book",
                            "local_title": "Example Book",
                            "status": "recommendation",
                            "formats": ["ebook"],
                        }],
                    }],
                }],
            })
        finally:
            session.close()
            engine.dispose()

        response = self.client.post(
            "/api/recommendations/ebook/flag-duplicate",
            json={"title": "Example Book", "author": "Example Author"},
        )
        self.assertEqual(response.status_code, 200)

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            saved = web_app.load_reconciliation_result(session)
            book = saved["authors"][0]["series"][0]["books"][0]
            self.assertEqual(book["status"], "other")
            self.assertEqual(book["formats"], [])
        finally:
            session.close()
            engine.dispose()

    def test_series_review_mark_read_updates_every_recommended_format(self):
        response = self.client.post(
            "/api/series-review/mark-read",
            json={
                "title": "A Previously Read Book",
                "author": "Example Author",
                "formats": ["ebook", "audiobook"],
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["formats_marked"], ["audiobook", "ebook"])

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            recommendations = session.query(Recommendation).order_by(Recommendation.format).all()
            self.assertEqual([row.format for row in recommendations], ["audiobook", "ebook"])
            self.assertTrue(all(row.already_read for row in recommendations))
        finally:
            session.close()
            engine.dispose()

    def test_series_review_mark_read_rejects_unknown_format(self):
        response = self.client.post(
            "/api/series-review/mark-read",
            json={"title": "Book", "author": "Author", "formats": ["print"]},
        )
        self.assertEqual(response.status_code, 400)

    def test_series_review_marks_a_full_series_in_one_request(self):
        response = self.client.post(
            "/api/series-review/mark-read",
            json={
                "author": "Example Author",
                "books": [
                    {"title": "First Book", "formats": ["ebook"]},
                    {"title": "Second Book", "formats": ["ebook", "audiobook"]},
                ],
            },
        )
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["titles_marked"], 2)
        self.assertEqual(result["format_records_marked"], 3)

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            recommendations = session.query(Recommendation).all()
            self.assertEqual(len(recommendations), 3)
            self.assertEqual(
                {(row.title, row.format) for row in recommendations},
                {
                    ("First Book", "ebook"),
                    ("Second Book", "ebook"),
                    ("Second Book", "audiobook"),
                },
            )
            self.assertTrue(all(row.already_read for row in recommendations))
        finally:
            session.close()
            engine.dispose()

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
