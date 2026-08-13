from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine

from src.models import Author, AuthorCatalogBook, Recommendation, get_session, init_db
import web.app as web_app


class RecommendationFeedbackConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.original_db_path = web_app.DB_PATH
        web_app.DB_PATH = Path(self.temp_directory.name) / "bookpilot.db"
        web_app.app.config.update(TESTING=True)

        # Initialize the expression-based uniqueness index before starting
        # simultaneous requests, without creating the rows they will contend on.
        engine = init_db(str(web_app.DB_PATH))
        engine.dispose()
        self.database_key = str(web_app.DB_PATH.resolve())
        web_app._INITIALIZED_DATABASES.add(self.database_key)

    def tearDown(self):
        web_app._INITIALIZED_DATABASES.discard(self.database_key)
        web_app.DB_PATH = self.original_db_path
        self.temp_directory.cleanup()

    def _simultaneous_posts(self, path, payload, count=4):
        barrier = threading.Barrier(count)

        def post_once():
            with web_app.app.test_client() as client:
                barrier.wait(timeout=5)
                response = client.post(path, json=payload)
                return response.status_code, response.get_json()

        with ThreadPoolExecutor(max_workers=count) as executor:
            return list(executor.map(lambda _index: post_once(), range(count)))

    def test_concurrent_identical_feedback_and_flags_are_idempotent(self):
        actions = [
            (
                "/api/recommendations/ebook/feedback",
                {"thumbs_up": True},
                "thumbs_up",
            ),
            (
                "/api/recommendations/ebook/flag-non-english",
                {},
                "non_english",
            ),
            (
                "/api/recommendations/ebook/flag-already-read",
                {},
                "already_read",
            ),
            (
                "/api/recommendations/ebook/flag-duplicate",
                {},
                "duplicate",
            ),
        ]
        rounds = 10

        for path, action_payload, _field in actions:
            for round_number in range(rounds):
                title = f"Concurrent {path.rsplit('/', 1)[-1]} {round_number}"
                payload = {
                    "title": title,
                    "author": "Race Test Author",
                    **action_payload,
                }
                responses = self._simultaneous_posts(path, payload)
                self.assertTrue(
                    all(status == 200 and body.get("success") for status, body in responses),
                    responses,
                )

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            rows = session.query(Recommendation).all()
            self.assertEqual(len(rows), len(actions) * rounds)
            for path, _payload, field in actions:
                action_name = path.rsplit('/', 1)[-1]
                action_rows = [row for row in rows if action_name in row.title]
                self.assertEqual(len(action_rows), rounds)
                self.assertTrue(all(getattr(row, field) is True for row in action_rows))
        finally:
            session.close()
            engine.dispose()

    def test_actions_merge_without_erasing_existing_feedback(self):
        identity = {"title": "Shared Action", "author": "One Author"}
        with web_app.app.test_client() as client:
            responses = [
                client.post(
                    "/api/recommendations/ebook/feedback",
                    json={**identity, "thumbs_up": True},
                ),
                client.post(
                    "/api/recommendations/ebook/flag-non-english",
                    json=identity,
                ),
                client.post(
                    "/api/recommendations/ebook/flag-already-read",
                    json=identity,
                ),
                client.post(
                    "/api/recommendations/ebook/flag-duplicate",
                    json=identity,
                ),
            ]

        self.assertTrue(all(response.status_code == 200 for response in responses))
        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            row = session.query(Recommendation).one()
            self.assertTrue(row.thumbs_up)
            self.assertFalse(row.thumbs_down)
            self.assertTrue(row.non_english)
            self.assertEqual(row.language_flag_source, "manual")
            self.assertTrue(row.already_read)
            self.assertTrue(row.duplicate)
        finally:
            session.close()
            engine.dispose()

    def test_concurrent_missing_catalog_recategorization_is_idempotent(self):
        rounds = 10
        for round_number in range(rounds):
            responses = self._simultaneous_posts(
                "/api/recommendations/ebook/recategorize",
                {
                    "title": f"Missing Catalog Book {round_number}",
                    "author": "Missing Catalog Author",
                },
            )
            self.assertTrue(
                all(status == 200 and body.get("success") for status, body in responses),
                responses,
            )

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            rows = session.query(Recommendation).all()
            self.assertEqual(len(rows), rounds)
            self.assertTrue(all(row.category == "Uncategorized" for row in rows))
        finally:
            session.close()
            engine.dispose()

    def test_catalog_recategorization_still_toggles_both_directions(self):
        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            author = Author(name="Toggle Author", normalized_name="Toggle Author")
            session.add(author)
            session.flush()
            catalog_book = AuthorCatalogBook(
                author_id=author.id,
                title="Toggle Book",
                categories="Romance, Fiction",
            )
            session.add(catalog_book)
            session.commit()
        finally:
            session.close()
            engine.dispose()

        payload = {"title": "Toggle Book", "author": "Toggle Author"}
        with web_app.app.test_client() as client:
            to_nonfiction = client.post(
                "/api/recommendations/ebook/recategorize",
                json=payload,
            )
            to_fiction = client.post(
                "/api/recommendations/ebook/recategorize",
                json=payload,
            )

        self.assertEqual(to_nonfiction.status_code, 200)
        self.assertIn("Non-Fiction", to_nonfiction.get_json()["message"])
        self.assertEqual(to_fiction.status_code, 200)
        self.assertIn("Fiction", to_fiction.get_json()["message"])

        engine = init_db(str(web_app.DB_PATH))
        session = get_session(engine)
        try:
            catalog_book = session.query(AuthorCatalogBook).one()
            recommendation = session.query(Recommendation).one()
            self.assertEqual(catalog_book.categories, "Romance, Fiction")
            self.assertEqual(recommendation.category, "Romance, Fiction")
        finally:
            session.close()
            engine.dispose()

    def test_silently_failed_migration_is_retried_on_the_next_request(self):
        # Simulate migrate_database swallowing a transient failure before it can
        # create the unique expression index required by feedback upserts.
        web_app._INITIALIZED_DATABASES.discard(self.database_key)
        engine = create_engine(f"sqlite:///{web_app.DB_PATH}")
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP INDEX ux_recommendation_identity")
        engine.dispose()

        incomplete_engine = create_engine(
            f"sqlite:///{web_app.DB_PATH}",
            connect_args={"check_same_thread": False},
        )
        with patch.object(web_app, "init_db", return_value=incomplete_engine):
            with web_app.app.test_client() as client:
                response = client.get("/api/status")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(self.database_key, web_app._INITIALIZED_DATABASES)

        # With initialization left uncached, the next request reruns the real
        # migration and the atomic feedback write succeeds without a restart.
        with web_app.app.test_client() as client:
            response = client.post(
                "/api/recommendations/ebook/feedback",
                json={
                    "title": "Recovered Migration",
                    "author": "Recovery Author",
                    "thumbs_up": True,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn(self.database_key, web_app._INITIALIZED_DATABASES)


if __name__ == "__main__":
    unittest.main()
