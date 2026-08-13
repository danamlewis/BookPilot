import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.api.openlibrary import OpenLibraryAPIError
from src.catalog import fetch_all_author_catalogs, fetch_author_catalog
from src.ingest import ingest_csv
from src.models import Author, AuthorCatalogBook, Base, Book, Recommendation, SystemMetadata, migrate_database
from src.recommend import save_recommendations
from src.series import analyze_all_series, analyze_author_series
import web.app as web_app


class FakeCatalogClient:
    def __init__(self, title="Updated Title"):
        self.title = title
        self.details_calls = 0

    def get_author_works(self, _author_key, limit=200, **_kwargs):
        return [{"key": "/works/SHARED", "title": self.title}]

    def get_work_details(self, _work_key, **_kwargs):
        self.details_calls += 1
        return {
            "key": "/works/SHARED",
            "title": self.title,
            "languages": [{"key": "/languages/eng"}],
        }

    def get_editions(self, _work_key, **_kwargs):
        return [{
            "languages": [{"key": "/languages/eng"}],
            "isbn_13": ["9780000000002"],
        }]


class AuditRegressionTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def add_history_author(self, name):
        author = Author(name=name, normalized_name=name, open_library_id=f"/authors/{name}")
        self.session.add(author)
        self.session.flush()
        self.session.add(Book(title=f"Read by {name}", author=name, format="ebook"))
        self.session.commit()
        return author

    def test_series_analysis_uses_bulk_queries_and_omits_unused_standalone_payload(self):
        for number in range(4):
            author = self.add_history_author(f"Author {number}")
            self.session.add(AuthorCatalogBook(
                author_id=author.id,
                title=f"Series Book {number}",
                series_name=f"Series {number}",
                is_read=False,
            ))
        self.session.add(AuthorCatalogBook(
            author_id=author.id,
            title="Standalone Book",
            is_read=False,
        ))
        self.session.commit()
        statements = []
        event.listen(self.engine, "before_cursor_execute", lambda *args: statements.append(args[2]))

        result = analyze_all_series(self.session, format_filter=None)

        self.assertEqual(result["total_series"], 4)
        self.assertEqual(result["total_standalone"], 1)
        self.assertNotIn("standalone_books", result)
        self.assertLessEqual(len(statements), 4)

    def test_single_author_series_analysis_handles_a_missing_normalized_name(self):
        author = Author(name="Display Author", normalized_name="Display Author")
        self.session.add(author)
        self.session.flush()
        self.session.add(AuthorCatalogBook(
            author_id=author.id,
            title="Series Book",
            series_name="Example Series",
            is_read=False,
        ))
        self.session.commit()
        # Exercise the public helper defensively with a detached legacy-style
        # object; its query construction must not call .lower() on None.
        self.session.refresh(author)
        author_id = author.id
        author_name = author.name
        self.session.expunge(author)
        author.id = author_id
        author.name = author_name
        author.normalized_name = None

        result = analyze_author_series(author, self.session)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["series_name"], "Example Series")

    def test_force_refresh_updates_an_existing_work(self):
        author = self.add_history_author("Refresh Author")
        existing = AuthorCatalogBook(
            author_id=author.id,
            title="Old Title",
            open_library_key="/works/SHARED",
            is_read=False,
        )
        self.session.add(existing)
        self.session.commit()
        client = FakeCatalogClient()

        result = fetch_author_catalog(author, self.session, force_refresh=True, ol_client=client)

        self.assertEqual(result["books_updated"], 1)
        self.assertEqual(existing.title, "Updated Title")
        self.assertEqual(client.details_calls, 1)

    def test_catalog_update_refreshes_work_and_isbn_lookup_maps(self):
        author = self.add_history_author("Mapped Refresh Author")
        existing = AuthorCatalogBook(
            author_id=author.id,
            title="Updated Title",
            open_library_key="/works/OLD",
            isbn="9780000000001",
            is_read=False,
        )
        self.session.add(existing)
        self.session.commit()
        work_lookup = {existing.open_library_key: existing}
        isbn_lookup = {existing.isbn: existing}

        result = fetch_author_catalog(
            author,
            self.session,
            force_refresh=True,
            ol_client=FakeCatalogClient(),
            global_isbn_lookup=isbn_lookup,
            global_work_lookup=work_lookup,
        )

        self.assertEqual(result["books_updated"], 1)
        self.assertEqual(existing.open_library_key, "/works/SHARED")
        self.assertEqual(existing.isbn, "9780000000002")
        self.assertNotIn("/works/OLD", work_lookup)
        self.assertIs(work_lookup["/works/SHARED"], existing)
        self.assertNotIn("9780000000001", isbn_lookup)
        self.assertIs(isbn_lookup["9780000000002"], existing)

    def test_new_catalog_rows_have_ids_before_scoped_cleanup_collection(self):
        author = self.add_history_author("Cleanup ID Author")
        collected_ids = []

        result = fetch_author_catalog(
            author,
            self.session,
            force_refresh=True,
            ol_client=FakeCatalogClient("Cleanup Target"),
            collect_new_or_updated_ids=collected_ids,
        )

        self.assertEqual(result["books_added"], 1)
        self.assertEqual(len(collected_ids), 1)
        self.assertIsInstance(collected_ids[0], int)
        self.assertEqual(
            collected_ids[0],
            self.session.query(AuthorCatalogBook.id).filter_by(title="Cleanup Target").scalar(),
        )

    def test_catalog_discovery_is_fresh_complete_and_full_refresh_bypasses_detail_cache(self):
        author = self.add_history_author("Fresh Author")

        class RecordingClient(FakeCatalogClient):
            def __init__(self):
                super().__init__()
                self.works_call = None
                self.details_fresh = None
                self.editions_fresh = None

            def get_author_works(self, author_key, limit=200, **kwargs):
                self.works_call = (author_key, limit, kwargs.get("fresh"))
                return super().get_author_works(author_key, limit)

            def get_work_details(self, work_key, **kwargs):
                self.details_fresh = kwargs.get("fresh")
                return super().get_work_details(work_key)

            def get_editions(self, work_key, **kwargs):
                self.editions_fresh = kwargs.get("fresh")
                return super().get_editions(work_key)

        client = RecordingClient()
        fetch_author_catalog(author, self.session, force_refresh=True, ol_client=client)

        self.assertEqual(client.works_call, ("/authors/Fresh Author", None, True))
        self.assertTrue(client.details_fresh)
        self.assertTrue(client.editions_fresh)

    def test_api_failure_is_not_recorded_as_a_successful_check(self):
        author = self.add_history_author("Failure Author")

        class FailingClient:
            def get_author_works(self, _author_key, limit=200, **_kwargs):
                raise OpenLibraryAPIError("temporary outage")

        result = fetch_author_catalog(author, self.session, force_refresh=True, ol_client=FailingClient())

        self.assertTrue(result["is_systemic"])
        self.assertIsNone(author.last_catalog_check)

    def test_same_work_from_two_authors_is_added_once_per_batch(self):
        first = self.add_history_author("First Author")
        second = self.add_history_author("Second Author")
        first.open_library_id = "/authors/FIRST"
        second.open_library_id = "/authors/SECOND"
        self.session.commit()

        with patch("src.catalog.OpenLibraryClient", return_value=FakeCatalogClient("Shared Work")):
            result = fetch_all_author_catalogs(self.session, force_refresh=True)

        self.assertEqual(result["total_books_added"], 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 1)

    def test_stopped_catalog_batch_does_not_claim_a_completed_global_check(self):
        author = self.add_history_author("Outage Author")
        author.open_library_id = "/authors/OUTAGE"
        self.session.commit()

        class OutageClient:
            def get_author_works(self, *_args, **_kwargs):
                raise OpenLibraryAPIError("temporary outage")

        with patch("src.catalog.OpenLibraryClient", return_value=OutageClient()):
            result = fetch_all_author_catalogs(
                self.session,
                force_refresh=True,
                max_consecutive_errors=1,
            )

        self.assertTrue(result["stopped_early"])
        self.assertIsNone(
            self.session.query(SystemMetadata).filter_by(key="last_catalog_check").first()
        )

    def test_fetch_author_catalog_preserves_legacy_positional_lookup_order(self):
        first = self.add_history_author("Legacy First Author")
        second = self.add_history_author("Legacy Second Author")
        existing = AuthorCatalogBook(
            author_id=first.id,
            title="Existing Shared Work",
            open_library_key="/works/SHARED",
        )
        self.session.add(existing)
        self.session.commit()

        collected_ids = []
        result = fetch_author_catalog(
            second,
            self.session,
            True,
            False,
            3,
            FakeCatalogClient("Current Shared Work"),
            {"unrelated title": existing},  # legacy global_title_lookup position
            {},  # legacy global_isbn_lookup position
            None,
            collected_ids,
        )

        self.assertEqual(result["books_added"], 0)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 1)

    def test_saved_recommendations_keep_formats_separate_and_dedupe_input(self):
        common = {
            "title": "Same Title",
            "author": "Same Author",
            "similarity_score": 0.8,
            "reason": "Because",
            "recommendation_type": "same_author",
            "categories": [],
        }
        save_recommendations([
            {**common, "format": "ebook"},
            {**common, "format": "ebook"},
            {**common, "format": "audiobook"},
        ], self.session)

        self.assertEqual(self.session.query(Recommendation).count(), 2)
        self.assertEqual({row.format for row in self.session.query(Recommendation)}, {"ebook", "audiobook"})

    def test_update_import_drops_a_replaced_isbn_from_its_identity_cache(self):
        self.session.add_all([
            Author(name="Import Author", normalized_name="Import Author"),
            Book(
                title="Existing Book",
                author="Import Author",
                isbn="9780000000001",
                format="ebook",
            ),
        ])
        self.session.commit()

        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "history.csv"
            csv_path.write_text(
                "title,author,publisher,isbn,timestamp\n"
                "Existing Book,Import Author,Publisher,9780000000002,\n"
                "Different Book,Import Author,Publisher,9780000000001,\n",
                encoding="utf-8",
            )
            result = ingest_csv(csv_path, self.session, update_existing=True)

        rows = {book.title: book for book in self.session.query(Book).all()}
        self.assertEqual(result["books_added"], 1)
        self.assertEqual(set(rows), {"Existing Book", "Different Book"})
        self.assertEqual(rows["Existing Book"].isbn, "9780000000002")
        self.assertEqual(rows["Different Book"].isbn, "9780000000001")

    def test_migration_merges_legacy_duplicate_feedback_before_adding_unique_index(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory) / 'legacy.db'}")
            Base.metadata.create_all(engine)
            session = sessionmaker(bind=engine)()
            session.add_all([
                Recommendation(title="Duplicate", author="An Author", format="ebook", thumbs_up=True),
                Recommendation(title=" duplicate ", author="an author", format="ebook", already_read=True),
            ])
            session.commit()

            migrate_database(engine)
            session.expire_all()

            rows = session.query(Recommendation).all()
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0].thumbs_up)
            self.assertTrue(rows[0].already_read)
            session.close()
            engine.dispose()

    def test_migration_preserves_metadata_from_legacy_duplicate_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory) / 'legacy-metadata.db'}")
            Base.metadata.create_all(engine)
            session = sessionmaker(bind=engine)()
            oldest = datetime(2026, 1, 1, 12, 0, 0)
            newest = oldest + timedelta(days=2)
            session.add_all([
                Recommendation(
                    title="Metadata Book",
                    author="Metadata Author",
                    format="ebook",
                    thumbs_up=True,
                    feedback_date=oldest,
                ),
                Recommendation(
                    title=" metadata book ",
                    author="metadata author",
                    format="ebook",
                    non_english=True,
                    language_flag_source="manual",
                    book_id=101,
                    catalog_book_id=201,
                    isbn="9780000000002",
                    category="History",
                    recommendation_type="same_author",
                    similarity_score=0.75,
                    reason="Older populated reason",
                    feedback_date=oldest + timedelta(days=1),
                ),
                Recommendation(
                    title="METADATA BOOK",
                    author="METADATA AUTHOR",
                    format="ebook",
                    already_read=True,
                    duplicate=True,
                    catalog_book_id=202,
                    category="Biography",
                    reason="Newest populated reason",
                    feedback_date=newest,
                ),
            ])
            session.commit()

            migrate_database(engine)
            session.expire_all()

            row = session.query(Recommendation).one()
            self.assertTrue(row.thumbs_up)
            self.assertTrue(row.non_english)
            self.assertTrue(row.already_read)
            self.assertTrue(row.duplicate)
            self.assertEqual(row.language_flag_source, "manual")
            self.assertEqual(row.book_id, 101)
            self.assertEqual(row.catalog_book_id, 202)
            self.assertEqual(row.isbn, "9780000000002")
            self.assertEqual(row.category, "Biography")
            self.assertEqual(row.recommendation_type, "same_author")
            self.assertEqual(row.similarity_score, 0.75)
            self.assertEqual(row.reason, "Newest populated reason")
            self.assertEqual(row.feedback_date, newest)
            session.close()
            engine.dispose()


class WebAuditRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.original_db_path = web_app.DB_PATH
        web_app.DB_PATH = Path(self.temp_directory.name) / "bookpilot.db"
        web_app.app.config.update(TESTING=True)
        self.client = web_app.app.test_client()

    def tearDown(self):
        web_app.DB_PATH = self.original_db_path
        self.temp_directory.cleanup()

    def test_status_does_not_run_series_analysis(self):
        with patch.object(web_app, "analyze_all_series") as analyze:
            response = self.client.get("/api/status")
        self.assertEqual(response.status_code, 200)
        analyze.assert_not_called()

    def test_repeated_content_loads_replace_delegated_handlers(self):
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("content.onclick = (e) =>", page)
        self.assertNotIn("content.addEventListener('click'", page)


if __name__ == "__main__":
    unittest.main()
