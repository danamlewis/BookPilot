import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.catalog import (
    auto_split_author_group,
    fetch_all_author_catalogs,
    fetch_author_catalog,
    fix_author_mismatches,
    purge_historyless_authors,
)
from src.models import Author, AuthorCatalogBook, Base, Book, Recommendation, Series


class CatalogHistoryEligibilityTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_direct_fetch_skips_author_without_history(self):
        author = Author(
            name="Historyless Author",
            normalized_name="Historyless Author",
            open_library_id="/authors/EXAMPLE",
        )
        self.session.add(author)
        self.session.commit()

        result = fetch_author_catalog(author, self.session, force_refresh=True)

        self.assertTrue(result["skipped"])
        self.assertIn("No matching books", result["reason"])
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 0)

    def test_forced_batch_fetches_only_history_linked_authors(self):
        eligible = Author(name="Current Author", normalized_name="Current Author")
        historyless = Author(name="Historyless Author", normalized_name="Historyless Author")
        self.session.add_all([eligible, historyless])
        self.session.flush()
        self.session.add(
            Book(
                title="A Current Book",
                author="  current author  ",
                format="audiobook",
            )
        )
        self.session.commit()

        fetched_author_ids = []

        def fake_fetch(author, *_args, **_kwargs):
            fetched_author_ids.append(author.id)
            return {
                "books_added": 1,
                "books_updated": 0,
                "added_books": [{"title": "A New Book", "author": author.name}],
            }

        with patch("src.catalog.auto_split_author_group", return_value=False), patch(
            "src.catalog.fetch_author_catalog", side_effect=fake_fetch
        ):
            result = fetch_all_author_catalogs(self.session, force_refresh=True)

        self.assertEqual(fetched_author_ids, [eligible.id])
        self.assertEqual(result["history_eligible_authors"], 1)
        self.assertEqual(result["historyless_authors_skipped"], 1)
        self.assertEqual(
            result["added_books"],
            [{"title": "A New Book", "author": "Current Author"}],
        )
        self.assertIsNotNone(self.session.get(Author, historyless.id))

    def test_direct_fetch_accepts_display_name_history_match(self):
        author = Author(
            name="Display Name",
            normalized_name="Different Normalized Name",
            open_library_id="/authors/EXAMPLE",
        )
        self.session.add_all([
            author,
            Book(title="A History Book", author="Display Name", format="ebook"),
        ])
        self.session.commit()

        class EmptyCatalogClient:
            def get_author_works(self, *_args, **_kwargs):
                return []

        result = fetch_author_catalog(
            author,
            self.session,
            force_refresh=True,
            ol_client=EmptyCatalogClient(),
        )

        self.assertFalse(result.get("skipped", False))

    def test_direct_fetch_reports_each_added_title(self):
        author = Author(
            name="Example Writer",
            normalized_name="Example Writer",
            open_library_id="/authors/EXAMPLE",
        )
        self.session.add_all([
            author,
            Book(title="A History Book", author="Example Writer", format="ebook"),
        ])
        self.session.commit()

        class OneBookCatalogClient:
            def get_author_works(self, *_args, **_kwargs):
                return [{"key": "/works/OL1W", "title": "A New Book"}]

            def get_work_details(self, *_args, **_kwargs):
                return {"title": "A New Book", "languages": ["/languages/eng"]}

            def get_editions(self, *_args, **_kwargs):
                return []

        result = fetch_author_catalog(
            author,
            self.session,
            force_refresh=True,
            ol_client=OneBookCatalogClient(),
        )

        self.assertEqual(result["books_added"], 1)
        self.assertEqual(
            result["added_books"],
            [{"title": "A New Book", "author": "Example Writer"}],
        )

    def test_regular_batch_fetches_only_history_linked_authors(self):
        eligible = Author(name="Current Author", normalized_name="Current Author")
        historyless = Author(name="Catalog Artifact", normalized_name="Catalog Artifact")
        self.session.add_all([eligible, historyless])
        self.session.flush()
        self.session.add(Book(title="A Loan", author="Current Author", format="ebook"))
        self.session.commit()

        fetched_author_ids = []

        def fake_fetch(author, *_args, **_kwargs):
            fetched_author_ids.append(author.id)
            return {"books_added": 0, "books_updated": 0}

        with patch("src.catalog.auto_split_author_group", return_value=False), patch(
            "src.catalog.fetch_author_catalog", side_effect=fake_fetch
        ):
            result = fetch_all_author_catalogs(self.session)

        self.assertEqual(fetched_author_ids, [eligible.id])
        self.assertEqual(result["history_eligible_authors"], 1)
        self.assertEqual(result["historyless_authors_skipped"], 1)

    def test_batch_skips_company_credit_backed_by_legal_publisher_name(self):
        company = Author(
            name="Innovative Language Learning",
            normalized_name="Innovative Language Learning",
        )
        person = Author(name="Example Writer", normalized_name="Example Writer")
        self.session.add_all([company, person])
        self.session.flush()
        self.session.add_all([
            Book(
                title="A Language Course",
                author="Innovative Language Learning",
                publisher="Innovative Language Learning, LLC",
                format="ebook",
            ),
            Book(
                title="A Novel",
                author="Example Writer",
                publisher="Unrelated Books, LLC",
                format="ebook",
            ),
        ])
        self.session.commit()

        fetched_author_ids = []

        def fake_fetch(author, *_args, **_kwargs):
            fetched_author_ids.append(author.id)
            return {"books_added": 0, "books_updated": 0}

        with patch("src.catalog.auto_split_author_group", return_value=False), patch(
            "src.catalog.fetch_author_catalog", side_effect=fake_fetch
        ):
            result = fetch_all_author_catalogs(self.session, force_refresh=True)

        self.assertEqual(fetched_author_ids, [person.id])
        self.assertEqual(result["organization_authors_skipped"], 1)
        self.assertEqual(result["refresh_eligible_authors"], 1)

    def test_direct_fetch_skips_explicit_publisher_credit(self):
        publisher = Author(
            name="Example Publishing",
            normalized_name="Example Publishing",
            open_library_id="/authors/EXAMPLE",
        )
        self.session.add_all([
            publisher,
            Book(title="A Catalog", author="Example Publishing", format="ebook"),
        ])
        self.session.commit()

        result = fetch_author_catalog(publisher, self.session, force_refresh=True)

        self.assertTrue(result["skipped"])
        self.assertIn("Organization-like author credit", result["reason"])

    def test_person_with_unrelated_corporate_publisher_is_not_excluded(self):
        person = Author(name="Example Writer", normalized_name="Example Writer")
        self.session.add_all([
            person,
            Book(
                title="A Novel",
                author="Example Writer",
                publisher="Example House, LLC",
                format="ebook",
            ),
        ])
        self.session.commit()

        fetched_author_ids = []

        def fake_fetch(author, *_args, **_kwargs):
            fetched_author_ids.append(author.id)
            return {"books_added": 0, "books_updated": 0}

        with patch("src.catalog.auto_split_author_group", return_value=False), patch(
            "src.catalog.fetch_author_catalog", side_effect=fake_fetch
        ):
            result = fetch_all_author_catalogs(self.session, force_refresh=True)

        self.assertEqual(fetched_author_ids, [person.id])
        self.assertEqual(result["organization_authors_skipped"], 0)

    def test_historyless_purge_removes_catalog_lists_series_and_author(self):
        retained = Author(name="History Author", normalized_name="History Author")
        removed = Author(name="Catalog Artifact", normalized_name="Catalog Artifact")
        self.session.add_all([retained, removed])
        self.session.flush()
        self.session.add(Book(title="A Real Loan", author="History Author", format="ebook"))
        retained_catalog = AuthorCatalogBook(author_id=retained.id, title="Keep This")
        removed_catalog = AuthorCatalogBook(author_id=removed.id, title="Remove This")
        self.session.add_all([retained_catalog, removed_catalog])
        self.session.flush()
        self.session.add_all([
            Recommendation(title="Keep This", author="History Author", format="ebook"),
            Recommendation(title="Remove by name", author="Catalog Artifact", format="ebook"),
            Recommendation(
                title="Remove by catalog link",
                author="Legacy Alias",
                format="audiobook",
                catalog_book_id=removed_catalog.id,
            ),
            Series(name="Orphan Series", author_id=removed.id),
        ])
        self.session.commit()

        result = purge_historyless_authors(self.session, dry_run=False)

        self.assertEqual(result["authors_removed"], 1)
        self.assertEqual(result["catalog_rows_removed"], 1)
        self.assertEqual(result["recommendations_removed"], 2)
        self.assertEqual(result["series_rows_removed"], 1)
        self.assertEqual(self.session.query(Author).all(), [retained])
        self.assertEqual([row.title for row in self.session.query(AuthorCatalogBook).all()], ["Keep This"])
        self.assertEqual([row.title for row in self.session.query(Recommendation).all()], ["Keep This"])
        self.assertEqual(self.session.query(Series).count(), 0)

    def test_historyless_purge_dry_run_is_non_mutating(self):
        orphan = Author(name="Catalog Artifact", normalized_name="Catalog Artifact")
        self.session.add(orphan)
        self.session.flush()
        self.session.add(AuthorCatalogBook(author_id=orphan.id, title="Remove This"))
        self.session.commit()

        result = purge_historyless_authors(self.session, dry_run=True)

        self.assertEqual(result["authors_removed"], 1)
        self.assertEqual(result["catalog_rows_removed"], 1)
        self.assertEqual(self.session.query(Author).count(), 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 1)

    def test_group_split_does_not_create_historyless_contributors(self):
        group = Author(
            name="History Author, Catalog Contributor",
            normalized_name="History Author",
        )
        self.session.add(group)
        self.session.flush()
        self.session.add(Book(title="A Real Loan", author="History Author", format="ebook"))
        self.session.commit()

        split = auto_split_author_group(group, self.session)

        self.assertFalse(split)
        self.assertEqual(self.session.query(Author).count(), 1)

    def test_mismatch_repair_does_not_create_historyless_work_author(self):
        parent = Author(
            name="History Author",
            normalized_name="History Author",
            open_library_id="/authors/PARENT",
        )
        self.session.add(parent)
        self.session.flush()
        self.session.add(Book(title="A Real Loan", author="History Author", format="ebook"))
        self.session.add_all([
            AuthorCatalogBook(author_id=parent.id, title="A Real Loan", open_library_key="/works/ONE"),
            AuthorCatalogBook(author_id=parent.id, title="An Anthology", open_library_key="/works/TWO"),
        ])
        self.session.commit()

        class FakeOpenLibraryClient:
            def get_work_details(self, work_key):
                author_key = "/authors/PARENT" if work_key == "/works/ONE" else "/authors/ORPHAN"
                return {"authors": [{"author": {"key": author_key}}]}

            def get_author_works(self, author_key, limit=50):
                return [{"title": "A Real Loan"}] if author_key == "/authors/PARENT" else []

            def _request(self, _endpoint):
                return {"name": "Unrelated Contributor"}

        with patch("src.api.openlibrary.OpenLibraryClient", FakeOpenLibraryClient):
            result = fix_author_mismatches(self.session, only_cataloged=True)

        self.assertEqual(result["authors_created"], 0)
        self.assertEqual(self.session.query(Author).count(), 1)
        self.assertEqual(
            {row.author_id for row in self.session.query(AuthorCatalogBook).all()},
            {parent.id},
        )


if __name__ == "__main__":
    unittest.main()
