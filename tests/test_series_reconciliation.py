import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base, Book
from src.series_reconciliation import (
    eligible_reconciliation_authors,
    ignore_series,
    load_ignored_series,
    load_reconciliation_result,
    match_series_recommendations,
    reconcile_author,
    restore_series,
    run_series_reconciliation,
    save_reconciliation_result,
)
from src.series_enrichment import record_hardcover_book_action
from src.series_review import ReviewCandidate


class _Provider:
    def lookup_author_series(self, author_name, known_titles=(), excluded_series_ids=()):
        excluded = {int(value) for value in excluded_series_ids}
        all_series = [
            {
                "name": "Harbor Series",
                "slug": "harbor-series",
                "hardcover_series_id": 101,
                "books": [
                    {"position": 1, "title": "Already Known", "hardcover_book_id": 11},
                    {"position": 2, "title": "Recommended Two", "hardcover_book_id": 12},
                    {"position": 3, "title": "Missing Three", "hardcover_book_id": 13},
                ],
            },
            {
                "name": "Unrelated Series",
                "slug": "unrelated",
                "hardcover_series_id": 202,
                "books": [{"position": 1, "title": "Elsewhere"}],
            },
        ]
        return {
            "author": {"id": 1, "name": author_name, "slug": "author"},
            "series": [series for series in all_series if series["hardcover_series_id"] not in excluded],
        }


class SeriesReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    @staticmethod
    def candidates(author, count=6):
        titles = ["Recommended Two", *[f"Standalone {number}" for number in range(1, count)]]
        return [ReviewCandidate(author, title, {"ebook"}, None) for title in titles]

    def test_eligibility_is_strictly_more_than_five_unique_recommendations(self):
        candidates = [
            *self.candidates("Eligible", 6),
            *self.candidates("Too Small", 5),
        ]
        self.assertEqual(
            eligible_reconciliation_authors(candidates),
            [{"author": "Eligible", "recommendations": 6}],
        )

    def test_reconciliation_keeps_full_relevant_series_and_comparison_status(self):
        self.session.add(Book(title="Already Known", author="Example Author", format="audiobook"))
        self.session.commit()

        result = reconcile_author(
            self.session,
            _Provider(),
            "Example Author",
            6,
            self.candidates("Example Author"),
        )

        self.assertEqual([series["name"] for series in result["series"]], ["Harbor Series"])
        books = result["series"][0]["books"]
        self.assertEqual([book["status"] for book in books], ["read", "recommendation", "other"])
        self.assertEqual([book["hardcover_book_id"] for book in books], [11, 12, 13])
        self.assertEqual(result["matched_recommendations"], 1)

    def test_saved_reconciliation_dedupes_rows_and_honors_scoped_actions(self):
        duplicate = {
            "book": "Recommended Two",
            "hardcover_book_id": 12,
            "series_number": 2,
            "status": "recommendation",
            "local_title": "Recommended Two",
            "formats": ["ebook"],
            "match": "exact",
        }
        save_reconciliation_result(self.session, {
            "authors": [{
                "author": "Example Author",
                "matched_recommendations": 2,
                "series": [{
                    "name": "Harbor Series",
                    "hardcover_series_id": 101,
                    "recommended_matches": 2,
                    "books": [duplicate, dict(duplicate)],
                }],
            }],
        })

        loaded = load_reconciliation_result(self.session)
        self.assertEqual(len(loaded["authors"][0]["series"][0]["books"]), 1)
        self.assertEqual(loaded["authors"][0]["matched_recommendations"], 1)

        record_hardcover_book_action(
            self.session,
            action="duplicate",
            series_id=101,
            book_id=12,
            position=2,
            title="Recommended Two",
            author="Example Author",
        )
        loaded = load_reconciliation_result(self.session)
        self.assertEqual(loaded["authors"][0]["series"][0]["books"], [])
        self.assertEqual(loaded["authors"][0]["matched_recommendations"], 0)

    def test_legacy_saved_rows_receive_stable_scoped_identities(self):
        save_reconciliation_result(self.session, {
            "authors": [{
                "author": "Legacy Author",
                "series": [{
                    "name": "Legacy Series",
                    "hardcover_series_id": 303,
                    "books": [{
                        "book": "Legacy Book",
                        "series_number": 1,
                        "status": "recommendation",
                        "local_title": "Legacy Book",
                        "formats": ["ebook"],
                    }],
                }],
            }],
        })

        first = load_reconciliation_result(self.session)["authors"][0]["series"][0]["books"][0]
        second = load_reconciliation_result(self.session)["authors"][0]["series"][0]["books"][0]

        self.assertEqual(first["hardcover_book_id"], second["hardcover_book_id"])
        self.assertGreater(first["hardcover_book_id"], 9_000_000_000_000)
        self.assertEqual(first["hardcover_identity_source"], "legacy_saved_result")

    def test_ignored_series_matching_includes_local_packaging_variants(self):
        matches = match_series_recommendations(
            {"books": [{"title": "Go for the Glory"}]},
            [
                ReviewCandidate("Example Author", "Go for the Glory", {"audiobook"}, None),
                ReviewCandidate("Example Author", "Go for the Glory (Golden Filly)", {"ebook"}, None),
                ReviewCandidate("Example Author", "A Different Book", {"ebook"}, None),
            ],
            "Example Author",
        )
        self.assertEqual(
            [row["title"] for row in matches],
            ["Go for the Glory", "Go for the Glory (Golden Filly)"],
        )

    def test_batches_continue_without_repeating_processed_authors(self):
        candidates = [*self.candidates("First Author"), *self.candidates("Second Author")]
        first = run_series_reconciliation(
            self.session, _Provider(), candidates, batch_size=1, reset=True,
        )
        second = run_series_reconciliation(
            self.session, _Provider(), candidates, batch_size=1, reset=False,
        )

        self.assertEqual(first["processed_authors"], 1)
        self.assertEqual(first["remaining_authors"], 1)
        self.assertEqual(second["processed_authors"], 2)
        self.assertEqual(second["remaining_authors"], 0)
        self.assertEqual([row["author"] for row in second["authors"]], ["First Author", "Second Author"])
        self.assertEqual(load_reconciliation_result(self.session)["processed_authors"], 2)

    def test_ignored_series_is_removed_saved_and_excluded_until_restored(self):
        candidates = self.candidates("Example Author")
        initial = run_series_reconciliation(
            self.session, _Provider(), candidates, batch_size=1, reset=True,
        )
        self.assertEqual(initial["authors"][0]["series"][0]["hardcover_series_id"], 101)

        ignored = ignore_series(
            self.session,
            series_id=101,
            name="Harbor Series",
            author="Example Author",
        )
        self.assertEqual(ignored[0]["series_id"], 101)
        self.assertEqual(load_reconciliation_result(self.session)["authors"][0]["series"], [])

        rerun = run_series_reconciliation(
            self.session, _Provider(), candidates, batch_size=1, reset=True,
        )
        self.assertEqual(rerun["authors"][0]["series"], [])
        self.assertEqual(len(load_ignored_series(self.session)), 1)

        self.assertEqual(restore_series(self.session, 101), [])
        restored = run_series_reconciliation(
            self.session, _Provider(), candidates, batch_size=1, reset=True,
        )
        self.assertEqual(restored["authors"][0]["series"][0]["hardcover_series_id"], 101)


if __name__ == "__main__":
    unittest.main()
