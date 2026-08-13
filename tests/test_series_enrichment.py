from datetime import datetime, timedelta, timezone
import unittest
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base
from src.series_enrichment import (
    CACHE_DAYS,
    apply_enrichment,
    enrichment_status,
    load_enrichment_cache,
    load_hardcover_book_actions,
    record_hardcover_book_action,
    run_enrichment,
)


class StubProvider:
    def __init__(self):
        self.request_callback = None
        self.lookup_calls = 0
        self.refresh_calls = 0

    @staticmethod
    def _series():
        return [{
            "name": "Example Series",
            "hardcover_series_id": 101,
            "books": [
                {"title": "First Book", "position": 1, "hardcover_book_id": 1},
                {"title": "Second Book", "position": 2, "hardcover_book_id": 2},
                {"title": "Third Book", "position": 3, "hardcover_book_id": 3},
            ],
        }]

    def lookup_author_series(self, author, known_titles):
        self.lookup_calls += 1
        self.request_callback()
        self.request_callback()
        return {
            "author": {"id": 9, "name": author, "slug": "example-author"},
            "series": self._series(),
        }

    def get_author_series(self, author_id):
        self.refresh_calls += 1
        self.request_callback()
        return self._series()


def local_series():
    return [{
        "series_name": "Example Series",
        "author": "Example Author",
        "status": "partial",
        "total_books": 2,
        "books_read": 1,
        "completion_pct": 50,
        "read_books": [{"title": "First Book", "position": 1}],
        "unread_books": [{"title": "Second Book", "position": 2}],
    }]


class SeriesEnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_first_build_uses_two_calls_cache_skips_and_refresh_uses_one(self):
        provider = StubProvider()
        first = run_enrichment(self.session, provider, local_series())
        self.assertEqual(first["api_calls"], 2)
        self.assertEqual(provider.lookup_calls, 1)

        cached = run_enrichment(self.session, provider, local_series())
        self.assertEqual(cached["api_calls"], 0)
        self.assertEqual(provider.lookup_calls, 1)

        refreshed = run_enrichment(
            self.session, provider, local_series(), force_refresh=True,
        )
        self.assertEqual(refreshed["api_calls"], 1)
        self.assertEqual(provider.refresh_calls, 1)

    def test_enrichment_adds_full_order_and_local_statuses(self):
        run_enrichment(self.session, StubProvider(), local_series())
        cache = load_enrichment_cache(self.session)
        enriched = apply_enrichment(local_series(), cache)[0]

        self.assertEqual(enriched["enrichment"]["status"], "matched")
        self.assertEqual(enriched["total_books"], 3)
        self.assertEqual(
            [book["status"] for book in enriched["series_books"]],
            ["read", "unread", "other"],
        )

    def test_manual_feedback_controls_hardcover_only_rows(self):
        run_enrichment(self.session, StubProvider(), local_series())
        cache = load_enrichment_cache(self.session)
        feedback = [
            SimpleNamespace(
                title="Third Book", author="Example Author", format="ebook",
                already_read=True, thumbs_up=False, thumbs_down=False,
                duplicate=False, non_english=False,
            ),
        ]

        enriched = apply_enrichment(local_series(), cache, feedback)[0]

        self.assertEqual(
            [book["status"] for book in enriched["series_books"]],
            ["read", "unread", "read"],
        )

        feedback[0].already_read = False
        feedback[0].duplicate = True
        enriched = apply_enrichment(local_series(), cache, feedback)[0]
        self.assertEqual(
            [book["title"] for book in enriched["series_books"]],
            ["First Book", "Second Book"],
        )

        feedback[0].duplicate = False
        feedback[0].thumbs_up = True
        enriched = apply_enrichment(local_series(), cache, feedback)[0]
        self.assertEqual(enriched["series_books"][2]["status"], "unread")

    def test_hardcover_action_hides_only_one_exact_membership(self):
        run_enrichment(self.session, StubProvider(), local_series())
        cache = load_enrichment_cache(self.session)
        cache["example author"]["series"][0]["books"].append({
            "title": "Third Book", "position": 4, "hardcover_book_id": 4,
        })
        actions = record_hardcover_book_action(
            self.session,
            action="duplicate",
            series_id=101,
            book_id=3,
            position=3,
            title="Third Book",
            author="Example Author",
        )

        enriched = apply_enrichment(local_series(), cache, (), actions)[0]

        self.assertEqual(
            [(book["hardcover_book_id"], book["position"]) for book in enriched["series_books"]],
            [(1, 1), (2, 2), (4, 4)],
        )
        self.assertEqual(load_hardcover_book_actions(self.session), actions)

    def test_multiple_local_records_resolving_to_one_hardcover_series_are_merged(self):
        run_enrichment(self.session, StubProvider(), local_series())
        cache = load_enrichment_cache(self.session)
        duplicate_local = {
            **local_series()[0],
            "series_name": "Example Series Books",
            "read_books": [{"title": "Third Book", "position": 3}],
            "unread_books": [],
        }

        enriched = apply_enrichment([local_series()[0], duplicate_local], cache)

        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched[0]["enrichment"]["hardcover_series_id"], 101)
        self.assertEqual(enriched[0]["enrichment"]["merged_local_records"], 2)
        self.assertEqual(
            [book["status"] for book in enriched[0]["series_books"]],
            ["read", "unread", "read"],
        )

    def test_status_explains_first_build_and_long_cache(self):
        status = enrichment_status(local_series(), {})
        self.assertEqual(status["candidate_series"], 1)
        self.assertEqual(status["build_api_calls"], 2)
        self.assertEqual(status["refresh_api_calls"], 2)
        self.assertEqual(status["cache_days"], CACHE_DAYS)
        self.assertGreaterEqual(CACHE_DAYS, 180)

        run_enrichment(self.session, StubProvider(), local_series())
        status = enrichment_status(local_series(), load_enrichment_cache(self.session))
        self.assertEqual(status["fresh_authors"], 1)
        self.assertEqual(status["build_api_calls"], 0)
        self.assertEqual(status["refresh_api_calls"], 1)


if __name__ == "__main__":
    unittest.main()
