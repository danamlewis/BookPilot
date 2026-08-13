import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Author, AuthorCatalogBook, Base, Book
from src.series_review import (
    ReviewCandidate, build_official_series_rows, build_series_review_rows,
    infer_explicit_series,
)


class SeriesReviewTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def _candidate(self, author, book):
        return ReviewCandidate(author, book.title, {"ebook"}, book.id)

    def test_explicit_description_series_is_detected(self):
        book = AuthorCatalogBook(
            title="Example",
            description="A new Harbor Watch Mystery from a bestselling author.",
        )
        label, evidence = infer_explicit_series(book)
        self.assertEqual(label, "Harbor Watch")
        self.assertIn("Harbor Watch Mystery", evidence)

    def test_other_series_by_same_author_is_not_suggested(self):
        author = Author(name="Multi Series", normalized_name="Multi Series")
        self.session.add(author)
        self.session.flush()
        read = AuthorCatalogBook(
            author_id=author.id, title="Harbor Latest", is_read=True,
            description="The latest Harbor Watch series mystery.",
        )
        harbor = AuthorCatalogBook(
            author_id=author.id, title="Harbor Earlier", is_read=False,
            description="A Harbor Watch series mystery.",
        )
        meadow = AuthorCatalogBook(
            author_id=author.id, title="Meadow Earlier", is_read=False,
            description="A Meadow Casefiles mystery series adventure.",
        )
        filler = [
            AuthorCatalogBook(author_id=author.id, title=f"Standalone {number}", is_read=False)
            for number in range(4)
        ]
        self.session.add_all([read, harbor, meadow, *filler])
        self.session.flush()
        self.session.add(Book(title="Harbor Latest", author=author.name, format="ebook"))
        self.session.commit()
        candidates = [self._candidate(author.name, item) for item in [harbor, meadow, *filler]]

        rows = build_series_review_rows(self.session, candidates, min_unread=5)
        self.assertEqual([row["title"] for row in rows], ["Harbor Earlier"])

    def test_threshold_uses_recommendation_count(self):
        author = Author(name="Small Author", normalized_name="Small Author")
        self.session.add(author)
        self.session.flush()
        read = AuthorCatalogBook(
            author_id=author.id, title="Known", is_read=True, series_name="Series A",
        )
        unread = AuthorCatalogBook(
            author_id=author.id, title="Unknown", is_read=False, series_name="Series A",
        )
        self.session.add_all([read, unread])
        self.session.flush()
        self.session.add(Book(title="Known", author=author.name, format="ebook"))
        self.session.commit()
        rows = build_series_review_rows(
            self.session, [self._candidate(author.name, unread)], min_unread=5,
        )
        self.assertEqual(rows, [])

    def test_official_order_maps_reads_and_recommendations(self):
        author = Author(name="Ordered Author", normalized_name="Ordered Author")
        self.session.add(author)
        self.session.flush()
        recommended = AuthorCatalogBook(author_id=author.id, title="Second Book", is_read=False)
        self.session.add(recommended)
        self.session.flush()
        self.session.add(Book(title="First Book", author=author.name, format="ebook"))
        self.session.commit()
        reference = {
            author.name: [{
                "series": "Example Series", "source_url": "https://example.test/series",
                "books": [[1, "The First Book"], [2, "Second Book"], [3, "Missing Book"]],
            }]
        }
        candidates = [self._candidate(author.name, recommended)]
        result = build_official_series_rows(self.session, candidates, reference)
        self.assertEqual([row["series_number"] for row in result[author.name]], [1, 2])
        self.assertEqual(result[author.name][0]["already_read"], "Already read")
        self.assertEqual(result[author.name][1]["already_read"], "")
        self.assertEqual(result[author.name][1]["recommendation_formats"], "ebook")

    def test_official_order_accepts_retail_subtitle(self):
        author = Author(name="Subtitle Author", normalized_name="Subtitle Author")
        self.session.add(author)
        self.session.flush()
        item = AuthorCatalogBook(
            author_id=author.id,
            title="Finding Rowan: A Harbor Redemption",
            is_read=False,
        )
        self.session.add(item)
        self.session.commit()
        reference = {
            author.name: [{
                "series": "Casebook", "source_url": "https://example.test/series",
                "books": [[5, "Finding Rowan"]],
            }]
        }
        result = build_official_series_rows(
            self.session, [self._candidate(author.name, item)], reference,
        )
        self.assertEqual(result[author.name][0]["series_number"], 5)


if __name__ == "__main__":
    unittest.main()
