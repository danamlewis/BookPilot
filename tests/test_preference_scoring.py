import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Author, AuthorCatalogBook, Base, Book
from src.preference_scoring import build_preference_profile, classify_catalog_item, score_catalog_item


class PreferenceScoringTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.author = Author(name="Example Author", normalized_name="Example Author")
        self.session.add(self.author)
        self.session.flush()
        self.session.add_all([
            Book(title="Markets and Change", author="Example Author", format="ebook"),
            Book(title="Ideas and Growth", author="Example Author", format="ebook"),
            AuthorCatalogBook(
                author_id=self.author.id,
                title="Markets and Change",
                categories="Business & Economics",
                description="Economics, innovation, and social change.",
                is_read=True,
            ),
        ])
        self.session.commit()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_course_products_are_batched(self):
        result = classify_catalog_item("Economics Access Card (12 Month Access)")
        self.assertEqual(result["content_type"], "course_material")

    def test_numbered_edition_is_batched(self):
        result = classify_catalog_item("Sample Reference, 3rd Edition")
        self.assertEqual(result["content_type"], "edition_or_bundle")

    def test_trade_book_scores_above_access_card(self):
        profile = build_preference_profile(self.session)
        trade = AuthorCatalogBook(
            title="Innovation and Society", categories="Business & Economics",
            description="Innovation, markets, economics, and culture.", isbn="1",
        )
        access = AuthorCatalogBook(title="Economics Access Card and Student Remote")
        trade_score = score_catalog_item(profile, trade, 2)
        access_score = score_catalog_item(profile, access, 2)
        self.assertGreater(trade_score["match_score"], access_score["match_score"])
        self.assertEqual(access_score["interest_tier"], "batch")

    def test_score_explains_sparse_metadata(self):
        profile = build_preference_profile(self.session)
        item = AuthorCatalogBook(title="An Unmapped Work")
        result = score_catalog_item(profile, item, 2)
        self.assertIn("limited catalog metadata", result["score_reason"])


if __name__ == "__main__":
    unittest.main()
