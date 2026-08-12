import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.history_crosscheck import cross_check_read_audiobooks, normalize_work_title
from src.models import Author, AuthorCatalogBook, Base, Book, Recommendation
from src.recommend import recommend_audiobooks, recommend_new_books


class AudiobookHistoryCrosscheckTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_audio_edition_suffix_matches_ebook_title(self):
        self.assertEqual(
            normalize_work_title("Sample Work with Bonus Material"),
            normalize_work_title("A Sample Work"),
        )

    def test_recommender_excludes_work_read_as_ebook(self):
        author = Author(name="Example Author", normalized_name="Example Author")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            Book(title="An Earlier Audiobook", author="Example Author", format="audiobook"),
            Book(title="A Sample Work", author="Example Author", format="ebook"),
            AuthorCatalogBook(author_id=author.id, title="Sample Work with Bonus Material", is_read=False),
            AuthorCatalogBook(author_id=author.id, title="A New Story", is_read=False),
        ])
        self.session.commit()

        titles = {rec["title"] for rec in recommend_audiobooks(self.session)}
        self.assertNotIn("Sample Work with Bonus Material", titles)
        self.assertIn("A New Story", titles)

    def test_audiobook_recommender_honors_ebook_already_read_flag(self):
        author = Author(name="Example Author", normalized_name="Example Author")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            Book(title="An Earlier Audiobook", author="Example Author", format="audiobook"),
            Recommendation(
                title="First Story/Second Story",
                author="Example Author",
                format="ebook",
                already_read=True,
            ),
            AuthorCatalogBook(
                author_id=author.id,
                title="First Story/Second Story",
                is_read=False,
            ),
        ])
        self.session.commit()

        titles = {rec["title"] for rec in recommend_audiobooks(self.session)}
        self.assertNotIn("First Story/Second Story", titles)

    def test_ebook_recommender_honors_audiobook_already_read_flag(self):
        author = Author(name="Example Author", normalized_name="Example Author")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            Book(title="An Earlier Book", author="Example Author", format="ebook"),
            Recommendation(
                title="Sample Adventure with Bonus Material",
                author="Example Author",
                format="audiobook",
                already_read=True,
            ),
            AuthorCatalogBook(
                author_id=author.id,
                title="Sample Adventure",
                is_read=False,
            ),
        ])
        self.session.commit()

        grouped = recommend_new_books(self.session)
        titles = {rec["title"] for recs in grouped.values() for rec in recs}
        self.assertNotIn("Sample Adventure", titles)

    def test_script_updates_catalog_and_saved_recommendation(self):
        author = Author(name="Another Author", normalized_name="Another Author")
        self.session.add(author)
        self.session.flush()
        read_book = Book(title="A Place to Return", author="Another Author", format="ebook")
        catalog_book = AuthorCatalogBook(
            author_id=author.id,
            title="Place to Return (Example Series Book #3)",
            is_read=False,
        )
        recommendation = Recommendation(
            title="Place to Return (Example Series Book #3)",
            author="Another Author",
            format="audiobook",
            already_read=False,
            thumbs_up=True,
        )
        self.session.add_all([read_book, catalog_book, recommendation])
        self.session.commit()

        result = cross_check_read_audiobooks(self.session)

        self.assertEqual(result["catalog_updates"], 1)
        self.assertTrue(catalog_book.is_read)
        self.assertEqual(catalog_book.matched_book_id, read_book.id)
        self.assertTrue(recommendation.already_read)
        self.assertFalse(recommendation.thumbs_up)


if __name__ == "__main__":
    unittest.main()
