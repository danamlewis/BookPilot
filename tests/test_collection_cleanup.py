import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.collection_cleanup import cleanup_collection_titles, collection_title_reason
from src.models import Author, AuthorCatalogBook, Base, Recommendation


class CollectionCleanupTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_requested_patterns(self):
        for title in (
            "The Complete Box Set",
            "The Complete Boxed Set",
            "Holiday Bundle",
            "The Series Books 1-4",
            "The Series: Books 2 – 6",
            "Books 1 to 3 Collection",
            "The Harbor Daughter [Large Print]",
            "A Novel (large print edition)",
            "Select Editions Large Type",
            "A Novel on Audio CD",
            "A Novel Book/CD",
            "A Novel on 2 CDs",
            "Reference CD-ROM Version",
            "A Novel [sound recording]",
            "Rango Audio Pack",
            "A Mystery Denied (Audio)",
            "Pearl [Unabridged]",
            "Harbor Castle - Unabridged",
            "A Novel [Hardcover]",
            "A Novel in Hardback",
            "A Novel [Paperback]",
            "A Novel - Mass Market",
            "A Novel (Library Binding Edition)",
            "Two Editions in Slipcase",
        ):
            self.assertIsNotNone(collection_title_reason(title), title)

    def test_does_not_match_ordinary_titles(self):
        for title in (
            "The Box",
            "Book 4",
            "Activity Book 8-10",
            "Books and Libraries",
            "Acid Test",
            "cd Player Stories",
            "An Unabridged History of the World",
            "Record Set Right",
        ):
            self.assertIsNone(collection_title_reason(title), title)

    def test_removes_catalog_and_saved_recommendations(self):
        author = Author(name="Example", normalized_name="Example")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            AuthorCatalogBook(author_id=author.id, title="Series Books 1-3", is_read=False),
            AuthorCatalogBook(author_id=author.id, title="A Normal Novel", is_read=False),
            Recommendation(title="Holiday Box Set", author="Example", format="ebook"),
            Recommendation(title="A Normal Novel", author="Example", format="ebook"),
        ])
        self.session.commit()

        result = cleanup_collection_titles(self.session)

        self.assertEqual(result["catalog_removed"], 1)
        self.assertEqual(result["recommendations_removed"], 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 1)
        self.assertEqual(self.session.query(Recommendation).count(), 1)


if __name__ == "__main__":
    unittest.main()
