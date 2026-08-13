import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.catalog import remove_duplicate_titles
from src.models import Author, AuthorCatalogBook, Base, Recommendation


class CatalogDuplicateCleanupTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.author = Author(name="Example Writer", normalized_name="Example Writer")
        self.session.add(self.author)
        self.session.flush()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def add_catalog(self, title, **kwargs):
        book = AuthorCatalogBook(author_id=self.author.id, title=title, is_read=False, **kwargs)
        self.session.add(book)
        self.session.flush()
        return book

    def test_scoped_cleanup_compares_new_row_with_full_author_catalog(self):
        existing = self.add_catalog(
            "Harbor Lantern",
            isbn="9780000000001",
            open_library_key="/works/EXISTING",
        )
        new = self.add_catalog(
            "The Harbor Lantern",
            description="Complete description",
            categories="Fiction",
            open_library_key="/works/NEW",
        )
        saved = Recommendation(
            catalog_book_id=existing.id,
            title=existing.title,
            author=self.author.name,
            format="ebook",
        )
        self.session.add(saved)
        self.session.commit()

        result = remove_duplicate_titles(
            self.session,
            dry_run=False,
            catalog_book_ids=[new.id],
        )

        survivors = self.session.query(AuthorCatalogBook).all()
        self.assertEqual(result["catalog_duplicates_removed"], 1)
        self.assertEqual(len(survivors), 1)
        self.assertEqual(survivors[0].title, "Harbor Lantern")
        self.assertEqual(survivors[0].description, "Complete description")
        self.assertEqual(saved.catalog_book_id, survivors[0].id)
        self.assertEqual(saved.title, "Harbor Lantern")

    def test_packaging_variant_merges_automatically(self):
        self.add_catalog("Winter Road")
        self.add_catalog("Winter Road (Reissue)", publication_date="2001")
        self.session.commit()

        result = remove_duplicate_titles(self.session, dry_run=False)

        self.assertEqual(result["catalog_duplicates_removed"], 1)
        survivor = self.session.query(AuthorCatalogBook).one()
        self.assertEqual(survivor.title, "Winter Road")
        self.assertEqual(survivor.publication_date, "2001")

    def test_probable_typo_remains_for_manual_review(self):
        self.add_catalog("The Garden of Glass")
        self.add_catalog("The Garden of Glas")
        self.session.commit()

        result = remove_duplicate_titles(self.session, dry_run=False)

        self.assertEqual(result["catalog_duplicates_removed"], 0)
        self.assertGreaterEqual(result["catalog_review_candidates"], 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 2)

    def test_numbered_works_are_never_merged(self):
        self.add_catalog("Example Chronicle Volume 1")
        self.add_catalog("Example Chronicle Volume 2")
        self.session.commit()

        result = remove_duplicate_titles(self.session, dry_run=False)

        self.assertEqual(result["catalog_duplicates_removed"], 0)
        self.assertGreaterEqual(result["catalog_protected_candidates"], 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 2)

    def test_dry_run_does_not_change_rows(self):
        self.add_catalog("Quiet Harbor")
        self.add_catalog("A Quiet Harbor")
        self.session.commit()

        result = remove_duplicate_titles(self.session, dry_run=True)

        self.assertEqual(result["catalog_duplicates_found"], 1)
        self.assertEqual(result["catalog_duplicates_removed"], 0)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 2)


if __name__ == "__main__":
    unittest.main()
