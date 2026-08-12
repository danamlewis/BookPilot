import unittest
import tempfile

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Author, AuthorCatalogBook, Base, Book, Recommendation
from src.personal_language_cleanup import (
    apply_flags,
    collect_candidates,
    delete_catalog_candidates,
    get_training_examples,
    run_personalized_language_cleanup,
)
from src.personal_language_review import PersonalLanguageModel
from src.deduplication.language_detection import detect_non_english_title
from src.recommend import recommend_audiobooks, recommend_new_books


class PersonalLanguageReviewTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_exact_flagged_title_is_high_confidence(self):
        model = PersonalLanguageModel(
            [("Author", "El ejemplo desconocido")],
            ["An English Counterexample"],
        )
        result = model.assess("El ejemplo desconocido")
        self.assertEqual(result.confidence, "high")

    def test_ambiguous_english_die_is_not_high_confidence(self):
        examples = [
            ("A", "Die erste Beispielgeschichte"),
            ("B", "Die zweite Beispielgeschichte"),
            ("C", "Die dritte Beispielgeschichte"),
        ]
        model = PersonalLanguageModel(examples, ["When Examples Die", "We Die in Sample Text"])
        self.assertNotEqual(model.assess("When Examples Die").confidence, "high")

    def test_language_name_must_not_be_substring(self):
        model = PersonalLanguageModel([], ["Stories from the Cornish Coast"])
        self.assertNotEqual(
            model.assess("Stories from the Cornish Coast (Cornish Coast Series)").confidence,
            "high",
        )

    def test_ocr_question_mark_is_not_spanish_punctuation(self):
        model = PersonalLanguageModel([], ["Example Chronicle"])
        self.assertNotEqual(model.assess("Example Chronicle (1159¿81)").confidence, "high")

    def test_false_legacy_warnings_are_not_candidates(self):
        model = PersonalLanguageModel([], [
            "Stories from the Cornish Coast",
            "An English Example",
            "A Sample Chronicle",
        ])
        for title in (
            "Stories from the Cornish Coast (Cornish Coast Series, Book 8)",
            "An English Example Lib/E",
            "A Sample Chronicle (the Russians Book #3)",
        ):
            self.assertEqual(model.assess(title).confidence, "none", title)

    def test_legacy_detector_does_not_flag_known_false_positives(self):
        for title in (
            "Stories from the Cornish Coast (Cornish Coast Series, Book 8)",
            "An English Example Lib/E",
            "A Sample Chronicle (the Russians Book #3)",
            "When Examples Die",
        ):
            detected, reasons = detect_non_english_title(title)
            self.assertFalse(detected, f"{title}: {reasons}")

    def test_automated_flags_are_not_reused_as_training_examples(self):
        self.session.add_all([
            Recommendation(
                title="El ejemplo desconocido", author="Human", format="ebook",
                non_english=True, language_flag_source="manual",
            ),
            Recommendation(
                title="Die falsche Beispielspur", author="Robot", format="catalog",
                non_english=True, language_flag_source="personalized_high",
            ),
        ])
        self.session.commit()
        self.assertEqual(get_training_examples(self.session), [("Human", "El ejemplo desconocido")])

    def test_auto_cleanup_applies_high_confidence_and_records_provenance(self):
        author = Author(name="Catalog Author", normalized_name="Catalog Author")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            Book(title="An English Counterexample", author="English Author", format="ebook"),
            Recommendation(
                title="El ejemplo desconocido", author="Training Author", format="ebook",
                non_english=True, language_flag_source="manual",
            ),
            AuthorCatalogBook(
                author_id=author.id, title="El ejemplo desconocido", is_read=False,
            ),
        ])
        self.session.commit()

        with tempfile.TemporaryDirectory() as report_directory:
            result = run_personalized_language_cleanup(
                self.session,
                report_directory=report_directory,
            )

        self.assertEqual(result["high_count"], 1)
        self.assertEqual(result["catalog_rows_deleted"], 1)
        suppression = self.session.query(Recommendation).filter_by(
            author="Catalog Author", title="El ejemplo desconocido"
        ).one()
        self.assertTrue(suppression.non_english)
        self.assertEqual(suppression.language_flag_source, "personalized_high")

    def test_medium_confidence_candidate_is_report_only(self):
        author = Author(name="Review Author", normalized_name="Review Author")
        self.session.add(author)
        self.session.flush()
        catalog_book = AuthorCatalogBook(
            author_id=author.id,
            title="Candidate for human review",
            is_read=False,
        )
        self.session.add(catalog_book)
        self.session.commit()
        candidate = {
            "catalog_book_id": catalog_book.id,
            "author": author.name,
            "title": catalog_book.title,
            "isbn": "",
            "confidence": "medium",
            "score": 2.5,
            "reasons": "learned phrase: 'example phrase'",
        }

        flagged, created = apply_flags(self.session, [candidate], minimum_confidence="high")
        deleted = delete_catalog_candidates(self.session, [candidate], minimum_confidence="high")

        self.assertEqual((flagged, created, deleted), (0, 0, 0))
        self.assertIsNotNone(self.session.get(AuthorCatalogBook, catalog_book.id))
        self.assertEqual(self.session.query(Recommendation).count(), 0)

    def test_catalog_suppression_applies_to_both_formats(self):
        author = Author(name="Example Author", normalized_name="Example Author")
        self.session.add(author)
        self.session.flush()
        self.session.add_all([
            Book(title="An English Audiobook", author="Example Author", format="audiobook"),
            Book(title="An English Ebook", author="Example Author", format="ebook"),
            AuthorCatalogBook(author_id=author.id, title="El ejemplo desconocido", is_read=False),
            Recommendation(
                title="El ejemplo desconocido",
                author="Training Author",
                format="ebook",
                non_english=True,
            ),
        ])
        self.session.commit()

        model = PersonalLanguageModel(
            [("Training Author", "El ejemplo desconocido")],
            ["An English Audiobook", "An English Ebook"],
        )
        candidates = collect_candidates(self.session, model)
        apply_flags(self.session, candidates)

        audio_titles = {item["title"] for item in recommend_audiobooks(self.session)}
        ebook_groups = recommend_new_books(self.session)
        ebook_titles = {item["title"] for group in ebook_groups.values() for item in group}
        self.assertNotIn("El ejemplo desconocido", audio_titles)
        self.assertNotIn("El ejemplo desconocido", ebook_titles)

        deleted = delete_catalog_candidates(self.session, candidates)
        self.assertEqual(deleted, 1)
        self.assertEqual(self.session.query(AuthorCatalogBook).count(), 0)


if __name__ == "__main__":
    unittest.main()
