import unittest

from src.deduplication.within_author import assess_duplicate_pair


class WithinAuthorDedupeTests(unittest.TestCase):
    def assertTier(self, expected, first, second, **kwargs):
        result = assess_duplicate_pair(first, second, **kwargs)
        self.assertEqual(result.tier, expected, result)

    def test_punctuation_and_packaging_can_auto_merge(self):
        self.assertTier("auto", "I.N.I.T.", "I.N.I.T ")
        self.assertTier("auto", "Sample Story", "Sample Story (Reissue)")
        self.assertTier("auto", "Example from the Past", "Example From the Past (Large Print)")

    def test_leading_article_stays_review_first(self):
        self.assertTier("review", "Hidden Path", "A Hidden Path")
        self.assertTier("review", "Sample of Honor", "A Sample of Honor")

    def test_probable_typos_stay_review_first(self):
        self.assertTier("review", "Silver Meadows", "Silver Medows")
        self.assertTier("review", "The Complete Encyclopedia of Sample Ideas", "The Complete Encylopedia of Sample Ideas")

    def test_distinct_numbered_works_never_merge(self):
        self.assertTier("never", "Untitled Example Novella 1", "Untitled Example Novella 2")
        self.assertTier("never", "Example Series vol. 1", "Example Series vol. 2")
        self.assertTier("never", "Sample Saga Volume 1", "Sample Saga Volume 2")

    def test_shared_retail_boilerplate_does_not_merge_different_books(self):
        self.assertTier(
            "unrelated",
            "First Story : Written by Example Author, 1997 Edition, Publisher",
            "Second Story : Written by Example Author, 1997 Edition, Publisher",
        )

    def test_similar_but_substantive_words_do_not_merge(self):
        self.assertTier("never", "Foundations of Macroeconomics", "Foundations of Microeconomics")

    def test_same_isbn_with_conflicting_titles_is_review_only(self):
        self.assertTier("review", "First Candidate", "Second Candidate", isbn_a="9780000000002", isbn_b="9780000000002")

    def test_colour_spelling_is_review_candidate(self):
        self.assertTier("review", "The Color of Morning", "Colour of Morning")

    def test_html_apostrophe_is_auto_safe(self):
        self.assertTier("auto", "Writer's Example", "Writer&#39;s Example")


if __name__ == "__main__":
    unittest.main()
