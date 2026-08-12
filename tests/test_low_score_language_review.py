import unittest

from scripts.review_low_score_non_english import (
    CynicalTokenModel,
    confidence_for_score,
    personalized_component,
    score_language_risk,
    title_signal_score,
)


class LowScoreLanguageReviewTests(unittest.TestCase):
    def test_transliterated_title_gets_strong_title_signal(self):
        points, reasons = title_signal_score("Mikhtav mi-Ṿirtual")
        self.assertGreaterEqual(points, 40)
        self.assertTrue(any("transliteration" in reason for reason in reasons))

    def test_plain_english_title_has_no_title_signal(self):
        points, reasons = title_signal_score("The English Book Title")
        self.assertEqual(points, 0)
        self.assertEqual(reasons, [])

    def test_generic_edition_warning_is_ignored(self):
        points, reasons = personalized_component("medium", 4.6, ["learned phrase: 'edition'"])
        self.assertEqual(points, 0)
        self.assertEqual(reasons, [])

    def test_near_match_is_warning_not_high_confidence(self):
        points, reasons = personalized_component(
            "high", 8.9, ["near match to flagged title: 'Exámple Phrase'"]
        )
        self.assertEqual(points, 38)
        self.assertTrue(reasons)

    def test_single_proper_noun_is_not_learned_language_evidence(self):
        model = CynicalTokenModel(
            [("A", "SampleName"), ("B", "SampleName"), ("C", "SampleName")],
            ["An Unrelated English Example"],
        )
        points, reasons = model.assess("SampleName First Edition")
        self.assertEqual(points, 0)
        self.assertEqual(reasons, [])

    def test_single_personalized_learned_word_is_ignored(self):
        points, reasons = personalized_component("high", 6.5, ["learned phrase: 'samplename'"])
        self.assertEqual(points, 0)
        self.assertEqual(reasons, [])

    def test_prolific_author_bonus_applies_above_five(self):
        score, confidence, prolific_bonus, _, reasons = score_language_risk(
            match_score=40,
            personalized_confidence="none",
            personalized_score=0,
            personalized_reasons=[],
            learned_token_points=0,
            learned_token_reasons=[],
            title_points=0,
            title_reasons=[],
            author_low_count=12,
            author_signal_count=0,
        )
        self.assertGreater(prolific_bonus, 0)
        self.assertGreater(score, 2)
        self.assertTrue(any("titles below 50" in reason for reason in reasons))

    def test_trained_high_confidence_ranks_high(self):
        score, confidence, _, _, _ = score_language_risk(
            match_score=49,
            personalized_confidence="high",
            personalized_score=9,
            personalized_reasons=["Non-English script detected"],
            learned_token_points=0,
            learned_token_reasons=[],
            title_points=0,
            title_reasons=[],
            author_low_count=1,
            author_signal_count=1,
        )
        self.assertGreaterEqual(score, 74)
        self.assertIn(confidence, {"high", "very_high"})

    def test_confidence_boundaries(self):
        self.assertEqual(confidence_for_score(85), "very_high")
        self.assertEqual(confidence_for_score(65), "high")
        self.assertEqual(confidence_for_score(45), "medium")
        self.assertEqual(confidence_for_score(25), "low")
        self.assertEqual(confidence_for_score(24), "very_low")


if __name__ == "__main__":
    unittest.main()
