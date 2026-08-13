import unittest

from scripts.apply_within_author_dedupe_review import (
    connected_components,
    protected_component_conflicts,
    select_approved_rows,
)


def row(tier, first, second, decision=""):
    return {
        "tier": tier,
        "keep_catalog_book_id": str(first),
        "review_catalog_book_id": str(second),
        "keep_title": f"Candidate {first}",
        "review_title": f"Candidate {second}",
        "review_decision": decision,
    }


class ApplyWithinAuthorDedupeReviewTests(unittest.TestCase):
    def test_default_requires_review_tier_approval(self):
        rows = [
            row("auto", 1, 2),
            row("review", 3, 4),
            row("review", 5, 6, "merge"),
            row("never", 7, 8, "merge"),
        ]

        selected = select_approved_rows(rows)

        self.assertEqual([(item["tier"], item["keep_catalog_book_id"]) for item in selected], [
            ("auto", "1"),
            ("review", "5"),
        ])

    def test_explicit_override_includes_unreviewed_review_rows(self):
        selected = select_approved_rows([row("review", 1, 2)], include_unreviewed_review=True)
        self.assertEqual(len(selected), 1)

    def test_protected_relationship_blocks_transitive_bridge(self):
        rows = [
            row("review", 1, 2, "merge"),
            row("review", 2, 3, "merge"),
            row("never", 1, 3),
        ]
        components = connected_components([(1, 2), (2, 3)])

        conflicts = protected_component_conflicts(rows, components)

        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["tier"], "never")


if __name__ == "__main__":
    unittest.main()
