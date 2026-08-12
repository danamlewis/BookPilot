#!/usr/bin/env python3
"""Review catalog titles using the user's manually flagged language examples."""

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import Book, get_session, init_db
from src.personal_language_review import PersonalLanguageModel
from src.personal_language_cleanup import (
    CONFIDENCE_ORDER,
    apply_flags,
    collect_candidates,
    delete_catalog_candidates,
    get_training_examples,
    write_report,
)


def load_report(path):
    """Load an exact prior review report for deterministic application."""
    candidates = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row["catalog_book_id"] = int(row["catalog_book_id"])
            row["score"] = float(row["score"])
            candidates.append(row)
    return candidates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(__file__).parent.parent / "data" / "bookpilot.db"))
    parser.add_argument("--execute", action="store_true", help="Apply high-confidence flags")
    parser.add_argument(
        "--delete-catalog",
        action="store_true",
        help="Delete applied candidates from the author catalog",
    )
    parser.add_argument("--include-medium", action="store_true", help="Also apply medium-confidence flags")
    parser.add_argument("--author", help="Only review one author")
    parser.add_argument("--limit", type=int, default=100, help="Maximum candidates printed")
    parser.add_argument("--output", help="CSV report path")
    parser.add_argument(
        "--input-report",
        help="Apply exactly the candidates in an existing CSV review report",
    )
    args = parser.parse_args()

    session = get_session(init_db(args.db))
    try:
        flagged_examples = get_training_examples(session)
        english_titles = [book.title for book in session.query(Book).all() if book.title]
        model = PersonalLanguageModel(flagged_examples, english_titles)
        candidates = (
            load_report(args.input_report)
            if args.input_report
            else collect_candidates(session, model, args.author)
        )

        high_count = sum(item["confidence"] == "high" for item in candidates)
        medium_count = sum(item["confidence"] == "medium" for item in candidates)
        if args.input_report:
            print(f"Loaded exact review report: {args.input_report}")
        else:
            print(f"Learned from {len(flagged_examples)} manually flagged titles")
            print(f"Learned {len(model.learned_features)} recurring language words/phrases")
        print(f"Catalog candidates: {high_count} high confidence, {medium_count} medium confidence")

        if not args.input_report or args.output:
            output_path = Path(args.output) if args.output else Path(args.db).parent / (
                f"non_english_review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            write_report(candidates, output_path)
            print(f"Review report: {output_path}")

        for candidate in candidates[:args.limit]:
            print(
                f"[{candidate['confidence'].upper():6}] {candidate['author']} — {candidate['title']}"
                f"\n         {candidate['reasons']}"
            )

        if args.execute:
            minimum = "medium" if args.include_medium else "high"
            source = "review_approved" if args.include_medium or args.input_report else "personalized_high"
            rows_flagged, rows_created = apply_flags(session, candidates, minimum, source=source)
            rows_deleted = 0
            if args.delete_catalog:
                rows_deleted = delete_catalog_candidates(session, candidates, minimum)
            print(f"Applied {minimum}-or-higher results")
            print(f"  Existing recommendation rows flagged: {rows_flagged}")
            print(f"  Catalog suppression rows created: {rows_created}")
            print(f"  Catalog rows deleted: {rows_deleted}")
        else:
            print("Dry run only; use --execute to apply high-confidence results.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
