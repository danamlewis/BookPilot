#!/usr/bin/env python3
"""Create a read-only CSV of recommendations likely read before Libby tracking."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models import get_session, init_db
from src.series_review import build_series_review_rows, load_visible_recommendation_candidates


FIELDS = (
    "author", "author_remaining_recommendations", "title", "recommendation_formats",
    "inferred_series", "inferred_position", "publication_date",
    "prior_read_likelihood", "known_read_anchors", "series_evidence",
    "boundary_evidence", "suggested_action", "your_decision",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/bookpilot.db")
    parser.add_argument("--output", default="data/prior-series-read-review.csv")
    parser.add_argument(
        "--author", action="append", dest="authors",
        help="Restrict to an exact author name; repeat for multiple authors.",
    )
    parser.add_argument(
        "--min-unread", type=int, default=5,
        help="Inspect authors with more than this many remaining recommendations (default: 5).",
    )
    args = parser.parse_args()
    if args.min_unread < 0:
        parser.error("--min-unread must be non-negative")

    session = get_session(init_db(args.db))
    try:
        candidates = load_visible_recommendation_candidates(session)
        rows = build_series_review_rows(
            session, candidates, min_unread=args.min_unread, author_names=args.authors,
        )
    finally:
        session.close()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    authors = len({row["author"] for row in rows})
    print(f"Wrote {len(rows)} review candidates for {authors} authors to {output}")
    print("No books were marked read and the database was not changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
