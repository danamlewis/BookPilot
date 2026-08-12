#!/usr/bin/env python3
"""Cross-check audiobook and ebook recommendations against all read works."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.history_crosscheck import cross_check_read_audiobooks
from src.models import get_session, init_db


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=str(Path(__file__).parent.parent / "data" / "bookpilot.db"),
        help="Path to the BookPilot SQLite database",
    )
    parser.add_argument(
        "--author",
        action="append",
        dest="authors",
        help="Only check this author (repeat for multiple authors)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report matches without changing the database",
    )
    parser.add_argument(
        "--show-matches",
        action="store_true",
        help="Print every matched catalog/history title pair",
    )
    args = parser.parse_args()

    session = get_session(init_db(args.db))
    try:
        result = cross_check_read_audiobooks(
            session,
            author_names=args.authors,
            apply_changes=not args.dry_run,
        )
    finally:
        session.close()

    mode = "Dry run" if args.dry_run else "Cross-check complete"
    print(f"{mode}: {result['authors_checked']} authors checked")
    print(f"  Matching catalog editions: {len(result['matches'])}")
    print(f"  Catalog books marked read: {result['catalog_updates']}")
    print(f"  Saved recommendations marked read: {result['recommendation_updates']}")
    print(f"  Removed from Books to Read: {result['removed_thumbs_up']}")

    if args.show_matches:
        for match in result["matches"]:
            status = "update" if match["changed"] else "already linked"
            print(
                f"  - {match['author']}: {match['catalog_title']}"
                f" <- {match['history_title']} ({match['history_format']}; {status})"
            )


if __name__ == "__main__":
    main()
