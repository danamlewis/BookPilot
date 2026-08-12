#!/usr/bin/env python3
"""Remove authors and catalog/list data unsupported by imported reading history."""

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.catalog import purge_historyless_authors
from src.models import get_session, init_db


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/bookpilot.db")
    parser.add_argument("--execute", action="store_true", help="Apply the deletion")
    parser.add_argument("--show-authors", action="store_true")
    parser.add_argument("--output", help="Write the targeted authors and row counts to CSV")
    args = parser.parse_args()

    session = get_session(init_db(args.db))
    try:
        result = purge_historyless_authors(session, dry_run=not args.execute)
    finally:
        session.close()

    action = "Removed" if args.execute else "Would remove"
    print(f"{action} {result['authors_removed']} no-history authors")
    print(f"{action} {result['catalog_rows_removed']} catalog rows")
    print(f"{action} {result['recommendations_removed']} recommendation/list rows")
    print(f"{action} {result['series_rows_removed']} series rows")
    if args.show_authors:
        for name in result["author_names"]:
            print(f"  - {name}")
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            fields = ("author_id", "author", "catalog_rows", "recommendation_rows", "series_rows")
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(result["author_details"])
        print(f"Wrote review CSV: {output_path}")
    if not args.execute:
        print("Dry run only; use --execute to apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
