#!/usr/bin/env python3
"""Preview or remove excluded packages and editions from BookPilot."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.collection_cleanup import cleanup_collection_titles
from src.models import get_session, init_db


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(__file__).parent.parent / "data" / "bookpilot.db"))
    parser.add_argument("--execute", action="store_true", help="Apply deletions")
    parser.add_argument("--limit", type=int, default=100, help="Maximum matches to print")
    args = parser.parse_args()

    session = get_session(init_db(args.db))
    try:
        result = cleanup_collection_titles(session, dry_run=not args.execute)
        action = "Removed" if args.execute else "Would remove"
        print(f"{action} {result['catalog_removed']} catalog books")
        print(f"{action} {result['recommendations_removed']} saved recommendations")
        for item in result["samples"][:args.limit]:
            author = f" by {item['author']}" if item["author"] else ""
            format_name = f"/{item['format']}" if item["format"] else ""
            print(
                f"  [{item['reason']}] {item['source']}{format_name} #{item['id']}: "
                f"{item['title']}{author}"
            )
        if not args.execute:
            print("Dry run only; use --execute to apply.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
