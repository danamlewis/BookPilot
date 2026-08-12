#!/usr/bin/env python3
"""Audit BookPilot's personal-fit ranking without changing the database."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models import Author, AuthorCatalogBook, Book, get_session, init_db
from src.preference_scoring import build_preference_profile, score_catalog_item
from src.recommend import recommend_new_books


def flatten(groups):
    return [item for values in groups.values() for item in values] if isinstance(groups, dict) else groups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/bookpilot.db")
    parser.add_argument("--author", help="Limit the audit to one author (case-insensitive substring).")
    parser.add_argument("--tier", choices=("strong", "possible", "low", "batch"))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--csv", dest="csv_path", help="Optionally write the full filtered result to CSV.")
    args = parser.parse_args()

    session = get_session(init_db(args.db))
    try:
        if args.author:
            needle = args.author.casefold()
            authors = [author for author in session.query(Author).all() if needle in author.name.casefold()]
            profile = build_preference_profile(session)
            items = []
            for author in authors:
                read_count = session.query(Book).filter_by(author=author.normalized_name).count()
                for book in session.query(AuthorCatalogBook).filter_by(author_id=author.id, is_read=False).all():
                    score = score_catalog_item(profile, book, read_count)
                    items.append({"author": author.name, "title": book.title, **score})
        else:
            items = flatten(recommend_new_books(session))
        if args.tier:
            items = [item for item in items if item["interest_tier"] == args.tier]
        items.sort(key=lambda item: (-item["match_score"], item["author"].casefold(), item["title"].casefold()))

        counts = {tier: sum(item["interest_tier"] == tier for item in items) for tier in ("strong", "possible", "low", "batch")}
        print(f"Candidates: {len(items)} | strong {counts['strong']} | possible {counts['possible']} | low {counts['low']} | likely non-reads {counts['batch']}")
        for item in items[: args.limit]:
            print(f"{item['match_score']:>3}  {item['interest_label']:<16}  {item['author']} — {item['title']}")

        if args.csv_path:
            fields = ("match_score", "interest_tier", "content_type", "author", "title", "score_reason")
            with open(args.csv_path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(items)
            print(f"Wrote {len(items)} rows to {args.csv_path}")
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
