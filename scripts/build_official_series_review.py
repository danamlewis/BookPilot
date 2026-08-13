#!/usr/bin/env python3
"""Map official online series orders to local reads and recommendations."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.models import get_session, init_db
from src.series_review import (
    build_official_series_rows,
    load_official_series_reference,
    load_visible_recommendation_candidates,
)


FIELDS = (
    "series", "series_number", "book", "already_read", "recommendation_formats",
    "local_title", "match", "source_url",
)


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/bookpilot.db")
    parser.add_argument("--reference", required=True, help="Ordered-series JSON reference file.")
    parser.add_argument("--output-dir", default="outputs/official-series-review")
    parser.add_argument("--author", action="append", dest="authors", help="Exact author name; repeat as needed.")
    args = parser.parse_args()

    reference = load_official_series_reference(args.reference)
    session = get_session(init_db(args.db))
    try:
        candidates = load_visible_recommendation_candidates(session)
        rows_by_author = build_official_series_rows(session, candidates, reference, args.authors)
    finally:
        session.close()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for author, rows in rows_by_author.items():
        path = output_dir / f"{safe_filename(author)}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"{author}: {len(rows)} matched series titles -> {path}")

    manifest = output_dir / "official-series-review.json"
    with manifest.open("w", encoding="utf-8") as handle:
        json.dump(rows_by_author, handle, ensure_ascii=False, indent=2)
    print(f"Workbook manifest -> {manifest}")
    print("No books were marked read and the database was not changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
