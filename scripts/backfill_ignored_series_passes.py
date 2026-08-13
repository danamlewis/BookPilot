#!/usr/bin/env python3
"""Pass current recommendations belonging to already-ignored Hardcover series."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.api.hardcover import HardcoverClient
from src.local_env import load_local_env
from src.models import Recommendation, get_session, init_db
from src.series_reconciliation import (
    load_ignored_series,
    match_series_recommendations,
    pass_titles_for_both_formats,
    record_ignored_series_passes,
)
from src.series_review import load_visible_recommendation_candidates
from src.series_review import ReviewCandidate


def load_passed_candidates(session):
    combined = {}
    for row in session.query(Recommendation).filter(Recommendation.thumbs_down.is_(True)).all():
        if not row.title or not row.author:
            continue
        key = (row.author.casefold().strip(), row.title.casefold().strip())
        candidate = combined.setdefault(key, ReviewCandidate(row.author, row.title, set(), row.catalog_book_id))
        if row.format:
            candidate.formats.add(row.format)
    return list(combined.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/bookpilot.db")
    parser.add_argument("--apply", action="store_true", help="Persist Pass feedback; default is preview only.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    load_local_env(project_root / ".env.local")
    client = HardcoverClient(os.environ.get("HARDCOVER_API_TOKEN", ""))
    session = get_session(init_db(args.db))
    try:
        ignored = load_ignored_series(session)
        candidates = load_visible_recommendation_candidates(session)
        external_by_id = {
            int(series["hardcover_series_id"]): series
            for series in client.get_series_by_ids(row["series_id"] for row in ignored)
        }
        total = 0
        for row in ignored:
            series_id = int(row["series_id"])
            matches = match_series_recommendations(
                external_by_id.get(series_id, {}), candidates, str(row.get("author") or ""),
            )
            titles = [match["title"] for match in matches]
            total += len(titles)
            print(f'{row.get("author")} — {row.get("name")}: {len(titles)} current recommendation(s)')
            for match in matches:
                print(f'  - {match["title"]} ({", ".join(match["formats"])})')
            if args.apply:
                pass_titles_for_both_formats(
                    session, author=str(row.get("author") or ""), titles=titles,
                )
        if args.apply:
            passed_candidates = load_passed_candidates(session)
            for row in ignored:
                series_id = int(row["series_id"])
                all_passed_matches = match_series_recommendations(
                    external_by_id.get(series_id, {}), passed_candidates,
                    str(row.get("author") or ""),
                )
                record_ignored_series_passes(
                    session,
                    series_id=series_id,
                    titles=[match["title"] for match in all_passed_matches],
                )
        print(f'{"Passed" if args.apply else "Would pass"} {total} title(s) across {len(ignored)} ignored series.')
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
