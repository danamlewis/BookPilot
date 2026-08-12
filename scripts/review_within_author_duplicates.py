#!/usr/bin/env python3
"""Generate a review-only CSV of within-author duplicate candidates."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime
from itertools import combinations
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.deduplication.within_author import assess_duplicate_pair, choose_keeper
from src.models import Author, AuthorCatalogBook, get_session, init_db
from scripts.review_low_score_non_english import collect_recommendation_rows


TIER_ORDER = {"auto": 3, "review": 2, "never": 1}


def visible_books_by_author(session, author_names):
    rows = collect_recommendation_rows(session, threshold=101, author_filter=None, excluded_authors=set())
    requested = {name.casefold() for name in author_names}
    grouped = {}
    for row in rows:
        author = row["author"]
        if author.name.casefold() not in requested:
            continue
        grouped.setdefault(author.id, {"author": author, "books": {}})
        grouped[author.id]["books"][row["book"].id] = row["book"]
    return grouped


def build_review_rows(session, author_names, include_never=True):
    grouped = visible_books_by_author(session, author_names)
    output = []
    related_edges = []
    group_number = 0
    for data in grouped.values():
        author = data["author"]
        books = list(data["books"].values())
        for first, second in combinations(books, 2):
            assessment = assess_duplicate_pair(
                first.title, second.title,
                isbn_a=first.isbn, isbn_b=second.isbn,
                work_key_a=first.open_library_key, work_key_b=second.open_library_key,
            )
            if assessment.tier == "unrelated" or (assessment.tier == "never" and not include_never):
                continue
            if assessment.tier in {"auto", "review"}:
                related_edges.append((author.id, first.id, second.id))
            group_number += 1
            keeper = choose_keeper((first, second))
            other = second if keeper.id == first.id else first
            output.append({
                "rank": 0,
                "candidate_id": group_number,
                "duplicate_cluster": "",
                "duplicate_cluster_size": 0,
                "tier": assessment.tier,
                "confidence": assessment.confidence,
                "author": author.name,
                "visible_author_book_count": len(books),
                "keep_catalog_book_id": keeper.id,
                "keep_title": keeper.title,
                "keep_isbn": keeper.isbn or "",
                "keep_open_library_key": keeper.open_library_key or "",
                "review_catalog_book_id": other.id,
                "review_title": other.title,
                "review_isbn": other.isbn or "",
                "review_open_library_key": other.open_library_key or "",
                "reason_codes": "; ".join(assessment.reason_codes),
                "explanation": assessment.explanation,
                "title_similarity": f"{assessment.title_similarity:.3f}",
                "token_jaccard": f"{assessment.token_jaccard:.3f}",
                "normalized_keep_title": (
                    assessment.normalized_title_a if keeper.id == first.id else assessment.normalized_title_b
                ),
                "normalized_review_title": (
                    assessment.normalized_title_b if keeper.id == first.id else assessment.normalized_title_a
                ),
                "review_decision": "",
                "review_notes": "",
            })

    # Label transitive candidate families without automatically collapsing them.
    parent = {}
    def find(node):
        parent.setdefault(node, node)
        if parent[node] != node:
            parent[node] = find(parent[node])
        return parent[node]
    def union(first, second):
        first_root, second_root = find(first), find(second)
        if first_root != second_root:
            parent[second_root] = first_root
    for author_id, first_id, second_id in related_edges:
        union((author_id, first_id), (author_id, second_id))
    members = Counter(find(node) for node in parent)
    cluster_labels = {
        root: f"D{index:03d}"
        for index, root in enumerate(sorted(members, key=lambda node: (node[0], node[1])), 1)
    }
    author_ids = {author.name: author.id for data in grouped.values() for author in (data["author"],)}
    for row in output:
        if row["tier"] not in {"auto", "review"}:
            continue
        node = (author_ids[row["author"]], row["keep_catalog_book_id"])
        root = find(node)
        row["duplicate_cluster"] = cluster_labels[root]
        row["duplicate_cluster_size"] = members[root]
    output.sort(key=lambda row: (
        -TIER_ORDER[row["tier"]], -row["confidence"],
        row["author"].casefold(), row["keep_title"].casefold(), row["review_title"].casefold(),
    ))
    for rank, row in enumerate(output, 1):
        row["rank"] = rank
    return output, grouped


def write_report(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else ["rank", "candidate_id", "tier", "confidence", "author"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(__file__).resolve().parents[1] / "data" / "bookpilot.db"))
    parser.add_argument(
        "--author",
        action="append",
        help="Exact author name; repeat for multiple authors. If omitted, read one author per line from stdin.",
    )
    parser.add_argument("--exclude-never", action="store_true", help="Omit explicit do-not-merge examples.")
    parser.add_argument("--output")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    authors = tuple(args.author or ())
    if not authors and not sys.stdin.isatty():
        authors = tuple(line.strip() for line in sys.stdin if line.strip())
    if not authors:
        parser.error("provide --author or pipe newline-delimited author names on stdin")
    session = get_session(init_db(args.db))
    try:
        rows, grouped = build_review_rows(session, authors, include_never=not args.exclude_never)
    finally:
        session.close()
    output = Path(args.output) if args.output else Path(args.db).resolve().parent / (
        f"within_author_dedupe_review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    write_report(rows, output)
    counts = Counter(row["tier"] for row in rows)
    print("Visible authors reviewed:")
    for data in sorted(grouped.values(), key=lambda data: data["author"].name.casefold()):
        print(f"  {data['author'].name}: {len(data['books'])} unique visible books")
    print(f"Candidates: {len(rows)} | auto={counts['auto']} review={counts['review']} never={counts['never']}")
    for row in rows[:args.limit]:
        print(
            f"{row['rank']:>3}. [{row['tier'].upper():6}] {row['confidence']:>2} "
            f"{row['author']} — {row['keep_title']}  <>  {row['review_title']}"
        )
    print(f"Review CSV: {output}")
    print("No database rows or flags were changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
