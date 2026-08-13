#!/usr/bin/env python3
"""Apply approved within-author duplicate clusters from a review CSV.

Automatic-tier rows are eligible by default. Review-tier rows require a
``review_decision`` value such as ``merge`` unless the explicit
``--include-unreviewed-review`` override is supplied. Protected relationships
are validated after pairwise matches are collapsed into connected components.
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "bookpilot.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--report",
        type=Path,
        required=True,
        help="Review CSV to apply.",
    )
    parser.add_argument(
        "--include-unreviewed-review",
        action="store_true",
        help="Also merge blank review-tier rows. Use only after reviewing the entire report.",
    )
    parser.add_argument("--audit", type=Path, help="Output path for the merge audit CSV.")
    parser.add_argument("--execute", action="store_true", help="Commit changes; otherwise only preview them")
    return parser.parse_args()


def connected_components(edges: list[tuple[int, int]]) -> list[list[int]]:
    neighbors: dict[int, set[int]] = defaultdict(set)
    for first, second in edges:
        neighbors[first].add(second)
        neighbors[second].add(first)
    components = []
    unseen = set(neighbors)
    while unseen:
        seed = min(unseen)
        stack = [seed]
        component = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(neighbors[current] - component)
        unseen -= component
        components.append(sorted(component))
    return sorted(components, key=lambda values: (values[0], len(values)))


def select_approved_rows(rows: list[dict], include_unreviewed_review: bool = False) -> list[dict]:
    merge_values = {"approve", "approved", "merge", "yes", "y"}
    approved = []
    for row in rows:
        decision = (row.get("review_decision") or "").strip().casefold()
        if row.get("tier") == "auto" and decision not in {"keep", "keep separate", "no", "n"}:
            approved.append(row)
        elif row.get("tier") == "review" and (
            decision in merge_values or include_unreviewed_review
        ):
            approved.append(row)
    return approved


def protected_component_conflicts(rows: list[dict], components: list[list[int]]) -> list[dict]:
    component_by_id = {
        book_id: index
        for index, component in enumerate(components)
        for book_id in component
    }
    conflicts = []
    for row in rows:
        decision = (row.get("review_decision") or "").strip().casefold()
        protected = row.get("tier") == "never" or decision in {"keep", "keep separate", "no", "n"}
        if not protected:
            continue
        first = int(row["keep_catalog_book_id"])
        second = int(row["review_catalog_book_id"])
        if component_by_id.get(first) is not None and component_by_id.get(first) == component_by_id.get(second):
            conflicts.append(row)
    return conflicts


def backup_database(database: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = database.with_name(f"{database.stem}_before_dedupe_{timestamp}{database.suffix}")
    source = sqlite3.connect(database)
    target = sqlite3.connect(backup_path)
    try:
        with target:
            source.backup(target)
    finally:
        target.close()
        source.close()
    return backup_path


def title_penalty(title: str) -> tuple[int, int, str]:
    lowered = title.casefold()
    penalty = 0
    penalty += 20 * bool(re.search(r"\bby\b.+\(\d{4}-\d{2}-\d{2}\)\s*$", title, re.I))
    penalty += 15 * bool(re.search(r"\b(?:reissue|large print|unabridged|abridged|hardcover)\b", title, re.I))
    penalty += 25 * bool(re.search(r"written by.+\b\d{4}\s+edition\b", title, re.I))
    penalty += 10 * bool(re.search(r"\[\d+/\d+\]\s*$", title))
    return penalty, len(title), lowered


def record_score(row: sqlite3.Row) -> tuple[int, int, int]:
    metadata = (
        5 * bool(row["description"])
        + 3 * bool(row["categories"])
        + 3 * bool(row["open_library_key"])
        + 2 * bool(row["isbn"])
        + bool(row["publication_date"])
    )
    clean_penalty = title_penalty(row["title"])[0]
    return metadata - clean_penalty, -len(row["title"]), -row["id"]


def merge_component(connection: sqlite3.Connection, ids: list[int]) -> dict[str, object]:
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"SELECT * FROM author_catalog_books WHERE id IN ({placeholders}) ORDER BY id", ids
    ).fetchall()
    if len(rows) != len(ids):
        found = {row["id"] for row in rows}
        raise RuntimeError(f"Missing catalog IDs: {sorted(set(ids) - found)}")
    if len({row["author_id"] for row in rows}) != 1:
        raise RuntimeError(f"Cross-author component refused: {ids}")

    keeper = max(rows, key=record_score)
    clean_title = min((row["title"] for row in rows), key=title_penalty)
    removed_ids = sorted(row["id"] for row in rows if row["id"] != keeper["id"])

    merge_fields = (
        "isbn", "publication_date", "series_name", "series_position",
        "open_library_key", "google_books_id", "description", "categories",
        "matched_book_id",
    )
    updates: dict[str, object] = {"title": clean_title}
    for field in merge_fields:
        if not keeper[field]:
            donor = next((row[field] for row in rows if row[field]), None)
            if donor is not None:
                updates[field] = donor

    formats = {row["format_available"] for row in rows if row["format_available"] not in (None, "unknown")}
    if "both" in formats or formats == {"ebook", "audiobook"}:
        updates["format_available"] = "both"
    elif keeper["format_available"] in (None, "unknown") and formats:
        updates["format_available"] = sorted(formats)[0]
    updates["is_read"] = int(any(bool(row["is_read"]) for row in rows))

    assignments = ", ".join(f"{field} = ?" for field in updates)
    connection.execute(
        f"UPDATE author_catalog_books SET {assignments} WHERE id = ?",
        [*updates.values(), keeper["id"]],
    )
    if removed_ids:
        removed_placeholders = ",".join("?" for _ in removed_ids)
        connection.execute(
            f"UPDATE recommendations SET catalog_book_id = ? WHERE catalog_book_id IN ({removed_placeholders})",
            [keeper["id"], *removed_ids],
        )
        connection.execute(
            f"DELETE FROM author_catalog_books WHERE id IN ({removed_placeholders})", removed_ids
        )
    return {
        "keeper_id": keeper["id"],
        "title": clean_title,
        "removed_ids": ";".join(map(str, removed_ids)),
        "cluster_size": len(ids),
    }


def main() -> None:
    args = parse_args()
    with args.report.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    approved = select_approved_rows(rows, args.include_unreviewed_review)
    edges = [
        (int(row["keep_catalog_book_id"]), int(row["review_catalog_book_id"]))
        for row in approved
    ]
    components = connected_components(edges)
    conflicts = protected_component_conflicts(rows, components)
    if conflicts:
        examples = "; ".join(
            f"{row['keep_title']} <> {row['review_title']}" for row in conflicts[:3]
        )
        raise SystemExit(
            f"Refusing to merge {len(conflicts)} protected relationship(s) bridged by a component: {examples}"
        )

    backup_path = backup_database(args.database) if args.execute else None

    connection = sqlite3.connect(args.database)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    audit = []
    try:
        connection.execute("BEGIN IMMEDIATE" if args.execute else "BEGIN")
        for component in components:
            audit.append(merge_component(connection, component))
        if not args.execute:
            connection.rollback()
            print(f"Dry run: {len(components)} clusters; {sum(len(c) - 1 for c in components)} rows would be removed")
            return
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    audit_path = args.audit or args.report.with_name(args.report.stem + "_merge_audit.csv")
    with audit_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=("keeper_id", "title", "removed_ids", "cluster_size"))
        writer.writeheader()
        writer.writerows(audit)
    print(f"Merged {len(components)} clusters; removed {sum(len(c) - 1 for c in components)} duplicate rows")
    print(f"Backup: {backup_path}")
    print(f"Audit: {audit_path}")


if __name__ == "__main__":
    main()
