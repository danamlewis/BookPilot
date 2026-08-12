"""Detect and remove multi-book packages from recommendations and catalogs."""

import re

from .models import AuthorCatalogBook, Recommendation


COLLECTION_PATTERNS = (
    ("box set", re.compile(r"\bbox\s+set\b", re.IGNORECASE)),
    ("boxed set", re.compile(r"\bboxed\s+set\b", re.IGNORECASE)),
    ("bundle", re.compile(r"\bbundles?\b", re.IGNORECASE)),
    (
        "book range",
        re.compile(r"\bbooks\s+\d+\s*(?:-|–|—|to)\s*\d+\b", re.IGNORECASE),
    ),
)


def collection_title_reason(title):
    """Return the matching multi-book-package rule, or None."""
    for reason, pattern in COLLECTION_PATTERNS:
        if pattern.search(title or ""):
            return reason
    return None


def cleanup_collection_titles(session, catalog_book_ids=None, dry_run=False):
    """Remove collection packages from the catalog and saved recommendations."""
    catalog_query = session.query(AuthorCatalogBook)
    if catalog_book_ids is not None:
        catalog_query = catalog_query.filter(AuthorCatalogBook.id.in_(catalog_book_ids))

    catalog_matches = [
        row for row in catalog_query.all() if collection_title_reason(row.title)
    ]

    # Saved recommendations are cleaned globally even during an incremental
    # catalog run, since this is inexpensive and keeps the visible list clean.
    recommendation_matches = [
        row for row in session.query(Recommendation).all()
        if collection_title_reason(row.title)
    ]

    samples = []
    seen = set()
    for row in catalog_matches + recommendation_matches:
        key = ((row.author if isinstance(row, Recommendation) else ""), row.title)
        if key in seen:
            continue
        seen.add(key)
        samples.append({
            "title": row.title,
            "author": row.author if isinstance(row, Recommendation) else None,
            "reason": collection_title_reason(row.title),
        })

    if not dry_run:
        for row in recommendation_matches:
            session.delete(row)
        for row in catalog_matches:
            session.delete(row)
        session.commit()

    return {
        "catalog_removed": len(catalog_matches),
        "recommendations_removed": len(recommendation_matches),
        "samples": samples,
        "dry_run": dry_run,
    }
