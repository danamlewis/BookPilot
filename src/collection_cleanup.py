"""Detect and remove unwanted packages/editions from recommendations and catalogs."""

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
    ("large print/type", re.compile(r"\blarge\s+(?:print|type)\b", re.IGNORECASE)),
    # Keep this case-sensitive and require token boundaries. This catches
    # "Audio CD", "Book/CD", "CD-ROM", and "2 CDs" without matching the
    # letters "cd" inside an ordinary word.
    ("cd edition", re.compile(r"\bCD(?:s|-ROM)?\b")),
    (
        "audio-media listing",
        re.compile(
            r"\bsound\s+recording\b|\baudio\s+pack\b|[([]\s*audio\s*[)\]]",
            re.IGNORECASE,
        ),
    ),
    # Limit abridgement markers to edition-like suffixes. This avoids matching
    # an ordinary work whose actual title begins with words such as
    # "An Unabridged History...".
    (
        "abridged edition marker",
        re.compile(
            r"(?:[-–—/]\s*|[([]\s*)(?:unabridged|abridged)\s*[)\]]?\s*$",
            re.IGNORECASE,
        ),
    ),
    (
        "physical-format listing",
        re.compile(
            r"\b(?:hardcover|hardback|paperback|mass\s+market|"
            r"library\s+binding|slipcase)\b",
            re.IGNORECASE,
        ),
    ),
)


def collection_title_reason(title):
    """Return the matching excluded-edition/package rule, or None."""
    for reason, pattern in COLLECTION_PATTERNS:
        if pattern.search(title or ""):
            return reason
    return None


def cleanup_collection_titles(session, catalog_book_ids=None, dry_run=False):
    """Remove excluded editions/packages from the catalog and recommendations."""
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
        is_recommendation = isinstance(row, Recommendation)
        author = row.author if is_recommendation else (row.author.name if row.author else None)
        source = "recommendation" if is_recommendation else "catalog"
        key = (source, row.id)
        if key in seen:
            continue
        seen.add(key)
        samples.append({
            "source": source,
            "id": row.id,
            "title": row.title,
            "author": author,
            "format": row.format if is_recommendation else None,
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
