"""Cross-check ebook and audiobook recommendations against all read works."""

import re
from typing import Iterable, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from .ingest import normalize_author_name, normalize_title_for_matching
from .models import Author, AuthorCatalogBook, Book, Recommendation


# Catalogs often describe the audiobook/retail edition in the title.  These
# suffixes do not make it a different book.
EDITION_SUFFIX_RE = re.compile(
    r"\s+(?:with\s+)?(?:bonus\s+material|unabridged(?:\s+edition)?|"
    r"abridged(?:\s+edition)?|audio\s*cd|compact\s+disc|cd\s+low\s+price|"
    r"low\s+price\s+cd|mass\s+market\s+paperback|large\s+print(?:\s+edition)?)\s*$",
    flags=re.IGNORECASE,
)


def normalize_work_title(title: str) -> str:
    """Return a conservative title key shared by print, ebook, and audio editions."""
    title = (title or "").replace("’", "'").replace("‘", "'")
    normalized = normalize_title_for_matching(title)
    previous = None
    while normalized and normalized != previous:
        previous = normalized
        normalized = EDITION_SUFFIX_RE.sub("", normalized).strip(" -:;,.")
    return normalized


def same_work_title(first: str, second: str) -> bool:
    """Whether two edition titles represent the same work."""
    first_key = normalize_work_title(first)
    return bool(first_key and first_key == normalize_work_title(second))


def _author_key(name: str) -> str:
    return normalize_author_name(name or "").casefold().strip()


def get_read_title_keys(db_session: Session, author: Author):
    """Return every work read by this author, regardless of source or format."""
    author_keys = {_author_key(author.name), _author_key(author.normalized_name)}
    books = db_session.query(Book).filter(
        func.lower(Book.author).in_(author_keys)
    ).all()
    marked_recommendations = db_session.query(Recommendation).filter(
        func.lower(Recommendation.author).in_(author_keys),
        Recommendation.already_read.is_(True),
    ).all()
    return {
        normalize_work_title(item.title)
        for item in [*books, *marked_recommendations]
        if item.title and normalize_work_title(item.title)
    }


def get_non_english_title_keys(db_session: Session, author: Author):
    """Return user-suppressed non-English works across every recommendation format."""
    author_keys = {_author_key(author.name), _author_key(author.normalized_name)}
    flagged = db_session.query(Recommendation).filter(
        func.lower(Recommendation.author).in_(author_keys),
        Recommendation.non_english.is_(True),
    ).all()
    return {normalize_work_title(item.title) for item in flagged if item.title}


def cross_check_read_audiobooks(
    db_session: Session,
    author_names: Optional[Iterable[str]] = None,
    apply_changes: bool = True,
):
    """
    Match catalog entries and saved recommendations to any work already read,
    regardless of whether either side is an audiobook or ebook.

    Catalog entries are marked read so freshly generated recommendations omit
    them. Existing recommendation rows in either format are marked
    ``already_read``.
    """
    requested = {_author_key(name) for name in author_names or []}
    authors = db_session.query(Author).all()
    if requested:
        authors = [
            author for author in authors
            if _author_key(author.name) in requested
            or _author_key(author.normalized_name) in requested
        ]

    matches = []
    catalog_updates = 0
    recommendation_updates = 0
    removed_thumbs_up = 0

    for author in authors:
        author_keys = {_author_key(author.name), _author_key(author.normalized_name)}
        history_books = db_session.query(Book).filter(
            func.lower(Book.author).in_(author_keys)
        ).all()
        marked_read = db_session.query(Recommendation).filter(
            func.lower(Recommendation.author).in_(author_keys),
            Recommendation.already_read.is_(True),
        ).all()
        if not history_books and not marked_read:
            continue

        read_by_title = {}
        for item in [*history_books, *marked_read]:
            key = normalize_work_title(item.title)
            if key:
                read_by_title.setdefault(key, item)

        catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
        for catalog_book in catalog_books:
            read_item = read_by_title.get(normalize_work_title(catalog_book.title))
            if not read_item:
                continue

            matched_book_id = read_item.id if isinstance(read_item, Book) else None
            changed = not catalog_book.is_read or (
                matched_book_id is not None
                and catalog_book.matched_book_id != matched_book_id
            )
            matches.append({
                "author": author.name,
                "catalog_title": catalog_book.title,
                "history_title": read_item.title,
                "history_format": read_item.format,
                "changed": changed,
            })
            if changed:
                catalog_updates += 1
                if apply_changes:
                    catalog_book.is_read = True
                    if matched_book_id is not None:
                        catalog_book.matched_book_id = matched_book_id

        saved_recommendations = db_session.query(Recommendation).filter(
            func.lower(Recommendation.author).in_(author_keys),
        ).all()
        for recommendation in saved_recommendations:
            if normalize_work_title(recommendation.title) not in read_by_title:
                continue
            if not recommendation.already_read:
                recommendation_updates += 1
                if apply_changes:
                    recommendation.already_read = True
            if recommendation.thumbs_up:
                removed_thumbs_up += 1
                if apply_changes:
                    recommendation.thumbs_up = False

    if apply_changes:
        db_session.commit()

    return {
        "matches": matches,
        "catalog_updates": catalog_updates,
        "recommendation_updates": recommendation_updates,
        "removed_thumbs_up": removed_thumbs_up,
        "authors_checked": len(authors),
    }
