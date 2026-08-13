"""Series analysis for ebooks"""
from collections import defaultdict
from datetime import datetime, timezone
import json
from typing import List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from .models import Author, AuthorCatalogBook, Series, Book, Recommendation, SystemMetadata


PROGRESS_IGNORED_METADATA_KEY = "series_progress_ignored"


def load_ignored_progress_series(session: Session) -> List[Dict]:
    row = session.query(SystemMetadata).filter_by(key=PROGRESS_IGNORED_METADATA_KEY).first()
    if not row or not row.value:
        return []
    try:
        values = json.loads(row.value)
    except (TypeError, ValueError):
        return []
    return [value for value in values if isinstance(value, dict) and value.get("name") and value.get("author")]


def save_ignored_progress_series(session: Session, ignored: List[Dict]) -> None:
    row = session.query(SystemMetadata).filter_by(key=PROGRESS_IGNORED_METADATA_KEY).first()
    if row is None:
        row = SystemMetadata(key=PROGRESS_IGNORED_METADATA_KEY)
        session.add(row)
    row.value = json.dumps(ignored, ensure_ascii=False, separators=(",", ":"))
    row.updated_at = datetime.utcnow()
    session.commit()


def ignore_progress_series(session: Session, *, author: str, name: str) -> List[Dict]:
    ignored = load_ignored_progress_series(session)
    key = (author.casefold().strip(), name.casefold().strip())
    by_key = {
        (str(row["author"]).casefold().strip(), str(row["name"]).casefold().strip()): row
        for row in ignored
    }
    by_key[key] = {
        "author": author.strip(),
        "name": name.strip(),
        "ignored_at": datetime.now(timezone.utc).isoformat(),
    }
    output = sorted(by_key.values(), key=lambda row: (
        str(row["author"]).casefold(), str(row["name"]).casefold(),
    ))
    save_ignored_progress_series(session, output)
    return output


def restore_progress_series(session: Session, *, author: str, name: str) -> List[Dict]:
    key = (author.casefold().strip(), name.casefold().strip())
    output = [
        row for row in load_ignored_progress_series(session)
        if (str(row["author"]).casefold().strip(), str(row["name"]).casefold().strip()) != key
    ]
    save_ignored_progress_series(session, output)
    return output


def _analyze_author_series_rows(author: Author, catalog_books, recommendation_rows) -> List[Dict]:
    """Build one author's series result from already-loaded rows."""
    author_keys = {
        value.casefold().strip()
        for value in (author.name, author.normalized_name)
        if value
    }
    author_recs = [
        rec for rec in recommendation_rows
        if rec.author and rec.author.casefold().strip() in author_keys
    ]
    already_read_set = {
        rec.title.casefold().strip()
        for rec in author_recs
        if rec.title and rec.already_read
    }
    filtered_set = {
        rec.title.casefold().strip()
        for rec in author_recs
        if rec.title and (rec.thumbs_down or rec.duplicate or rec.non_english)
    }

    series_dict = {}
    for book in catalog_books:
        if not book.series_name:
            continue
        data = series_dict.setdefault(book.series_name, {
            'books': [], 'read_books': [], 'unread_books': []
        })
        data['books'].append(book)
        title_key = (book.title or '').casefold().strip()
        if book.is_read or title_key in already_read_set:
            data['read_books'].append(book)
        elif title_key not in filtered_set:
            data['unread_books'].append(book)

    results = []
    for series_name, data in series_dict.items():
        books = data['books']
        read_books = data['read_books']
        unread_books = data['unread_books']
        position_key = lambda book: book.series_position if book.series_position is not None else 999
        books.sort(key=position_key)
        read_books.sort(key=position_key)
        unread_books.sort(key=position_key)
        if not read_books and not unread_books:
            continue
        status = 'not_started' if not read_books else ('complete' if not unread_books else 'partial')
        results.append({
            'series_name': series_name,
            'author': author.name,
            'total_books': len(books),
            'books_read': len(read_books),
            'completion_pct': (len(read_books) / len(books) * 100) if books else 0,
            'status': status,
            'unread_books': [
                {'title': book.title, 'isbn': book.isbn, 'position': book.series_position,
                 'categories': book.categories}
                for book in unread_books
            ],
            'read_books': [
                {'title': book.title, 'isbn': book.isbn, 'position': book.series_position}
                for book in read_books
            ],
        })
    status_order = {'partial': 0, 'not_started': 1, 'complete': 2}
    results.sort(key=lambda item: (status_order.get(item['status'], 99), -item['completion_pct']))
    return results


def analyze_author_series(author: Author, db_session: Session) -> List[Dict]:
    """
    Analyze series for an author

    Returns list of series with:
    - Series name
    - Total books in series
    - Books you've read
    - Missing books (unread books in series)
    - Status: 'complete', 'partial', 'not_started'
    """
    # Get all catalog books for this author that are in series
    catalog_books = db_session.query(AuthorCatalogBook).filter_by(
        author_id=author.id
    ).filter(AuthorCatalogBook.series_name.isnot(None)).all()

    author_names = [
        value.casefold().strip()
        for value in (author.name, author.normalized_name)
        if value
    ]
    recommendation_rows = []
    if author_names:
        recommendation_rows = db_session.query(Recommendation).filter(
            func.lower(func.trim(Recommendation.author)).in_(author_names),
            or_(
                Recommendation.already_read.is_(True),
                Recommendation.thumbs_down.is_(True),
                Recommendation.duplicate.is_(True),
                Recommendation.non_english.is_(True),
            )
        ).all()
    return _analyze_author_series_rows(author, catalog_books, recommendation_rows)


def get_standalone_books(author: Author, db_session: Session) -> List[Dict]:
    """
    Get standalone books (not in series) by author that you haven't read
    
    Returns list of unread standalone books
    """
    catalog_books = db_session.query(AuthorCatalogBook).filter_by(
        author_id=author.id,
        is_read=False
    ).filter(
        (AuthorCatalogBook.series_name.is_(None)) | 
        (AuthorCatalogBook.series_name == '')
    ).all()
    
    return [
        {
            'title': b.title,
            'isbn': b.isbn,
            'categories': b.categories,
            'description': b.description[:200] + '...' if b.description and len(b.description) > 200 else b.description
        }
        for b in catalog_books
    ]


def analyze_all_series(
    db_session: Session,
    format_filter: str = 'ebook',
    *,
    include_enrichment: bool = True,
) -> Dict:
    """
    Analyze all series across all authors
    
    Args:
        format_filter: 'ebook', 'audiobook', or None for all
    
    Returns:
        Dict with series analysis results
    """
    authors = db_session.query(Author).all()
    catalog_query = db_session.query(AuthorCatalogBook).filter(
        AuthorCatalogBook.series_name.isnot(None),
        AuthorCatalogBook.series_name != '',
    )
    standalone_count_query = db_session.query(func.count(AuthorCatalogBook.id)).filter(
        AuthorCatalogBook.is_read.is_(False),
        or_(
            AuthorCatalogBook.series_name.is_(None),
            AuthorCatalogBook.series_name == '',
        ),
    )
    if format_filter in {'ebook', 'audiobook'}:
        catalog_query = catalog_query.filter(or_(
            AuthorCatalogBook.format_available.in_([format_filter, 'both', 'unknown']),
            AuthorCatalogBook.format_available.is_(None),
        ))
        standalone_count_query = standalone_count_query.filter(or_(
            AuthorCatalogBook.format_available.in_([format_filter, 'both', 'unknown']),
            AuthorCatalogBook.format_available.is_(None),
        ))
    catalog_by_author = defaultdict(list)
    for book in catalog_query.all():
        catalog_by_author[book.author_id].append(book)
    total_standalone = standalone_count_query.scalar()
    recommendation_rows = db_session.query(Recommendation).filter(or_(
        Recommendation.thumbs_up.is_(True),
        Recommendation.already_read.is_(True),
        Recommendation.thumbs_down.is_(True),
        Recommendation.duplicate.is_(True),
        Recommendation.non_english.is_(True),
    )).all()
    all_series = []
    for author in authors:
        all_series.extend(_analyze_author_series_rows(
            author, catalog_by_author.get(author.id, []), recommendation_rows
        ))
    ignored_series = load_ignored_progress_series(db_session)
    ignored_keys = {
        (str(row['author']).casefold().strip(), str(row['name']).casefold().strip())
        for row in ignored_series
    }
    all_series = [
        series for series in all_series
        if (series['author'].casefold().strip(), series['series_name'].casefold().strip()) not in ignored_keys
    ]
    enrichment = {}
    if include_enrichment:
        from .series_enrichment import (
            apply_enrichment,
            enrichment_status,
            load_enrichment_state,
        )
        enrichment_cache, hardcover_book_actions = load_enrichment_state(db_session)
        enrichment = enrichment_status(all_series, enrichment_cache)
        all_series = apply_enrichment(
            all_series,
            enrichment_cache,
            recommendation_rows,
            hardcover_book_actions,
        )
    
    return {
        'series': all_series,
        'total_series': len(all_series),
        'partial_series': len([s for s in all_series if s['status'] == 'partial']),
        'not_started_series': len([s for s in all_series if s['status'] == 'not_started']),
        'complete_series': len([s for s in all_series if s['status'] == 'complete']),
        'ignored_series': ignored_series,
        'enrichment': enrichment,
        # Preserve the CLI summary contract without returning the large,
        # browser-unused standalone_books payload.
        'total_standalone': total_standalone,
    }
