"""Series analysis for ebooks"""
from typing import List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from .models import Author, AuthorCatalogBook, Series, Book, Recommendation


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
    
    # Get all recommendations marked as already_read for this author
    # Check both by exact author name and normalized name
    already_read_recs = db_session.query(Recommendation).filter(
        or_(
            func.lower(Recommendation.author) == author.name.lower(),
            func.lower(Recommendation.author) == author.normalized_name.lower()
        ),
        Recommendation.already_read.is_(True)
    ).all()
    
    # Create a set of (title_lower, author_lower) for quick lookup
    already_read_set = {
        (rec.title.lower().strip() if rec.title else '', 
         rec.author.lower().strip() if rec.author else '')
        for rec in already_read_recs
        if rec.title and rec.author
    }
    
    # Get all filtered recommendations (thumbs_down, duplicate, non_english) for this author
    # These should be excluded from the unread_books list
    filtered_recs = db_session.query(Recommendation).filter(
        or_(
            func.lower(Recommendation.author) == author.name.lower(),
            func.lower(Recommendation.author) == author.normalized_name.lower()
        ),
        or_(
            Recommendation.thumbs_down == True,
            Recommendation.duplicate == True,
            Recommendation.non_english == True
        )
    ).all()
    
    # Create a set of filtered books for quick lookup
    filtered_set = {
        (rec.title.lower().strip() if rec.title else '', 
         rec.author.lower().strip() if rec.author else '')
        for rec in filtered_recs
        if rec.title and rec.author
    }
    
    # Group by series
    series_dict = {}
    for book in catalog_books:
        series_name = book.series_name
        if not series_name:
            continue
        
        if series_name not in series_dict:
            series_dict[series_name] = {
                'books': [],
                'read_books': [],
                'unread_books': []
            }
        
        series_dict[series_name]['books'].append(book)
        
        # Check if book is read: either is_read=True OR marked as already_read in Recommendation
        book_title_lower = (book.title.lower().strip() if book.title else '')
        book_author_lower = (author.name.lower().strip() if author.name else '')
        is_read = book.is_read or (book_title_lower, book_author_lower) in already_read_set
        
        if is_read:
            series_dict[series_name]['read_books'].append(book)
        else:
            # Only add to unread_books if it's not filtered out (thumbs_down, duplicate, non_english)
            book_key = (book_title_lower, book_author_lower)
            if book_key not in filtered_set:
                series_dict[series_name]['unread_books'].append(book)
    
    # Build results
    results = []
    for series_name, data in series_dict.items():
        books = data['books']
        read_books = data['read_books']
        unread_books = data['unread_books']
        
        # Sort by series position if available
        books.sort(key=lambda b: b.series_position if b.series_position else 999)
        unread_books.sort(key=lambda b: b.series_position if b.series_position else 999)
        
        # Determine status based on read_books vs unread_books (excluding filtered books from consideration)
        # If all non-filtered books are read, series is complete
        # If no books are read but there are unread books available, series is not_started
        # If no books are read AND all unread books are filtered out, skip the series (don't show it)
        # Otherwise, it's partial
        
        # Skip series that have no read books and all unread books are filtered out
        if len(read_books) == 0 and len(unread_books) == 0:
            continue  # Skip this series entirely - all books filtered out and none read
        
        if len(read_books) == 0:
            status = 'not_started'
        elif len(unread_books) == 0:
            # All non-filtered books are read
            status = 'complete'
        else:
            status = 'partial'
        
        # Calculate completion percentage
        completion_pct = (len(read_books) / len(books) * 100) if books else 0
        
        results.append({
            'series_name': series_name,
            'author': author.name,
            'total_books': len(books),
            'books_read': len(read_books),
            'completion_pct': completion_pct,
            'status': status,
            'unread_books': [
                {
                    'title': b.title,
                    'isbn': b.isbn,
                    'position': b.series_position,
                    'categories': b.categories
                }
                for b in unread_books
            ],
            'read_books': [
                {
                    'title': b.title,
                    'isbn': b.isbn,
                    'position': b.series_position
                }
                for b in read_books
            ]
        })
    
    # Sort by status (partial first, then not_started, then complete)
    status_order = {'partial': 0, 'not_started': 1, 'complete': 2}
    results.sort(key=lambda x: (status_order.get(x['status'], 99), -x['completion_pct']))
    
    return results


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


def analyze_all_series(db_session: Session, format_filter: str = 'ebook') -> Dict:
    """
    Analyze all series across all authors
    
    Args:
        format_filter: 'ebook', 'audiobook', or None for all
    
    Returns:
        Dict with series analysis results
    """
    authors = db_session.query(Author).all()
    
    all_series = []
    all_standalone = []
    
    for author in authors:
        # Get series for this author
        series_list = analyze_author_series(author, db_session)
        all_series.extend(series_list)
        
        # Get standalone books
        standalone = get_standalone_books(author, db_session)
        for book in standalone:
            book['author'] = author.name
        all_standalone.extend(standalone)
    
    # Filter by format if needed (this would require checking if books are available in format)
    # For now, we'll return all and filter in the UI
    
    return {
        'series': all_series,
        'standalone_books': all_standalone,
        'total_series': len(all_series),
        'partial_series': len([s for s in all_series if s['status'] == 'partial']),
        'not_started_series': len([s for s in all_series if s['status'] == 'not_started']),
        'complete_series': len([s for s in all_series if s['status'] == 'complete']),
        'total_standalone': len(all_standalone)
    }
