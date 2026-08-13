"""Recommendation engine"""
from collections import Counter, defaultdict
from typing import List, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func
from .models import Book, Author, AuthorCatalogBook, Recommendation
from .history_crosscheck import get_non_english_title_keys, get_read_title_keys, normalize_work_title
from .preference_scoring import build_preference_profile, score_catalog_item
from .ingest import normalize_author_name


# Common non-fiction categories
NON_FICTION_KEYWORDS = [
    'biography', 'autobiography', 'memoir', 'history', 'historical',
    'science', 'philosophy', 'psychology', 'sociology', 'economics',
    'business', 'self-help', 'health', 'medicine', 'education',
    'reference', 'travel', 'cooking', 'crafts', 'hobbies',
    'religion', 'spirituality', 'politics', 'government', 'law',
    'true crime', 'essays', 'journalism', 'nonfiction', 'non-fiction'
]


def is_fiction(categories: List[str]) -> bool:
    """
    Determine if a book is fiction based on its categories.
    Returns True for fiction, False for non-fiction.
    """
    if not categories:
        return True  # Default to fiction if unknown
    
    categories_lower = [cat.lower() for cat in categories]
    
    # Check for explicit non-fiction indicators
    for keyword in NON_FICTION_KEYWORDS:
        if any(keyword in cat for cat in categories_lower):
            return False
    
    # Default to fiction if no non-fiction indicators found
    return True


def count_books_by_author(db_session: Session, normalized_author_name: str, display_author_name: str) -> int:
    """
    Count books read by an author, including:
    1. Books from Libby CSV import (Book table)
    2. Recommendations marked as 'already_read'
    
    Args:
        db_session: Database session
        normalized_author_name: Normalized author name (from Book table)
        display_author_name: Display author name (from Author.name, used in Recommendation table)
    
    Returns:
        Total count of books read by this author
    """
    # Count books from Book table (Libby CSV import)
    book_count = db_session.query(Book).filter_by(author=normalized_author_name).count()
    
    # Count recommendations marked as already_read
    # Match by display author name (case-insensitive)
    already_read_count = db_session.query(Recommendation).filter(
        func.lower(Recommendation.author) == display_author_name.lower(),
        Recommendation.already_read == True
    ).count()
    
    return book_count + already_read_count


def categorize_recommendations(recommendations: List[Dict]) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Categorize recommendations into Fiction/Non-Fiction, then by sub-category.
    Returns: {'Fiction': {'category1': [recs], ...}, 'Non-Fiction': {...}}
    Sorts recommendations within each subcategory by book count per author (descending).
    """
    fiction = {}
    nonfiction = {}
    
    for rec in recommendations:
        rec_cats = rec.get('categories', [])
        if isinstance(rec_cats, str):
            rec_cats = [c.strip() for c in rec_cats.split(',')]
        
        is_fict = is_fiction(rec_cats)
        target_dict = fiction if is_fict else nonfiction
        
        # Use first category or 'Uncategorized'
        primary_cat = rec_cats[0] if rec_cats else 'Uncategorized'
        
        if primary_cat not in target_dict:
            target_dict[primary_cat] = []
        target_dict[primary_cat].append(rec)
    
    # Sort categories by count (descending)
    def sort_by_count(cat_dict):
        sorted_dict = {}
        for cat, recs in cat_dict.items():
            # Sort recommendations within each category by book count per author (descending)
            sorted_recs = sorted(recs, key=lambda x: x.get('books_by_author_count', 0), reverse=True)
            sorted_dict[cat] = sorted_recs
        return dict(sorted(sorted_dict.items(), key=lambda x: len(x[1]), reverse=True))
    
    return {
        'Fiction': sort_by_count(fiction),
        'Non-Fiction': sort_by_count(nonfiction)
    }


def recommend_audiobooks(db_session: Session) -> List[Dict]:
    """
    Generate audiobook recommendations
    
    Priority:
    1. Same author audiobooks you haven't listened to
    2. Similar books (by genre/theme)
    """
    all_books = db_session.query(Book).all()
    source_authors = {book.author for book in all_books if book.format == 'audiobook' and book.author}
    preference_profile = build_preference_profile(db_session)
    all_authors = db_session.query(Author).all()
    authors_by_name = {(author.name or '').casefold().strip(): author for author in all_authors}
    authors_by_normalized = defaultdict(list)
    for author in all_authors:
        authors_by_normalized[(author.normalized_name or '').casefold().strip()].append(author)

    selected = {}
    for source_name in source_authors:
        key = source_name.casefold().strip()
        author = authors_by_name.get(key)
        if not author:
            candidates = authors_by_normalized.get(key, [])
            author = next((candidate for candidate in candidates if candidate.name == source_name),
                          candidates[0] if candidates else None)
        if author:
            selected[author.id] = (author, source_name)

    history_titles = defaultdict(set)
    book_counts = Counter()
    for book in all_books:
        if not book.author:
            continue
        book_counts[book.author] += 1
        if book.title:
            history_titles[normalize_author_name(book.author).casefold().strip()].add(
                normalize_work_title(book.title)
            )
    marked_read = defaultdict(set)
    non_english = defaultdict(set)
    marked_read_counts = Counter()
    for feedback in db_session.query(Recommendation).all():
        if not feedback.author or not feedback.title:
            continue
        key = normalize_author_name(feedback.author).casefold().strip()
        title_key = normalize_work_title(feedback.title)
        if feedback.already_read:
            marked_read[key].add(title_key)
            marked_read_counts[feedback.author.casefold().strip()] += 1
        if feedback.non_english:
            non_english[key].add(title_key)

    catalog_by_author = defaultdict(list)
    if selected:
        catalog_rows = db_session.query(AuthorCatalogBook).filter(
            AuthorCatalogBook.author_id.in_(selected),
            AuthorCatalogBook.is_read.is_(False),
        ).all()
        for catalog_book in catalog_rows:
            catalog_by_author[catalog_book.author_id].append(catalog_book)

    from src.deduplication.language_detection import is_english_title
    recommendations = []
    for author_id, (author, source_name) in selected.items():
        author_keys = {
            normalize_author_name(value).casefold().strip()
            for value in (author.name, author.normalized_name) if value
        }
        read_titles = set().union(*(
            history_titles.get(key, set()) | marked_read.get(key, set()) for key in author_keys
        ))
        blocked_titles = set().union(*(non_english.get(key, set()) for key in author_keys))
        books_by_author_count = book_counts[source_name] + marked_read_counts[author.name.casefold().strip()]
        for catalog_book in catalog_by_author.get(author_id, []):
            title_key = normalize_work_title(catalog_book.title)
            if title_key in read_titles or title_key in blocked_titles:
                continue
            if not is_english_title(catalog_book.title, catalog_book.isbn, catalog_book.open_library_key):
                continue
            rec_categories = catalog_book.categories.split(', ') if catalog_book.categories else []
            personal_fit = score_catalog_item(preference_profile, catalog_book, books_by_author_count)
            recommendations.append({
                'catalog_book_id': catalog_book.id,
                'open_library_key': catalog_book.open_library_key,
                'title': catalog_book.title,
                'author': author.name,
                'isbn': catalog_book.isbn,
                'recommendation_type': 'same_author',
                'reason': personal_fit['score_reason'],
                'categories': rec_categories,
                'format': 'audiobook',
                'books_by_author_count': books_by_author_count,
                'series_name': catalog_book.series_name or None,
                'series_position': catalog_book.series_position or None,
                'publication_date': catalog_book.publication_date,
                **personal_fit,
            })
    
    # Sort by score
    recommendations.sort(key=lambda x: x['similarity_score'], reverse=True)
    
    return recommendations


def recommend_new_books(db_session: Session, category: str = None) -> List[Dict]:
    """
    Generate new book recommendations based on reading history
    
    Args:
        category: Optional genre/category filter
    
    Returns:
        List of recommendations grouped by category
    """
    # Get all books you've read
    your_books = db_session.query(Book).all()
    your_authors = {b.author for b in your_books if b.author}
    preference_profile = build_preference_profile(db_session)

    # Load the supporting rows once. The previous implementation issued
    # multiple queries per author (author lookup, history, suppressions, read
    # count, and catalog), which made large libraries take tens of seconds.
    all_authors = db_session.query(Author).all()
    authors_by_name = {
        (author.name or "").casefold().strip(): author
        for author in all_authors
        if author.name
    }
    authors_by_normalized = defaultdict(list)
    for author in all_authors:
        if author.normalized_name:
            authors_by_normalized[author.normalized_name.casefold().strip()].append(author)

    selected_authors = {}
    source_name_by_author_id = {}
    for author_name in your_authors:
        lookup_key = author_name.casefold().strip()
        author = authors_by_name.get(lookup_key)
        if not author:
            candidates = authors_by_normalized.get(lookup_key, [])
            author = next(
                (candidate for candidate in candidates if candidate.name == author_name),
                candidates[0] if candidates else None,
            )
        if author:
            selected_authors.setdefault(author.id, author)
            source_name_by_author_id.setdefault(author.id, author_name)

    book_counts = Counter(book.author for book in your_books if book.author)
    history_titles = defaultdict(set)
    for book in your_books:
        if book.author and book.title:
            key = normalize_author_name(book.author).casefold().strip()
            title_key = normalize_work_title(book.title)
            if title_key:
                history_titles[key].add(title_key)

    feedback_rows = db_session.query(Recommendation).all()
    marked_read_titles = defaultdict(set)
    non_english_titles = defaultdict(set)
    marked_read_counts = Counter()
    for feedback in feedback_rows:
        if not feedback.author or not feedback.title:
            continue
        key = normalize_author_name(feedback.author).casefold().strip()
        title_key = normalize_work_title(feedback.title)
        if feedback.already_read:
            marked_read_titles[key].add(title_key)
            marked_read_counts[(feedback.author or "").casefold().strip()] += 1
        if feedback.non_english:
            non_english_titles[key].add(title_key)

    catalog_by_author_id = defaultdict(list)
    if selected_authors:
        catalog_rows = db_session.query(AuthorCatalogBook).filter(
            AuthorCatalogBook.author_id.in_(selected_authors),
            AuthorCatalogBook.is_read.is_(False),
        ).all()
        for catalog_book in catalog_rows:
            catalog_by_author_id[catalog_book.author_id].append(catalog_book)

    recommendations = []

    # 1. Same author recommendations (ebooks you haven't read)
    for author_id, author in selected_authors.items():
        author_name = source_name_by_author_id[author_id]
        # Count books by this author (from Libby CSV + already_read recommendations)
        books_by_author_count = (
            book_counts[author_name]
            + marked_read_counts[(author.name or "").casefold().strip()]
        )

        author_keys = {
            normalize_author_name(value).casefold().strip()
            for value in (author.name, author.normalized_name)
            if value
        }
        read_title_keys = set().union(
            *(history_titles.get(key, set()) | marked_read_titles.get(key, set()) for key in author_keys)
        )
        non_english_title_keys = set().union(
            *(non_english_titles.get(key, set()) for key in author_keys)
        )

        catalog_books = catalog_by_author_id.get(author.id, [])
        
        # Filter out non-English books
        from src.deduplication.language_detection import is_english_title
        catalog_books = [b for b in catalog_books if is_english_title(b.title, b.isbn, b.open_library_key)]
        
        for catalog_book in catalog_books:
            # A manually flagged or imported read in either format counts.
            already_read = normalize_work_title(catalog_book.title) in read_title_keys
            flagged_non_english = normalize_work_title(catalog_book.title) in non_english_title_keys
            
            if not already_read and not flagged_non_english:
                rec_categories = catalog_book.categories.split(', ') if catalog_book.categories else []
                
                # Filter by category if specified
                if category and category.lower() not in [c.lower() for c in rec_categories]:
                    continue

                personal_fit = score_catalog_item(preference_profile, catalog_book, books_by_author_count)
                
                recommendations.append({
                    'catalog_book_id': catalog_book.id,
                    'open_library_key': catalog_book.open_library_key,
                    'title': catalog_book.title,
                    'author': author.name,
                    'isbn': catalog_book.isbn,
                    'recommendation_type': 'same_author',
                    'reason': personal_fit['score_reason'],
                    'categories': rec_categories,
                    'format': 'ebook',
                    'description': catalog_book.description[:200] + '...' if catalog_book.description and len(catalog_book.description) > 200 else catalog_book.description,
                    'books_by_author_count': books_by_author_count,
                    'series_name': catalog_book.series_name if catalog_book.series_name else None,
                    'series_position': catalog_book.series_position if catalog_book.series_position else None,
                    'publication_date': catalog_book.publication_date,
                    **personal_fit,
                })
    
    # 2. Genre-based recommendations (simpler version - can enhance later)
    # For now, we'll focus on same-author recommendations
    
    # Sort by score
    recommendations.sort(key=lambda x: x['similarity_score'], reverse=True)
    
    # Group by category
    if not category:
        grouped = {}
        for rec in recommendations:
            rec_cats = rec.get('categories', [])
            if not rec_cats:
                rec_cats = ['Uncategorized']
            
            for cat in rec_cats:
                if cat not in grouped:
                    grouped[cat] = []
                grouped[cat].append(rec)
        
        return grouped
    
    return recommendations


def save_recommendations(recommendations: List[Dict], db_session: Session, 
                        rec_type: str = 'audiobook'):
    """Save recommendations to database"""
    existing_by_identity = {
        (
            (recommendation.title or '').casefold().strip(),
            (recommendation.author or '').casefold().strip(),
            recommendation.format or rec_type,
        ): recommendation
        for recommendation in db_session.query(Recommendation).all()
    }
    seen = set()
    for rec_data in recommendations:
        rec_format = rec_data.get('format', rec_type)
        identity = (
            rec_data['title'].casefold().strip(),
            rec_data['author'].casefold().strip(),
            rec_format,
        )
        if identity in seen:
            continue
        seen.add(identity)
        existing = existing_by_identity.get(identity)
        
        if existing:
            # Update
            existing.similarity_score = rec_data['similarity_score']
            existing.reason = rec_data['reason']
            existing.category = ', '.join(rec_data.get('categories', []))
        else:
            # Create new
            recommendation = Recommendation(
                title=rec_data['title'],
                author=rec_data['author'],
                isbn=rec_data.get('isbn'),
                format=rec_format,
                category=', '.join(rec_data.get('categories', [])),
                recommendation_type=rec_data['recommendation_type'],
                similarity_score=rec_data['similarity_score'],
                reason=rec_data['reason']
            )
            db_session.add(recommendation)
            existing_by_identity[identity] = recommendation
    
    db_session.commit()
