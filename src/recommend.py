"""Recommendation engine"""
from typing import List, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func
from .models import Book, Author, AuthorCatalogBook, Recommendation


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
    # Get all audiobooks you've listened to
    your_audiobooks = db_session.query(Book).filter_by(format='audiobook').all()
    your_authors = {b.author for b in your_audiobooks}
    
    recommendations = []
    
    # 1. Same author recommendations
    for author_name in your_authors:
        # Try to find author by exact name match first (most reliable)
        # The author_name from Book table is the normalized_name, but we want to find
        # the Author record that matches. Since multiple authors can have the same
        # normalized_name (e.g., co-authors), we need to be smart about matching.
        
        # First, try to find an author whose name exactly matches the normalized_name
        # (this handles the case where the author name in Book is already normalized)
        author = db_session.query(Author).filter_by(name=author_name).first()
        
        # If no exact match, try normalized_name but prefer authors whose name is similar
        if not author:
            # Get all authors with this normalized_name
            candidates = db_session.query(Author).filter_by(normalized_name=author_name).all()
            
            # Prefer the one whose name matches the normalized_name exactly
            # (e.g., if author_name is "Author Name", prefer Author.name == "Author Name")
            for candidate in candidates:
                if candidate.name == author_name:
                    author = candidate
                    break
            
            # If still no match, use the first one (fallback)
            if not author and candidates:
                author = candidates[0]
        
        if not author:
            continue
        
        # Count books by this author (from Libby CSV + already_read recommendations)
        books_by_author_count = count_books_by_author(db_session, author_name, author.name)
        
        # Get catalog books by this author that are available as audiobooks
        # (We'll mark format_available when we fetch, for now get all)
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(
            author_id=author.id,
            is_read=False
        ).all()
        
        # Filter out non-English books
        from src.deduplication.language_detection import is_english_title
        catalog_books = [b for b in catalog_books if is_english_title(b.title, b.isbn, b.open_library_key)]
        
        for catalog_book in catalog_books:
            # Check if you've already listened to this (by title match)
            already_listened = any(
                b.title.lower() == catalog_book.title.lower() 
                for b in your_audiobooks
            )
            
            if not already_listened:
                rec_categories = catalog_book.categories.split(', ') if catalog_book.categories else []
                recommendations.append({
                    'title': catalog_book.title,
                    'author': author.name,
                    'isbn': catalog_book.isbn,
                    'recommendation_type': 'same_author',
                    'similarity_score': 0.95,  # TODO: not calculated; fixed for same-author. Future: compute from history/engagement.
                    'reason': f'You\'ve listened to other books by {author.name}',
                    'categories': rec_categories,
                    'format': 'audiobook',
                    'books_by_author_count': books_by_author_count,
                    'series_name': catalog_book.series_name if catalog_book.series_name else None,
                    'series_position': catalog_book.series_position if catalog_book.series_position else None
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
    your_authors = {b.author for b in your_books}
    
    recommendations = []
    
    # 1. Same author recommendations (ebooks you haven't read)
    for author_name in your_authors:
        # Try to find author by exact name match first (most reliable)
        # The author_name from Book table is the normalized_name, but we want to find
        # the Author record that matches. Since multiple authors can have the same
        # normalized_name (e.g., co-authors), we need to be smart about matching.
        
        # First, try to find an author whose name exactly matches the normalized_name
        # (this handles the case where the author name in Book is already normalized)
        author = db_session.query(Author).filter_by(name=author_name).first()
        
        # If no exact match, try normalized_name but prefer authors whose name is similar
        if not author:
            # Get all authors with this normalized_name
            candidates = db_session.query(Author).filter_by(normalized_name=author_name).all()
            
            # Prefer the one whose name matches the normalized_name exactly
            # (e.g., if author_name is "Author Name", prefer Author.name == "Author Name")
            for candidate in candidates:
                if candidate.name == author_name:
                    author = candidate
                    break
            
            # If still no match, use the first one (fallback)
            if not author and candidates:
                author = candidates[0]
        
        if not author:
            continue
        
        # Count books by this author (from Libby CSV + already_read recommendations)
        books_by_author_count = count_books_by_author(db_session, author_name, author.name)
        
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(
            author_id=author.id,
            is_read=False
        ).all()
        
        # Filter out non-English books
        from src.deduplication.language_detection import is_english_title
        catalog_books = [b for b in catalog_books if is_english_title(b.title, b.isbn, b.open_library_key)]
        
        for catalog_book in catalog_books:
            # Check if you've already read this
            already_read = any(
                b.title.lower() == catalog_book.title.lower() 
                for b in your_books
            )
            
            if not already_read:
                rec_categories = catalog_book.categories.split(', ') if catalog_book.categories else []
                
                # Filter by category if specified
                if category and category.lower() not in [c.lower() for c in rec_categories]:
                    continue
                
                recommendations.append({
                    'title': catalog_book.title,
                    'author': author.name,
                    'isbn': catalog_book.isbn,
                    'recommendation_type': 'same_author',
                    'similarity_score': 0.90,  # TODO: not calculated; fixed for same-author. Future: compute from history/engagement.
                    'reason': f'You\'ve read other books by {author.name}',
                    'categories': rec_categories,
                    'format': 'ebook',
                    'description': catalog_book.description[:200] + '...' if catalog_book.description and len(catalog_book.description) > 200 else catalog_book.description,
                    'books_by_author_count': books_by_author_count,
                    'series_name': catalog_book.series_name if catalog_book.series_name else None,
                    'series_position': catalog_book.series_position if catalog_book.series_position else None
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
    for rec_data in recommendations:
        # Check if recommendation already exists
        existing = db_session.query(Recommendation).filter_by(
            title=rec_data['title'],
            author=rec_data['author']
        ).first()
        
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
                format=rec_data.get('format', rec_type),
                category=', '.join(rec_data.get('categories', [])),
                recommendation_type=rec_data['recommendation_type'],
                similarity_score=rec_data['similarity_score'],
                reason=rec_data['reason']
            )
            db_session.add(recommendation)
    
    db_session.commit()
