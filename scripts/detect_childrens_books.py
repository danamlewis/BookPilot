#!/usr/bin/env python3
"""
Detect children's/junior fiction books in recommendations.

Detects books that are:
- Junior fiction
- Children's books
- Young adult (if desired)
- Books with age ranges in title/description
"""

import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author, Recommendation
from sqlalchemy import func


def is_childrens_book(book: AuthorCatalogBook) -> Tuple[bool, List[str]]:
    """
    Determine if a catalog book is a children's/junior fiction book.
    
    Returns:
        Tuple of (is_childrens: bool, reasons: list)
    """
    reasons = []
    title = book.title.lower() if book.title else ""
    categories = (book.categories or "").lower()
    description = (book.description or "").lower() if book.description else ""
    
    # Combine all text for searching
    all_text = f"{title} {categories} {description}"
    
    # Pattern 1: Explicit children's/junior indicators in title
    childrens_title_patterns = [
        r'\bjunior\s+fiction\b',
        r'\bchildren\'?s\s+(?:book|fiction|novel|story)',
        r'\bkids\s+(?:book|fiction)',
        r'\byoung\s+adult\b',  # YA books
        r'\bya\s+(?:book|fiction|novel)',
        r'\bmiddle\s+grade\b',
        r'\btween\s+(?:book|fiction)',
    ]
    
    for pattern in childrens_title_patterns:
        if re.search(pattern, title, re.IGNORECASE):
            match = re.search(pattern, title, re.IGNORECASE)
            reasons.append(f"Children's indicator in title: '{match.group()}'")
            return True, reasons
    
    # Pattern 1b: Series names that are typically children's (e.g., "Cul-de-Sac Kids")
    childrens_series_keywords = [
        r'\b(?:cul[-\s]?de[-\s]?sac\s+)?kids\b',  # "Cul-de-Sac Kids" or "Kids"
        r'\b(?:goosebumps|percy\s+jackson|diary\s+of\s+a\s+wimpy\s+kid)\b',
    ]
    
    for pattern in childrens_series_keywords:
        if re.search(pattern, title, re.IGNORECASE):
            # Check if it's in a series context (parentheses, or part of series name)
            # "Cul-de-Sac Kids" in title or series name is a strong indicator
            if book.series_name and re.search(pattern, book.series_name.lower(), re.IGNORECASE):
                match = re.search(pattern, book.series_name, re.IGNORECASE)
                reasons.append(f"Children's series name: '{match.group()}'")
                return True, reasons
            elif re.search(pattern, title, re.IGNORECASE):
                # If it's in the title and looks like a series name
                match = re.search(pattern, title, re.IGNORECASE)
                reasons.append(f"Children's series in title: '{match.group()}'")
                return True, reasons
    
    # Pattern 2: Age ranges in title or description
    age_range_patterns = [
        r'ages?\s+\d+\s*[-–—]?\s*\d+',  # "ages 8-12"
        r'for\s+ages?\s+\d+',  # "for ages 8"
        r'\d+\s*[-–—]\s*\d+\s+years?\s+old',  # "8-12 years old"
        r'grade\s+\d+',  # "grade 3"
        r'grades?\s+\d+\s*[-–—]\s*\d+',  # "grades 3-5"
    ]
    
    for pattern in age_range_patterns:
        if re.search(pattern, all_text, re.IGNORECASE):
            match = re.search(pattern, all_text, re.IGNORECASE)
            reasons.append(f"Age range indicator: '{match.group()}'")
            return True, reasons
    
    # Pattern 3: Category indicators
    childrens_categories = [
        'juvenile fiction',
        'juvenile literature',
        'children\'s fiction',
        'children\'s literature',
        'young adult fiction',
        'young adult literature',
        'middle grade',
        'picture book',
        'early reader',
        'chapter book',
    ]
    
    for cat in childrens_categories:
        if cat in categories:
            reasons.append(f"Children's category: '{cat}'")
            return True, reasons
    
    # Pattern 4: Common children's book series/keywords in description
    childrens_keywords = [
        'for children',
        'for kids',
        'for young readers',
        'suitable for ages',
        'recommended for ages',
        'target audience: children',
        'target audience: kids',
    ]
    
    for keyword in childrens_keywords:
        if keyword in description:
            reasons.append(f"Children's keyword in description: '{keyword}'")
            return True, reasons
    
    # Pattern 5: Series names that are typically children's
    # This is more heuristic - could be expanded
    childrens_series_patterns = [
        r'\bgoosebumps\b',
        r'\bharry potter\b',  # Could be debated, but often considered YA/children's
        r'\bpercy jackson\b',
        r'\bdiary of a wimpy kid\b',
    ]
    
    # Only check if we have strong indicators
    if any(re.search(pattern, all_text, re.IGNORECASE) for pattern in childrens_series_patterns):
        # Check if it's explicitly in a children's context
        if any(indicator in all_text for indicator in ['children', 'kids', 'young', 'juvenile']):
            reasons.append("Known children's series with children's context")
            return True, reasons
    
    return False, reasons


def analyze_author_childrens_books(author: Author, session) -> Dict:
    """
    Analyze an author's catalog books for children's books.
    Only checks catalog books that would become recommendations (same filtering logic).
    """
    # Get catalog books that would become recommendations
    # Filter: is_read=False, not non-English (same as recommendation system)
    from src.deduplication.language_detection import is_english_title
    
    catalog_books = session.query(AuthorCatalogBook).filter_by(
        author_id=author.id,
        is_read=False
    ).all()
    
    # Filter out non-English books (same as recommendation system)
    eligible_books = [b for b in catalog_books if is_english_title(b.title, b.isbn, b.open_library_key)]
    
    if not eligible_books:
        return {
            'author': author,
            'total_books': 0,
            'childrens_books': [],
            'matches': []
        }
    
    # Check each catalog book
    childrens_books = []
    for book in eligible_books:
        is_childrens, reasons = is_childrens_book(book)
        
        if is_childrens:
            childrens_books.append({
                'catalog_book': book,
                'reasons': reasons
            })
    
    return {
        'author': author,
        'total_books': len(eligible_books),
        'childrens_books': childrens_books,
        'matches': childrens_books
    }


def scan_all_authors(min_books: int = 1, limit: Optional[int] = None) -> List[Dict]:
    """
    Scan all authors for children's books in their catalog.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Get authors with at least min_books eligible catalog books
    # (same filtering as recommendations: is_read=False, not non-English)
    from src.deduplication.language_detection import is_english_title
    
    prolific_query = session.query(
        Author.id,
        Author.name,
        func.count(AuthorCatalogBook.id).label('catalog_count')
    ).join(
        AuthorCatalogBook, Author.id == AuthorCatalogBook.author_id
    ).filter(
        AuthorCatalogBook.is_read == False
    ).group_by(
        Author.id, Author.name
    ).having(
        func.count(AuthorCatalogBook.id) >= min_books
    ).order_by(
        func.count(AuthorCatalogBook.id).desc()
    )
    
    if limit:
        prolific_query = prolific_query.limit(limit)
    
    prolific_authors = prolific_query.all()
    
    results = []
    for author_id, author_name, catalog_count in prolific_authors:
        author = session.query(Author).filter_by(id=author_id).first()
        if author:
            result = analyze_author_childrens_books(author, session)
            if result['childrens_books']:  # Only include if there are children's books
                results.append(result)
    
    session.close()
    return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Detect children\'s/junior fiction books in recommendations'
    )
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of recommendations (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    
    args = parser.parse_args()
    
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("CHILDREN'S BOOK DETECTION")
    print("="*80)
    print()
    
    if args.author:
        # Check specific author
        author = session.query(Author).filter(
            Author.name.ilike(f'%{args.author}%')
        ).first()
        
        if not author:
            print(f"Author '{args.author}' not found.")
            session.close()
            sys.exit(1)
        
        result = analyze_author_childrens_books(author, session)
        results = [result]
    else:
        # Scan all authors
        results = scan_all_authors(min_books=args.min_books, limit=args.limit)
    
    # Print results
    total_childrens = 0
    
    for result in results:
        author = result['author']
        childrens_books = result['childrens_books']
        
        if not childrens_books:
            continue
        
        print("="*80)
        print(f"AUTHOR: {author.name} (ID: {author.id})")
        print(f"Total eligible catalog books: {result['total_books']}")
        print(f"Children's books found: {len(childrens_books)}")
        print("="*80)
        print()
        
        for i, book_info in enumerate(childrens_books, 1):
            book = book_info['catalog_book']
            reasons = book_info['reasons']
            
            print(f"{i}. {book.title}")
            if book.isbn:
                print(f"   ISBN: {book.isbn}")
            if book.categories:
                print(f"   Categories: {book.categories}")
            print(f"   Reasons: {', '.join(reasons)}")
            print()
        
        total_childrens += len(childrens_books)
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Authors analyzed: {len(results)}")
    print(f"Total children's books found: {total_childrens}")
    
    session.close()
