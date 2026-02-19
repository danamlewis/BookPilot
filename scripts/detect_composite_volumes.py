#!/usr/bin/env python3
"""
Detect composite volumes (books with slashes or multiple books in title)
that duplicate standalone books.

Scenarios:
1. "The Secret" (standalone) vs "The Secret/The Revealing/The Whatever" (composite)
2. "Book 1 / Book 2 / Book 3" (composite)
3. "Books 1-5" in title
4. Series position = 1 but title contains multiple books
"""

import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author, Recommendation
from scripts.check_duplicate_recommendations import normalize_title_advanced, extract_base_title
from sqlalchemy import func


def extract_books_from_composite(title: str) -> List[str]:
    """
    Extract individual book titles from a composite title.
    
    Examples:
    - "The Secret/The Revealing/The Whatever" -> ["The Secret", "The Revealing", "The Whatever"]
    - "Book 1 / Book 2 / Book 3" -> ["Book 1", "Book 2", "Book 3"]
    - "Books 1-5" -> [] (can't extract specific titles)
    """
    books = []
    
    # Split by common separators
    # Look for patterns like "Book1/Book2" or "Book 1 / Book 2"
    # Note: "&" is NOT included as it's a normal character in single book titles
    separators = [' / ', '/', ' | ', '|']
    
    for sep in separators:
        if sep in title:
            parts = [p.strip() for p in title.split(sep)]
            # Filter out empty parts and very short ones (likely not book titles)
            parts = [p for p in parts if len(p) > 3]
            if len(parts) > 1:
                return parts
    
    # Check for "Books 1-5" or "Volumes 1-3" patterns
    range_pattern = re.compile(r'(?:books?|volumes?|parts?)\s+(\d+)\s*[-–—]\s*(\d+)', re.IGNORECASE)
    match = range_pattern.search(title)
    if match:
        # Can't extract specific titles from ranges
        return []
    
    return books


def is_composite_volume(book: AuthorCatalogBook) -> bool:
    """
    Determine if a catalog book is a composite volume.
    
    Checks:
    1. Title contains slashes or multiple book separators
    2. Title contains "books 1-5" or similar range patterns
    3. Series position = 1 but title suggests multiple books
    """
    title = book.title
    
    # Check for separators
    # Note: "&" is NOT included as it's a normal character in single book titles
    separators = [' / ', '/', ' | ', '|']
    has_separator = any(sep in title for sep in separators)
    
    # Check for range patterns
    range_patterns = [
        r'books?\s+\d+\s*[-–—]\s*\d+',  # "Books 1-5" or "Books 1 - 5"
        r'volumes?\s+\d+\s*[-–—]\s*\d+',  # "Volumes 1-3"
        r'parts?\s+\d+\s*[-–—]\s*\d+',  # "Parts 1-2"
        r'\d+\s*[-–—]\s*\d+\s+books?',  # "1-5 books"
        r'box\s+\d+\s*[-–—]\s*\d+',  # "Box 1-4"
        r'boxed\s+set',  # "Boxed Set" or "boxed set"
        r'collection\s*:?\s*books?\s+\d+\s*[-–—]\s*\d+',  # "Collection : Books 1 - 4"
    ]
    has_range = any(re.search(pattern, title, re.IGNORECASE) for pattern in range_patterns)
    
    # Check for series position mismatch
    # If series_position = 1 but title has multiple books, it's likely composite
    series_mismatch = False
    if book.series_position == 1 and has_separator:
        # Count potential books in title
        parts = [p.strip() for p in re.split(r'[/|]', title) if len(p.strip()) > 3]
        if len(parts) > 1:
            series_mismatch = True
    
    return has_separator or has_range or series_mismatch


def find_composite_standalone_matches(catalog_books: List[AuthorCatalogBook], session) -> List[Dict]:
    """
    Find composite volumes and match them to standalone catalog books.
    
    Returns list of matches with:
    - composite_book: The composite volume catalog book
    - standalone_books: List of standalone catalog books that appear in the composite
    - confidence: How confident we are in the match
    """
    matches = []
    
    # Separate composite and standalone catalog books
    composite_books = [b for b in catalog_books if is_composite_volume(b)]
    standalone_books = [b for b in catalog_books if not is_composite_volume(b)]
    
    for composite in composite_books:
        # Get series info directly from catalog book
        composite_series_name = composite.series_name
        composite_series_position = composite.series_position
        
        # Extract individual book titles from composite
        component_titles = extract_books_from_composite(composite.title)
        
        if not component_titles:
            # Can't extract specific titles (e.g., "Books 1-5", "Box 1-4", "Boxed Set")
            # Still mark as composite - will be flagged as duplicate
            if is_composite_volume(composite):
                matches.append({
                    'composite_book': composite,
                    'composite_series_name': composite_series_name,
                    'composite_series_position': composite_series_position,
                    'standalone_books': [],
                    'component_titles': [],
                    'confidence': 'low',
                    'reason': 'Composite volume (range/boxed set pattern) - will be marked as duplicate'
                })
            continue
        
        # Try to match each component title to standalone catalog books
        matched_standalones = []
        for component_title in component_titles:
            # Normalize for matching
            component_normalized = normalize_title_advanced(component_title)
            component_base = extract_base_title(component_title)
            
            best_match = None
            best_score = 0
            
            for standalone in standalone_books:
                standalone_normalized = normalize_title_advanced(standalone.title)
                standalone_base = extract_base_title(standalone.title)
                
                # Check exact normalized match
                if component_normalized and standalone_normalized:
                    if component_normalized == standalone_normalized:
                        best_match = standalone
                        best_score = 1.0
                        break
                
                # Check base title match
                if component_base and standalone_base:
                    if component_base.lower() == standalone_base.lower():
                        if best_score < 0.8:
                            best_match = standalone
                            best_score = 0.8
                
                # Check if component title is contained in standalone (or vice versa)
                if component_title.lower() in standalone.title.lower():
                    if best_score < 0.7:
                        best_match = standalone
                        best_score = 0.7
                elif standalone.title.lower() in component_title.lower():
                    if best_score < 0.7:
                        best_match = standalone
                        best_score = 0.7
            
            if best_match and best_score >= 0.7:
                matched_standalones.append({
                    'standalone': best_match,
                    'component_title': component_title,
                    'match_score': best_score
                })
        
        if matched_standalones:
            matches.append({
                'composite_book': composite,
                'composite_series_name': composite_series_name,
                'composite_series_position': composite_series_position,
                'standalone_books': matched_standalones,
                'component_titles': component_titles,
                'confidence': 'high' if len(matched_standalones) == len(component_titles) else 'medium',
                'reason': f'Found {len(matched_standalones)}/{len(component_titles)} component books as standalones'
            })
        elif is_composite_volume(composite):
            # Composite but no matches found
            matches.append({
                'composite_book': composite,
                'composite_series_name': composite_series_name,
                'composite_series_position': composite_series_position,
                'standalone_books': [],
                'component_titles': component_titles,
                'confidence': 'low',
                'reason': 'Composite volume but no matching standalone books found'
            })
    
    return matches


def analyze_author_composites(author: Author, session) -> Dict:
    """
    Analyze an author's catalog books for composite volumes.
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
    
    if len(eligible_books) < 2:
        return {
            'author': author,
            'total_books': len(eligible_books),
            'composite_books': [],
            'matches': []
        }
    
    # Find composite volumes
    composite_books = [b for b in eligible_books if is_composite_volume(b)]
    
    # Find matches
    matches = find_composite_standalone_matches(eligible_books, session)
    
    return {
        'author': author,
        'total_books': len(eligible_books),
        'composite_books': composite_books,
        'matches': matches
    }


def scan_all_authors(min_books: int = 1, limit: Optional[int] = None) -> List[Dict]:
    """
    Scan all authors for composite volumes.
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
            result = analyze_author_composites(author, session)
            if result['matches']:  # Only include if there are matches
                results.append(result)
    
    session.close()
    return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Detect composite volumes that duplicate standalone books'
    )
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of catalog books (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    
    args = parser.parse_args()
    
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("COMPOSITE VOLUME DETECTION")
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
        
        result = analyze_author_composites(author, session)
        results = [result]
    else:
        # Scan all authors
        results = scan_all_authors(min_books=args.min_books, limit=args.limit)
    
    # Print results
    total_composites = 0
    total_matches = 0
    
    for result in results:
        author = result['author']
        matches = result['matches']
        
        if not matches:
            continue
        
        print("="*80)
        print(f"AUTHOR: {author.name} (ID: {author.id})")
        print(f"Total eligible catalog books: {result['total_books']}")
        print(f"Composite volumes found: {len(matches)}")
        print("="*80)
        print()
        
        for i, match in enumerate(matches, 1):
            composite = match['composite_book']
            standalones = match['standalone_books']
            
            print(f"{i}. COMPOSITE: {composite.title}")
            if composite.isbn:
                print(f"   ISBN: {composite.isbn}")
            if match.get('composite_series_name'):
                print(f"   Series: {match['composite_series_name']} #{match.get('composite_series_position', '?')}")
            print(f"   Confidence: {match['confidence']}")
            print(f"   Reason: {match['reason']}")
            
            if standalones:
                print(f"   Matched standalone catalog books:")
                for j, standalone_info in enumerate(standalones, 1):
                    standalone = standalone_info['standalone']
                    print(f"      {j}. {standalone.title} (match: {standalone_info['match_score']:.1%})")
                    if standalone.isbn:
                        print(f"         ISBN: {standalone.isbn}")
            else:
                print(f"   No matching standalone catalog books found")
            print()
        
        total_composites += len(matches)
        total_matches += sum(len(m['standalone_books']) for m in matches)
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Authors analyzed: {len(results)}")
    print(f"Total composite volumes: {total_composites}")
    print(f"Total standalone matches: {total_matches}")
    
    session.close()
