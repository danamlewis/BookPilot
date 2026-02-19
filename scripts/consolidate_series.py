#!/usr/bin/env python3
"""
Consolidate series with similar names.

Detects series that are the same but have different naming variations:
- "Series Name" vs "The Series Name Series" vs "the Series Name Series"
- Normalizes series names and merges them into a canonical form
"""

import sys
import re
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Set, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from sqlalchemy import func


def normalize_series_name(series_name: str) -> str:
    """
    Normalize a series name for comparison.
    
    Removes:
    - Leading "the", "a", "an"
    - "Series" suffix
    - Case differences
    - Extra whitespace
    - Common punctuation variations
    """
    if not series_name:
        return ""
    
    # Convert to lowercase
    normalized = series_name.lower().strip()
    
    # Remove leading articles
    normalized = re.sub(r'^(the|a|an)\s+', '', normalized)
    
    # Remove "Series" suffix (with or without punctuation)
    normalized = re.sub(r'\s+series\s*$', '', normalized)
    normalized = re.sub(r'\s*series\s*$', '', normalized)
    
    # Normalize whitespace
    normalized = ' '.join(normalized.split())
    
    # Remove common punctuation that doesn't affect meaning
    normalized = re.sub(r'[^\w\s]', '', normalized)  # Remove punctuation
    
    return normalized


def find_series_consolidations(author: Author, session) -> List[Dict]:
    """
    Find series that should be consolidated for an author.
    
    Returns list of consolidation groups with:
    - canonical_name: The name to use for all books
    - variant_names: List of current series names that should be merged
    - books: All books across all variants
    - confidence: How confident we are this is the same series
    """
    # Get all catalog books for this author that are in series
    catalog_books = session.query(AuthorCatalogBook).filter_by(
        author_id=author.id
    ).filter(AuthorCatalogBook.series_name.isnot(None)).all()
    
    if not catalog_books:
        return []
    
    # Group by normalized series name
    normalized_groups = defaultdict(list)
    for book in catalog_books:
        normalized = normalize_series_name(book.series_name)
        if normalized:
            normalized_groups[normalized].append(book)
    
    # Find groups with multiple variant names (these need consolidation)
    consolidations = []
    for normalized, books in normalized_groups.items():
        # Get unique original series names
        original_names = set(book.series_name for book in books)
        
        if len(original_names) > 1:
            # Multiple variants of the same series - needs consolidation
            # Choose canonical name (prefer one with "Series" and proper capitalization)
            canonical_candidates = sorted(original_names, key=lambda x: (
                'series' not in x.lower(),  # Prefer ones with "Series"
                x[0].islower(),  # Prefer capitalized
                len(x)  # Prefer shorter (more concise)
            ))
            canonical_name = canonical_candidates[0]
            
            # Check if books have overlapping positions (strong indicator of same series)
            positions = [b.series_position for b in books if b.series_position]
            has_overlapping_positions = len(positions) != len(set(positions))
            
            # Check if positions are sequential (1, 2, 3) or complementary
            unique_positions = sorted(set(positions))
            is_sequential = len(unique_positions) > 1 and all(
                unique_positions[i] == unique_positions[i-1] + 1 
                for i in range(1, len(unique_positions))
            )
            
            confidence = 'high' if (has_overlapping_positions or is_sequential) else 'medium'
            
            consolidations.append({
                'canonical_name': canonical_name,
                'variant_names': sorted(original_names),
                'normalized_name': normalized,
                'books': books,
                'total_books': len(books),
                'confidence': confidence,
                'positions': sorted(unique_positions) if positions else []
            })
    
    return consolidations


def preview_consolidations(consolidations: List[Dict], author: Author):
    """
    Show preview of series consolidations.
    """
    print("="*80)
    print(f"AUTHOR: {author.name} (ID: {author.id})")
    print(f"Series consolidations found: {len(consolidations)}")
    print("="*80)
    print()
    
    for i, consolidation in enumerate(consolidations, 1):
        print(f"{i}. CONSOLIDATION GROUP")
        print(f"   Normalized name: \"{consolidation['normalized_name']}\"")
        print(f"   Canonical name (will use): \"{consolidation['canonical_name']}\"")
        print(f"   Variant names to merge:")
        for variant in consolidation['variant_names']:
            if variant != consolidation['canonical_name']:
                print(f"      - \"{variant}\" → \"{consolidation['canonical_name']}\"")
        print(f"   Confidence: {consolidation['confidence']}")
        print(f"   Total books: {consolidation['total_books']}")
        if consolidation['positions']:
            print(f"   Series positions: {consolidation['positions']}")
        
        # Show books
        print(f"   Books:")
        for book in sorted(consolidation['books'], key=lambda x: x.series_position or 999):
            variant_marker = " ⚠" if book.series_name != consolidation['canonical_name'] else ""
            print(f"      #{book.series_position or '?'}: {book.title} (series: \"{book.series_name}\"){variant_marker}")
        print()


def execute_consolidation(consolidation: Dict, session, dry_run: bool = True) -> int:
    """
    Execute a single consolidation by updating all books to use canonical name.
    
    Returns number of books updated.
    """
    canonical_name = consolidation['canonical_name']
    variant_names = consolidation['variant_names']
    books = consolidation['books']
    
    updated_count = 0
    
    for book in books:
        if book.series_name != canonical_name:
            if not dry_run:
                book.series_name = canonical_name
            updated_count += 1
    
    if not dry_run:
        session.commit()
    
    return updated_count


def scan_all_authors(min_books: int = 1, limit: Optional[int] = None) -> List[Dict]:
    """
    Scan all authors for series consolidations.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Get authors with series
    authors_with_series = session.query(
        Author.id,
        Author.name,
        func.count(AuthorCatalogBook.id).label('series_count')
    ).join(
        AuthorCatalogBook, Author.id == AuthorCatalogBook.author_id
    ).filter(
        AuthorCatalogBook.series_name.isnot(None)
    ).group_by(
        Author.id, Author.name
    ).having(
        func.count(AuthorCatalogBook.id) >= min_books
    ).order_by(
        func.count(AuthorCatalogBook.id).desc()
    )
    
    if limit:
        authors_with_series = authors_with_series.limit(limit)
    
    authors_with_series = authors_with_series.all()
    
    all_consolidations = []
    for author_id, author_name, series_count in authors_with_series:
        author = session.query(Author).filter_by(id=author_id).first()
        if author:
            consolidations = find_series_consolidations(author, session)
            if consolidations:
                all_consolidations.append({
                    'author': author,
                    'consolidations': consolidations
                })
    
    session.close()
    return all_consolidations


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Consolidate series with similar names'
    )
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of series books (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    parser.add_argument('--execute', action='store_true',
                       help='Actually consolidate (default is dry run)')
    
    args = parser.parse_args()
    
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("SERIES CONSOLIDATION")
    print("="*80)
    if not args.execute:
        print("(DRY RUN - no changes will be made)")
    else:
        print("(LIVE MODE - series will be consolidated)")
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
        
        consolidations = find_series_consolidations(author, session)
        if consolidations:
            preview_consolidations(consolidations, author)
            
            if args.execute:
                print("="*80)
                print("EXECUTING CONSOLIDATION")
                print("="*80)
                total_updated = 0
                for consolidation in consolidations:
                    updated = execute_consolidation(consolidation, session, dry_run=False)
                    total_updated += updated
                    print(f"✓ Consolidated \"{consolidation['normalized_name']}\": updated {updated} books")
                print(f"\n✓ Total books updated: {total_updated}")
        else:
            print(f"No series consolidations needed for {author.name}")
    else:
        # Scan all authors
        results = scan_all_authors(min_books=args.min_books, limit=args.limit)
        
        if not results:
            print("No series consolidations found.")
            session.close()
            sys.exit(0)
        
        # Show preview
        total_consolidations = 0
        total_books_to_update = 0
        
        for result in results:
            author = result['author']
            consolidations = result['consolidations']
            
            preview_consolidations(consolidations, author)
            
            total_consolidations += len(consolidations)
            for consolidation in consolidations:
                variant_count = sum(1 for b in consolidation['books'] 
                                  if b.series_name != consolidation['canonical_name'])
                total_books_to_update += variant_count
        
        print("="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Authors with consolidations: {len(results)}")
        print(f"Total consolidation groups: {total_consolidations}")
        print(f"Total books to update: {total_books_to_update}")
        
        if args.execute and total_books_to_update > 0:
            print("\n" + "="*80)
            print("EXECUTING CONSOLIDATIONS")
            print("="*80)
            
            updated_total = 0
            for result in results:
                author = result['author']
                consolidations = result['consolidations']
                
                for consolidation in consolidations:
                    updated = execute_consolidation(consolidation, session, dry_run=False)
                    updated_total += updated
                    print(f"✓ [{author.name}] Consolidated \"{consolidation['normalized_name']}\": updated {updated} books")
            
            print(f"\n✓ Total books updated: {updated_total}")
    
    session.close()
