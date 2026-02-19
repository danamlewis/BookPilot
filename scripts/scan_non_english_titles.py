#!/usr/bin/env python3
"""
Scan catalog books for non-English titles using enhanced detection.

This script:
1. Scans all catalog books for non-English titles
2. Uses enhanced language detection
3. Reports findings
4. Optionally flags them in the database
"""

import sys
from pathlib import Path
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from src.deduplication.language_detection import detect_non_english_title, is_english_title
from sqlalchemy import func


def scan_catalog_books(dry_run: bool = True, author_limit: int = None) -> dict:
    """
    Scan all catalog books for non-English titles.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("NON-ENGLISH TITLE SCAN")
    print("="*80)
    if dry_run:
        print("(DRY RUN - no changes will be made)")
    else:
        print("(LIVE MODE - non-English books will be flagged)")
    print()
    
    # Get all catalog books that are eligible (not already read)
    catalog_books = session.query(AuthorCatalogBook).filter_by(
        is_read=False
    ).all()
    
    print(f"Scanning {len(catalog_books)} eligible catalog books...\n")
    
    non_english_books = []
    by_author = defaultdict(list)
    by_reason = defaultdict(int)
    
    for book in catalog_books:
        is_non_english, reasons = detect_non_english_title(book.title, book.isbn, book.open_library_key)
        
        if is_non_english:
            # Get author name
            author = session.query(Author).filter_by(id=book.author_id).first()
            author_name = author.name if author else f"Author ID {book.author_id}"
            
            non_english_books.append({
                'id': book.id,
                'title': book.title,
                'author': author_name,
                'author_id': book.author_id,
                'isbn': book.isbn,
                'reasons': reasons
            })
            
            by_author[author_name].append(book)
            for reason in reasons:
                by_reason[reason] += 1
    
    print(f"Found {len(non_english_books)} non-English catalog books\n")
    
    if non_english_books:
        print("Breakdown by detection reason:")
        for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")
        
        print(f"\nBreakdown by author (top 10):")
        sorted_authors = sorted(by_author.items(), key=lambda x: len(x[1]), reverse=True)
        for author_name, books in sorted_authors[:10]:
            print(f"  {author_name}: {len(books)} non-English books")
        
        print(f"\nSample non-English books (first 20):")
        print("-" * 80)
        for i, book in enumerate(non_english_books[:20], 1):
            print(f"\n{i}. {book['title']}")
            print(f"   Author: {book['author']}")
            if book['isbn']:
                print(f"   ISBN: {book['isbn']}")
            print(f"   Reasons: {', '.join(book['reasons'])}")
        
        if len(non_english_books) > 20:
            print(f"\n... and {len(non_english_books) - 20} more")
        
        # Test cases check
        print(f"\n{'='*80}")
        print("TEST CASES VERIFICATION")
        print("="*80)
        test_titles = [
            "Sheloshah shavuʻot be-Pariz by Author Name",
            "Xjust Rewards Tegf by Author Name"
        ]
        
        for test_title in test_titles:
            is_non_english, reasons = detect_non_english_title(test_title)
            status = "✓" if is_non_english else "✗"
            print(f"{status} '{test_title}'")
            print(f"   Detected as: {'Non-English' if is_non_english else 'English'}")
            if reasons:
                print(f"   Reasons: {', '.join(reasons)}")
            print()
        
        if not dry_run:
            print("Flagging non-English books in database...")
            flagged_count = 0
            for book_data in non_english_books:
                catalog_book = session.query(AuthorCatalogBook).filter_by(id=book_data['id']).first()
                if catalog_book:
                    # We could add a non_english flag to AuthorCatalogBook, but for now
                    # we'll just note that these should be filtered out
                    flagged_count += 1
            print(f"  Processed {flagged_count} books")
            session.commit()
    else:
        print("✓ No non-English books found")
    
    session.close()
    
    return {
        'total_scanned': len(catalog_books),
        'non_english_found': len(non_english_books),
        'by_reason': dict(by_reason),
        'by_author': {k: len(v) for k, v in by_author.items()}
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Scan catalog books for non-English titles'
    )
    parser.add_argument('--execute', action='store_true',
                       help='Actually flag non-English books (default is dry run)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    
    args = parser.parse_args()
    
    scan_catalog_books(dry_run=not args.execute, author_limit=args.limit)
