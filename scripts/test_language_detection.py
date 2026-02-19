#!/usr/bin/env python3
"""
Test language detection on prolific authors (dry run only).

Shows consolidated lists of English vs non-English books for each author
with >100 catalog books.
"""

import sys
from pathlib import Path
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from src.deduplication.language_detection import detect_non_english_title, is_english_title
from sqlalchemy import func


def test_language_detection(min_books: int = 100, limit: int = 10):
    """
    Test language detection on prolific authors.
    
    Args:
        min_books: Minimum number of catalog books to consider
        limit: Limit number of authors to process
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("LANGUAGE DETECTION TEST (DRY RUN)")
    print("="*80)
    print(f"Testing authors with >{min_books} catalog books")
    if limit:
        print(f"Processing first {limit} authors")
    print()
    
    # Find prolific authors
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
        func.count(AuthorCatalogBook.id) > min_books
    ).order_by(
        func.count(AuthorCatalogBook.id).desc()
    )
    
    if limit:
        prolific_query = prolific_query.limit(limit)
    
    prolific_authors = prolific_query.all()
    
    print(f"Found {len(prolific_authors)} author(s) with >{min_books} catalog books\n")
    
    if not prolific_authors:
        print("No prolific authors found.")
        session.close()
        return
    
    # Process each author
    all_results = []
    
    for author_id, author_name, catalog_count in prolific_authors:
        print("="*80)
        print(f"AUTHOR: {author_name} (ID: {author_id})")
        print(f"Total catalog books (not read): {catalog_count}")
        print("="*80)
        
        # Get all catalog books for this author
        catalog_books = session.query(AuthorCatalogBook).filter_by(
            author_id=author_id,
            is_read=False
        ).order_by(AuthorCatalogBook.title).all()
        
        english_books = []
        non_english_books = []
        
        for book in catalog_books:
            is_non_english, reasons = detect_non_english_title(
                book.title, book.isbn, book.open_library_key
            )
            
            if is_non_english:
                non_english_books.append({
                    'title': book.title,
                    'isbn': book.isbn,
                    'reasons': reasons
                })
            else:
                english_books.append({
                    'title': book.title,
                    'isbn': book.isbn
                })
        
        # Print summary
        print(f"\n📊 SUMMARY:")
        print(f"   English books: {len(english_books)} ({len(english_books)/len(catalog_books)*100:.1f}%)")
        print(f"   Non-English books: {len(non_english_books)} ({len(non_english_books)/len(catalog_books)*100:.1f}%)")
        
        # Print non-English books
        if non_english_books:
            print(f"\n❌ NON-ENGLISH BOOKS ({len(non_english_books)}):")
            print("-" * 80)
            for i, book in enumerate(non_english_books, 1):
                print(f"{i:3d}. {book['title']}")
                if book['isbn']:
                    print(f"      ISBN: {book['isbn']}")
                print(f"      Reasons: {', '.join(book['reasons'])}")
                print()
        else:
            print(f"\n✅ No non-English books detected")
        
        # Print English books (condensed view)
        if english_books:
            print(f"\n✅ ENGLISH BOOKS ({len(english_books)}):")
            print("-" * 80)
            # Show first 20, then summary
            for i, book in enumerate(english_books[:20], 1):
                isbn_str = f" (ISBN: {book['isbn']})" if book['isbn'] else ""
                print(f"{i:3d}. {book['title']}{isbn_str}")
            
            if len(english_books) > 20:
                print(f"\n... and {len(english_books) - 20} more English books")
        
        # Store results
        all_results.append({
            'author': author_name,
            'author_id': author_id,
            'total': len(catalog_books),
            'english_count': len(english_books),
            'non_english_count': len(non_english_books),
            'english_books': english_books,
            'non_english_books': non_english_books
        })
        
        print()
    
    # Overall summary
    print("="*80)
    print("OVERALL SUMMARY")
    print("="*80)
    total_books = sum(r['total'] for r in all_results)
    total_english = sum(r['english_count'] for r in all_results)
    total_non_english = sum(r['non_english_count'] for r in all_results)
    
    print(f"\nAuthors analyzed: {len(all_results)}")
    print(f"Total books: {total_books}")
    print(f"  English: {total_english} ({total_english/total_books*100:.1f}%)")
    print(f"  Non-English: {total_non_english} ({total_non_english/total_books*100:.1f}%)")
    
    print(f"\nBreakdown by author:")
    print("-" * 80)
    for result in sorted(all_results, key=lambda x: x['non_english_count'], reverse=True):
        print(f"{result['author']:40s} | Total: {result['total']:4d} | "
              f"English: {result['english_count']:4d} ({result['english_count']/result['total']*100:5.1f}%) | "
              f"Non-English: {result['non_english_count']:4d} ({result['non_english_count']/result['total']*100:5.1f}%)")
    
    session.close()
    
    return all_results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Test language detection on prolific authors (dry run)'
    )
    parser.add_argument('--min-books', type=int, default=100,
                       help='Minimum number of catalog books (default: 100)')
    parser.add_argument('--limit', type=int, default=10,
                       help='Limit number of authors to process (default: 10)')
    
    args = parser.parse_args()
    
    test_language_detection(min_books=args.min_books, limit=args.limit)
