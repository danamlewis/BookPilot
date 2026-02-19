#!/usr/bin/env python3
"""
Check what books might have been deleted - analyze current state
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from collections import defaultdict

def analyze_current_state():
    """Analyze current database state to understand what might be missing"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    print("CURRENT DATABASE STATE ANALYSIS")
    print("=" * 80)
    print()
    
    # Current counts
    total_catalog_books = session.query(AuthorCatalogBook).count()
    total_authors = session.query(Author).count()
    
    print(f"Current catalog books: {total_catalog_books}")
    print(f"Current authors: {total_authors}")
    print()
    
    # Check for authors with very few books (might indicate deletions)
    print("Authors with catalog books:")
    authors_with_books = session.query(Author).join(AuthorCatalogBook).distinct().all()
    print(f"  Total authors with catalog: {len(authors_with_books)}")
    
    # Show authors with most books
    from sqlalchemy import func
    book_counts = session.query(
        Author.name,
        Author.id,
        func.count(AuthorCatalogBook.id).label('book_count')
    ).join(AuthorCatalogBook).group_by(Author.id).order_by(func.count(AuthorCatalogBook.id).desc()).limit(10).all()
    
    print("\nTop 10 authors by catalog book count:")
    for author_name, author_id, count in book_counts:
        print(f"  {author_name}: {count} books")
    
    # Check for potential issues
    print("\n" + "=" * 80)
    print("RECOVERY OPTIONS:")
    print("=" * 80)
    print()
    print("1. Check cloud storage version history (e.g. Dropbox, OneDrive):")
    print("   - Right-click on data/bookpilot.db in Finder (or your file manager)")
    print("   - Select 'Version History' or 'Previous Versions'")
    print("   - Restore an earlier version if available")
    print()
    print("2. Check if you have Time Machine backups:")
    print("   - Open Time Machine")
    print("   - Navigate to the BookPilot folder")
    print("   - Restore data/bookpilot.db from before the cleanup")
    print()
    print("3. Re-fetch catalogs:")
    print("   - If books were incorrectly deleted, you can re-fetch author catalogs")
    print("   - Run: python scripts/bookpilot.py catalog")
    print()
    print("4. Check SQLite journal/WAL files (if they exist):")
    print("   - These might contain transaction history")
    
    session.close()

if __name__ == '__main__':
    analyze_current_state()
