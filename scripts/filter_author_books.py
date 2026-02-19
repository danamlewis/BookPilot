#!/usr/bin/env python3
"""
Filter books from an author's catalog by title patterns
Removes books matching specified regex patterns (e.g., textbooks, specific editions)
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
import re

def filter_author_books(author_name, patterns=None, dry_run=True):
    """
    Remove books from an author's catalog that match specified patterns
    
    Args:
        author_name: Name of the author
        patterns: List of regex patterns to match against book titles (default: common textbook patterns)
        dry_run: If True, only show what would be removed
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Default patterns for textbooks if none provided
    if patterns is None:
        patterns = [
            r'modern\s+principle',
            r'macroeconomics',
            r'microeconomics',
        ]
    
    # Find author
    author = session.query(Author).filter(
        (Author.name.ilike(f'%{author_name}%')) | 
        (Author.normalized_name.ilike(f'%{author_name}%'))
    ).first()
    
    if not author:
        print(f"❌ Author matching '{author_name}' not found in database")
        session.close()
        return
    
    print(f"✓ Found: {author.name} (ID: {author.id})")
    print()
    
    # Get all catalog books for this author
    catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
    print(f"Total catalog books: {len(catalog_books)}")
    print()
    
    # Compile patterns
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
    
    # Find matching books
    books_to_remove = []
    for book in catalog_books:
        if book.title:
            for pattern in compiled_patterns:
                if pattern.search(book.title):
                    books_to_remove.append(book)
                    break  # Only add once even if multiple patterns match
    
    if not books_to_remove:
        print("✓ No matching books found")
        session.close()
        return
    
    print(f"Found {len(books_to_remove)} books to remove:")
    print()
    for book in books_to_remove:
        print(f"  - {book.title}")
        if book.isbn:
            print(f"    ISBN: {book.isbn}")
    print()
    
    if dry_run:
        print("DRY RUN - No books were actually removed")
        print("Run with --execute to remove these books")
    else:
        print("Removing books...")
        print("  Note: Close the web UI if it's running to avoid database locks")
        print()
        
        removed_count = 0
        failed_count = 0
        
        for i, book in enumerate(books_to_remove, 1):
            try:
                session.delete(book)
                removed_count += 1
                
                # Commit every 10 deletions to avoid long-held locks
                if removed_count % 10 == 0:
                    session.commit()
                    print(f"  Committed {removed_count}/{len(books_to_remove)} deletions...")
            except Exception as e:
                error_msg = str(e).lower()
                if "locked" in error_msg:
                    print(f"  ⚠ Warning: Database locked while deleting '{book.title}'")
                    print(f"    Try closing the web UI and retry")
                    failed_count += 1
                    try:
                        session.rollback()
                    except:
                        pass
                    # Small delay to allow lock to be released
                    import time
                    time.sleep(0.5)
                else:
                    print(f"  ⚠ Error deleting '{book.title}': {e}")
                    failed_count += 1
                    try:
                        session.rollback()
                    except:
                        pass
        
        # Final commit for any remaining deletions
        try:
            session.commit()
            print(f"\n✓ Successfully removed {removed_count} books")
            if failed_count > 0:
                print(f"⚠ {failed_count} books could not be removed (likely due to database locks)")
                print(f"  Close the web UI and run the script again to remove the remaining books")
        except Exception as e:
            print(f"\n⚠ Error during final commit: {e}")
            if "locked" in str(e).lower():
                print(f"  Database is locked. Close the web UI and retry.")
            session.rollback()
            if removed_count > 0:
                print(f"  {removed_count} books were deleted before the error")
    
    session.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Filter books from an author\'s catalog by title patterns',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Filter textbooks from an author (dry run)
  python scripts/filter_author_books.py --author "Author Name"
  
  # Use custom patterns
  python scripts/filter_author_books.py --author "Author Name" --pattern "textbook" --pattern "edition"
  
  # Actually remove books
  python scripts/filter_author_books.py --author "Author Name" --execute
        """
    )
    parser.add_argument('--author', required=True, help='Author name to filter books for')
    parser.add_argument('--pattern', action='append', dest='patterns',
                       help='Regex pattern to match against book titles (can be used multiple times)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually remove the books (default is dry-run)')
    args = parser.parse_args()
    
    filter_author_books(args.author, patterns=args.patterns, dry_run=not args.execute)
