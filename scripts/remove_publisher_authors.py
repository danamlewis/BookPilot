#!/usr/bin/env python3
"""Remove author(s) by name and all associated books"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook, Book, Recommendation
from sqlalchemy import or_, func
import time


def remove_author_by_name(author_names, dry_run=True):
    """
    Remove author(s) by name and all associated books
    
    Args:
        author_names: List of author names to remove, or single string
        dry_run: If True, only show what would be deleted
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Normalize input to list
    if isinstance(author_names, str):
        author_names_to_remove = [author_names]
    else:
        author_names_to_remove = list(author_names)
    
    print("=" * 80)
    print("REMOVING AUTHOR(S) BY NAME")
    print("=" * 80)
    if dry_run:
        print("DRY RUN MODE - No changes will be made\n")
    else:
        print("LIVE MODE - Changes will be committed\n")
    
    total_catalog_deleted = 0
    total_books_deleted = 0
    total_recommendations_deleted = 0
    authors_deleted = []
    
    # Find all authors to remove by name (case-insensitive, supports partial matches)
    authors_to_remove = []
    
    for name in author_names_to_remove:
        authors = session.query(Author).filter(
            or_(
                func.lower(Author.name) == name.lower(),
                func.lower(Author.normalized_name) == name.lower(),
                func.lower(Author.name).like(f'%{name.lower()}%'),
                func.lower(Author.normalized_name).like(f'%{name.lower()}%')
            )
        ).all()
        for author in authors:
            if author not in authors_to_remove:
                authors_to_remove.append(author)
    
    if not authors_to_remove:
        print("No authors found to remove.")
        return
    
    for author in authors_to_remove:
        print(f"\n--- Processing: {author.name} (ID: {author.id}) ---")
        print(f"   Normalized: {author.normalized_name}")
        
        # Count catalog books
        catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
        catalog_count = len(catalog_books)
        print(f"   Catalog books: {catalog_count}")
        
        # Count books from Book table
        books = session.query(Book).filter(
            or_(
                func.lower(Book.author) == author.normalized_name.lower(),
                func.lower(Book.author) == author.name.lower()
            )
        ).all()
        book_count = len(books)
        print(f"   Books read (from Libby): {book_count}")
        
        # Count recommendations
        recommendations = session.query(Recommendation).filter(
            or_(
                func.lower(Recommendation.author) == author.name.lower(),
                func.lower(Recommendation.author) == author.normalized_name.lower()
            )
        ).all()
        rec_count = len(recommendations)
        print(f"   Recommendations: {rec_count}")
        
        # Update totals
        total_catalog_deleted += catalog_count
        total_books_deleted += book_count
        total_recommendations_deleted += rec_count
        
        if not dry_run:
            # Delete in batches with retry logic
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    with session.no_autoflush:
                        # Delete catalog books
                        for catalog_book in catalog_books:
                            session.delete(catalog_book)
                        
                        # Delete books from Book table
                        for book in books:
                            session.delete(book)
                        
                        # Delete recommendations
                        for rec in recommendations:
                            session.delete(rec)
                        
                        # Delete author
                        session.delete(author)
                    
                    # Commit with retry
                    session.commit()
                    authors_deleted.append(author.name)
                    print(f"   ✓ Deleted")
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if 'locked' in error_str and attempt < max_retries - 1:
                        session.rollback()
                        wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                        print(f"   Database locked, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        # Re-fetch objects after rollback
                        catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
                        books = session.query(Book).filter(
                            or_(
                                func.lower(Book.author) == author.normalized_name.lower(),
                                func.lower(Book.author) == author.name.lower()
                            )
                        ).all()
                        recommendations = session.query(Recommendation).filter(
                            or_(
                                func.lower(Recommendation.author) == author.name.lower(),
                                func.lower(Recommendation.author) == author.normalized_name.lower()
                            )
                        ).all()
                    else:
                        session.rollback()
                        print(f"\nERROR: Failed to delete {author.name}: {e}")
                        raise
        else:
            print(f"   [DRY RUN] Would delete {catalog_count} catalog books, {book_count} books, {rec_count} recommendations, and the author")
    
    if not dry_run:
        print(f"\n{'=' * 80}")
        print("DELETION COMPLETE")
        print(f"{'=' * 80}")
        print(f"Authors deleted: {len(authors_deleted)}")
        for name in authors_deleted:
            print(f"  - {name}")
        print(f"\nCatalog books deleted: {total_catalog_deleted}")
        print(f"Books deleted: {total_books_deleted}")
        print(f"Recommendations deleted: {total_recommendations_deleted}")
    else:
        print(f"\n{'=' * 80}")
        print("DRY RUN SUMMARY")
        print(f"{'=' * 80}")
        print(f"Authors that would be deleted: {len(authors_to_remove)}")
        for author in authors_to_remove:
            print(f"  - {author.name} (ID: {author.id})")
        print(f"\nTotal catalog books that would be deleted: {total_catalog_deleted}")
        print(f"Total books that would be deleted: {total_books_deleted}")
        print(f"Total recommendations that would be deleted: {total_recommendations_deleted}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Remove author(s) by name and all associated books',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Remove a single author (dry run)
  python scripts/remove_publisher_authors.py --author "Author or Publisher Name"
  
  # Remove multiple authors
  python scripts/remove_publisher_authors.py --author "Publisher A" --author "Publisher B"
  
  # Actually delete (requires --execute)
  python scripts/remove_publisher_authors.py --author "Author or Publisher Name" --execute
        """
    )
    parser.add_argument('--author', action='append', required=True,
                       help='Author name(s) to remove (can be used multiple times)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete (default is dry run)')
    args = parser.parse_args()
    
    remove_author_by_name(args.author, dry_run=not args.execute)
