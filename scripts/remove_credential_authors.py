#!/usr/bin/env python3
"""Remove authors that are only credentials (e.g., 'PhD', 'MD')"""
import sys
from pathlib import Path
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook, Book, Recommendation
from sqlalchemy import func, or_
import time


def is_credential_only(name):
    """Check if a name is only a credential with no actual author name"""
    name_lower = name.strip().lower()
    
    credential_patterns = [
        r'^ph\.?\s*d\.?$',  # Just "PhD" or "Ph.D."
        r'^m\.?\s*d\.?$',  # Just "MD" or "M.D." or "M D" or "M. D."
        r'^md$',  # Just "MD" (explicit check)
        r'^dr\.?$',  # Just "Dr" or "Dr."
        r'^prof\.?$',  # Just "Prof" or "Prof."
        r'^professor$',  # Just "Professor"
        r'^rev\.?$',  # Just "Rev" or "Rev."
        r'^reverend$',  # Just "Reverend"
    ]
    
    for pattern in credential_patterns:
        if re.match(pattern, name_lower):
            return True
    
    return False


def remove_credential_authors(dry_run=True):
    """Remove authors that are only credentials"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    print("REMOVING AUTHORS THAT ARE ONLY CREDENTIALS")
    print("=" * 80)
    if dry_run:
        print("DRY RUN MODE - No changes will be made\n")
    else:
        print("LIVE MODE - Changes will be committed\n")
    
    all_authors = session.query(Author).all()
    credential_authors = []
    
    for author in all_authors:
        if is_credential_only(author.name):
            catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
            book_count = session.query(Book).filter(
                or_(
                    func.lower(Book.author) == author.name.lower(),
                    func.lower(Book.author) == author.normalized_name.lower()
                )
            ).count()
            rec_count = session.query(Recommendation).filter(
                or_(
                    func.lower(Recommendation.author) == author.name.lower(),
                    func.lower(Recommendation.author) == author.normalized_name.lower()
                )
            ).count()
            
            # Include even if no data - orphaned credential-only authors should be removed
            credential_authors.append({
                'author': author,
                'catalog_count': catalog_count,
                'book_count': book_count,
                'rec_count': rec_count
            })
    
    if not credential_authors:
        print("No credential-only authors found!")
        return
    
    print(f"Found {len(credential_authors)} credential-only authors:\n")
    
    total_catalog = 0
    total_books = 0
    total_recs = 0
    
    for item in credential_authors:
        author = item['author']
        print(f"{author.name} (ID: {author.id})")
        print(f"  Normalized: {author.normalized_name}")
        print(f"  Open Library ID: {author.open_library_id or 'N/A'}")
        print(f"  Catalog books: {item['catalog_count']}")
        print(f"  Books (Libby): {item['book_count']}")
        print(f"  Recommendations: {item['rec_count']}")
        
        # Show sample titles
        if item['catalog_count'] > 0:
            sample_books = session.query(AuthorCatalogBook).filter_by(
                author_id=author.id
            ).limit(5).all()
            print(f"  Sample catalog titles:")
            for book in sample_books:
                print(f"    - {book.title}")
        
        print()
        
        total_catalog += item['catalog_count']
        total_books += item['book_count']
        total_recs += item['rec_count']
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total credential-only authors: {len(credential_authors)}")
    print(f"Total catalog books to delete: {total_catalog}")
    print(f"Total books (Libby) to delete: {total_books}")
    print(f"Total recommendations to delete: {total_recs}")
    print()
    
    if not dry_run:
        print("Deleting credential-only authors and associated data...")
        max_retries = 10
        authors_deleted = []
        
        for item in credential_authors:
            author = item['author']
            
            for attempt in range(max_retries):
                try:
                    with session.no_autoflush:
                        # Delete catalog books
                        catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
                        for catalog_book in catalog_books:
                            session.delete(catalog_book)
                        
                        # Delete books
                        books = session.query(Book).filter(
                            or_(
                                func.lower(Book.author) == author.name.lower(),
                                func.lower(Book.author) == author.normalized_name.lower()
                            )
                        ).all()
                        for book in books:
                            session.delete(book)
                        
                        # Delete recommendations
                        recommendations = session.query(Recommendation).filter(
                            or_(
                                func.lower(Recommendation.author) == author.name.lower(),
                                func.lower(Recommendation.author) == author.normalized_name.lower()
                            )
                        ).all()
                        for rec in recommendations:
                            session.delete(rec)
                        
                        # Delete author
                        session.delete(author)
                    
                    session.commit()
                    authors_deleted.append(author.name)
                    print(f"✓ Deleted: {author.name}")
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if 'locked' in error_str and attempt < max_retries - 1:
                        session.rollback()
                        wait_time = 0.1 * (2 ** attempt)
                        print(f"  Database locked, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        # Re-fetch objects after rollback
                        catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
                        books = session.query(Book).filter(
                            or_(
                                func.lower(Book.author) == author.name.lower(),
                                func.lower(Book.author) == author.normalized_name.lower()
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
                        print(f"✗ ERROR: Failed to delete {author.name}: {e}")
                        raise
        
        print(f"\n{'=' * 80}")
        print("DELETION COMPLETE")
        print(f"{'=' * 80}")
        print(f"Deleted {len(authors_deleted)} authors:")
        for name in authors_deleted:
            print(f"  - {name}")
    else:
        print(f"\n{'=' * 80}")
        print("DRY RUN SUMMARY")
        print(f"{'=' * 80}")
        print(f"Would delete {len(credential_authors)} authors and all associated data")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Remove authors that are only credentials')
    parser.add_argument('--execute', action='store_true', help='Actually delete (default is dry run)')
    args = parser.parse_args()
    
    remove_credential_authors(dry_run=not args.execute)
