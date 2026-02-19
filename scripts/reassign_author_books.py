#!/usr/bin/env python3
"""Reassign books from one author to another (fixes incorrect author assignments)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook, Book, Recommendation
from src.ingest import normalize_author_name
from sqlalchemy import or_

def reassign_author_books(source_author_name, target_author_name, dry_run=True):
    """
    Reassign books from source author to target author
    
    Args:
        source_author_name: Name of the author to reassign books from
        target_author_name: Name of the author to reassign books to
        dry_run: If True, only show what would be changed
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    try:
        # Find source author
        source_author = session.query(Author).filter(
            or_(
                Author.name.ilike(f'%{source_author_name}%'),
                Author.normalized_name.ilike(f'%{source_author_name}%')
            )
        ).first()
        
        if not source_author:
            print(f"Source author '{source_author_name}' not found - nothing to fix")
            session.close()
            return
        
        print(f"Found source author: {source_author.name} (ID: {source_author.id})")
        print(f"  Normalized name: {source_author.normalized_name}")
        print()
        
        # Count books assigned to source author
        catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=source_author.id).count()
        book_count = session.query(Book).filter_by(author=source_author.normalized_name).count()
        rec_count = session.query(Recommendation).filter_by(author=source_author.normalized_name).count()
        
        print(f"  Catalog books assigned: {catalog_count}")
        print(f"  Libby books assigned: {book_count}")
        print(f"  Recommendations assigned: {rec_count}")
        print()
        
        if catalog_count == 0 and book_count == 0 and rec_count == 0:
            print("No books assigned to source author - nothing to fix")
            session.close()
            return
        
        # Find or create target author
        target_author = session.query(Author).filter(
            or_(
                Author.name.ilike(f'%{target_author_name}%'),
                Author.normalized_name.ilike(f'%{target_author_name}%')
            )
        ).first()
        
        if not target_author:
            if dry_run:
                print(f"[DRY RUN] Would create target author: {target_author_name}")
            else:
                print(f"\nCreating target author: {target_author_name}...")
                target_author = Author(
                    name=target_author_name,
                    normalized_name=normalize_author_name(target_author_name)
                )
                session.add(target_author)
                session.flush()
                print(f"  ✓ Created {target_author_name} (ID: {target_author.id})")
        else:
            print(f"\nFound existing target author: {target_author.name} (ID: {target_author.id})")
        
        if dry_run:
            print(f"\n[DRY RUN] Would reassign:")
            print(f"  {catalog_count} catalog books from {source_author.name} to {target_author_name}")
            print(f"  {book_count} Libby books from {source_author.name} to {target_author_name}")
            print(f"  {rec_count} recommendations from {source_author.name} to {target_author_name}")
            print(f"\n  Would fix {source_author.name}'s normalized_name to: {normalize_author_name(source_author.name)}")
            print("\nRun with --execute to actually make these changes")
        else:
            # Reassign catalog books
            if catalog_count > 0:
                print(f"\nReassigning {catalog_count} catalog books from {source_author.name} to {target_author.name}...")
                catalog_books = session.query(AuthorCatalogBook).filter_by(author_id=source_author.id).all()
                for cb in catalog_books:
                    cb.author_id = target_author.id
                print(f"  ✓ Reassigned {len(catalog_books)} catalog books")
            
            # Reassign Libby books
            if book_count > 0:
                print(f"\nReassigning {book_count} Libby books from {source_author.name} to {target_author.name}...")
                books = session.query(Book).filter_by(author=source_author.normalized_name).all()
                for book in books:
                    book.author = target_author.normalized_name
                print(f"  ✓ Reassigned {len(books)} Libby books")
            
            # Reassign recommendations
            if rec_count > 0:
                print(f"\nReassigning {rec_count} recommendations from {source_author.name} to {target_author.name}...")
                recommendations = session.query(Recommendation).filter_by(author=source_author.normalized_name).all()
                for rec in recommendations:
                    rec.author = target_author.normalized_name
                print(f"  ✓ Reassigned {len(recommendations)} recommendations")
            
            # Fix source author's normalized_name
            print(f"\nFixing {source_author.name}'s normalized_name...")
            source_author.normalized_name = normalize_author_name(source_author.name)
            print(f"  ✓ Set normalized_name to '{source_author.normalized_name}'")
            
            # Commit changes
            session.commit()
            print("\n✓ All changes committed successfully!")
            
            # Verify
            target_catalog = session.query(AuthorCatalogBook).filter_by(author_id=target_author.id).count()
            target_books = session.query(Book).filter_by(author=target_author.normalized_name).count()
            target_recs = session.query(Recommendation).filter_by(author=target_author.normalized_name).count()
            print(f"\nVerification:")
            print(f"  {target_author.name}: {target_catalog} catalog books, {target_books} Libby books, {target_recs} recommendations")
            print(f"  {source_author.name}: normalized_name is now '{source_author.normalized_name}'")
        
    except Exception as e:
        session.rollback()
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Reassign books from one author to another',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview reassignment (dry run)
  python scripts/reassign_author_books.py --source "Source Author" --target "Target Author"
  
  # Actually reassign books
  python scripts/reassign_author_books.py --source "Source Author" --target "Target Author" --execute
        """
    )
    parser.add_argument('--source', required=True, dest='source_author',
                       help='Source author name (books will be moved from this author)')
    parser.add_argument('--target', required=True, dest='target_author',
                       help='Target author name (books will be moved to this author)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually make the changes (default is dry run)')
    args = parser.parse_args()
    
    reassign_author_books(args.source_author, args.target_author, dry_run=not args.execute)
