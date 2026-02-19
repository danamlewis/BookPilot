#!/usr/bin/env python3
"""
Preview and delete non-English catalog books.

1. Scans all catalog books for non-English titles
2. Shows numbered preview list
3. Allows selective deletion (keep specific numbers, delete rest)
"""

import sys
from pathlib import Path
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from src.deduplication.language_detection import detect_non_english_title, is_english_title


def preview_non_english_books(dry_run: bool = True):
    """
    Preview all non-English catalog books.
    
    Returns:
        List of non-English books with metadata
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print("NON-ENGLISH BOOKS PREVIEW")
    print("="*80)
    if dry_run:
        print("(DRY RUN - no changes will be made)")
    print()
    
    # Get all catalog books that are eligible (not already read)
    catalog_books = session.query(AuthorCatalogBook).filter_by(
        is_read=False
    ).order_by(AuthorCatalogBook.author_id, AuthorCatalogBook.title).all()
    
    print(f"Scanning {len(catalog_books)} eligible catalog books...\n")
    
    non_english_books = []
    by_author = defaultdict(list)
    
    for book in catalog_books:
        is_non_english, reasons = detect_non_english_title(
            book.title, book.isbn, book.open_library_key
        )
        
        if is_non_english:
            # Get author name
            author = session.query(Author).filter_by(id=book.author_id).first()
            author_name = author.name if author else f"Author ID {book.author_id}"
            
            book_data = {
                'id': book.id,
                'title': book.title,
                'author': author_name,
                'author_id': book.author_id,
                'isbn': book.isbn,
                'reasons': reasons
            }
            
            non_english_books.append(book_data)
            by_author[author_name].append(book_data)
    
    print(f"Found {len(non_english_books)} non-English catalog books\n")
    
    # Show breakdown by author
    if by_author:
        print("Breakdown by author:")
        sorted_authors = sorted(by_author.items(), key=lambda x: len(x[1]), reverse=True)
        for author_name, books in sorted_authors:
            print(f"  {author_name}: {len(books)} non-English books")
        print()
    
    # Show numbered list
    print("="*80)
    print(f"NON-ENGLISH BOOKS LIST")
    print(f"Total: {len(non_english_books)} books / {len(catalog_books)} scanned")
    print("="*80)
    print()
    
    for i, book in enumerate(non_english_books, 1):
        isbn_str = f" (ISBN: {book['isbn']})" if book['isbn'] else ""
        reasons_str = ", ".join(book['reasons'])
        print(f"{i:4d}. [{book['author']}] {book['title']}{isbn_str}")
        print(f"      Reasons: {reasons_str}")
        print()
    
    session.close()
    
    return non_english_books


def delete_non_english_books(keep_numbers: list = None, dry_run: bool = True):
    """
    Delete non-English catalog books, optionally keeping specific ones.
    
    Args:
        keep_numbers: List of book numbers (1-indexed) to keep. If None, delete all.
        dry_run: If True, don't actually delete, just show what would be deleted.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Get all non-English books
    non_english_books = preview_non_english_books(dry_run=True)
    
    if not non_english_books:
        print("No non-English books found.")
        session.close()
        return
    
    # Determine which books to delete
    keep_numbers_set = set(keep_numbers) if keep_numbers else set()
    books_to_delete = []
    books_to_keep = []
    
    for i, book in enumerate(non_english_books, 1):
        if i in keep_numbers_set:
            books_to_keep.append((i, book))
        else:
            books_to_delete.append((i, book))
    
    print("="*80)
    print("DELETION SUMMARY")
    print("="*80)
    print(f"\nTotal non-English books: {len(non_english_books)}")
    print(f"Books to DELETE: {len(books_to_delete)}")
    print(f"Books to KEEP: {len(books_to_keep)}")
    
    if books_to_keep:
        print(f"\nBooks to KEEP:")
        for num, book in books_to_keep:
            print(f"  {num:4d}. [{book['author']}] {book['title']}")
    
    if books_to_delete:
        print(f"\nBooks to DELETE:")
        for num, book in books_to_delete[:20]:  # Show first 20
            print(f"  {num:4d}. [{book['author']}] {book['title']}")
        if len(books_to_delete) > 20:
            print(f"  ... and {len(books_to_delete) - 20} more")
    
    if dry_run:
        print("\n(DRY RUN - no changes will be made)")
        session.close()
        return
    
    # Actually delete
    print("\nDeleting books...")
    deleted_count = 0
    
    for num, book in books_to_delete:
        catalog_book = session.query(AuthorCatalogBook).filter_by(id=book['id']).first()
        if catalog_book:
            session.delete(catalog_book)
            deleted_count += 1
    
    session.commit()
    print(f"✓ Deleted {deleted_count} non-English catalog books")
    
    session.close()


def interactive_delete():
    """
    Interactive mode: preview, then ask for confirmation and optional keep list.
    """
    # First, show preview
    non_english_books = preview_non_english_books(dry_run=True)
    
    if not non_english_books:
        print("No non-English books found.")
        return
    
    total_books = len(non_english_books)
    print(f"\n{'='*80}")
    print(f"PREVIEW COMPLETE")
    print(f"{'='*80}")
    print(f"Found: {total_books} non-English books")
    print(f"{'='*80}\n")
    
    # Ask for confirmation
    print("Options:")
    print("  1. Delete ALL non-English books")
    print("  2. Delete all EXCEPT specific numbers (e.g., enter '75, 84' to keep those)")
    print("  3. Cancel")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == '3':
        print("Cancelled.")
        return
    
    keep_numbers = None
    if choice == '2':
        keep_input = input("Enter numbers to KEEP (comma-separated, e.g., '75, 84'): ").strip()
        try:
            keep_numbers = [int(x.strip()) for x in keep_input.split(',') if x.strip()]
            print(f"Will keep books: {keep_numbers}")
        except ValueError:
            print("Invalid input. Cancelling.")
            return
    
    # Confirm deletion
    if choice == '1':
        confirm = input(f"\nAre you sure you want to DELETE ALL {total_books} non-English books? (yes/no): ").strip().lower()
    else:
        delete_count = total_books - len(keep_numbers)
        confirm = input(f"\nAre you sure you want to DELETE {delete_count} books (keeping {len(keep_numbers)})? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("Cancelled.")
        return
    
    # Show dry run first
    print("\n" + "="*80)
    print("DRY RUN PREVIEW")
    print("="*80)
    delete_non_english_books(keep_numbers=keep_numbers, dry_run=True)
    
    # Final confirmation
    final_confirm = input("\nProceed with deletion? (yes/no): ").strip().lower()
    if final_confirm != 'yes':
        print("Cancelled.")
        return
    
    # Actually delete
    print("\n" + "="*80)
    print("EXECUTING DELETION")
    print("="*80)
    delete_non_english_books(keep_numbers=keep_numbers, dry_run=False)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Preview and delete non-English catalog books'
    )
    parser.add_argument('--preview-only', action='store_true',
                       help='Only show preview, do not delete')
    parser.add_argument('--delete-all', action='store_true',
                       help='Delete all non-English books (requires --execute)')
    parser.add_argument('--keep', type=str,
                       help='Comma-separated list of book numbers to keep (e.g., "75, 84")')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete (default is dry run)')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive mode: preview then ask for confirmation')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_delete()
    elif args.preview_only:
        preview_non_english_books(dry_run=True)
    elif args.delete_all or args.keep:
        keep_numbers = None
        if args.keep:
            try:
                keep_numbers = [int(x.strip()) for x in args.keep.split(',') if x.strip()]
            except ValueError:
                print("Error: Invalid keep list. Use comma-separated numbers (e.g., '75, 84')")
                sys.exit(1)
        
        delete_non_english_books(keep_numbers=keep_numbers, dry_run=not args.execute)
    else:
        # Default: just preview
        preview_non_english_books(dry_run=True)
