#!/usr/bin/env python3
"""
Review and delete/flag children's/junior fiction books from recommendations.

Interactive workflow:
1. Detect all children's books
2. Show numbered list with reasons
3. Allow selective deletion (keep specific books, delete rest)
"""

import sys
from pathlib import Path
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Recommendation, Author, AuthorCatalogBook
from scripts.detect_childrens_books import scan_all_authors, analyze_author_childrens_books
from sqlalchemy import func
from src.deduplication.language_detection import is_english_title


def collect_all_childrens_books(min_books: int = 1, limit: int = None) -> List[Dict]:
    """
    Collect all children's books across all authors.
    Returns a flat list with numbering.
    """
    results = scan_all_authors(min_books=min_books, limit=limit)
    
    all_childrens = []
    global_number = 1
    
    for result in results:
        author = result['author']
        childrens_books = result['childrens_books']
        
        for book_info in childrens_books:
            book = book_info['catalog_book']
            
            all_childrens.append({
                'number': global_number,
                'author': author.name,
                'author_id': author.id,
                'catalog_book': book,
                'catalog_book_id': book.id,
                'reasons': book_info['reasons']
            })
            global_number += 1
    
    return all_childrens


def preview_childrens_books(childrens_books: List[Dict]):
    """
    Show numbered preview of all children's books.
    """
    print("="*80)
    print(f"CHILDREN'S BOOKS PREVIEW")
    print(f"Total: {len(childrens_books)} children's books found")
    print("="*80)
    print()
    
    # Group by author for summary
    by_author = {}
    for book in childrens_books:
        author = book['author']
        if author not in by_author:
            by_author[author] = []
        by_author[author].append(book)
    
    print("Breakdown by author:")
    for author, books in sorted(by_author.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {author}: {len(books)} children's books")
    print()
    
    # Show numbered list
    print("="*80)
    print("NUMBERED LIST OF CHILDREN'S BOOKS")
    print("="*80)
    print()
    
    for book in childrens_books:
        num = book['number']
        catalog_book = book['catalog_book']
        reasons = book['reasons']
        
        print(f"{num:4d}. [{book['author']}] {catalog_book.title}")
        if catalog_book.isbn:
            print(f"      ISBN: {catalog_book.isbn}")
        if catalog_book.categories:
            print(f"      Categories: {catalog_book.categories}")
        print(f"      Reasons: {', '.join(reasons)}")
        print()


def delete_childrens_books(childrens_books: List[Dict], keep_numbers: List[int] = None, dry_run: bool = True):
    """
    Flag children's books as duplicate (same as marking in UI).
    """
    keep_numbers_set = set(keep_numbers) if keep_numbers else set()
    
    to_flag = []
    to_keep = []
    
    for book in childrens_books:
        if book['number'] in keep_numbers_set:
            to_keep.append(book)
        else:
            to_flag.append(book)
    
    print("="*80)
    print("DELETION SUMMARY")
    print("="*80)
    print(f"\nTotal children's books: {len(childrens_books)}")
    print(f"Books to FLAG as duplicate: {len(to_flag)}")
    print(f"Books to KEEP: {len(to_keep)}")
    
    if to_keep:
        print(f"\nBooks to KEEP:")
        for book in to_keep:
            print(f"  {book['number']:4d}. [{book['author']}] {book['catalog_book'].title}")
    
    if to_flag:
        print(f"\nBooks to DELETE:")
        for book in to_flag[:20]:  # Show first 20
            print(f"  {book['number']:4d}. [{book['author']}] {book['catalog_book'].title}")
        if len(to_flag) > 20:
            print(f"  ... and {len(to_flag) - 20} more")
    
    if dry_run:
        print("\n(DRY RUN - no changes will be made)")
        return
    
    # Actually delete (delete catalog books, same as composite volumes)
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("\nDeleting children's catalog books...")
    deleted_count = 0
    
    for book in to_flag:
        catalog_book = session.query(AuthorCatalogBook).filter_by(id=book['catalog_book_id']).first()
        if catalog_book:
            session.delete(catalog_book)
            deleted_count += 1
    
    session.commit()
    print(f"✓ Deleted {deleted_count} children's catalog books")
    
    session.close()


def interactive_review():
    """
    Interactive mode: detect, preview, then allow selective flagging.
    """
    print("="*80)
    print("CHILDREN'S BOOK DEDUPLICATION")
    print("="*80)
    print()
    
    # Ask for scope
    print("Scan scope:")
    print("  1. All authors")
    print("  2. Authors with minimum number of recommendations")
    print("  3. Specific author")
    
    scope_choice = input("\nEnter choice (1/2/3): ").strip()
    
    min_books = 1
    limit = None
    author_name = None
    
    if scope_choice == '2':
        min_input = input("Minimum number of recommendations: ").strip()
        try:
            min_books = int(min_input)
        except ValueError:
            print("Invalid input. Using default: 1")
        limit_input = input("Limit number of authors (press Enter for all): ").strip()
        if limit_input:
            try:
                limit = int(limit_input)
            except ValueError:
                pass
    elif scope_choice == '3':
        author_name = input("Author name: ").strip()
    
    # Detect children's books
    print("\nScanning for children's books...")
    
    if author_name:
        db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
        engine = init_db(str(db_path))
        session = get_session(engine)
        
        author = session.query(Author).filter(
            Author.name.ilike(f'%{author_name}%')
        ).first()
        
        if not author:
            print(f"Author '{author_name}' not found.")
            session.close()
            return
        
        result = analyze_author_childrens_books(author, session)
        childrens_books = []
        for i, book_info in enumerate(result['childrens_books'], 1):
            childrens_books.append({
                'number': i,
                'author': author.name,
                'author_id': author.id,
                'recommendation': book_info['recommendation'],
                'recommendation_id': book_info['recommendation'].id,
                'catalog_book': book_info.get('catalog_book'),
                'reasons': book_info['reasons']
            })
        session.close()
    else:
        childrens_books = collect_all_childrens_books(min_books=min_books, limit=limit)
    
    if not childrens_books:
        print("No children's books found.")
        return
    
    # Show preview
    preview_childrens_books(childrens_books)
    
    # Ask for action
    print("="*80)
    print("DELETION OPTIONS")
    print("="*80)
    print("  1. Delete ALL children's books")
    print("  2. Delete all EXCEPT specific numbers (e.g., enter '5, 12' to keep those)")
    print("  3. Cancel")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == '3':
        print("Cancelled.")
        return
    
    keep_numbers = None
    if choice == '2':
        keep_input = input("Enter numbers to KEEP (comma-separated, e.g., '5, 12'): ").strip()
        try:
            keep_numbers = [int(x.strip()) for x in keep_input.split(',') if x.strip()]
            print(f"Will keep books: {keep_numbers}")
        except ValueError:
            print("Invalid input. Cancelling.")
            return
    
    # Confirm deletion
    if choice == '1':
        confirm = input(f"\nAre you sure you want to DELETE ALL {len(childrens_books)} children's books? (yes/no): ").strip().lower()
    else:
        delete_count = len(childrens_books) - len(keep_numbers)
        confirm = input(f"\nAre you sure you want to DELETE {delete_count} children's books (keeping {len(keep_numbers)})? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("Cancelled.")
        return
    
    # Show dry run first
    print("\n" + "="*80)
    print("DRY RUN PREVIEW")
    print("="*80)
    delete_childrens_books(childrens_books, keep_numbers=keep_numbers, dry_run=True)
    
    # Final confirmation
    final_confirm = input("\nProceed with flagging? (yes/no): ").strip().lower()
    if final_confirm != 'yes':
        print("Cancelled.")
        return
    
    # Actually delete
    print("\n" + "="*80)
    print("EXECUTING DELETION")
    print("="*80)
    delete_childrens_books(childrens_books, keep_numbers=keep_numbers, dry_run=False)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Review and delete children\'s/junior fiction books from catalog'
    )
    parser.add_argument('--preview-only', action='store_true',
                       help='Only show preview, do not delete')
    parser.add_argument('--delete-all', action='store_true',
                       help='Delete all children\'s books (requires --execute)')
    parser.add_argument('--keep', type=str,
                       help='Comma-separated list of book numbers to keep (e.g., "5, 12")')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete (default is dry run)')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive mode: detect, preview, then ask for confirmation')
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of recommendations (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_review()
    elif args.preview_only or (not args.flag_all and not args.keep):
        # Default: just preview
        if args.author:
            db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
            engine = init_db(str(db_path))
            session = get_session(engine)
            
            author = session.query(Author).filter(
                Author.name.ilike(f'%{args.author}%')
            ).first()
            
            if not author:
                print(f"Author '{args.author}' not found.")
                session.close()
                sys.exit(1)
            
            result = analyze_author_childrens_books(author, session)
            childrens_books = []
            for i, book_info in enumerate(result['childrens_books'], 1):
                childrens_books.append({
                    'number': i,
                    'author': author.name,
                    'author_id': author.id,
                    'catalog_book': book_info['catalog_book'],
                    'catalog_book_id': book_info['catalog_book'].id,
                    'reasons': book_info['reasons']
                })
            session.close()
        else:
            childrens_books = collect_all_childrens_books(min_books=args.min_books, limit=args.limit)
        
        preview_childrens_books(childrens_books)
    elif args.delete_all or args.keep:
        keep_numbers = None
        if args.keep:
            try:
                keep_numbers = [int(x.strip()) for x in args.keep.split(',') if x.strip()]
            except ValueError:
                print("Error: Invalid keep list. Use comma-separated numbers (e.g., '5, 12')")
                sys.exit(1)
        
        childrens_books = collect_all_childrens_books(min_books=args.min_books, limit=args.limit)
        delete_childrens_books(childrens_books, keep_numbers=keep_numbers, dry_run=not args.execute)
