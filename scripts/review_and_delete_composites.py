#!/usr/bin/env python3
"""
Review and delete composite volumes that duplicate standalone books.

Interactive workflow:
1. Detect all composite volumes
2. Show numbered list with matches
3. Allow selective deletion (keep specific composites, delete rest)
"""

import sys
import json
from pathlib import Path
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author, Recommendation
from scripts.detect_composite_volumes import (
    scan_all_authors, analyze_author_composites, 
    is_composite_volume, find_composite_standalone_matches
)


def collect_all_composites(min_books: int = 1, limit: int = None) -> List[Dict]:
    """
    Collect all composite volumes across all authors.
    Returns a flat list with numbering.
    """
    results = scan_all_authors(min_books=min_books, limit=limit)
    
    all_composites = []
    global_number = 1
    
    for result in results:
        author = result['author']
        matches = result['matches']
        
        for match in matches:
            composite = match['composite_book']
            standalones = match['standalone_books']
            
            all_composites.append({
                'number': global_number,
                'author': author.name,
                'author_id': author.id,
                'composite': composite,
                'composite_id': composite.id,
                'composite_series_name': match.get('composite_series_name'),
                'composite_series_position': match.get('composite_series_position'),
                'standalone_books': standalones,
                'component_titles': match['component_titles'],
                'confidence': match['confidence'],
                'reason': match['reason']
            })
            global_number += 1
    
    return all_composites


def preview_composites(composites: List[Dict], show_all: bool = False):
    """
    Show numbered preview of all composite volumes.
    """
    print("="*80)
    print(f"COMPOSITE VOLUMES PREVIEW")
    print(f"Total: {len(composites)} composite volumes found")
    print("="*80)
    print()
    
    # Group by author for summary
    by_author = {}
    for comp in composites:
        author = comp['author']
        if author not in by_author:
            by_author[author] = []
        by_author[author].append(comp)
    
    print("Breakdown by author:")
    for author, comps in sorted(by_author.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {author}: {len(comps)} composite volumes")
    print()
    
    # Show numbered list
    print("="*80)
    print("NUMBERED LIST OF COMPOSITE VOLUMES")
    print("="*80)
    print()
    
    for comp in composites:
        num = comp['number']
        composite = comp['composite']
        standalones = comp['standalone_books']
        confidence = comp['confidence']
        
        print(f"{num:4d}. [{comp['author']}] {composite.title}")
        if composite.isbn:
            print(f"      ISBN: {composite.isbn}")
        if comp.get('composite_series_name'):
            print(f"      Series: {comp['composite_series_name']} #{comp.get('composite_series_position', '?')}")
        print(f"      Confidence: {confidence}")
        print(f"      Reason: {comp['reason']}")
        
        if standalones:
            print(f"      Matched {len(standalones)} standalone catalog book(s):")
            for i, standalone_info in enumerate(standalones, 1):
                standalone = standalone_info['standalone']
                match_score = standalone_info['match_score']
                print(f"         {i}. {standalone.title} (match: {match_score:.0%})")
                if standalone.isbn:
                    print(f"            ISBN: {standalone.isbn}")
        else:
            print(f"      No matching standalone catalog books found")
        print()


def delete_composites(composites: List[Dict], keep_numbers: List[int] = None, dry_run: bool = True):
    """
    Delete composite volumes, optionally keeping specific ones.
    """
    keep_numbers_set = set(keep_numbers) if keep_numbers else set()
    
    to_delete = []
    to_keep = []
    
    for comp in composites:
        if comp['number'] in keep_numbers_set:
            to_keep.append(comp)
        else:
            to_delete.append(comp)
    
    print("="*80)
    print("DELETION SUMMARY")
    print("="*80)
    print(f"\nTotal composite volumes: {len(composites)}")
    print(f"Composite volumes to DELETE: {len(to_delete)}")
    print(f"Composite volumes to KEEP: {len(to_keep)}")
    
    if to_keep:
        print(f"\nComposite volumes to KEEP:")
        for comp in to_keep:
            print(f"  {comp['number']:4d}. [{comp['author']}] {comp['composite'].title}")
    
    if to_delete:
        print(f"\nComposite volumes to DELETE:")
        for comp in to_delete[:20]:  # Show first 20
            print(f"  {comp['number']:4d}. [{comp['author']}] {comp['composite'].title}")
        if len(to_delete) > 20:
            print(f"  ... and {len(to_delete) - 20} more")
    
    if dry_run:
        print("\n(DRY RUN - no changes will be made)")
        return
    
    # Actually delete (delete composite catalog books and update series info for standalones)
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("\nDeleting composite volumes and updating series info...")
    deleted_count = 0
    updated_count = 0
    
    for comp in to_delete:
        # Mark composite catalog book as duplicate (same as UI marking)
        # We don't delete it, just mark it so it won't show in recommendations
        catalog_book = session.query(AuthorCatalogBook).filter_by(id=comp['composite_id']).first()
        if catalog_book:
            # Mark as duplicate by setting is_read=True (this prevents it from being recommended)
            # OR we could add a duplicate flag, but for now use is_read
            # Actually, let's check if there's a better way - maybe we should just delete it
            # since the user wants to keep only standalone versions
            session.delete(catalog_book)
            deleted_count += 1
        
        # Update series info for standalone catalog books (only if we have matches)
        composite_series_name = comp.get('composite_series_name')
        if composite_series_name and comp.get('standalone_books'):
            for i, standalone_info in enumerate(comp['standalone_books'], 1):
                standalone_book = standalone_info['standalone']
                # Update series info: series name from composite, position = i (1, 2, 3, etc.)
                standalone_book.series_name = composite_series_name
                standalone_book.series_position = i
                updated_count += 1
    
    session.commit()
    print(f"✓ Deleted {deleted_count} composite catalog books")
    print(f"✓ Updated series info for {updated_count} standalone catalog books")
    
    session.close()


def interactive_review():
    """
    Interactive mode: detect, preview, then allow selective deletion.
    """
    print("="*80)
    print("COMPOSITE VOLUME DEDUPLICATION")
    print("="*80)
    print()
    
    # Ask for scope
    print("Scan scope:")
    print("  1. All authors")
    print("  2. Authors with minimum number of books")
    print("  3. Specific author")
    
    scope_choice = input("\nEnter choice (1/2/3): ").strip()
    
    min_books = 1
    limit = None
    author_name = None
    
    if scope_choice == '2':
        min_input = input("Minimum number of catalog books: ").strip()
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
    
    # Detect composites
    print("\nScanning for composite volumes...")
    
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
        
        result = analyze_author_composites(author, session)
        composites = []
        for i, match in enumerate(result['matches'], 1):
            composites.append({
                'number': i,
                'author': author.name,
                'author_id': author.id,
                'composite': match['composite_book'],
                'composite_id': match['composite_book'].id,
                'composite_series_name': match.get('composite_series_name'),
                'composite_series_position': match.get('composite_series_position'),
                'standalone_books': match['standalone_books'],
                'component_titles': match['component_titles'],
                'confidence': match['confidence'],
                'reason': match['reason']
            })
        session.close()
    else:
        composites = collect_all_composites(min_books=min_books, limit=limit)
    
    if not composites:
        print("No composite volumes found.")
        return
    
    # Show preview
    preview_composites(composites)
    
    # Ask for action
    print("="*80)
    print("DELETION OPTIONS")
    print("="*80)
    print("  1. Delete ALL composite volumes")
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
            print(f"Will keep composite volumes: {keep_numbers}")
        except ValueError:
            print("Invalid input. Cancelling.")
            return
    
    # Confirm deletion
    if choice == '1':
        confirm = input(f"\nAre you sure you want to DELETE ALL {len(composites)} composite volumes? (yes/no): ").strip().lower()
    else:
        delete_count = len(composites) - len(keep_numbers)
        confirm = input(f"\nAre you sure you want to DELETE {delete_count} composite volumes (keeping {len(keep_numbers)})? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("Cancelled.")
        return
    
    # Show dry run first
    print("\n" + "="*80)
    print("DRY RUN PREVIEW")
    print("="*80)
    delete_composites(composites, keep_numbers=keep_numbers, dry_run=True)
    
    # Final confirmation
    final_confirm = input("\nProceed with deletion? (yes/no): ").strip().lower()
    if final_confirm != 'yes':
        print("Cancelled.")
        return
    
    # Actually delete
    print("\n" + "="*80)
    print("EXECUTING DELETION")
    print("="*80)
    delete_composites(composites, keep_numbers=keep_numbers, dry_run=False)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Review and delete composite volumes that duplicate standalone books'
    )
    parser.add_argument('--preview-only', action='store_true',
                       help='Only show preview, do not delete')
    parser.add_argument('--delete-all', action='store_true',
                       help='Delete all composite volumes (requires --execute)')
    parser.add_argument('--keep', type=str,
                       help='Comma-separated list of composite numbers to keep (e.g., "5, 12")')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete (default is dry run)')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive mode: detect, preview, then ask for confirmation')
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of catalog books (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_review()
    elif args.preview_only or (not args.delete_all and not args.keep):
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
            
            result = analyze_author_composites(author, session)
            composites = []
            for i, match in enumerate(result['matches'], 1):
                composites.append({
                    'number': i,
                    'author': author.name,
                    'author_id': author.id,
                    'composite': match['composite_book'],
                    'composite_id': match['composite_book'].id,
                    'standalone_books': match['standalone_books'],
                    'component_titles': match['component_titles'],
                    'confidence': match['confidence'],
                    'reason': match['reason']
                })
            session.close()
        else:
            composites = collect_all_composites(min_books=args.min_books, limit=args.limit)
        
        preview_composites(composites)
    elif args.delete_all or args.keep:
        keep_numbers = None
        if args.keep:
            try:
                keep_numbers = [int(x.strip()) for x in args.keep.split(',') if x.strip()]
            except ValueError:
                print("Error: Invalid keep list. Use comma-separated numbers (e.g., '5, 12')")
                sys.exit(1)
        
        composites = collect_all_composites(min_books=args.min_books, limit=args.limit)
        delete_composites(composites, keep_numbers=keep_numbers, dry_run=not args.execute)
