#!/usr/bin/env python3
"""
Review and consolidate series with similar names.

Interactive workflow:
1. Detect all series consolidations
2. Show numbered list with details
3. Allow selective consolidation (keep specific variants, consolidate rest)
"""

import sys
from pathlib import Path
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook, Author
from scripts.consolidate_series import (
    scan_all_authors, find_series_consolidations,
    normalize_series_name, execute_consolidation
)


def collect_all_consolidations(min_books: int = 1, limit: int = None) -> List[Dict]:
    """
    Collect all series consolidations across all authors.
    Returns a flat list with numbering.
    """
    results = scan_all_authors(min_books=min_books, limit=limit)
    
    all_consolidations = []
    global_number = 1
    
    for result in results:
        author = result['author']
        consolidations = result['consolidations']
        
        for consolidation in consolidations:
            all_consolidations.append({
                'number': global_number,
                'author': author.name,
                'author_id': author.id,
                'consolidation': consolidation,
                'canonical_name': consolidation['canonical_name'],
                'variant_names': consolidation['variant_names'],
                'normalized_name': consolidation['normalized_name'],
                'books': consolidation['books'],
                'confidence': consolidation['confidence'],
                'positions': consolidation['positions']
            })
            global_number += 1
    
    return all_consolidations


def preview_consolidations(consolidations: List[Dict]):
    """
    Show numbered preview of all series consolidations.
    """
    print("="*80)
    print(f"SERIES CONSOLIDATION PREVIEW")
    print(f"Total: {len(consolidations)} consolidation groups found")
    print("="*80)
    print()
    
    # Group by author for summary
    by_author = {}
    for cons in consolidations:
        author = cons['author']
        if author not in by_author:
            by_author[author] = []
        by_author[author].append(cons)
    
    print("Breakdown by author:")
    for author, cons_list in sorted(by_author.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {author}: {len(cons_list)} consolidation group(s)")
    print()
    
    # Show numbered list
    print("="*80)
    print("NUMBERED LIST OF SERIES CONSOLIDATIONS")
    print("="*80)
    print()
    
    for cons in consolidations:
        num = cons['number']
        consolidation = cons['consolidation']
        canonical = cons['canonical_name']
        variants = cons['variant_names']
        books = cons['books']
        confidence = cons['confidence']
        positions = cons['positions']
        
        print(f"{num:4d}. [{cons['author']}] Normalized: \"{cons['normalized_name']}\"")
        print(f"      Canonical name: \"{canonical}\"")
        print(f"      Variants to merge: {len(variants)}")
        for variant in variants:
            if variant != canonical:
                print(f"         - \"{variant}\" → \"{canonical}\"")
        print(f"      Confidence: {confidence}")
        print(f"      Total books: {len(books)}")
        if positions:
            print(f"      Series positions: {positions}")
        print(f"      Books:")
        for book in sorted(books, key=lambda x: x.series_position or 999):
            variant_marker = " ⚠" if book.series_name != canonical else ""
            print(f"         #{book.series_position or '?'}: {book.title}")
            if book.series_name != canonical:
                print(f"            Current series: \"{book.series_name}\" → will change to \"{canonical}\"")
        print()


def execute_consolidations(consolidations: List[Dict], keep_numbers: List[int] = None, dry_run: bool = True):
    """
    Execute series consolidations, optionally keeping specific ones.
    """
    keep_numbers_set = set(keep_numbers) if keep_numbers else set()
    
    to_consolidate = []
    to_keep = []
    
    for cons in consolidations:
        if cons['number'] in keep_numbers_set:
            to_keep.append(cons)
        else:
            to_consolidate.append(cons)
    
    print("="*80)
    print("CONSOLIDATION SUMMARY")
    print("="*80)
    print(f"\nTotal consolidation groups: {len(consolidations)}")
    print(f"Groups to CONSOLIDATE: {len(to_consolidate)}")
    print(f"Groups to KEEP (skip): {len(to_keep)}")
    
    if to_keep:
        print(f"\nGroups to KEEP (skip consolidation):")
        for cons in to_keep:
            print(f"  {cons['number']:4d}. [{cons['author']}] \"{cons['normalized_name']}\"")
    
    if to_consolidate:
        print(f"\nGroups to CONSOLIDATE:")
        total_books_to_update = 0
        for cons in to_consolidate[:20]:  # Show first 20
            books_to_update = sum(1 for b in cons['books'] 
                                if b.series_name != cons['canonical_name'])
            total_books_to_update += books_to_update
            print(f"  {cons['number']:4d}. [{cons['author']}] \"{cons['normalized_name']}\" → \"{cons['canonical_name']}\" ({books_to_update} books)")
        if len(to_consolidate) > 20:
            print(f"  ... and {len(to_consolidate) - 20} more")
        print(f"\n  Total books to update: {total_books_to_update}")
    
    if dry_run:
        print("\n(DRY RUN - no changes will be made)")
        return
    
    # Actually consolidate
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("\nConsolidating series...")
    updated_total = 0
    
    for cons in to_consolidate:
        consolidation = cons['consolidation']
        updated = execute_consolidation(consolidation, session, dry_run=False)
        updated_total += updated
    
    session.commit()
    print(f"✓ Consolidated {len(to_consolidate)} series groups")
    print(f"✓ Updated {updated_total} catalog books")
    
    session.close()


def interactive_review():
    """
    Interactive mode: detect, preview, then allow selective consolidation.
    """
    print("="*80)
    print("SERIES CONSOLIDATION")
    print("="*80)
    print()
    
    # Ask for scope
    print("Scan scope:")
    print("  1. All authors")
    print("  2. Authors with minimum number of series books")
    print("  3. Specific author")
    
    scope_choice = input("\nEnter choice (1/2/3): ").strip()
    
    min_books = 1
    limit = None
    author_name = None
    
    if scope_choice == '2':
        min_input = input("Minimum number of series books: ").strip()
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
    
    # Detect consolidations
    print("\nScanning for series consolidations...")
    
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
        
        consolidations_list = find_series_consolidations(author, session)
        consolidations = []
        for i, consolidation in enumerate(consolidations_list, 1):
            consolidations.append({
                'number': i,
                'author': author.name,
                'author_id': author.id,
                'consolidation': consolidation,
                'canonical_name': consolidation['canonical_name'],
                'variant_names': consolidation['variant_names'],
                'normalized_name': consolidation['normalized_name'],
                'books': consolidation['books'],
                'confidence': consolidation['confidence'],
                'positions': consolidation['positions']
            })
        session.close()
    else:
        consolidations = collect_all_consolidations(min_books=min_books, limit=limit)
    
    if not consolidations:
        print("No series consolidations found.")
        return
    
    # Show preview
    preview_consolidations(consolidations)
    
    # Ask for action
    print("="*80)
    print("CONSOLIDATION OPTIONS")
    print("="*80)
    print("  1. Consolidate ALL series groups")
    print("  2. Consolidate all EXCEPT specific numbers (e.g., enter '5, 12' to skip those)")
    print("  3. Cancel")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == '3':
        print("Cancelled.")
        return
    
    keep_numbers = None
    if choice == '2':
        keep_input = input("Enter numbers to KEEP (skip consolidation, comma-separated, e.g., '5, 12'): ").strip()
        try:
            keep_numbers = [int(x.strip()) for x in keep_input.split(',') if x.strip()]
            print(f"Will skip consolidation for: {keep_numbers}")
        except ValueError:
            print("Invalid input. Cancelling.")
            return
    
    # Confirm consolidation
    if choice == '1':
        confirm = input(f"\nAre you sure you want to CONSOLIDATE ALL {len(consolidations)} series groups? (yes/no): ").strip().lower()
    else:
        consolidate_count = len(consolidations) - len(keep_numbers)
        confirm = input(f"\nAre you sure you want to CONSOLIDATE {consolidate_count} series groups (skipping {len(keep_numbers)})? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("Cancelled.")
        return
    
    # Show dry run first
    print("\n" + "="*80)
    print("DRY RUN PREVIEW")
    print("="*80)
    execute_consolidations(consolidations, keep_numbers=keep_numbers, dry_run=True)
    
    # Final confirmation
    final_confirm = input("\nProceed with consolidation? (yes/no): ").strip().lower()
    if final_confirm != 'yes':
        print("Cancelled.")
        return
    
    # Actually consolidate
    print("\n" + "="*80)
    print("EXECUTING CONSOLIDATION")
    print("="*80)
    execute_consolidations(consolidations, keep_numbers=keep_numbers, dry_run=False)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Review and consolidate series with similar names'
    )
    parser.add_argument('--preview-only', action='store_true',
                       help='Only show preview, do not consolidate')
    parser.add_argument('--consolidate-all', action='store_true',
                       help='Consolidate all series (requires --execute)')
    parser.add_argument('--keep', type=str,
                       help='Comma-separated list of consolidation numbers to skip (e.g., "5, 12")')
    parser.add_argument('--execute', action='store_true',
                       help='Actually consolidate (default is dry run)')
    parser.add_argument('--interactive', action='store_true',
                       help='Interactive mode: detect, preview, then ask for confirmation')
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of series books (default: 1, all authors)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of authors to process')
    parser.add_argument('--author', type=str,
                       help='Check specific author by name')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_review()
    elif args.preview_only or (not args.consolidate_all and not args.keep):
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
            
            consolidations_list = find_series_consolidations(author, session)
            consolidations = []
            for i, consolidation in enumerate(consolidations_list, 1):
                consolidations.append({
                    'number': i,
                    'author': author.name,
                    'author_id': author.id,
                    'consolidation': consolidation,
                    'canonical_name': consolidation['canonical_name'],
                    'variant_names': consolidation['variant_names'],
                    'normalized_name': consolidation['normalized_name'],
                    'books': consolidation['books'],
                    'confidence': consolidation['confidence'],
                    'positions': consolidation['positions']
                })
            session.close()
        else:
            consolidations = collect_all_consolidations(min_books=args.min_books, limit=args.limit)
        
        preview_consolidations(consolidations)
    elif args.consolidate_all or args.keep:
        keep_numbers = None
        if args.keep:
            try:
                keep_numbers = [int(x.strip()) for x in args.keep.split(',') if x.strip()]
            except ValueError:
                print("Error: Invalid keep list. Use comma-separated numbers (e.g., '5, 12')")
                sys.exit(1)
        
        consolidations = collect_all_consolidations(min_books=args.min_books, limit=args.limit)
        execute_consolidations(consolidations, keep_numbers=keep_numbers, dry_run=not args.execute)
