#!/usr/bin/env python3
"""
Check duplicates for a specific author by name.
Useful for testing duplicate detection on specific authors like Lauraine Snelling.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, Recommendation
from sqlalchemy import func
from scripts.check_duplicate_recommendations import analyze_author_recommendations


def check_specific_author(author_name: str, min_books: int = 1, dry_run: bool = True):
    """
    Check duplicates for a specific author.
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("="*80)
    print(f"CHECKING DUPLICATES FOR: {author_name}")
    print("="*80)
    if dry_run:
        print("(DRY RUN - no changes will be made)")
    print()
    
    # Find author
    author = session.query(Author).filter(
        func.lower(Author.name) == author_name.lower()
    ).first()
    
    if not author:
        print(f"❌ Author '{author_name}' not found in database")
        print("\nSimilar author names:")
        similar = session.query(Author).filter(
            func.lower(Author.name).like(f'%{author_name.lower()}%')
        ).limit(10).all()
        for a in similar:
            print(f"  - {a.name}")
        session.close()
        return
    
    # Get recommendations for this author that are visible in the UI
    # Filter out: thumbs_down, already_read, non_english, duplicate
    # This matches the filtering logic in web/app.py
    # A recommendation is visible if NONE of these flags are True
    all_recs = session.query(Recommendation).filter(
        func.lower(Recommendation.author) == author_name.lower()
    ).all()
    
    # Filter to only visible ones (where all flags are False or None)
    recommendations = [
        rec for rec in all_recs
        if not (rec.thumbs_down == True or 
                rec.already_read == True or 
                rec.non_english == True or 
                rec.duplicate == True)
    ]
    
    total_recs = len(all_recs)
    if total_recs > len(recommendations):
        filtered_count = total_recs - len(recommendations)
        print(f"Note: {filtered_count} recommendation(s) are filtered out (duplicate/non-English/already read/thumbs down)")
        print()
    
    if not recommendations:
        print(f"❌ No recommendations found for '{author_name}'")
        session.close()
        return
    
    print(f"Found {len(recommendations)} recommendation(s) for {author.name}\n")
    
    # Analyze
    result = analyze_author_recommendations(author, recommendations, min_books=min_books, dry_run=dry_run)
    
    session.close()
    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Check for duplicate recommendations for a specific author'
    )
    parser.add_argument('author_name', type=str,
                       help='Name of the author to check')
    parser.add_argument('--min-books', type=int, default=1,
                       help='Minimum number of recommendations (default: 1)')
    parser.add_argument('--no-dry-run', action='store_true',
                       help='Actually make changes (default is dry run)')
    
    args = parser.parse_args()
    
    check_specific_author(
        args.author_name,
        min_books=args.min_books,
        dry_run=not args.no_dry_run
    )
