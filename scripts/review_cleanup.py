#!/usr/bin/env python3
"""
Review cleanup results - run a dry-run and show detailed examples
Best way to verify what will be removed before running the actual cleanup
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
from src.catalog import cleanup_non_english_books

def review_cleanup(limit=100, offset=0, sample_size=20):
    """Run a dry-run and show detailed examples"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    print("REVIEWING CLEANUP - DRY RUN")
    print("=" * 80)
    print()
    print("This will show you what books would be removed WITHOUT actually removing them.")
    print()
    
    # Run the actual cleanup function in dry-run mode
    result = cleanup_non_english_books(session, dry_run=True, limit=limit, offset=offset)
    
    session.close()
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Books checked: {result.get('checked', 'N/A')}")
    print(f"Non-English books found: {result.get('removed', 'N/A')}")
    print()
    print("Review the output above to verify the books being flagged are correct.")
    print("If they look good, run: python3 cli/bookpilot.py cleanup --yes --limit {limit} --offset {offset}")
    print("=" * 80)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Review cleanup results (dry-run)')
    parser.add_argument('--limit', type=int, default=100, help='Limit number of books to check')
    parser.add_argument('--offset', type=int, default=0, help='Offset for batch processing')
    args = parser.parse_args()
    
    review_cleanup(limit=args.limit, offset=args.offset)
