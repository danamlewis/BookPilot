#!/usr/bin/env python3
"""Fix authors with mismatched normalized_name fields"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, AuthorCatalogBook
from src.ingest import normalize_author_name
from sqlalchemy import func
import time


def fix_mismatched_normalized_names(dry_run=True):
    """Fix authors where normalized_name doesn't match their actual name"""
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("=" * 80)
    print("FIXING MISMATCHED NORMALIZED NAMES")
    print("=" * 80)
    if dry_run:
        print("DRY RUN MODE - No changes will be made\n")
    else:
        print("LIVE MODE - Changes will be committed\n")
    
    all_authors = session.query(Author).all()
    mismatches = []
    
    for author in all_authors:
        if author.normalized_name and author.name:
            # Calculate similarity
            name_words = set(author.name.lower().split())
            norm_words = set(author.normalized_name.lower().split())
            
            if len(name_words) > 1 and len(norm_words) > 1:
                overlap = len(name_words & norm_words)
                total_unique = len(name_words | norm_words)
                if total_unique > 0:
                    similarity = overlap / total_unique
                    if similarity < 0.3:  # Less than 30% word overlap
                        catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
                        if catalog_count > 0:  # Only fix if they have catalog books
                            # Calculate what the normalized name should be
                            correct_normalized = normalize_author_name(author.name)
                            mismatches.append({
                                'author': author,
                                'similarity': similarity,
                                'catalog_count': catalog_count,
                                'current_normalized': author.normalized_name,
                                'correct_normalized': correct_normalized
                            })
    
    mismatches.sort(key=lambda x: x['catalog_count'], reverse=True)
    
    print(f"Found {len(mismatches)} authors with mismatched normalized names\n")
    
    if not mismatches:
        print("No mismatches found!")
        return
    
    # Show what will be fixed
    for i, item in enumerate(mismatches, 1):
        author = item['author']
        print(f"{i}. {author.name} (ID: {author.id})")
        print(f"   Current normalized: {item['current_normalized']}")
        print(f"   Correct normalized: {item['correct_normalized']}")
        print(f"   Catalog books: {item['catalog_count']}")
        print()
    
    if not dry_run:
        print("Fixing normalized names...")
        max_retries = 10
        
        for item in mismatches:
            author = item['author']
            correct_normalized = item['correct_normalized']
            
            for attempt in range(max_retries):
                try:
                    author.normalized_name = correct_normalized
                    session.commit()
                    print(f"✓ Fixed: {author.name} -> {correct_normalized}")
                    break
                except Exception as e:
                    error_str = str(e).lower()
                    if 'locked' in error_str and attempt < max_retries - 1:
                        session.rollback()
                        wait_time = 0.1 * (2 ** attempt)
                        print(f"  Database locked, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        # Re-fetch author after rollback
                        author = session.query(Author).filter_by(id=author.id).first()
                    else:
                        session.rollback()
                        print(f"✗ ERROR: Failed to fix {author.name}: {e}")
                        break
        
        print(f"\n{'=' * 80}")
        print("FIX COMPLETE")
        print(f"{'=' * 80}")
        print(f"Fixed {len(mismatches)} authors")
    else:
        print(f"\n{'=' * 80}")
        print("DRY RUN SUMMARY")
        print(f"{'=' * 80}")
        print(f"Would fix {len(mismatches)} authors")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Fix mismatched normalized author names')
    parser.add_argument('--execute', action='store_true', help='Actually fix (default is dry run)')
    args = parser.parse_args()
    
    fix_mismatched_normalized_names(dry_run=not args.execute)
