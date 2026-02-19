#!/usr/bin/env python3
"""
Extract series information from existing catalog book titles.

This script processes all AuthorCatalogBook records and extracts series name
and position from titles that contain series information in parentheses,
e.g., "What Comes My Way (Brookstone Brides Book #3)".

It updates the database records with the extracted series information.
"""

import sys
import re
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, AuthorCatalogBook
from sqlalchemy import func
from sqlalchemy.exc import OperationalError
import sqlite3


def extract_series_from_title(title: str) -> tuple:
    """
    Extract series name and position from a title.
    
    Handles patterns like:
    - "Title (Series Name Book #3)"
    - "Title (Series Name #3)"
    - "Title (Series Name, Book 3)"
    
    Returns:
        (series_name, series_position) or (None, None)
    """
    if not title:
        return (None, None)
    
    # Pattern: "Title (Series Name Book #3)" or "Title (Series Name #3)"
    paren_match = re.search(r'\(([^)]+)\)', title)
    if paren_match:
        paren_content = paren_match.group(1)
        
        # Look for "Book #N" or "#N" or "Book N" pattern
        # Examples: "Brookstone Brides Book #3", "Brookstone Brides #3", "Series Name, Book 3"
        book_pattern = re.search(r'(.+?)(?:\s+Book)?\s*#?\s*(\d+)', paren_content, re.IGNORECASE)
        if book_pattern:
            # Extract series name (everything before "Book #N" or "#N")
            potential_series = book_pattern.group(1).strip()
            position_str = book_pattern.group(2).strip()
            
            # Clean up series name - remove trailing "Book" if present
            potential_series = re.sub(r'\s+Book\s*$', '', potential_series, flags=re.IGNORECASE).strip()
            
            # Only use if it looks like a series name (not just a number or very short)
            if potential_series and len(potential_series) > 2:
                try:
                    position = int(position_str)
                    return (potential_series, position)
                except ValueError:
                    pass
    
    return (None, None)


def process_catalog_books(dry_run: bool = True, limit: int = None, offset: int = 0):
    """
    Process catalog books to extract series information from titles.
    
    Args:
        dry_run: If True, only report what would be updated without making changes
        limit: Maximum number of books to process (None for all)
        offset: Number of books to skip before starting (for batch processing)
    """
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    
    # Check if database is accessible before starting
    if not dry_run:
        try:
            # Try to get a test connection
            engine = init_db(str(db_path))
            test_session = get_session(engine)
            # Try a simple query to check if database is locked
            test_session.query(AuthorCatalogBook).limit(1).all()
            test_session.close()
        except (OperationalError, sqlite3.OperationalError) as e:
            if 'locked' in str(e).lower():
                print("✗ Database is currently locked")
                print("  Please close the web UI and any other processes using the database")
                print("  Then run this script again")
                return
            else:
                raise
    
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    if dry_run:
        print("Extracting series information from catalog book titles (DRY RUN - no changes will be made)...")
    else:
        print("Extracting series information from catalog book titles...")
        print("  Note: If you get 'database is locked' errors, close the web UI and try again.")
    
    # Query books that don't have series_name or have empty series_name
    query = session.query(AuthorCatalogBook).filter(
        (AuthorCatalogBook.series_name.is_(None)) | 
        (AuthorCatalogBook.series_name == '')
    ).filter(
        AuthorCatalogBook.title.isnot(None)
    ).order_by(AuthorCatalogBook.id)
    
    if offset:
        query = query.offset(offset)
    if limit:
        query = query.limit(limit)
    
    catalog_books = query.all()
    total = len(catalog_books)
    
    total_in_db = session.query(AuthorCatalogBook).filter(
        (AuthorCatalogBook.series_name.is_(None)) | 
        (AuthorCatalogBook.series_name == '')
    ).filter(
        AuthorCatalogBook.title.isnot(None)
    ).count()
    
    if limit or offset:
        range_str = f"books {offset + 1}-{offset + total}" if limit else f"books starting from {offset + 1}"
        print(f"  Processing {total} books ({range_str} of {total_in_db} total without series info)...")
    else:
        print(f"  Processing {total} books without series information...")
    
    updated = 0
    skipped = 0
    errors = 0
    pending_updates = []  # Store updates to apply in batches
    
    for i, book in enumerate(catalog_books, 1):
        if i % 100 == 0:
            print(f"  Processing {i}/{total}...")
        
        try:
            title = book.title or ""
            if not title:
                skipped += 1
                continue
            
            # Extract series info from title
            series_name, series_position = extract_series_from_title(title)
            
            if series_name:
                if dry_run:
                    print(f"  Would update: '{title[:60]}...' -> Series: '{series_name}' #{series_position}")
                    updated += 1
                else:
                    # Store update to apply later in batches
                    pending_updates.append({
                        'book': book,
                        'series_name': series_name,
                        'series_position': series_position
                    })
                    updated += 1
            else:
                skipped += 1
                
        except Exception as e:
            errors += 1
            print(f"  ✗ Error processing book ID {book.id}: {e}")
            try:
                session.rollback()
            except:
                pass
    
    if not dry_run and updated > 0:
        # Apply updates in batches with retry logic
        print(f"\n  Applying {len(pending_updates)} updates to database...")
        batch_size = 50
        committed = 0
        
        for batch_start in range(0, len(pending_updates), batch_size):
            batch = pending_updates[batch_start:batch_start + batch_size]
            max_retries = 5
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # Apply updates in this batch
                    for update in batch:
                        update['book'].series_name = update['series_name']
                        if update['series_position']:
                            update['book'].series_position = update['series_position']
                    
                    # Commit the batch
                    session.commit()
                    committed += len(batch)
                    success = True
                    
                    if batch_start + batch_size < len(pending_updates):
                        print(f"  Committed batch {batch_start // batch_size + 1} ({committed}/{len(pending_updates)})...")
                    
                except (OperationalError, sqlite3.OperationalError) as e:
                    error_str = str(e).lower()
                    if 'locked' in error_str:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = min(2 ** retry_count, 10)  # Exponential backoff, max 10 seconds
                            print(f"  ⚠ Database locked, retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                            try:
                                session.rollback()
                            except:
                                pass
                            time.sleep(wait_time)
                        else:
                            print(f"\n✗ Database locked after {max_retries} retries")
                            print(f"  Please close the web UI and try again")
                            print(f"  {committed} books were successfully updated")
                            print(f"  {len(pending_updates) - committed} books still need to be updated")
                            print(f"\n  You can resume by running the script again - it will skip already-updated books")
                            break
                    else:
                        raise
                except Exception as e:
                    try:
                        session.rollback()
                    except:
                        pass
                    print(f"\n✗ Error committing batch: {e}")
                    print(f"  {committed} books were successfully updated")
                    print(f"  {len(pending_updates) - committed} books failed to update")
                    break
            
            if not success:
                break
        
        if committed == len(pending_updates):
            print(f"\n✓ Successfully updated {committed} books with series information")
        elif committed > 0:
            print(f"\n⚠ Partially completed: {committed}/{len(pending_updates)} books updated")
            print(f"  Run the script again to update the remaining books")
    elif dry_run:
        print(f"\n✓ Would update {updated} books with series information")
    
    print(f"\nSummary:")
    print(f"  Updated: {updated}")
    print(f"  Skipped (no series in title): {skipped}")
    if errors > 0:
        print(f"  Errors: {errors}")
    
    session.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract series information from existing catalog book titles'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Actually update the database (default is dry-run)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Show what would be updated without making changes (default)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of books to process (useful for processing in batches, e.g., --limit 1000)'
    )
    parser.add_argument(
        '--offset',
        type=int,
        default=0,
        help='Number of books to skip before starting (for batch processing, e.g., --offset 1000 for second batch)'
    )
    
    args = parser.parse_args()
    
    # If --yes is provided, disable dry-run
    dry_run = not args.yes if args.yes else args.dry_run
    
    if not dry_run:
        print("This will update the database with series information extracted from titles.")
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            return 0
    
    process_catalog_books(dry_run=dry_run, limit=args.limit, offset=args.offset)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
