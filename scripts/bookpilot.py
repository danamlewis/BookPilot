#!/usr/bin/env python3
"""BookPilot CLI tool"""
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, SystemMetadata, Book, Author, AuthorCatalogBook
from src.ingest import ingest_csv
from src.catalog import fetch_all_author_catalogs, cleanup_non_english_books, fix_author_mismatches, remove_duplicate_titles, merge_authors, detect_duplicate_authors
from src.series import analyze_all_series
from src.recommend import recommend_audiobooks, recommend_new_books, save_recommendations


def format_date_delta(date_str):
    """Format date delta as human-readable string"""
    if not date_str:
        return "Never"
    
    try:
        date = datetime.fromisoformat(date_str)
        delta = datetime.utcnow() - date
        total_seconds = int(delta.total_seconds())
        days = delta.days
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        
        if days < 1:
            if hours < 1:
                if minutes < 1:
                    return "Just now"
                return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
            elif hours == 1:
                return "1 hour ago"
            else:
                return f"{hours} hours ago"
        elif days == 1:
            return "1 day ago"
        elif days < 7:
            return f"{days} days ago"
        elif days < 30:
            weeks = days // 7
            return f"{weeks} week{'s' if weeks > 1 else ''} ago"
        else:
            months = days // 30
            return f"{months} month{'s' if months > 1 else ''} ago"
    except:
        return "Unknown"


def cmd_ingest(args):
    """Ingest Libby CSV export"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Handle glob patterns (e.g., *.csv)
    import glob
    csv_files = glob.glob(args.csv_file)
    
    if not csv_files:
        csv_path = Path(args.csv_file)
        if not csv_path.exists():
            print(f"Error: CSV file not found: {csv_path}")
            return 1
        csv_files = [str(csv_path)]
    
    # If multiple files found, use the most recent one
    if len(csv_files) > 1:
        csv_files.sort(key=lambda f: Path(f).stat().st_mtime, reverse=True)
        print(f"Found {len(csv_files)} CSV file(s), using most recent: {Path(csv_files[0]).name}")
        if len(csv_files) > 1:
            print(f"  (Other files: {', '.join([Path(f).name for f in csv_files[1:]])})")
    
    csv_path = Path(csv_files[0])
    print(f"Ingesting {csv_path}...")
    result = ingest_csv(csv_path, session, update_existing=args.update)
    
    print(f"\n✓ Ingestion complete!")
    print(f"  Books added: {result['books_added']}")
    print(f"  Authors added: {result['authors_added']}")
    print(f"  Total books: {result['total_books']}")
    print(f"  Total authors: {result['total_authors']}")
    if result.get('marked_as_already_read', 0) > 0:
        print(f"  Recommendations marked as already read: {result['marked_as_already_read']}")
    if result.get('removed_from_books_to_read', 0) > 0:
        print(f"  Removed from Books to Read: {result['removed_from_books_to_read']}")
    
    return 0


def cmd_catalog(args):
    """Fetch author catalogs"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("Fetching author catalogs...")
    print("(This may take a while due to API rate limits)")
    
    if args.only_recent:
        print(f"Only fetching recent books (published in last {args.recent_years} years) for existing authors")
    
    result = fetch_all_author_catalogs(session, force_refresh=args.force,
                                       only_recent=args.only_recent,
                                       recent_years=args.recent_years,
                                       auto_cleanup=args.auto_cleanup)
    
    # Update system metadata: last catalog check date
    metadata = session.query(SystemMetadata).filter_by(key='last_catalog_check').first()
    if metadata:
        metadata.value = datetime.utcnow().isoformat()
        metadata.updated_at = datetime.utcnow()
    else:
        metadata = SystemMetadata(
            key='last_catalog_check',
            value=datetime.utcnow().isoformat()
        )
        session.add(metadata)
    session.commit()
    
    if result.get('stopped_early'):
        print(f"\n⚠️  Catalog fetch stopped early due to errors!")
        print(f"  Total authors: {result['total_authors']}")
        print(f"  Catalogs fetched: {result['catalogs_fetched']}")
        print(f"  Catalogs skipped: {result['catalogs_skipped']}")
        print(f"  Books added: {result.get('total_books_added', 0)}")
        print(f"  Books updated: {result.get('total_books_updated', 0)}")
        print(f"  Errors: {len(result['errors'])}")
        if result['errors']:
            print(f"\n  Recent errors:")
            for error in result['errors'][-5:]:
                print(f"    - {error}")
        print(f"\n  Progress has been saved. You can resume by running the command again.")
        return 1
    else:
        print(f"\n✓ Catalog fetch complete!")
        print(f"  Total authors: {result['total_authors']}")
        print(f"  Catalogs fetched: {result['catalogs_fetched']}")
        print(f"  Catalogs skipped: {result['catalogs_skipped']}")
        print(f"  Books added: {result.get('total_books_added', 0)}")
        print(f"  Books updated: {result.get('total_books_updated', 0)}")
        if result['errors']:
            print(f"  Errors: {len(result['errors'])}")
            for error in result['errors'][:5]:
                print(f"    - {error}")
        
        # Auto-cleanup if requested (when not only_recent; with only_recent, cleanup runs inside fetch)
        if args.auto_cleanup and not args.only_recent:
            print(f"\n{'='*60}")
            print("Running automatic cleanup...")
            print(f"{'='*60}\n")
            
            # 1. Remove non-English books
            print("Step 1: Removing non-English books...")
            cleanup_result = cleanup_non_english_books(session, dry_run=False)
            removed_non_english = cleanup_result.get('removed', 0)
            print(f"  Removed {removed_non_english} non-English books\n")
            
            # 2. Remove duplicate titles
            print("Step 2: Removing duplicate titles...")
            dedupe_result = remove_duplicate_titles(session, dry_run=False)
            removed_duplicates = dedupe_result.get('catalog_duplicates_removed', 0)
            print(f"  Removed {removed_duplicates} duplicate catalog books\n")
            
            print(f"✓ Cleanup complete!")
            print(f"  Non-English books removed: {removed_non_english}")
            print(f"  Duplicates removed: {removed_duplicates}")
        
        # Always check for duplicate authors after catalog fetch
        print(f"\n{'='*60}")
        print("Checking for duplicate authors...")
        print(f"{'='*60}\n")
        
        duplicates = detect_duplicate_authors(session, min_overlapping_books=1)
        
        if duplicates:
            print(f"Found {len(duplicates)} potential duplicate author pair(s):\n")
            
            for i, dup in enumerate(duplicates, 1):
                author1 = dup['author1']
                author2 = dup['author2']
                catalog1 = session.query(AuthorCatalogBook).filter_by(author_id=author1.id).count()
                catalog2 = session.query(AuthorCatalogBook).filter_by(author_id=author2.id).count()
                books1 = session.query(Book).filter_by(author=author1.normalized_name).count()
                books2 = session.query(Book).filter_by(author=author2.normalized_name).count()
                
                print(f"{i}. {author1.name} (ID: {author1.id})")
                print(f"   vs {author2.name} (ID: {author2.id})")
                print(f"   Reason: {dup['reason']}")
                print(f"   Confidence: {dup['confidence']}")
                print(f"   Overlapping books: {dup['overlapping_books']}")
                print(f"   Author 1: {catalog1} catalog books, {books1} books read")
                print(f"   Author 2: {catalog2} catalog books, {books2} books read")
                if dup['overlapping_titles']:
                    print(f"   Sample overlapping titles: {', '.join(dup['overlapping_titles'][:3])}")
                print()
            
            # Prompt user to merge
            if not args.yes:
                print("Would you like to merge these duplicate authors?")
                print("Options:")
                print("  - Type 'all' to merge all pairs automatically")
                print("  - Type 'none' to skip merging")
                print("  - Type numbers (e.g., '1 3 5') to merge specific pairs")
                response = input("\nYour choice: ").strip().lower()
                
                if response == 'all':
                    # Merge all
                    merged_count = 0
                    for dup in duplicates:
                        result = merge_authors(session, 
                                             author1_id=dup['author1'].id,
                                             author2_id=dup['author2'].id,
                                             dry_run=False)
                        if result.get('success'):
                            merged_count += 1
                    print(f"\n✓ Merged {merged_count} author pair(s)")
                elif response == 'none':
                    print("\nSkipping merge.")
                elif response:
                    # Merge specific pairs
                    try:
                        indices = [int(x.strip()) - 1 for x in response.split() if x.strip().isdigit()]
                        merged_count = 0
                        for idx in indices:
                            if 0 <= idx < len(duplicates):
                                dup = duplicates[idx]
                                result = merge_authors(session,
                                                     author1_id=dup['author1'].id,
                                                     author2_id=dup['author2'].id,
                                                     dry_run=False)
                                if result.get('success'):
                                    merged_count += 1
                        print(f"\n✓ Merged {merged_count} author pair(s)")
                    except ValueError:
                        print("\nInvalid input. Skipping merge.")
            else:
                # Auto-merge if --yes flag is set
                print("Auto-merging duplicate authors (--yes flag set)...")
                merged_count = 0
                for dup in duplicates:
                    result = merge_authors(session,
                                         author1_id=dup['author1'].id,
                                         author2_id=dup['author2'].id,
                                         dry_run=False)
                    if result.get('success'):
                        merged_count += 1
                print(f"✓ Merged {merged_count} author pair(s)")
        else:
            print("No duplicate authors found. ✓")
    
    return 0


def cmd_series(args):
    """Analyze series"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("Analyzing series...")
    result = analyze_all_series(session, format_filter=args.format)
    
    print(f"\n✓ Series analysis complete!")
    print(f"\nSummary:")
    print(f"  Total series: {result['total_series']}")
    print(f"  Partially read: {result['partial_series']}")
    print(f"  Not started: {result['not_started_series']}")
    print(f"  Complete: {result['complete_series']}")
    print(f"  Standalone books: {result['total_standalone']}")
    
    # Show partial series
    partial_series = [s for s in result['series'] if s['status'] == 'partial']
    if partial_series:
        print(f"\n📚 Partially Read Series ({len(partial_series)}):")
        for series in partial_series[:10]:  # Top 10
            print(f"\n  {series['series_name']} by {series['author']}")
            print(f"    Read: {series['books_read']}/{series['total_books']} ({series['completion_pct']:.0f}%)")
            if series['unread_books']:
                print(f"    Missing books:")
                for book in series['unread_books'][:3]:
                    pos_str = f"#{book['position']} " if book['position'] else ""
                    print(f"      - {pos_str}{book['title']}")
    
    return 0


def cmd_recommend(args):
    """Generate recommendations"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    if args.type == 'audiobook':
        print("Generating audiobook recommendations...")
        recommendations = recommend_audiobooks(session)
        
        if args.save:
            save_recommendations(recommendations, session, 'audiobook')
            print(f"\n✓ Saved {len(recommendations)} recommendations to database")
        else:
            print(f"\n✓ Found {len(recommendations)} recommendations")
            for rec in recommendations[:20]:  # Top 20
                print(f"\n  {rec['title']} by {rec['author']}")
                print(f"    Score: {rec['similarity_score']:.2f}")
                print(f"    Reason: {rec['reason']}")
    
    elif args.type == 'ebook':
        print("Generating ebook recommendations...")
        recommendations = recommend_new_books(session, category=args.category)
        
        if isinstance(recommendations, dict):
            # Grouped by category
            print(f"\n✓ Found recommendations in {len(recommendations)} categories")
            for category, recs in list(recommendations.items())[:5]:
                print(f"\n  {category} ({len(recs)} recommendations):")
                for rec in recs[:5]:
                    print(f"    - {rec['title']} by {rec['author']} (score: {rec['similarity_score']:.2f})")
        else:
            print(f"\n✓ Found {len(recommendations)} recommendations")
            for rec in recommendations[:20]:
                print(f"\n  {rec['title']} by {rec['author']}")
                print(f"    Score: {rec['similarity_score']:.2f}")
                print(f"    Reason: {rec['reason']}")
    
    return 0


def cmd_status(args):
    """Show system status"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    from src.models import Book, Author, AuthorCatalogBook
    
    total_books = session.query(Book).count()
    total_authors = session.query(Author).count()
    audiobooks = session.query(Book).filter_by(format='audiobook').count()
    ebooks = session.query(Book).filter_by(format='ebook').count()
    catalog_books = session.query(AuthorCatalogBook).count()
    
    # Get metadata
    libby_import = session.query(SystemMetadata).filter_by(key='last_libby_import').first()
    catalog_check = session.query(SystemMetadata).filter_by(key='last_catalog_check').first()
    
    print("BookPilot Status")
    print("=" * 50)
    print(f"\n📚 Books: {total_books}")
    print(f"   Audiobooks: {audiobooks}")
    print(f"   Ebooks: {ebooks}")
    print(f"\n👤 Authors: {total_authors}")
    print(f"\n📖 Catalog Books: {catalog_books}")
    print(f"\n📅 Last Libby import: {format_date_delta(libby_import.value if libby_import else None)}")
    print(f"📅 Last catalog check: {format_date_delta(catalog_check.value if catalog_check else None)}")
    
    return 0


def cmd_cleanup(args):
    """Clean up non-English books from catalog"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    if args.dry_run:
        print("This will check for non-English books in your catalog (DRY RUN - no changes will be made).")
    else:
        print("This will remove non-English books from your catalog.")
        if not args.yes:
            response = input("Continue? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Cancelled.")
                return 0
    
    result = cleanup_non_english_books(session, dry_run=args.dry_run, limit=args.limit, offset=args.offset)
    
    return 0


def cmd_fix_authors(args):
    """Fix author mismatches in catalog (when multiple authors have same name)"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("This will fix catalog books assigned to wrong authors.")
    print("(This happens when multiple authors have the same name)")
    if args.limit:
        print(f"Will process first {args.limit} duplicate author groups.")
    if args.only_cataloged:
        print("Will only process authors that have already been cataloged.")
    if not args.yes:
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            return 0
    
    result = fix_author_mismatches(session, max_groups=args.limit, only_cataloged=args.only_cataloged)
    
    return 0


def cmd_remove_duplicates(args):
    """Remove duplicate titles within the same author"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    print("This will remove duplicate titles within the same author.")
    print("For catalog books: Keeps the one with most complete data (ISBN, description, etc.)")
    print("For books: Keeps the first one found")
    
    if args.limit or args.offset:
        print(f"\nProcessing in batch: limit={args.limit}, offset={args.offset}")
    
    if args.dry_run:
        print("\n(DRY RUN - no changes will be made)")
    elif not args.yes:
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            return 0
    
    result = remove_duplicate_titles(session, dry_run=args.dry_run, author_limit=args.limit, author_offset=args.offset)
    
    print(f"\nSummary:")
    print(f"  Catalog duplicates found: {result['catalog_duplicates_found']}")
    print(f"  Catalog duplicates removed: {result['catalog_duplicates_removed']}")
    print(f"  Book duplicates found: {result['book_duplicates_found']}")
    print(f"  Book duplicates removed: {result['book_duplicates_removed']}")
    
    return 0


def cmd_merge_authors(args):
    """Merge two duplicate authors into one"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    # Check if using IDs or names
    using_ids = args.author1_id is not None or args.author2_id is not None
    using_names = args.author1 is not None or args.author2 is not None
    
    if using_ids and using_names:
        print("Error: Cannot mix --author1/--author2 with --author1-id/--author2-id")
        print("Use either names or IDs, not both")
        return 1
    
    if not using_ids and (not args.author1 or not args.author2):
        print("Error: Both --author1 and --author2 are required (or use --author1-id and --author2-id)")
        return 1
    
    if using_ids and (args.author1_id is None or args.author2_id is None):
        print("Error: Both --author1-id and --author2-id are required")
        return 1
    
    print("This will merge two authors into one, consolidating all catalog books and books read.")
    print("Duplicate catalog books and books read will be removed.")
    
    if args.dry_run:
        print("\n(DRY RUN - no changes will be made)")
    elif not args.yes:
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            return 0
    
    keep_author = None
    if args.keep:
        if args.keep.lower() in ['1', 'author1', 'first']:
            keep_author = 'author1'
        elif args.keep.lower() in ['2', 'author2', 'second']:
            keep_author = 'author2'
        else:
            print(f"Warning: Invalid --keep value '{args.keep}', will auto-select")
    
    if using_ids:
        result = merge_authors(session, author1_id=args.author1_id, author2_id=args.author2_id, 
                             keep_author=keep_author, dry_run=args.dry_run)
    else:
        result = merge_authors(session, author1_name=args.author1, author2_name=args.author2, 
                             keep_author=keep_author, dry_run=args.dry_run)
    
    if not result.get('success'):
        print(f"\n✗ Error: {result.get('error', 'Unknown error')}")
        return 1
    
    return 0


def cmd_list_authors(args):
    """List authors, optionally filtered by search term"""
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    from src.models import Author, AuthorCatalogBook
    from sqlalchemy import func
    
    query = session.query(Author)
    
    if args.search:
        # Search by name (case-insensitive)
        query = query.filter(Author.name.ilike(f'%{args.search}%'))
    
    authors = query.order_by(Author.name).all()
    
    if not authors:
        print(f"No authors found" + (f" matching '{args.search}'" if args.search else ""))
        return 0
    
    print(f"\nFound {len(authors)} author(s):\n")
    
    for author in authors:
        catalog_count = session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
        book_count = session.query(Book).filter_by(author=author.normalized_name).count()
        print(f"  {author.name}")
        print(f"    ID: {author.id}")
        print(f"    Normalized: {author.normalized_name}")
        if author.open_library_id:
            print(f"    Open Library ID: {author.open_library_id}")
        print(f"    Catalog books: {catalog_count}")
        print(f"    Books read: {book_count}")
        print()
    
    return 0


def main():
    parser = argparse.ArgumentParser(description='BookPilot - Personal Reading Intelligence')
    parser.add_argument('--db', help='Path to database file', default=None)
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest Libby CSV export')
    ingest_parser.add_argument('csv_file', help='Path to Libby CSV file')
    ingest_parser.add_argument('--update', action='store_true', help='Update existing records')
    
    # Catalog command
    catalog_parser = subparsers.add_parser('catalog', help='Fetch author catalogs')
    catalog_parser.add_argument('--force', action='store_true', help='Force refresh even if recently checked')
    catalog_parser.add_argument('--only-recent', action='store_true', 
                              help='Only fetch books published in the last N years for existing authors (authors that already have catalog books)')
    catalog_parser.add_argument('--recent-years', type=int, default=3,
                              help='Number of years to look back for recent books (default: 3, only used with --only-recent). Examples: --recent-years 1 for last year, --recent-years 3 for last 3 years')
    catalog_parser.add_argument('--auto-cleanup', action='store_true',
                              help='Automatically run cleanup (remove non-English books and duplicates) after catalog fetch completes')
    catalog_parser.add_argument('--yes', action='store_true',
                              help='Auto-merge duplicate authors without prompting (use with caution)')
    
    # Series command
    series_parser = subparsers.add_parser('series', help='Analyze series')
    series_parser.add_argument('--format', choices=['ebook', 'audiobook'], help='Filter by format')
    
    # Recommend command
    recommend_parser = subparsers.add_parser('recommend', help='Generate recommendations')
    recommend_parser.add_argument('type', choices=['audiobook', 'ebook'], help='Recommendation type')
    recommend_parser.add_argument('--category', help='Filter by category/genre')
    recommend_parser.add_argument('--save', action='store_true', help='Save recommendations to database')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Remove non-English books from catalog')
    cleanup_parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    cleanup_parser.add_argument('--dry-run', action='store_true', help='Show what would be removed without actually removing')
    cleanup_parser.add_argument('--limit', type=int, help='Maximum number of books to check (useful for processing in batches, e.g., --limit 1000)')
    cleanup_parser.add_argument('--offset', type=int, default=0, help='Number of books to skip before starting (for batch processing, e.g., --offset 1000 for second batch)')
    
    # Fix authors command
    fix_authors_parser = subparsers.add_parser('fix-authors', help='Fix author mismatches in catalog')
    fix_authors_parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    fix_authors_parser.add_argument('--limit', type=int, help='Maximum number of duplicate author groups to process (useful for fixing in batches)')
    fix_authors_parser.add_argument('--only-cataloged', action='store_true', help='Only process authors that have already been cataloged (have catalog books)')
    
    # Remove duplicates command
    remove_dups_parser = subparsers.add_parser('remove-duplicates', help='Remove duplicate titles within the same author')
    remove_dups_parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    remove_dups_parser.add_argument('--dry-run', action='store_true', help='Show what would be removed without making changes')
    remove_dups_parser.add_argument('--limit', type=int, help='Maximum number of authors to process (useful for processing in batches, e.g., --limit 100)')
    remove_dups_parser.add_argument('--offset', type=int, default=0, help='Number of authors to skip before starting (for batch processing, e.g., --offset 100 for second batch)')
    
    # Merge authors command
    merge_parser = subparsers.add_parser('merge-authors', help='Merge two duplicate authors into one')
    merge_parser.add_argument('--author1', help='First author name to merge')
    merge_parser.add_argument('--author2', help='Second author name to merge')
    merge_parser.add_argument('--author1-id', type=int, help='First author ID to merge (alternative to --author1)')
    merge_parser.add_argument('--author2-id', type=int, help='Second author ID to merge (alternative to --author2)')
    merge_parser.add_argument('--keep', choices=['author1', 'author2', '1', '2', 'first', 'second'], 
                             help='Which author to keep (default: auto-select based on catalog book count)')
    merge_parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    merge_parser.add_argument('--dry-run', action='store_true', help='Show what would be merged without making changes')
    
    # List authors command
    list_authors_parser = subparsers.add_parser('list-authors', help='List authors, optionally filtered by search term')
    list_authors_parser.add_argument('--search', help='Search term to filter authors by name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    commands = {
        'ingest': cmd_ingest,
        'catalog': cmd_catalog,
        'series': cmd_series,
        'recommend': cmd_recommend,
        'status': cmd_status,
        'cleanup': cmd_cleanup,
        'fix-authors': cmd_fix_authors,
        'remove-duplicates': cmd_remove_duplicates,
        'merge-authors': cmd_merge_authors,
        'list-authors': cmd_list_authors
    }
    
    return commands[args.command](args)


if __name__ == '__main__':
    sys.exit(main())
