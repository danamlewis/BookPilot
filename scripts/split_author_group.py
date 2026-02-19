#!/usr/bin/env python3
"""Script to split author groups into individual authors and re-catalog books"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, Author, Book, AuthorCatalogBook, Recommendation
from src.ingest import normalize_author_name
from src.api.openlibrary import OpenLibraryClient
from sqlalchemy import or_, func
from collections import defaultdict


def search_author_group(db_session, author_group_name):
    """Search for all books and catalog entries with the author group name"""
    print(f"\nSearching for author group: '{author_group_name}'")
    print("=" * 80)
    
    # Normalize the group name
    normalized_group = normalize_author_name(author_group_name)
    
    # Find the author record (if it exists)
    author_record = db_session.query(Author).filter(
        or_(
            Author.name == author_group_name,
            Author.name.ilike(f'%{author_group_name}%'),
            Author.normalized_name == normalized_group
        )
    ).first()
    
    results = {
        'author_record': author_record,
        'books': [],
        'catalog_books': [],
        'recommendations': []
    }
    
    # Search in Books table
    books = db_session.query(Book).filter(
        or_(
            Book.author == author_group_name,
            Book.author.ilike(f'%{author_group_name}%'),
            Book.author == normalized_group
        )
    ).all()
    results['books'] = books
    
    # Search in AuthorCatalogBook if author record exists
    if author_record:
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(
            author_id=author_record.id
        ).all()
        results['catalog_books'] = catalog_books
    
    # Search in Recommendations
    recommendations = db_session.query(Recommendation).filter(
        or_(
            Recommendation.author == author_group_name,
            Recommendation.author.ilike(f'%{author_group_name}%'),
            Recommendation.author == normalized_group
        )
    ).all()
    results['recommendations'] = recommendations
    
    # Print results
    print(f"\nFound Author Record: {author_record.name if author_record else 'None'} (ID: {author_record.id if author_record else 'N/A'})")
    print(f"  Books in Libby history: {len(books)}")
    print(f"  Catalog books: {len(results['catalog_books'])}")
    print(f"  Recommendations: {len(recommendations)}")
    
    if books:
        print(f"\n  Books found:")
        for book in books:
            print(f"    - {book.title} (format: {book.format})")
    
    if results['catalog_books']:
        print(f"\n  Catalog books found:")
        for cat_book in results['catalog_books']:
            print(f"    - {cat_book.title}")
    
    if recommendations:
        print(f"\n  Recommendations found:")
        for rec in recommendations:
            print(f"    - {rec.title} (format: {rec.format})")
    
    return results


def match_author_from_open_library(ol_client, work_key, individual_authors):
    """
    Get the actual author(s) from Open Library work and match to individual authors
    
    Returns:
        Index of matching author in individual_authors list, or None if no match
    """
    if not work_key:
        return None
    
    try:
        work_details = ol_client.get_work_details(work_key)
        if not work_details:
            return None
        
        # Get authors from work
        authors_list = work_details.get('authors', [])
        for auth in authors_list:
            # Extract author key - can be nested
            author_key = None
            if isinstance(auth, dict):
                if 'author' in auth and isinstance(auth['author'], dict):
                    author_key = auth['author'].get('key', '')
                elif 'key' in auth:
                    author_key = auth.get('key', '')
            elif isinstance(auth, str):
                author_key = auth
            
            if author_key:
                # Normalize author key
                if not author_key.startswith('/authors/'):
                    if author_key.startswith('/'):
                        author_key = f"/authors{author_key}"
                    else:
                        author_key = f"/authors/{author_key}"
                
                # Fetch author details to get the name
                try:
                    author_data = ol_client._request(f"{author_key}.json")
                    if author_data:
                        author_name = author_data.get('name', '')
                        if author_name:
                            # Try to match to individual authors
                            author_name_normalized = normalize_author_name(author_name)
                            for i, individual_author in enumerate(individual_authors):
                                individual_normalized = normalize_author_name(individual_author)
                                # Check if names match (exact or last name match)
                                if author_name_normalized == individual_normalized:
                                    return i
                                # Check last name match
                                author_parts = author_name.split()
                                individual_parts = individual_author.split()
                                if len(author_parts) >= 1 and len(individual_parts) >= 1:
                                    if author_parts[-1].lower() == individual_parts[-1].lower():
                                        return i
                except Exception:
                    pass
    except Exception as e:
        pass
    
    return None


def split_author_group(db_session, author_group_name, individual_authors, dry_run=True, limit=None):
    """
    Split author group into individual authors and re-associate books
    
    Args:
        db_session: Database session
        author_group_name: The full author group name (e.g., "Author A, Author B, Author C")
        individual_authors: List of individual author names (e.g., ["Author A", "Author B", "Author C"])
        dry_run: If True, only show what would be done without making changes
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Splitting author group: '{author_group_name}'")
    print("=" * 80)
    print(f"Individual authors: {', '.join(individual_authors)}")
    
    # Initialize Open Library client
    ol_client = OpenLibraryClient()
    
    # Search for existing records
    results = search_author_group(db_session, author_group_name)
    
    if not results['author_record'] and not results['books'] and not results['catalog_books']:
        print("\n⚠ No records found for this author group.")
        return
    
    # Apply limit if specified (for testing)
    if limit and len(results['catalog_books']) > limit:
        print(f"\n⚠ Limiting to first {limit} catalog books for testing")
        results['catalog_books'] = results['catalog_books'][:limit]
    
    # Get the group author record ID to exclude it from individual author searches
    group_author_id = results['author_record'].id if results['author_record'] else None
    
    # Create or find individual author records
    # IMPORTANT: We need to be very careful about matching to avoid matching wrong authors
    individual_author_records = []
    for author_name in individual_authors:
        normalized = normalize_author_name(author_name)
        
        # Step 1: Try exact name match first (most reliable)
        # We ONLY match by exact name to avoid false matches (e.g. one author matching another's normalized_name)
        author = db_session.query(Author).filter(Author.name == author_name)
        
        # Exclude the group author if it exists
        if group_author_id:
            author = author.filter(Author.id != group_author_id)
        
        author = author.first()
        
        # Step 2: If no exact match, create a new author record
        # We do NOT match by normalized_name to avoid false matches between different authors
        if not author:
            if not dry_run:
                # Make sure we get a unique ID by flushing and checking
                # SQLAlchemy will handle this automatically, but let's be explicit
                author = Author(
                    name=author_name,
                    normalized_name=normalized
                )
                db_session.add(author)
                db_session.flush()  # This will assign an ID
                
                # Verify the author was created with a unique ID
                # (SQLAlchemy handles this, but let's double-check it's not a duplicate)
                existing_check = db_session.query(Author).filter_by(
                    name=author_name,
                    normalized_name=normalized
                ).all()
                if len(existing_check) > 1:
                    # This shouldn't happen, but if it does, use the first one
                    print(f"  ⚠ Warning: Multiple authors with name '{author_name}' found, using first")
                    author = existing_check[0]
                    # Remove the duplicate we just created
                    for dup in existing_check[1:]:
                        if dup.id == author.id:
                            continue
                        db_session.delete(dup)
                    db_session.flush()
                
                print(f"  ✓ Created new author: {author_name} (ID: {author.id})")
            else:
                print(f"  [Would create] new author: {author_name}")
        else:
            # Verify we found the right author
            if author.name != author_name:
                print(f"  ⚠ Found author with different name: '{author.name}' (ID: {author.id})")
                print(f"     Looking for: '{author_name}'")
                print(f"     This might be a mismatch - will use it anyway")
            else:
                print(f"  ✓ Found existing author: {author_name} (ID: {author.id})")
        
        individual_author_records.append(author if not dry_run or author else None)
    
    # Initialize assignment counters
    assignment_counts = {
        'catalog_books': {author: 0 for author in individual_authors},
        'books': {author: 0 for author in individual_authors},
        'recommendations': {author: 0 for author in individual_authors}
    }
    
    if dry_run:
        print("\n⚠ DRY RUN MODE - No changes will be made")
        print("\nAnalyzing catalog books to determine author assignments...")
        print(f"  (This may take a while for {len(results['catalog_books'])} books...)\n")
        # Still analyze in dry run mode to show what would happen
        matched_count = 0
        for idx, cat_book in enumerate(results['catalog_books'], 1):
            if idx % 10 == 0:
                print(f"  Processing {idx}/{len(results['catalog_books'])}...")
            author_idx = match_author_from_open_library(
                ol_client, 
                cat_book.open_library_key, 
                individual_authors
            )
            if author_idx is not None:
                matched_count += 1
                assigned_author = individual_authors[author_idx]
                assignment_counts['catalog_books'][assigned_author] += 1
                if idx <= 20:  # Show first 20 matches
                    print(f"  [Would assign] '{cat_book.title}' → {assigned_author}")
            else:
                # Default to first author
                assigned_author = individual_authors[0]
                assignment_counts['catalog_books'][assigned_author] += 1
                if idx <= 20:  # Show first 20 defaults
                    print(f"  [Would assign] '{cat_book.title}' → {assigned_author} (default, no match found)")
        
        # Analyze Libby books (dry run)
        print("\nAnalyzing Libby books...")
        title_to_author = {}
        for cat_book in results['catalog_books']:
            author_idx = match_author_from_open_library(
                ol_client, 
                cat_book.open_library_key, 
                individual_authors
            )
            if cat_book.title:
                if author_idx is not None:
                    title_to_author[cat_book.title.lower().strip()] = individual_authors[author_idx]
                else:
                    title_to_author[cat_book.title.lower().strip()] = individual_authors[0]
        
        for book in results['books']:
            book_title_lower = book.title.lower().strip() if book.title else ""
            assigned_author = individual_authors[0]  # default
            
            if book_title_lower in title_to_author:
                assigned_author = title_to_author[book_title_lower]
            else:
                # Try title-based matching
                for i, author_name in enumerate(individual_authors):
                    author_first_last = author_name.split()
                    if len(author_first_last) >= 2:
                        last_name = author_first_last[-1].lower()
                        if last_name in book_title_lower:
                            assigned_author = author_name
                            break
            
            assignment_counts['books'][assigned_author] += 1
        
        # Analyze recommendations (dry run)
        for rec in results['recommendations']:
            rec_title_lower = rec.title.lower() if rec.title else ""
            assigned_author = individual_authors[0]  # default
            
            if rec_title_lower in title_to_author:
                assigned_author = title_to_author[rec_title_lower]
            else:
                # Try title-based matching
                for i, author_name in enumerate(individual_authors):
                    author_first_last = author_name.split()
                    if len(author_first_last) >= 2:
                        last_name = author_first_last[-1].lower()
                        if last_name in rec_title_lower:
                            assigned_author = author_name
                            break
            
            assignment_counts['recommendations'][assigned_author] += 1
        
        # Print summary report
        print_summary_report(assignment_counts, individual_authors, dry_run=True)
        return
    
    # Build a mapping of book titles to authors from catalog books
    # This will help us match Libby books to authors
    title_to_author = {}
    
    # Initialize assignment counters (for execute mode)
    assignment_counts = {
        'catalog_books': {author: 0 for author in individual_authors},
        'books': {author: 0 for author in individual_authors},
        'recommendations': {author: 0 for author in individual_authors}
    }
    
    # Re-associate AuthorCatalogBook entries (do this first to build title mapping)
    print("\nRe-assigning catalog books...")
    print(f"  (Processing {len(results['catalog_books'])} books, this may take a while...)\n")
    catalog_updated = 0
    catalog_unmatched = 0
    
    for idx, cat_book in enumerate(results['catalog_books'], 1):
        if idx % 20 == 0:
            print(f"  Processing {idx}/{len(results['catalog_books'])}...")
        # Try to get author from Open Library
        author_idx = match_author_from_open_library(
            ol_client, 
            cat_book.open_library_key, 
            individual_authors
        )
        
        assigned_author = None
        if author_idx is not None and individual_author_records[author_idx]:
            cat_book.author_id = individual_author_records[author_idx].id
            catalog_updated += 1
            assigned_author = individual_authors[author_idx]
            assignment_counts['catalog_books'][assigned_author] += 1
            print(f"  ✓ Re-assigned catalog book '{cat_book.title}' to {assigned_author}")
            
            # Add to title mapping for Libby books
            if cat_book.title:
                title_to_author[cat_book.title.lower().strip()] = assigned_author
        else:
            # Try title-based matching as fallback
            book_title_lower = cat_book.title.lower() if cat_book.title else ""
            assigned = False
            
            for i, author_record in enumerate(individual_author_records):
                if not author_record:
                    continue
                author_name = individual_authors[i]
                author_first_last = author_name.split()
                if len(author_first_last) >= 2:
                    last_name = author_first_last[-1].lower()
                    if last_name in book_title_lower:
                        cat_book.author_id = author_record.id
                        catalog_updated += 1
                        assigned = True
                        assigned_author = author_name
                        assignment_counts['catalog_books'][assigned_author] += 1
                        print(f"  ✓ Re-assigned catalog book '{cat_book.title}' to {author_name} (title match)")
                        if cat_book.title:
                            title_to_author[cat_book.title.lower().strip()] = author_name
                        break
            
            if not assigned and individual_author_records:
                # Default to first author
                cat_book.author_id = individual_author_records[0].id
                catalog_updated += 1
                catalog_unmatched += 1
                assigned_author = individual_authors[0]
                assignment_counts['catalog_books'][assigned_author] += 1
                print(f"  ⚠ Re-assigned catalog book '{cat_book.title}' to {assigned_author} (default, no match found)")
                if cat_book.title:
                    title_to_author[cat_book.title.lower().strip()] = assigned_author
    
    # Re-associate Books from Libby history
    print("\nRe-assigning Libby books...")
    books_updated = 0
    books_unmatched = 0
    
    for book in results['books']:
        assigned = False
        book_title_lower = book.title.lower().strip() if book.title else ""
        assigned_author = None
        
        # First, try to match via catalog book title
        if book_title_lower in title_to_author:
            assigned_author = title_to_author[book_title_lower]
            book.author = normalize_author_name(assigned_author)
            books_updated += 1
            assigned = True
            assignment_counts['books'][assigned_author] += 1
            print(f"  ✓ Re-assigned book '{book.title}' to {assigned_author} (matched via catalog)")
        else:
            # Try title-based matching
            for i, author_name in enumerate(individual_authors):
                author_first_last = author_name.split()
                if len(author_first_last) >= 2:
                    last_name = author_first_last[-1].lower()
                    if last_name in book_title_lower:
                        assigned_author = author_name
                        book.author = normalize_author_name(assigned_author)
                        books_updated += 1
                        assigned = True
                        assignment_counts['books'][assigned_author] += 1
                        print(f"  ✓ Re-assigned book '{book.title}' to {assigned_author} (title match)")
                        break
        
        if not assigned:
            # Default to first author if can't determine
            assigned_author = individual_authors[0]
            book.author = normalize_author_name(assigned_author)
            books_updated += 1
            books_unmatched += 1
            assignment_counts['books'][assigned_author] += 1
            print(f"  ⚠ Re-assigned book '{book.title}' to {assigned_author} (default, no match found)")
    
    # Re-associate Recommendations
    print("\nRe-assigning recommendations...")
    recs_updated = 0
    for rec in results['recommendations']:
        rec_title_lower = rec.title.lower() if rec.title else ""
        assigned = False
        assigned_author = None
        
        # Try to match via catalog
        if rec_title_lower in title_to_author:
            assigned_author = title_to_author[rec_title_lower]
            rec.author = assigned_author
            recs_updated += 1
            assigned = True
            assignment_counts['recommendations'][assigned_author] += 1
            print(f"  ✓ Re-assigned recommendation '{rec.title}' to {assigned_author}")
        else:
            # Try title-based matching
            for i, author_name in enumerate(individual_authors):
                author_first_last = author_name.split()
                if len(author_first_last) >= 2:
                    last_name = author_first_last[-1].lower()
                    if last_name in rec_title_lower:
                        assigned_author = author_name
                        rec.author = assigned_author
                        recs_updated += 1
                        assigned = True
                        assignment_counts['recommendations'][assigned_author] += 1
                        print(f"  ✓ Re-assigned recommendation '{rec.title}' to {assigned_author}")
                        break
        
        if not assigned:
            assigned_author = individual_authors[0]
            rec.author = assigned_author
            recs_updated += 1
            assignment_counts['recommendations'][assigned_author] += 1
            print(f"  ⚠ Re-assigned recommendation '{rec.title}' to {assigned_author} (default)")
    
    # Optionally remove the group author record if it has no more catalog books
    if results['author_record']:
        remaining_catalog = db_session.query(AuthorCatalogBook).filter_by(
            author_id=results['author_record'].id
        ).count()
        
        if remaining_catalog == 0:
            print(f"\n  ✓ Removing empty author group record: {results['author_record'].name}")
            db_session.delete(results['author_record'])
    
    # Commit changes
    try:
        db_session.commit()
        print(f"\n✓ Successfully updated:")
        print(f"  - Books: {books_updated} ({books_unmatched} defaulted to first author)")
        print(f"  - Catalog books: {catalog_updated} ({catalog_unmatched} defaulted to first author)")
        print(f"  - Recommendations: {recs_updated}")
        
        # Print summary report
        print_summary_report(assignment_counts, individual_authors, dry_run=False)
    except Exception as e:
        db_session.rollback()
        print(f"\n✗ Error committing changes: {e}")
        raise


def print_summary_report(assignment_counts, individual_authors, dry_run=True):
    """Print a summary report of how many items were assigned to each author"""
    print("\n" + "=" * 80)
    print(f"{'DRY RUN: ' if dry_run else ''}ASSIGNMENT SUMMARY")
    print("=" * 80)
    
    total_catalog = sum(assignment_counts['catalog_books'].values())
    total_books = sum(assignment_counts['books'].values())
    total_recs = sum(assignment_counts['recommendations'].values())
    
    print(f"\nCatalog Books: {total_catalog} total")
    for author in individual_authors:
        count = assignment_counts['catalog_books'][author]
        percentage = (count / total_catalog * 100) if total_catalog > 0 else 0
        print(f"  {author:30s}: {count:4d} ({percentage:5.1f}%)")
    
    if total_books > 0:
        print(f"\nLibby Books: {total_books} total")
        for author in individual_authors:
            count = assignment_counts['books'][author]
            percentage = (count / total_books * 100) if total_books > 0 else 0
            print(f"  {author:30s}: {count:4d} ({percentage:5.1f}%)")
    
    if total_recs > 0:
        print(f"\nRecommendations: {total_recs} total")
        for author in individual_authors:
            count = assignment_counts['recommendations'][author]
            percentage = (count / total_recs * 100) if total_recs > 0 else 0
            print(f"  {author:30s}: {count:4d} ({percentage:5.1f}%)")
    
    print("=" * 80)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Split author groups into individual authors')
    parser.add_argument('--db', type=str, help='Path to database file')
    parser.add_argument('--author-group', type=str, required=True,
                       help='Author group name (e.g., "Author A, Author B, Author C")')
    parser.add_argument('--individual-authors', type=str, nargs='+', required=True,
                       help='Individual author names (e.g., "Author A" "Author B" "Author C")')
    parser.add_argument('--search-only', action='store_true',
                       help='Only search and display results, do not make changes')
    parser.add_argument('--execute', action='store_true',
                       help='Actually execute the changes (default is dry run)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of catalog books to process (for testing)')
    
    args = parser.parse_args()
    
    # Determine database path
    db_path = Path(args.db) if args.db else Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize database
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    try:
        if args.search_only:
            # Just search and display
            search_author_group(session, args.author_group)
        else:
            # Split the author group
            dry_run = not args.execute
            split_author_group(
                session,
                args.author_group,
                args.individual_authors,
                dry_run=dry_run,
                limit=args.limit
            )
    finally:
        session.close()


if __name__ == '__main__':
    main()
