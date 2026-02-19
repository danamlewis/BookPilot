"""Fetch and store author catalogs"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
import requests
from .models import Author, AuthorCatalogBook, Book, SystemMetadata
from .api.openlibrary import OpenLibraryClient, extract_series_info, extract_isbn, is_english_language
from .api.googlebooks import GoogleBooksClient
from .ingest import normalize_author_name


def detect_author_group(author_name: str) -> Optional[List[str]]:
    """
    Detect if an author name is actually a group of multiple authors.
    
    Args:
        author_name: Author name to check (e.g., "Author A, Author B, Author C")
    
    Returns:
        List of individual author names if it's a group, None otherwise
    """
    if ',' not in author_name:
        return None
    
    # Split by comma and clean up
    authors = [a.strip() for a in author_name.split(',')]
    
    # Filter out empty strings and very short names (likely not real authors)
    authors = [a for a in authors if a and len(a) > 2]
    
    # If we have 2+ authors, it's a group
    if len(authors) >= 2:
        return authors
    
    return None


def auto_split_author_group(author: Author, db_session: Session) -> bool:
    """
    Automatically detect and split an author group into individual authors.
    
    This is called before fetching a catalog to prevent creating catalogs for author groups.
    
    Args:
        author: Author record that might be a group
        db_session: Database session
    
    Returns:
        True if the author was split, False otherwise
    """
    individual_authors = detect_author_group(author.name)
    if not individual_authors:
        return False
    
    print(f"  ⚠ Detected author group: '{author.name}'")
    print(f"     Individual authors: {', '.join(individual_authors)}")
    print(f"     Auto-splitting before catalog fetch...")
    
    # Import the split function from the script
    # We'll inline the key logic here to avoid circular imports
    from .api.openlibrary import OpenLibraryClient as OLClient
    ol_client = OLClient()
    
    # Get the group author record ID to exclude it from individual author searches
    group_author_id = author.id
    
    # Create or find individual author records
    individual_author_records = []
    for author_name in individual_authors:
        normalized = normalize_author_name(author_name)
        
        # Try to find existing author by exact name match (exclude group author)
        existing = db_session.query(Author).filter(
            Author.name == author_name,
            Author.id != group_author_id
        ).first()
        
        if not existing:
            # Create new author
            existing = Author(
                name=author_name,
                normalized_name=normalized
            )
            db_session.add(existing)
            db_session.flush()
            print(f"     ✓ Created: {author_name} (ID: {existing.id})")
        else:
            print(f"     ✓ Found: {author_name} (ID: {existing.id})")
        
        individual_author_records.append(existing)
    
    # Get catalog books and books for this group author
    catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=group_author_id).all()
    books = db_session.query(Book).filter_by(author=author.normalized_name).all()
    
    # Build title mapping from catalog books
    title_to_author = {}
    
    # Re-assign catalog books using Open Library API
    for cat_book in catalog_books:
        # Try to get author from Open Library
        author_idx = None
        if cat_book.open_library_key:
            try:
                work_details = ol_client.get_work_details(cat_book.open_library_key)
                if work_details:
                    authors_list = work_details.get('authors', [])
                    for auth in authors_list:
                        author_key = None
                        if isinstance(auth, dict):
                            if 'author' in auth and isinstance(auth['author'], dict):
                                author_key = auth['author'].get('key', '')
                            elif 'key' in auth:
                                author_key = auth.get('key', '')
                        
                        if author_key:
                            if not author_key.startswith('/authors/'):
                                if author_key.startswith('/'):
                                    author_key = f"/authors{author_key}"
                                else:
                                    author_key = f"/authors/{author_key}"
                            
                            # Fetch author details
                            try:
                                author_data = ol_client._request(f"{author_key}.json")
                                if author_data:
                                    ol_author_name = author_data.get('name', '')
                                    if ol_author_name:
                                        # Match to individual authors
                                        for i, individual_author in enumerate(individual_authors):
                                            if ol_author_name == individual_author or ol_author_name.lower() == individual_author.lower():
                                                author_idx = i
                                                break
                                        if author_idx is not None:
                                            break
                            except:
                                pass
            except:
                pass
        
        # Default to first author if can't determine
        if author_idx is None:
            author_idx = 0
        
        cat_book.author_id = individual_author_records[author_idx].id
        if cat_book.title:
            title_to_author[cat_book.title.lower().strip()] = individual_authors[author_idx]
    
    # Re-assign Libby books
    for book in books:
        book_title_lower = book.title.lower().strip() if book.title else ""
        assigned_author = individual_authors[0]  # default
        
        if book_title_lower in title_to_author:
            assigned_author = title_to_author[book_title_lower]
        else:
            # Try title-based matching
            for i, author_name in enumerate(individual_authors):
                author_parts = author_name.split()
                if len(author_parts) >= 1:
                    last_name = author_parts[-1].lower()
                    if last_name in book_title_lower:
                        assigned_author = author_name
                        break
        
        book.author = normalize_author_name(assigned_author)
    
    # Remove the group author record if it has no more catalog books
    remaining_catalog = db_session.query(AuthorCatalogBook).filter_by(author_id=group_author_id).count()
    if remaining_catalog == 0:
        print(f"     ✓ Removing empty group author record")
        db_session.delete(author)
    
    try:
        db_session.commit()
        print(f"     ✓ Split complete: {len(catalog_books)} catalog books, {len(books)} Libby books reassigned")
    except Exception as e:
        db_session.rollback()
        print(f"     ✗ Error committing split: {e}")
        return False
    
    return True


def find_author_in_openlibrary(author_name: str, client: OpenLibraryClient, 
                                known_books: List[str] = None) -> str:
    """
    Find Open Library author key for an author name.
    If known_books is provided, tries to match by finding an author whose works include those books.
    """
    results = client.search_author(author_name)
    if not results:
        return ''
    
    # If we have known books by this author, try to match more precisely
    if known_books:
        known_titles_lower = {title.lower().strip() for title in known_books if title}
        
        # Try to find an author whose works match the known books
        for result in results:
            author_key = result.get('key', '')
            if not author_key:
                continue
                
            # Ensure proper format
            if not author_key.startswith('/'):
                author_key = f"/authors/{author_key}"
            if not author_key.startswith('/authors/'):
                author_key = f"/authors{author_key}"
            
            # Get some works by this author to check if they match
            try:
                works = client.get_author_works(author_key, limit=20)
                for work in works:
                    work_title = work.get('title', '').lower().strip()
                    if work_title in known_titles_lower:
                        # Found a match! This author has at least one book the user has read
                        return author_key
            except:
                # If we can't check works, continue to next author
                continue
    
    # Fall back to first result if no precise match found
    key = results[0].get('key', '')
    if key and not key.startswith('/'):
        key = f"/authors/{key}"
    return key


def fetch_author_catalog(author: Author, db_session: Session, 
                         force_refresh: bool = False,
                         only_recent: bool = False,
                         recent_years: int = 3,
                         ol_client: OpenLibraryClient = None,
                         global_title_lookup: Dict[str, AuthorCatalogBook] = None,
                         global_isbn_lookup: Dict[str, AuthorCatalogBook] = None,
                         catalog_count_hint: int = None,
                         collect_new_or_updated_ids: List = None) -> Dict:
    """
    Fetch catalog for an author and store in database
    
    Args:
        author: Author object
        db_session: Database session
        force_refresh: If True, refetch even if recently checked
        only_recent: If True, only fetch books published in the last N years (for existing authors)
        recent_years: Number of years to look back for recent books (default: 3)
        ol_client: Optional shared Open Library client (reused for speed)
        collect_new_or_updated_ids: If provided, append IDs of new/updated books (for only_recent + auto_cleanup).
    
    Returns:
        Dict with stats about fetched books, or {'error': str, 'is_systemic': bool}
        is_systemic=True means this is a network/API issue, not just author not found
    """
    # Check if recently fetched (unless force refresh)
    # Optimization: use catalog_count_hint if provided (from pre-filter) to avoid extra query
    if not force_refresh and author.last_catalog_check:
        days_since = (datetime.utcnow() - author.last_catalog_check).days
        if days_since < 7:  # Don't refetch if checked within 7 days
            # Only skip if they actually have catalog books (successful previous fetch)
            catalog_count = catalog_count_hint if catalog_count_hint is not None else db_session.query(AuthorCatalogBook).filter_by(
                author_id=author.id
            ).count()
            
            if catalog_count >= 1:
                return {'skipped': True, 'reason': f'Checked {days_since} days ago, has {catalog_count} catalog books'}
            else:
                # Was checked but has no catalog books - likely failed, so re-check
                print(f"  Previous check had 0 books, re-checking...")
    
    if ol_client is None:
        ol_client = OpenLibraryClient()
    
    # Find author in Open Library
    # Load your_books once (will be reused for match_catalog_to_history)
    your_books = db_session.query(Book).filter_by(author=author.normalized_name).all()
    
    if not author.open_library_id:
        try:
            # Get books by this author that the user has read to help with matching
            known_titles = [b.title for b in your_books if b.title]
            
            ol_id = find_author_in_openlibrary(author.normalized_name, ol_client, known_titles)
            if ol_id:
                author.open_library_id = ol_id
                db_session.commit()
            else:
                # Author not found - this is not a systemic error
                return {'error': 'Author not found in Open Library', 'is_systemic': False}
        except requests.RequestException as e:
            # Network/API error - this IS systemic
            return {'error': f'Network error: {str(e)}', 'is_systemic': True}
        except Exception as e:
            # Other errors - assume systemic for safety
            return {'error': str(e), 'is_systemic': True}
    
    books_added = 0
    books_updated = 0
    new_or_updated_books = []  # Track for match_catalog_to_history and optional cleanup
    
    # OPTION 1: Get existing catalog books and build lookup maps (avoid repeated queries)
    existing_catalog_books = db_session.query(AuthorCatalogBook).filter_by(
        author_id=author.id
    ).all()
    existing_work_keys = {book.open_library_key for book in existing_catalog_books if book.open_library_key}
    existing_catalog_count = len(existing_catalog_books)
    
    # Build lookup maps for fast duplicate checking (avoid repeated DB queries)
    existing_by_work_key = {book.open_library_key: book for book in existing_catalog_books if book.open_library_key}
    existing_by_title = {}  # (author_id, title_lower) -> book
    existing_by_isbn = {}  # (author_id, isbn) -> book
    for book in existing_catalog_books:
        if book.title:
            title_lower = book.title.lower().strip()
            existing_by_title[(author.id, title_lower)] = book
        if book.isbn:
            existing_by_isbn[(author.id, book.isbn)] = book
    
    if existing_work_keys:
        print(f"  Found {len(existing_work_keys)} existing catalog books - will skip processing these")
    
    # OPTION 2: Calculate cutoff year for recent books filtering
    should_filter_recent = only_recent and existing_catalog_count > 0
    cutoff_year = None
    if should_filter_recent:
        cutoff_year = datetime.utcnow().year - recent_years
        print(f"  Only fetching books published in {cutoff_year} or later (last {recent_years} years)")
    
    def extract_year_from_work_details(work_details: Dict) -> Optional[int]:
        """Extract publication year from work details (before getting editions)"""
        if not work_details:
            return None
        
        # Try first_publish_date or first_publish_year from work details
        pub_date = work_details.get('first_publish_date') or work_details.get('first_publish_year')
        if pub_date:
            import re
            year_match = re.search(r'\b(19|20)\d{2}\b', str(pub_date))
            if year_match:
                return int(year_match.group())
        
        return None
    
    # Fetch from Open Library
    if author.open_library_id:
        # Normalize the author ID (ensure it has proper format)
        author_key = author.open_library_id
        if not author_key.startswith('/authors/'):
            if author_key.startswith('/'):
                # Might be just /OL123456A
                author_key = f"/authors{author_key}"
            else:
                # Just OL123456A
                author_key = f"/authors/{author_key}"
            # Update in database
            author.open_library_id = author_key
            db_session.commit()
        
        try:
            works = ol_client.get_author_works(author_key, limit=200)
        except Exception as e:
            print(f"  Warning: Failed to fetch works for {author.name}: {e}")
            works = []
        
        if not works:
            print(f"  No works found for {author.name}")
        
        skipped_existing = 0
        skipped_old = 0
        
        for work in works:
            work_key = work.get('key', '')
            if not work_key:
                continue
            
            # OPTION 1: Skip if we already have this work (by Open Library key)
            if work_key in existing_work_keys:
                skipped_existing += 1
                continue  # Skip - already have this book, saves 2 API calls (work_details + editions)
            
            # Get work details (need this for title and early publication date check)
            work_details = ol_client.get_work_details(work_key)
            if not work_details:
                continue
            
            title = work_details.get('title', '') or work.get('title', '')
            if not title:
                continue
            
            # OPTION 2: Check publication date EARLY (before getting editions) if filtering recent
            if should_filter_recent and cutoff_year:
                pub_year = extract_year_from_work_details(work_details)
                if pub_year and pub_year < cutoff_year:
                    skipped_old += 1
                    continue  # Skip old book - saves 1 API call (editions)
                # If pub_year is None, we'll check again after getting editions
            
            # Get editions to check language and ISBN (only if we got this far)
            editions = ol_client.get_editions(work_key)
            english_edition = None
            publication_date = None
            
            # Find an English edition and extract publication date
            for edition in editions[:10]:  # Check first 10 editions
                if is_english_language(work_details, edition):
                    english_edition = edition
                    # Extract publication date from edition
                    pub_date = edition.get('publish_date') or edition.get('publish_year')
                    if pub_date:
                        publication_date = pub_date
                    break
            
            # If no English edition found, check work itself
            if not english_edition and not is_english_language(work_details):
                continue  # Skip non-English works
            
            # If no publication date from edition, try work details
            if not publication_date:
                publication_date = work_details.get('first_publish_date') or work_details.get('first_publish_year')
            
            # Final check: If filtering for recent books and we didn't have year from work_details
            if should_filter_recent and cutoff_year:
                # Try to extract year from publication_date string
                pub_year = None
                if publication_date:
                    # Handle various formats: "2023", "2023-01-01", "January 2023", etc.
                    import re
                    year_match = re.search(r'\b(19|20)\d{2}\b', str(publication_date))
                    if year_match:
                        pub_year = int(year_match.group())
                
                # Skip if publication year is before cutoff
                if pub_year and pub_year < cutoff_year:
                    skipped_old += 1
                    continue  # Skip old books
                elif pub_year is None:
                    # If we can't determine publication year, include it to be safe
                    # (better to include than exclude potentially new books)
                    pass
            
            # Extract series info from Open Library data (now also checks title)
            series_name, series_position = extract_series_info(work_details)
            
            # If still no series found, try extracting directly from title as fallback
            # This handles cases where Open Library doesn't have series data but title has it
            if not series_name and title:
                import re
                # Pattern: "Title (Series Name Book #3)" or "Title (Series Name #3)"
                paren_match = re.search(r'\(([^)]+)\)', title)
                if paren_match:
                    paren_content = paren_match.group(1)
                    # Look for "Book #N" or "#N" or "Book N" pattern
                    book_pattern = re.search(r'(.+?)(?:\s+Book)?\s*#?\s*(\d+)', paren_content, re.IGNORECASE)
                    if book_pattern:
                        potential_series = book_pattern.group(1).strip()
                        position_str = book_pattern.group(2).strip()
                        # Clean up series name
                        potential_series = re.sub(r'\s+Book\s*$', '', potential_series, flags=re.IGNORECASE).strip()
                        if potential_series and len(potential_series) > 2:
                            try:
                                series_position = int(position_str)
                                series_name = potential_series
                            except ValueError:
                                pass
            
            # Get ISBN from English edition or work
            isbn = extract_isbn(work_details, english_edition) if english_edition else extract_isbn(work_details)
            if not isbn and english_edition:
                isbn = extract_isbn(work_details, english_edition)
            
            # Check if catalog book already exists (using lookup maps - no DB queries)
            title_lower = title.lower().strip() if title else ""
            
            # Check by Open Library key first (fastest)
            existing = existing_by_work_key.get(work_key)
            
            # Also check for duplicates by title (case-insensitive) within same author
            if not existing and title_lower:
                existing = existing_by_title.get((author.id, title_lower))
            
            # Also check by ISBN if we have one (within same author)
            if not existing and isbn:
                existing = existing_by_isbn.get((author.id, isbn))
            
            # Also check for duplicates across ALL authors (to catch duplicates from author group splits)
            # This is important when author groups are split - same book might be added to multiple authors
            # Optimization: use global lookup maps if provided (built once in fetch_all_author_catalogs)
            if not existing:
                cross_author_duplicate = None
                
                if global_title_lookup and title_lower:
                    cross_author_duplicate = global_title_lookup.get(title_lower)
                
                if not cross_author_duplicate and global_isbn_lookup and isbn:
                    cross_author_duplicate = global_isbn_lookup.get(isbn)
                
                # Fall back to DB query if global lookups not available (backward compatibility)
                if not cross_author_duplicate and (not global_title_lookup or not global_isbn_lookup):
                    from sqlalchemy import func
                    
                    # Check by exact title match across all authors
                    cross_author_duplicate = db_session.query(AuthorCatalogBook).filter(
                        func.lower(AuthorCatalogBook.title) == title_lower
                    ).first()
                    
                    # Also check by ISBN across all authors
                    if not cross_author_duplicate and isbn:
                        cross_author_duplicate = db_session.query(AuthorCatalogBook).filter_by(
                            isbn=isbn
                        ).first()
                
                # If found a duplicate across authors, skip adding it again
                # (We keep the first one found to avoid duplicates from author group splits)
                if cross_author_duplicate:
                    # Skip this book - it's already in the catalog under a different author
                    continue  # Skip to next work
            
            if existing:
                # Update if needed
                existing.title = title
                existing.isbn = isbn or existing.isbn
                existing.series_name = series_name or existing.series_name
                existing.series_position = series_position if series_position else existing.series_position
                if publication_date:
                    existing.publication_date = publication_date
                existing.fetched_at = datetime.utcnow()
                books_updated += 1
                new_or_updated_books.append(existing)
            else:
                # Create new catalog entry
                catalog_book = AuthorCatalogBook(
                    author_id=author.id,
                    title=title,
                    isbn=isbn,
                    series_name=series_name,
                    series_position=series_position,
                    open_library_key=work_key,
                    format_available='unknown',
                    publication_date=publication_date
                )
                db_session.add(catalog_book)
                books_added += 1
                new_or_updated_books.append(catalog_book)
        
        # Print optimization stats
        if skipped_existing > 0:
            print(f"  ✓ Skipped {skipped_existing} existing books (saved ~{skipped_existing * 2} API calls)")
        if skipped_old > 0:
            print(f"  ✓ Skipped {skipped_old} old books (saved ~{skipped_old} API calls)")
    
    # Match catalog books to your reading history
    # Optimization: if we added/updated books, only match those (faster)
    # Reuse your_books we already loaded (avoid duplicate query)
    # Otherwise, match all catalog books (in case user read new books since last match)
    if new_or_updated_books:
        match_catalog_to_history(author, db_session, books_to_match=new_or_updated_books, your_books=your_books)
        if collect_new_or_updated_ids is not None:
            collect_new_or_updated_ids.extend(b.id for b in new_or_updated_books)
    else:
        # No new/updated books, but user might have read books - match all catalog books
        match_catalog_to_history(author, db_session, your_books=your_books)
    
    # Update author's last check time
    author.last_catalog_check = datetime.utcnow()
    
    db_session.commit()
    
    return {
        'books_added': books_added,
        'books_updated': books_updated,
        'total_catalog_books': db_session.query(AuthorCatalogBook).filter_by(
            author_id=author.id
        ).count()
    }


def match_catalog_to_history(author: Author, db_session: Session, 
                             books_to_match: List[AuthorCatalogBook] = None,
                             match_unmatched_only: bool = False,
                             your_books: List[Book] = None):
    """
    Match author's catalog books to your reading history (in-memory to avoid N+1 queries).
    
    Args:
        author: Author object
        db_session: Database session
        books_to_match: Optional list of specific books to match (if None, matches all)
        match_unmatched_only: If True, only match books that aren't already marked as read
        your_books: Optional pre-loaded list of books you've read (avoids duplicate query)
    """
    # Load your books once (or use provided list)
    if your_books is None:
        your_books = db_session.query(Book).filter_by(author=author.normalized_name).all()
    your_isbns = {b.isbn for b in your_books if b.isbn}
    your_titles = {b.title.lower().strip() for b in your_books if b.title}
    # Maps for resolving matched_book_id without extra queries
    book_by_isbn = {b.isbn: b for b in your_books if b.isbn}
    book_by_title_lower = {b.title.lower().strip(): b for b in your_books if b.title}
    
    # Load catalog books to match
    if books_to_match:
        catalog_books = books_to_match
    elif match_unmatched_only:
        # Only match books that aren't already marked as read
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(
            author_id=author.id,
            is_read=False
        ).all()
    else:
        # Match all catalog books (original behavior)
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(
            author_id=author.id
        ).all()
    
    for catalog_book in catalog_books:
        is_read = False
        matched_book_id = None
        
        # Match by ISBN (in-memory)
        if catalog_book.isbn and catalog_book.isbn in your_isbns:
            matched_book = book_by_isbn.get(catalog_book.isbn)
            if matched_book:
                is_read = True
                matched_book_id = matched_book.id
        
        # Match by title (in-memory exact match first)
        if not is_read and catalog_book.title:
            catalog_title_lower = catalog_book.title.lower().strip()
            if catalog_title_lower in your_titles:
                matched_book = book_by_title_lower.get(catalog_title_lower)
                if matched_book:
                    is_read = True
                    matched_book_id = matched_book.id
            # Fuzzy: catalog title contained in a read book title or vice versa
            if not is_read:
                for book in your_books:
                    if not book.title:
                        continue
                    if catalog_title_lower in book.title.lower() or book.title.lower() in catalog_title_lower:
                        is_read = True
                        matched_book_id = book.id
                        break
        
        catalog_book.is_read = is_read
        catalog_book.matched_book_id = matched_book_id


def fetch_all_author_catalogs(db_session: Session, force_refresh: bool = False, 
                              max_consecutive_errors: int = 5,
                              only_recent: bool = False,
                              recent_years: int = 3,
                              auto_cleanup: bool = False) -> Dict:
    """
    Fetch catalogs for all authors in database
    
    Args:
        db_session: Database session
        force_refresh: If True, refetch even if recently checked
        max_consecutive_errors: Stop if this many consecutive errors occur
        only_recent: If True, only fetch books published in the last N years (for existing authors)
        recent_years: Number of years to look back for recent books (default: 3)
        auto_cleanup: If True and only_recent, run dedupe and non-English cleanup on new/updated books (or on all catalog books for processed authors if none)
    """
    from sqlalchemy import func
    
    all_authors = db_session.query(Author).all()
    total_author_count = len(all_authors)
    
    # Pre-filter: only process authors that need a check (avoids work for recently-checked authors)
    catalog_counts = {}  # Initialize for use in loop
    if not force_refresh and total_author_count > 0:
        catalog_counts = dict(
            db_session.query(AuthorCatalogBook.author_id, func.count(AuthorCatalogBook.id))
            .group_by(AuthorCatalogBook.author_id)
            .all()
        )
        now = datetime.utcnow()
        authors_to_process = []
        skipped_count = 0
        for author in all_authors:
            days_since = (now - author.last_catalog_check).days if author.last_catalog_check else 999
            catalog_count = catalog_counts.get(author.id, 0)
            if days_since < 7 and catalog_count >= 1:
                skipped_count += 1
                continue
            authors_to_process.append(author)
        if skipped_count > 0:
            print(f"Skipping {skipped_count} authors (checked within 7 days with catalog). Processing {len(authors_to_process)} authors.\n")
        authors = authors_to_process
    else:
        authors = all_authors
    
    # Reuse API client across all authors (shared cache, fewer allocations)
    ol_client = OpenLibraryClient()
    
    # Build global lookup maps for cross-author duplicate checking (one-time cost)
    # This eliminates expensive DB queries for cross-author duplicates
    print("Building global catalog lookup maps for duplicate detection...")
    all_catalog_books = db_session.query(AuthorCatalogBook).all()
    global_title_lookup = {}  # title_lower -> first AuthorCatalogBook found
    global_isbn_lookup = {}  # isbn -> first AuthorCatalogBook found
    for book in all_catalog_books:
        if book.title:
            title_lower = book.title.lower().strip()
            if title_lower and title_lower not in global_title_lookup:
                global_title_lookup[title_lower] = book
        if book.isbn:
            if book.isbn not in global_isbn_lookup:
                global_isbn_lookup[book.isbn] = book
    print(f"  Built lookup maps: {len(global_title_lookup)} titles, {len(global_isbn_lookup)} ISBNs\n")
    
    results = {
        'total_authors': total_author_count,
        'catalogs_fetched': 0,
        'catalogs_skipped': 0,
        'total_books_added': 0,
        'total_books_updated': 0,
        'errors': [],
        'stopped_early': False
    }
    
    print(f"Processing {len(authors)} authors...")
    print(f"Will stop after {max_consecutive_errors} consecutive errors.\n")
    
    consecutive_errors = 0
    new_or_updated_ids = [] if (only_recent and auto_cleanup) else None
    processed_author_ids = [] if (only_recent and auto_cleanup) else None
    
    for i, author in enumerate(authors, 1):
        try:
            print(f"[{i}/{len(authors)}] Fetching catalog for {author.name}...")
            
            # Check if this is an author group and auto-split if needed
            if auto_split_author_group(author, db_session):
                # Author was split, skip catalog fetch for this one
                results['catalogs_skipped'] += 1
                print(f"  Skipped: Author group was split into individual authors")
                consecutive_errors = 0
                continue
            
            if processed_author_ids is not None:
                processed_author_ids.append(author.id)
            
            # Get catalog count hint from pre-filter to avoid duplicate query
            catalog_count_hint = catalog_counts.get(author.id, 0) if not force_refresh and total_author_count > 0 else None
            
            result = fetch_author_catalog(author, db_session, force_refresh, 
                                         only_recent=only_recent, recent_years=recent_years,
                                         ol_client=ol_client,
                                         global_title_lookup=global_title_lookup,
                                         global_isbn_lookup=global_isbn_lookup,
                                         catalog_count_hint=catalog_count_hint,
                                         collect_new_or_updated_ids=new_or_updated_ids)
            
            if result.get('skipped'):
                results['catalogs_skipped'] += 1
                print(f"  Skipped: {result.get('reason', 'Unknown')}")
                consecutive_errors = 0  # Reset error counter on skip
            elif result.get('error'):
                error_msg = f"{author.name}: {result.get('error')}"
                results['errors'].append(error_msg)
                
                # Only count systemic errors (network/API issues) toward the limit
                # Author-not-found errors are expected and shouldn't stop the process
                is_systemic = result.get('is_systemic', False)
                if is_systemic:
                    consecutive_errors += 1
                    print(f"  ✗ Error (systemic): {error_msg}")
                    
                    # Check if we should stop
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"\n⚠️  Stopping: {consecutive_errors} consecutive systemic errors detected.")
                        print("   This usually indicates a network issue or API problem.")
                        print("   Check your internet connection and try again later.")
                        results['stopped_early'] = True
                        break
                else:
                    # Author not found - not a systemic issue, don't count toward limit
                    print(f"  ⚠ Warning: {error_msg}")
                    consecutive_errors = 0  # Reset counter for non-systemic errors
            else:
                results['catalogs_fetched'] += 1
                books_added = result.get('books_added', 0)
                books_updated = result.get('books_updated', 0)
                results['total_books_added'] += books_added
                results['total_books_updated'] += books_updated
                print(f"  ✓ Added {books_added} books, updated {books_updated}")
                consecutive_errors = 0  # Reset error counter on success
                
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Progress saved.")
            break
        except requests.RequestException as e:
            # Network/API errors - these are systemic
            error_msg = f"{author.name}: Network error - {str(e)}"
            results['errors'].append(error_msg)
            consecutive_errors += 1
            print(f"  ✗ Error (systemic): {error_msg}")
            
            # Check if we should stop
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n⚠️  Stopping: {consecutive_errors} consecutive systemic errors detected.")
                print("   This usually indicates a network issue or API problem.")
                print("   Check your internet connection and try again later.")
                results['stopped_early'] = True
                break
        except Exception as e:
            # Other unexpected errors - assume systemic
            error_msg = f"{author.name}: {str(e)}"
            results['errors'].append(error_msg)
            consecutive_errors += 1
            print(f"  ✗ Error: {error_msg}")
            
            # Check if we should stop
            if consecutive_errors >= max_consecutive_errors:
                print(f"\n⚠️  Stopping: {consecutive_errors} consecutive errors detected.")
                print("   This usually indicates a network issue or API problem.")
                print("   Check your internet connection and try again later.")
                results['stopped_early'] = True
                break
    
    # When only_recent and auto_cleanup: run dedupe and non-English cleanup.
    # Use new/updated book IDs if any; otherwise run cleanup on all catalog books for the authors we just processed.
    if only_recent and auto_cleanup and processed_author_ids:
        cleanup_ids = new_or_updated_ids if new_or_updated_ids else [
            row[0] for row in db_session.query(AuthorCatalogBook.id).filter(
                AuthorCatalogBook.author_id.in_(processed_author_ids)
            ).all()
        ]
        if cleanup_ids:
            print(f"\n{'='*60}")
            if new_or_updated_ids:
                print(f"Running cleanup on {len(cleanup_ids)} new/updated books (dedupe, then non-English)...")
            else:
                print(f"No new/updated books this run; running cleanup on all catalog books for the {len(processed_author_ids)} authors processed ({len(cleanup_ids)} books)...")
            print(f"{'='*60}\n")
            remove_duplicate_titles(db_session, dry_run=False, catalog_book_ids=cleanup_ids)
            db_session.commit()
            cleanup_non_english_books(db_session, dry_run=False, catalog_book_ids=cleanup_ids)
            db_session.commit()
        else:
            print(f"\nNo catalog books to cleanup for the {len(processed_author_ids)} authors processed.")
    
    # Update system metadata: last catalog check
    metadata = db_session.query(SystemMetadata).filter_by(key='last_catalog_check').first()
    if metadata:
        metadata.value = datetime.utcnow().isoformat()
        metadata.updated_at = datetime.utcnow()
    else:
        metadata = SystemMetadata(
            key='last_catalog_check',
            value=datetime.utcnow().isoformat()
        )
        db_session.add(metadata)
    
    db_session.commit()
    
    return results


def cleanup_non_english_books(db_session: Session, dry_run: bool = False, limit: int = None, offset: int = 0,
                              catalog_book_ids: Optional[List[int]] = None) -> Dict:
    """
    Remove non-English books from existing catalog
    
    Args:
        db_session: Database session
        dry_run: If True, only report what would be removed without actually removing
        limit: Maximum number of books to check (None for all)
        offset: Number of books to skip before starting (for batch processing)
        catalog_book_ids: If provided, only check/clean these catalog book IDs (e.g. recent books only)
    
    Returns:
        Dict with cleanup stats
    """
    from .api.openlibrary import is_english_language, OpenLibraryClient
    from .api.googlebooks import GoogleBooksClient
    
    if dry_run:
        print("Checking for non-English books in catalog (DRY RUN - no changes will be made)...")
    else:
        print("Cleaning up non-English books from catalog...")
        print("  Note: If you get 'database is locked' errors, close the web UI and try again.")
    
    query = db_session.query(AuthorCatalogBook).order_by(AuthorCatalogBook.id)
    if catalog_book_ids:
        query = query.filter(AuthorCatalogBook.id.in_(catalog_book_ids))
    elif offset:
        query = query.offset(offset)
    if limit and not catalog_book_ids:
        query = query.limit(limit)
    catalog_books = query.all()
    total = len(catalog_books)
    
    total_in_db = db_session.query(AuthorCatalogBook).count()
    if catalog_book_ids:
        print(f"  Processing {total} books (scoped to given IDs)...")
    elif limit or offset:
        range_str = f"books {offset + 1}-{offset + total}" if limit else f"books starting from {offset + 1}"
        print(f"  Processing {total} books ({range_str} of {total_in_db} total)...")
    removed = 0
    checked = 0
    non_english_books = []
    
    # When scoped to specific book IDs, skip Google Books (use title + Open Library only) to avoid rate limits.
    use_google_books = catalog_book_ids is None
    if not use_google_books:
        print("  (Skipping Google Books in cleanup; using title + Open Library only.)")
    gb_client = GoogleBooksClient() if use_google_books else None
    ol_client = OpenLibraryClient()
    
    for catalog_book in catalog_books:
        checked += 1
        if checked % 50 == 0:
            print(f"  Checking {checked}/{total}...")
        
        # Store all values early to avoid accessing database attributes after potential rollbacks
        # Use no_autoflush to prevent premature flushes when accessing attributes
        try:
            with db_session.no_autoflush:
                book_title = catalog_book.title or ""
                book_isbn = catalog_book.isbn
                book_open_library_key = catalog_book.open_library_key
                book_author_id = catalog_book.author_id
        except Exception as e:
            # If we can't access attributes (e.g., after a rollback), skip this book
            error_msg = str(e).lower()
            if "locked" in error_msg:
                print(f"  ⚠ Warning: Database locked while accessing book attributes. Skipping this book.")
                print(f"  Continuing with next book...")
                try:
                    db_session.rollback()
                except:
                    pass
                continue
            else:
                # For other errors, try to continue with empty values
                book_title = ""
                book_isbn = None
                book_open_library_key = None
                book_author_id = None
        
        # Skip books without titles (can't process them)
        if not book_title:
            continue
        
        is_english = True
        
        # First, check title for language edition indicators (e.g., "Title (French Edition)")
        if book_title:
            import re
            # Pattern to match language editions: (Language Edition), (Language), [Language Edition], etc.
            # Common patterns: (French Edition), (Russian Edition), (Spanish Edition), (German Edition), etc.
            # Also match "Spanish Edition" without parentheses, and detect Spanish text in titles
            # List of non-English languages (excluding English variants)
            non_english_languages = (
                'french|russian|spanish|german|italian|portuguese|chinese|japanese|korean|arabic|hebrew|'
                'polish|dutch|swedish|norwegian|danish|finnish|greek|turkish|hindi|thai|vietnamese|'
                'indonesian|malay|tagalog|romanian|hungarian|czech|slovak|croatian|serbian|bulgarian|'
                'ukrainian|persian|urdu|bengali|tamil|telugu|marathi|gujarati|kannada|malayalam|'
                'punjabi|nepali|sinhala|myanmar|khmer|lao|mongolian|georgian|armenian|azerbaijani|'
                'kazakh|uzbek|turkmen|kyrgyz|tajik|afrikaans|swahili|zulu|xhosa|amharic|hausa|'
                'yoruba|igbo|somali|maltese|icelandic|basque|catalan|galician|welsh|irish|scottish|'
                'breton|cornish|manx'
            )
            # Match parentheses: (French Edition), (French), etc.
            paren_pattern = re.compile(
                rf'\([^)]*(?:{non_english_languages})\s*(?:edition|version|translation)?[^)]*\)',
                re.IGNORECASE
            )
            # Match square brackets: [French Edition], [French], etc.
            bracket_pattern = re.compile(
                rf'\[[^\]]*(?:{non_english_languages})\s*(?:edition|version|translation)?[^\]]*\]',
                re.IGNORECASE
            )
            # Match "Language Edition" without parentheses/brackets (e.g., "Spanish Edition")
            standalone_pattern = re.compile(
                rf'\b(?:{non_english_languages})\s+(?:edition|version|translation)\b',
                re.IGNORECASE
            )
            # Detect common Spanish words/phrases in titles (indicating Spanish language books)
            # But exclude English "House Edition" titles
            spanish_indicators = re.compile(
                r'\b(?:edici[oó]n|colecci[oó]n|estuche|libro|libros|misterio|pr[ií]ncipe)\b',
                re.IGNORECASE
            )
            # Detect non-English indicators
            if (paren_pattern.search(book_title) or 
                bracket_pattern.search(book_title) or
                standalone_pattern.search(book_title)):
                is_english = False
            # Check for Spanish indicators (but exclude house editions which are English)
            elif 'house edition' not in book_title.lower() and spanish_indicators.search(book_title):
                is_english = False
            
            # Check for accented/non-English characters in title
            if is_english:  # Only check if not already flagged
                # First, check for major non-English scripts (CJK, Cyrillic, Arabic, Hebrew)
                # This is the same check used in the API functions
                major_non_english_pattern = re.compile(
                    r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff]'
                )
                if major_non_english_pattern.search(book_title):
                    is_english = False
                else:
                    # Check for accented characters from European languages
                    # IMPORTANT: Do NOT use IGNORECASE - it causes false matches with 'ı' (dotless i) matching 'i'
                    # French: é, è, ê, ë, ç, à, â, ù, û, ü, ô, ö, î, ï, ÿ
                    # Spanish: á, é, í, ó, ú, ñ
                    # German: ä, ö, ü, ß
                    # Portuguese: á, à, â, ã, é, ê, í, ó, ô, õ, ú, ü, ç
                    # Italian: à, è, é, ì, ò, ù
                    # Other: å, æ, ø (Scandinavian), č, ć, đ, š, ž (Slavic), etc.
                    # Exclude 'ı' (U+0131) to avoid false matches with 'i' (U+0069)
                    accented_chars_pattern = re.compile(
                        r'[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿąćčđęěğłńňřśşšťůźżžÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞŸĄĆČĐĘĚĞŁŃŇŘŚŞŠŤŮŹŻŽ]'
                    )
                    # Spanish punctuation (definite indicator)
                    spanish_punct = re.compile(r'[¿¡]')
                    # German ß (definite indicator)
                    german_eszett = re.compile(r'[ß]')
                    
                    # Spanish punctuation or German ß are definite indicators
                    if spanish_punct.search(book_title) or german_eszett.search(book_title):
                        is_english = False
                    elif accented_chars_pattern.search(book_title):
                        # Count accented characters - be more conservative to avoid false positives
                        accented_count = len(accented_chars_pattern.findall(book_title))
                        total_alpha_chars = len([c for c in book_title if c.isalpha()])
                        
                        if total_alpha_chars > 0:
                            ratio = accented_count / total_alpha_chars
                            # Flag if:
                            # - More than 10% of characters are accented (conservative threshold)
                            # - Title is short (< 20 chars) and has 2+ accented chars (likely non-English word/phrase)
                            # - There are 3+ accented chars regardless of ratio (definitely non-English)
                            if ratio > 0.10 or (len(book_title) < 20 and accented_count >= 2) or accented_count >= 3:
                                is_english = False
        
        # Check via Google Books if we have ISBN (skip when scoped to avoid extra API calls)
        if use_google_books and is_english and book_isbn:
            try:
                gb_item = gb_client.get_by_isbn(book_isbn)
                if gb_item:
                    is_english = gb_client.is_english_language(gb_item)
            except:
                pass  # If check fails, assume English to be safe
        
        # If we have Open Library key, check that too
        if is_english and book_open_library_key:
            try:
                work_details = ol_client.get_work_details(book_open_library_key)
                if work_details:
                    # Try to get an edition to check language
                    editions = ol_client.get_editions(book_open_library_key)
                    if editions:
                        # Check if any edition is English
                        has_english = False
                        for edition in editions[:5]:
                            if is_english_language(work_details, edition):
                                has_english = True
                                break
                        if not has_english and not is_english_language(work_details):
                            is_english = False
            except:
                pass  # If check fails, assume English to be safe
        
        # Remove if not English
        if not is_english:
            # Values already stored at the start of the loop (including book_author_id)
            
            # Get author name for display (query with no_autoflush to avoid premature flush)
            author_name = f"Author ID {book_author_id}"
            try:
                with db_session.no_autoflush:
                    author = db_session.query(Author).filter_by(id=book_author_id).first()
                    if author:
                        author_name = author.name
            except Exception as e:
                # If query fails, use the fallback author_name we set above
                pass
            
            non_english_books.append({
                'title': book_title,
                'author': author_name,
                'author_id': book_author_id,
                'isbn': book_isbn
            })
            if not dry_run:
                try:
                    # Delete the catalog book
                    db_session.delete(catalog_book)
                    removed += 1
                    # Commit every 50 deletions to avoid long-held locks
                    if removed % 50 == 0:
                        db_session.commit()
                        print(f"  Committed {removed} deletions so far...")
                except Exception as e:
                    error_msg = str(e).lower()
                    print(f"\n  ⚠ Warning: Error deleting book '{book_title}': {e}")
                    if "locked" in error_msg:
                        print(f"  Database is locked. Try closing other connections (e.g., web UI) and retry.")
                        # Small delay to allow lock to be released
                        import time
                        time.sleep(0.5)
                    print(f"  Continuing with next book...")
                    try:
                        db_session.rollback()
                    except:
                        pass  # Ignore rollback errors
            else:
                removed += 1
    
    if not dry_run:
        try:
            db_session.commit()
            print(f"\n✓ Cleanup complete! Removed {removed} non-English books.")
            if non_english_books:
                print(f"\n  Complete list of removed books ({len(non_english_books)} total):")
                for book in non_english_books:
                    print(f"    - {book['title']} by {book['author']}")
        except Exception as e:
            print(f"\n⚠ Warning: Error during final commit: {e}")
            print(f"  {removed} books were marked for deletion but commit failed.")
            print(f"  You may need to close other database connections (e.g., web UI) and retry.")
            db_session.rollback()
            raise
    else:
        print(f"\n✓ Dry run complete!")
        if non_english_books:
            print(f"\n  Complete list of non-English books that would be removed ({len(non_english_books)} total):")
            for book in non_english_books:
                print(f"    - {book['title']} by {book['author']}")
    
    print(f"  Checked: {checked}")
    print(f"  {'Would remove' if dry_run else 'Removed'}: {removed} non-English books")
    print(f"  {'Would remain' if dry_run else 'Remaining'}: {total - removed}")
    
    return {
        'checked': checked,
        'removed': removed,
        'remaining': total - removed,
        'non_english_books': non_english_books if dry_run else []
    }


def fix_author_mismatches(db_session: Session, max_groups: int = None, only_cataloged: bool = False) -> Dict:
    """
    Fix catalog books that are assigned to the wrong author.
    
    This happens when:
    1. Multiple authors have the same name (e.g., multiple "Jane Smith" authors)
    2. One author record has catalog books from multiple different Open Library authors
    
    We match based on:
    1. Open Library author ID from work details
    2. Books the user has actually read by that author
    
    Args:
        db_session: Database session
        max_groups: Maximum number of author groups to process (None = all)
        only_cataloged: If True, only process authors that have catalog books (already been cataloged)
    
    Returns:
        Dict with cleanup stats
    """
    from collections import defaultdict
    from .api.openlibrary import OpenLibraryClient
    from sqlalchemy import func
    
    print("Fixing author mismatches in catalog...")
    
    ol_client = OpenLibraryClient()
    
    # First, check for authors with catalog books from different Open Library authors
    # This is the main issue - one Author record has books from multiple OL authors
    print("  Checking for authors with mixed catalog books...")
    
    authors_to_fix = []
    all_authors = db_session.query(Author).all()
    processed = 0
    
    for author in all_authors:
        if only_cataloged:
            catalog_count = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
            if catalog_count == 0:
                continue
        
        # Get catalog books for this author
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
        if len(catalog_books) < 2:
            continue  # Need at least 2 books to have a mismatch
        
        processed += 1
        if max_groups and processed > max_groups:
            break
        
        # Check Open Library author IDs from work details
        ol_author_ids = {}  # Maps OL author key -> list of catalog book IDs
        for catalog_book in catalog_books:
            if not catalog_book.open_library_key:
                continue
            
            try:
                work_details = ol_client.get_work_details(catalog_book.open_library_key)
                if work_details:
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
                            
                            if author_key not in ol_author_ids:
                                ol_author_ids[author_key] = []
                            ol_author_ids[author_key].append(catalog_book.id)
                            break  # Use first author found
            except Exception as e:
                # Skip if we can't get work details
                pass
        
        # If we found multiple different Open Library author IDs, this is a problem
        if len(ol_author_ids) > 1:
            authors_to_fix.append((author, ol_author_ids))
            print(f"    Found {author.name}: {len(ol_author_ids)} different Open Library authors in catalog")
    
    # Also check for duplicate author names (traditional approach)
    authors_by_name = defaultdict(list)
    for author in all_authors:
        authors_by_name[author.normalized_name].append(author)
    
    duplicate_groups = {name: authors for name, authors in authors_by_name.items() if len(authors) > 1}
    
    catalog_books_reassigned = 0
    authors_created = 0
    authors_to_remove = []
    duplicate_groups = {}  # Initialize for later use
    
    if not authors_to_fix:
        print("  No authors with mixed catalog books found.")
    
    # Fix authors with mixed catalog books (main issue)
    for author, ol_author_ids in authors_to_fix:
        print(f"\n  Fixing {author.name} (ID: {author.id})...")
        print(f"    Found {len(ol_author_ids)} different Open Library authors")
        
        # Get books the user has read by this author
        your_books = db_session.query(Book).filter_by(author=author.normalized_name).all()
        your_titles = {b.title.lower().strip() for b in your_books if b.title}
        
        # Determine which OL author ID matches the books you've read
        best_ol_author = None
        best_score = 0
        
        for ol_author_key, catalog_book_ids in ol_author_ids.items():
            score = 0
            # Check if this OL author's works match books you've read
            try:
                works = ol_client.get_author_works(ol_author_key, limit=50)
                for work in works:
                    work_title = work.get('title', '').lower().strip()
                    if work_title in your_titles:
                        score += 10
            except:
                pass
            
            # Check catalog books for this OL author
            for catalog_book_id in catalog_book_ids:
                catalog_book = db_session.query(AuthorCatalogBook).filter_by(id=catalog_book_id).first()
                if catalog_book and catalog_book.title:
                    catalog_title = catalog_book.title.lower().strip()
                    if catalog_title in your_titles:
                        score += 5
            
            if score > best_score:
                best_score = score
                best_ol_author = ol_author_key
        
        if not best_ol_author:
            # Can't determine - use the one with most catalog books
            best_ol_author = max(ol_author_ids.keys(), key=lambda k: len(ol_author_ids[k]))
            print(f"    ⚠ Could not determine best match, using author with most books")
        
        print(f"    ✓ Best match: {best_ol_author} (score: {best_score})")
        
        # Keep books from best_ol_author with current author
        # Create new authors for other OL authors
        for ol_author_key, catalog_book_ids in ol_author_ids.items():
            if ol_author_key == best_ol_author:
                continue  # Keep these with current author
            
            # Create a new Author record for this OL author
            # First, try to get author name from Open Library
            ol_author_name = author.name  # Default to same name
            try:
                ol_author_data = ol_client._request(f"{ol_author_key}.json")
                if ol_author_data:
                    ol_author_name = ol_author_data.get('name', author.name)
            except:
                pass
            
            # Check if author with this OL ID already exists
            existing_author = db_session.query(Author).filter_by(open_library_id=ol_author_key).first()
            
            # Also check by name (in case name is unique but OL ID is different)
            if not existing_author:
                existing_author = db_session.query(Author).filter_by(name=ol_author_name).first()
            
            if not existing_author:
                # Create new author with retry logic for database locks
                max_retries = 5
                new_author = None
                for attempt in range(max_retries):
                    try:
                        new_author = Author(
                            name=ol_author_name,
                            normalized_name=author.normalized_name,  # Same normalized name
                            open_library_id=ol_author_key
                        )
                        db_session.add(new_author)
                        db_session.flush()  # Get the ID
                        authors_created += 1
                        print(f"    Created new author: {ol_author_name} (ID: {new_author.id})")
                        break  # Success
                    except Exception as e:
                        error_msg = str(e).lower()
                        # Rollback on any error before retrying
                        db_session.rollback()
                        
                        if 'locked' in error_msg and attempt < max_retries - 1:
                            import time
                            wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s, 8s, 10s
                            print(f"    Database locked, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                        elif 'unique' in error_msg or 'constraint' in error_msg:
                            # Author already exists (by name or OL ID) - try to find it
                            print(f"    Author {ol_author_name} already exists, looking up...")
                            existing_by_ol = db_session.query(Author).filter_by(open_library_id=ol_author_key).first()
                            existing_by_name = db_session.query(Author).filter_by(name=ol_author_name).first()
                            if existing_by_ol:
                                new_author = existing_by_ol
                                print(f"    Found existing author by OL ID: {ol_author_name} (ID: {new_author.id})")
                                break
                            elif existing_by_name:
                                new_author = existing_by_name
                                # Update OL ID if it's not set
                                if not new_author.open_library_id:
                                    new_author.open_library_id = ol_author_key
                                    db_session.flush()
                                print(f"    Found existing author by name: {ol_author_name} (ID: {new_author.id})")
                                break
                            else:
                                print(f"    ⚠ Could not find existing author {ol_author_name} after constraint error")
                                break
                        else:
                            print(f"    ⚠ Failed to create author {ol_author_name}: {e}")
                            # Try to find if it was created by another process
                            existing_by_ol = db_session.query(Author).filter_by(open_library_id=ol_author_key).first()
                            existing_by_name = db_session.query(Author).filter_by(name=ol_author_name).first()
                            if existing_by_ol:
                                new_author = existing_by_ol
                                print(f"    Found existing author by OL ID: {ol_author_name} (ID: {new_author.id})")
                            elif existing_by_name:
                                new_author = existing_by_name
                                if not new_author.open_library_id:
                                    new_author.open_library_id = ol_author_key
                                    db_session.flush()
                                print(f"    Found existing author by name: {ol_author_name} (ID: {new_author.id})")
                            break
                
                if not new_author:
                    print(f"    ⚠ Skipping catalog books for {ol_author_key} - could not create/find author")
                    continue
            else:
                new_author = existing_author
                # Update OL ID if it's not set
                if not new_author.open_library_id:
                    new_author.open_library_id = ol_author_key
                    try:
                        db_session.flush()
                    except:
                        db_session.rollback()
                print(f"    Using existing author: {ol_author_name} (ID: {new_author.id})")
            
            # Reassign catalog books with retry logic
            for catalog_book_id in catalog_book_ids:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        catalog_book = db_session.query(AuthorCatalogBook).filter_by(id=catalog_book_id).first()
                        if not catalog_book:
                            break  # Already reassigned or deleted
                        
                        # Check for duplicate
                        title_lower = catalog_book.title.lower().strip() if catalog_book.title else ''
                        existing = db_session.query(AuthorCatalogBook).filter_by(
                            author_id=new_author.id
                        ).filter(func.lower(AuthorCatalogBook.title) == title_lower).first()
                        
                        if existing:
                            db_session.delete(catalog_book)
                            db_session.flush()
                            catalog_books_reassigned += 1
                        else:
                            catalog_book.author_id = new_author.id
                            db_session.flush()
                            catalog_books_reassigned += 1
                        break  # Success
                    except Exception as e:
                        error_msg = str(e).lower()
                        if 'locked' in error_msg and attempt < max_retries - 1:
                            import time
                            wait_time = (attempt + 1) * 1  # 1s, 2s, 3s
                            time.sleep(wait_time)
                            db_session.rollback()
                            continue
                        else:
                            print(f"    ⚠ Failed to reassign catalog book (ID: {catalog_book_id}): {e}")
                            break
        
        # Update current author's OL ID if it's not set or wrong
        if not author.open_library_id or author.open_library_id != best_ol_author:
            author.open_library_id = best_ol_author
    
    # Also handle duplicate author names (traditional approach)
    if not max_groups or len(authors_to_fix) < max_groups:
        authors_by_name = defaultdict(list)
        for author in all_authors:
            authors_by_name[author.normalized_name].append(author)
        
        duplicate_groups = {name: authors for name, authors in authors_by_name.items() if len(authors) > 1}
        
        if duplicate_groups:
            # Filter to only authors with catalog books if requested
            if only_cataloged:
                filtered_groups = {}
                for name, authors in duplicate_groups.items():
                    has_catalog = False
                    for author in authors:
                        catalog_count = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
                        if catalog_count > 0:
                            has_catalog = True
                            break
                    if has_catalog:
                        filtered_groups[name] = authors
                duplicate_groups = filtered_groups
            
            remaining_limit = max_groups - len(authors_to_fix) if max_groups else None
            if remaining_limit and remaining_limit > 0:
                duplicate_groups = dict(list(duplicate_groups.items())[:remaining_limit])
            
            if duplicate_groups:
                print(f"\n  Found {len(duplicate_groups)} author name(s) with multiple author records:")
                for name, authors in duplicate_groups.items():
                    print(f"    - {name}: {len(authors)} authors")
                
                # Process duplicate name groups
                for normalized_name, authors in duplicate_groups.items():
                    # Get books the user has read by this normalized name
                    your_books = db_session.query(Book).filter_by(author=normalized_name).all()
                    your_titles = {b.title.lower().strip() for b in your_books if b.title}
                    
                    if not your_titles:
                        # No books read by this name - can't determine which is correct
                        print(f"  ⚠ Skipping {normalized_name}: No books read by this author")
                        continue
                    
                    # For each author, check which one matches the books you've read
                    best_match = None
                    best_match_score = 0
                    
                    for author in authors:
                        # Get catalog books for this author
                        catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
                        
                        # Check if this author has Open Library ID and if their works match
                        match_score = 0
                        if author.open_library_id:
                            try:
                                author_key = author.open_library_id
                                if not author_key.startswith('/authors/'):
                                    if author_key.startswith('/'):
                                        author_key = f"/authors{author_key}"
                                    else:
                                        author_key = f"/authors/{author_key}"
                                
                                works = ol_client.get_author_works(author_key, limit=50)
                                for work in works:
                                    work_title = work.get('title', '').lower().strip()
                                    if work_title in your_titles:
                                        match_score += 10  # Strong match - found a book you've read in their Open Library works
                            except Exception as e:
                                # If we can't fetch works, that's okay
                                pass
                        
                        # Check catalog books for title matches with books you've read
                        for catalog_book in catalog_books:
                            catalog_title = catalog_book.title.lower().strip() if catalog_book.title else ''
                            if catalog_title in your_titles:
                                match_score += 5  # Medium match - catalog book matches a book you've read
                        
                        if match_score > best_match_score:
                            best_match_score = match_score
                            best_match = author
                    
                    if not best_match:
                        # Can't determine best match - keep all as is
                        print(f"  ⚠ Could not determine best match for {normalized_name}")
                        continue
                    
                    print(f"  ✓ Best match for {normalized_name}: {best_match.name} (ID: {best_match.id}, score: {best_match_score})")
                    
                    # Reassign catalog books from other authors to the best match
                    for author in authors:
                        if author.id == best_match.id:
                            continue  # Skip the best match itself
                        
                        catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
                        for catalog_book in catalog_books:
                            # Check if this book already exists for the best match author (by title, case-insensitive)
                            title_lower = catalog_book.title.lower().strip() if catalog_book.title else ''
                            existing = db_session.query(AuthorCatalogBook).filter_by(
                                author_id=best_match.id
                            ).filter(func.lower(AuthorCatalogBook.title) == title_lower).first()
                            
                            if existing:
                                # Book already exists for correct author - delete duplicate
                                db_session.delete(catalog_book)
                                catalog_books_reassigned += 1
                            else:
                                # Reassign to correct author
                                catalog_book.author_id = best_match.id
                                catalog_books_reassigned += 1
                        
                        # If this author has no more catalog books, mark for removal
                        remaining_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).count()
                        if remaining_books == 0:
                            authors_to_remove.append(author)
    
    # Remove authors with no catalog books (with retry)
    for author in authors_to_remove:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"  Removing author {author.name} (ID: {author.id}) - no catalog books")
                db_session.delete(author)
                db_session.flush()
                break  # Success
            except Exception as e:
                error_msg = str(e).lower()
                if 'locked' in error_msg and attempt < max_retries - 1:
                    import time
                    wait_time = (attempt + 1) * 1
                    time.sleep(wait_time)
                    db_session.rollback()
                    continue
                else:
                    print(f"  ⚠ Failed to remove author {author.name}: {e}")
                    break
    
    # Final commit with retry
    max_retries = 5
    for attempt in range(max_retries):
        try:
            db_session.commit()
            break  # Success
        except Exception as e:
            error_msg = str(e).lower()
            if 'locked' in error_msg and attempt < max_retries - 1:
                import time
                wait_time = (attempt + 1) * 2
                print(f"  Database locked during final commit, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"  ⚠ Warning: Could not commit changes: {e}")
                print(f"  Some changes may not have been saved. You may need to run the command again.")
                db_session.rollback()
                break
    
    print(f"\n✓ Author mismatch fix complete!")
    print(f"  Authors with mixed catalogs fixed: {len(authors_to_fix)}")
    print(f"  Duplicate author groups processed: {len(duplicate_groups)}")
    print(f"  New authors created: {authors_created}")
    print(f"  Catalog books reassigned: {catalog_books_reassigned}")
    print(f"  Authors removed: {len(authors_to_remove)}")
    
    return {
        'mixed_catalog_authors': len(authors_to_fix),
        'duplicate_groups': len(duplicate_groups),
        'authors_created': authors_created,
        'catalog_books_reassigned': catalog_books_reassigned,
        'authors_removed': len(authors_to_remove)
    }


def verify_author_fix(db_session: Session, author_name: str = None) -> Dict:
    """
    Verify that author fix worked correctly by checking an author's catalog books.
    
    Args:
        db_session: Database session
        author_name: Author name to check (if None, shows summary)
    
    Returns:
        Dict with verification results
    """
    from .models import Author, AuthorCatalogBook
    from collections import defaultdict
    
    if author_name:
        # Check specific author
        author = db_session.query(Author).filter_by(name=author_name).first()
        if not author:
            # Try normalized name
            from .ingest import normalize_author_name
            normalized = normalize_author_name(author_name)
            author = db_session.query(Author).filter_by(normalized_name=normalized).first()
        
        if not author:
            print(f"Author '{author_name}' not found")
            return {'found': False}
        
        print(f"\nAuthor: {author.name} (ID: {author.id})")
        print(f"  Normalized name: {author.normalized_name}")
        print(f"  Open Library ID: {author.open_library_id}")
        
        catalog_books = db_session.query(AuthorCatalogBook).filter_by(author_id=author.id).all()
        print(f"  Catalog books: {len(catalog_books)}")
        
        # Check for different Open Library authors in catalog
        ol_authors = defaultdict(list)
        for book in catalog_books:
            if book.open_library_key:
                try:
                    from .api.openlibrary import OpenLibraryClient
                    ol_client = OpenLibraryClient()
                    work_details = ol_client.get_work_details(book.open_library_key)
                    if work_details:
                        authors_list = work_details.get('authors', [])
                        for auth in authors_list:
                            author_key = None
                            if isinstance(auth, dict):
                                if 'author' in auth and isinstance(auth['author'], dict):
                                    author_key = auth['author'].get('key', '')
                                elif 'key' in auth:
                                    author_key = auth.get('key', '')
                            if author_key:
                                if not author_key.startswith('/authors/'):
                                    if author_key.startswith('/'):
                                        author_key = f"/authors{author_key}"
                                    else:
                                        author_key = f"/authors/{author_key}"
                                ol_authors[author_key].append(book.title)
                                break
                except:
                    pass
        
        if len(ol_authors) > 1:
            print(f"  ⚠ WARNING: Found {len(ol_authors)} different Open Library authors in catalog!")
            for ol_key, titles in ol_authors.items():
                print(f"    {ol_key}: {len(titles)} books")
        else:
            print(f"  ✓ All catalog books from same Open Library author")
        
        return {
            'found': True,
            'author_id': author.id,
            'catalog_books': len(catalog_books),
            'ol_authors': len(ol_authors)
        }
    else:
        # Show summary
        all_authors = db_session.query(Author).all()
        print(f"\nTotal authors: {len(all_authors)}")
        
        authors_with_catalog = db_session.query(Author).join(AuthorCatalogBook).distinct().count()
        print(f"Authors with catalog books: {authors_with_catalog}")
        
        return {
            'total_authors': len(all_authors),
            'authors_with_catalog': authors_with_catalog
        }


def remove_duplicate_titles(db_session: Session, dry_run: bool = True, author_limit: int = None, author_offset: int = 0,
                           catalog_book_ids: Optional[List[int]] = None) -> Dict:
    """
    Remove duplicate titles within the same author.
    
    For AuthorCatalogBook: Keep the one with most complete data (has ISBN, description, etc.)
    For Book: Keep the first one found (they should be identical from Libby import)
    
    Args:
        db_session: Database session
        dry_run: If True, only report duplicates without removing them
        author_limit: Maximum number of authors to process (None for all)
        author_offset: Number of authors to skip before starting (for batch processing)
        catalog_book_ids: If provided, only consider these catalog book IDs (e.g. recent books only)
    
    Returns:
        Dict with stats about duplicates found/removed
    """
    from collections import defaultdict
    from sqlalchemy import func, distinct
    
    print("Checking for duplicate titles...")
    
    # Get list of authors to process (for chunking) or restrict to catalog_book_ids
    authors_to_process = None
    if catalog_book_ids:
        authors_to_process = set(
            row[0] for row in db_session.query(AuthorCatalogBook.author_id).filter(
                AuthorCatalogBook.id.in_(catalog_book_ids)
            ).distinct().all()
        )
        print(f"  Processing {len(catalog_book_ids)} catalog books (scoped to given IDs)...")
    elif author_limit or author_offset:
        # Get unique author IDs for catalog books
        author_ids = db_session.query(distinct(AuthorCatalogBook.author_id)).order_by(AuthorCatalogBook.author_id).all()
        author_ids = [aid[0] for aid in author_ids]
        total_authors = len(author_ids)
        
        if author_offset:
            author_ids = author_ids[author_offset:]
        if author_limit:
            author_ids = author_ids[:author_limit]
        
        authors_to_process = set(author_ids)
        range_str = f"authors {author_offset + 1}-{author_offset + len(authors_to_process)}" if author_limit else f"authors starting from {author_offset + 1}"
        print(f"  Processing {len(authors_to_process)} {range_str} of {total_authors} total authors...")
    
    # Check AuthorCatalogBook duplicates
    print("\n  Checking AuthorCatalogBook table...")
    catalog_duplicates = defaultdict(list)
    
    # Get catalog books (filtered by author if chunking, or by catalog_book_ids)
    query = db_session.query(AuthorCatalogBook)
    if catalog_book_ids:
        query = query.filter(AuthorCatalogBook.id.in_(catalog_book_ids))
    elif authors_to_process:
        query = query.filter(AuthorCatalogBook.author_id.in_(authors_to_process))
    all_catalog_books = query.all()
    
    # Normalize titles for duplicate detection (remove split edition markers, etc.)
    def normalize_title_for_dedup(title):
        """Normalize title for duplicate detection, removing split edition markers"""
        if not title:
            return ''
        import re
        # Remove split edition markers like [1/2], [1/4], [2/2], etc.
        title = re.sub(r'\s*\[\d+/\d+\]\s*', ' ', title)
        # Remove common edition markers that don't affect content
        title = re.sub(r'\s*\([^)]*(?:edition|version|translation)[^)]*\)', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\s*\[[^\]]*(?:edition|version|translation)[^\]]*\]', '', title, flags=re.IGNORECASE)
        # Normalize whitespace and case
        return ' '.join(title.lower().split()).strip()
    
    for book in all_catalog_books:
        title_key = normalize_title_for_dedup(book.title)
        if title_key:
            catalog_duplicates[(book.author_id, title_key)].append(book)
    
    catalog_dups_found = {k: v for k, v in catalog_duplicates.items() if len(v) > 1}
    catalog_books_to_remove = []
    
    for (author_id, title_lower), books in catalog_dups_found.items():
        # Score each book by completeness (more complete = keep)
        def score_book(book):
            score = 0
            if book.isbn:
                score += 10
            if book.description:
                score += 5
            if book.open_library_key:
                score += 3
            if book.google_books_id:
                score += 2
            if book.publication_date:
                score += 1
            return score
        
        # Sort by score (highest first), then by ID (keep oldest)
        books_sorted = sorted(books, key=lambda b: (-score_book(b), b.id))
        keep_book = books_sorted[0]
        remove_books = books_sorted[1:]
        
        catalog_books_to_remove.extend(remove_books)
        if not dry_run:
            print(f"    Author ID {author_id}, '{books[0].title}': Keeping ID {keep_book.id}, removing {len(remove_books)} duplicate(s)")
        else:
            print(f"    Author ID {author_id}, '{books[0].title}': Would keep ID {keep_book.id}, would remove {len(remove_books)} duplicate(s)")
    
    # Check Book table duplicates
    print("\n  Checking Book table...")
    book_duplicates = defaultdict(list)
    
    # Get unique author names from catalog books we're processing (to filter Book table)
    author_names_to_process = None
    if authors_to_process:
        # Get author names for the authors we're processing
        authors = db_session.query(Author).filter(Author.id.in_(authors_to_process)).all()
        author_names_to_process = {author.name.lower().strip() for author in authors if author.name}
    
    # Get all books (we'll filter by author name in Python for chunking)
    all_books = db_session.query(Book).all()
    if author_names_to_process:
        # Filter to only books by authors we're processing
        all_books = [book for book in all_books if book.author and book.author.lower().strip() in author_names_to_process]
    
    for book in all_books:
        title_key = book.title.lower().strip() if book.title else ''
        author_key = book.author.lower().strip() if book.author else ''
        if title_key and author_key:
            book_duplicates[(author_key, title_key)].append(book)
    
    book_dups_found = {k: v for k, v in book_duplicates.items() if len(v) > 1}
    books_to_remove = []
    
    for (author, title_lower), books in book_dups_found.items():
        # Keep the first one (by ID), remove the rest
        books_sorted = sorted(books, key=lambda b: b.id)
        keep_book = books_sorted[0]
        remove_books = books_sorted[1:]
        
        books_to_remove.extend(remove_books)
        if not dry_run:
            print(f"    Author '{author}', '{books[0].title}': Keeping ID {keep_book.id}, removing {len(remove_books)} duplicate(s)")
        else:
            print(f"    Author '{author}', '{books[0].title}': Would keep ID {keep_book.id}, would remove {len(remove_books)} duplicate(s)")
    
    # Remove duplicates if not dry run
    if not dry_run:
        print(f"\n  Removing {len(catalog_books_to_remove)} duplicate catalog books...")
        for book in catalog_books_to_remove:
            db_session.delete(book)
        
        print(f"  Removing {len(books_to_remove)} duplicate books...")
        for book in books_to_remove:
            db_session.delete(book)
        
        try:
            db_session.commit()
            print(f"\n✓ Removed {len(catalog_books_to_remove)} duplicate catalog books and {len(books_to_remove)} duplicate books")
            return {
                'catalog_duplicates_found': len(catalog_dups_found),
                'catalog_duplicates_removed': len(catalog_books_to_remove),
                'book_duplicates_found': len(book_dups_found),
                'book_duplicates_removed': len(books_to_remove)
            }
        except Exception as e:
            print(f"\n⚠ Error committing changes: {e}")
            db_session.rollback()
            return {
                'catalog_duplicates_found': len(catalog_dups_found),
                'catalog_duplicates_removed': 0,
                'book_duplicates_found': len(book_dups_found),
                'book_duplicates_removed': 0,
                'error': str(e)
            }
    else:
        print(f"\n  (Dry run - no changes made)")
        return {
            'catalog_duplicates_found': len(catalog_dups_found),
            'catalog_duplicates_removed': 0,
            'book_duplicates_found': len(book_dups_found),
            'book_duplicates_removed': 0
        }


def merge_authors(db_session: Session, author1_name: str = None, author2_name: str = None,
                  author1_id: int = None, author2_id: int = None,
                  keep_author: str = None, dry_run: bool = False) -> Dict:
    """
    Merge two authors into one, consolidating all catalog books and books read.
    
    Args:
        db_session: Database session
        author1_name: First author name to merge (if not using ID)
        author2_name: Second author name to merge (if not using ID)
        author1_id: First author ID to merge (if not using name)
        author2_id: Second author ID to merge (if not using name)
        keep_author: Which author name to keep ('author1', 'author2', or None for auto-select)
        dry_run: If True, only show what would be merged without making changes
    
    Returns:
        Dict with merge results
    """
    from .models import Author, AuthorCatalogBook, Book
    from .ingest import normalize_author_name
    from sqlalchemy import func
    
    # Find both authors - by ID or by name
    author1 = None
    author2 = None
    
    if author1_id:
        author1 = db_session.query(Author).filter_by(id=author1_id).first()
        if not author1:
            return {'error': f'Author with ID {author1_id} not found', 'success': False}
    elif author1_name:
        # Try exact name match first
        author1 = db_session.query(Author).filter_by(name=author1_name).first()
        if not author1:
            # Try case-insensitive name match
            author1 = db_session.query(Author).filter(func.lower(Author.name) == author1_name.lower()).first()
        if not author1:
            # Try normalized name
            normalized1 = normalize_author_name(author1_name)
            author1 = db_session.query(Author).filter_by(normalized_name=normalized1).first()
        if not author1:
            # Try partial match (contains)
            author1 = db_session.query(Author).filter(Author.name.ilike(f'%{author1_name}%')).first()
    else:
        return {'error': 'Must provide either author1_name or author1_id', 'success': False}
    
    if author2_id:
        author2 = db_session.query(Author).filter_by(id=author2_id).first()
        if not author2:
            return {'error': f'Author with ID {author2_id} not found', 'success': False}
    elif author2_name:
        # Same for author2
        author2 = db_session.query(Author).filter_by(name=author2_name).first()
        if not author2:
            author2 = db_session.query(Author).filter(func.lower(Author.name) == author2_name.lower()).first()
        if not author2:
            normalized2 = normalize_author_name(author2_name)
            author2 = db_session.query(Author).filter_by(normalized_name=normalized2).first()
        if not author2:
            author2 = db_session.query(Author).filter(Author.name.ilike(f'%{author2_name}%')).first()
    else:
        return {'error': 'Must provide either author2_name or author2_id', 'success': False}
    
    if not author1:
        return {'error': f'Author "{author1_name}" not found', 'success': False}
    if not author2:
        return {'error': f'Author "{author2_name}" not found', 'success': False}
    
    if author1.id == author2.id:
        return {'error': 'Both authors are the same', 'success': False}
    
    # Get catalog book counts
    count1 = db_session.query(AuthorCatalogBook).filter_by(author_id=author1.id).count()
    count2 = db_session.query(AuthorCatalogBook).filter_by(author_id=author2.id).count()
    
    # Get books read counts
    books_read1 = db_session.query(Book).filter_by(author=author1.normalized_name).count()
    books_read2 = db_session.query(Book).filter_by(author=author2.normalized_name).count()
    
    # Determine which author to keep
    if keep_author == 'author1':
        keep_author_obj = author1
        remove_author_obj = author2
    elif keep_author == 'author2':
        keep_author_obj = author2
        remove_author_obj = author1
    else:
        # Auto-select: keep the one with more catalog books, or better Open Library ID
        if count1 > count2:
            keep_author_obj = author1
            remove_author_obj = author2
        elif count2 > count1:
            keep_author_obj = author2
            remove_author_obj = author1
        else:
            # Same count - prefer the one with Open Library ID
            if author1.open_library_id and not author2.open_library_id:
                keep_author_obj = author1
                remove_author_obj = author2
            elif author2.open_library_id and not author1.open_library_id:
                keep_author_obj = author2
                remove_author_obj = author1
            else:
                # Default to first one
                keep_author_obj = author1
                remove_author_obj = author2
    
    print(f"\nMerging authors:")
    print(f"  Author 1: {author1.name} (ID: {author1.id}, {count1} catalog books, {books_read1} books read)")
    print(f"  Author 2: {author2.name} (ID: {author2.id}, {count2} catalog books, {books_read2} books read)")
    print(f"\n  Keeping: {keep_author_obj.name} (ID: {keep_author_obj.id})")
    print(f"  Removing: {remove_author_obj.name} (ID: {remove_author_obj.id})")
    
    if dry_run:
        print(f"\n  (DRY RUN - no changes made)")
        return {
            'success': True,
            'dry_run': True,
            'keep_author': keep_author_obj.name,
            'remove_author': remove_author_obj.name,
            'catalog_books_to_move': count2 if keep_author_obj == author1 else count1,
            'books_read_to_move': books_read2 if keep_author_obj == author1 else books_read1
        }
    
    # Get all catalog books from the author to remove
    books_to_move = db_session.query(AuthorCatalogBook).filter_by(
        author_id=remove_author_obj.id
    ).all()
    
    moved_count = 0
    duplicate_count = 0
    
    for book in books_to_move:
        # Check if this book already exists for the keep author (by title, case-insensitive)
        title_lower = book.title.lower().strip() if book.title else ''
        existing = db_session.query(AuthorCatalogBook).filter_by(
            author_id=keep_author_obj.id
        ).filter(func.lower(AuthorCatalogBook.title) == title_lower).first()
        
        if existing:
            # Duplicate - delete the one from remove_author
            db_session.delete(book)
            duplicate_count += 1
        else:
            # Move to keep_author
            book.author_id = keep_author_obj.id
            moved_count += 1
    
    # Merge books read (Book table) - update author name and de-dupe
    books_to_merge = db_session.query(Book).filter_by(
        author=remove_author_obj.normalized_name
    ).all()
    
    # Get existing books for keep_author to check for duplicates
    existing_books = db_session.query(Book).filter_by(
        author=keep_author_obj.normalized_name
    ).all()
    existing_titles = {b.title.lower().strip() for b in existing_books if b.title}
    existing_isbns = {b.isbn for b in existing_books if b.isbn}
    
    updated_books_count = 0
    duplicate_books_count = 0
    
    for book in books_to_merge:
        # Check for duplicates by title (case-insensitive) or ISBN
        is_duplicate = False
        title_lower = book.title.lower().strip() if book.title else ''
        
        if title_lower and title_lower in existing_titles:
            is_duplicate = True
        elif book.isbn and book.isbn in existing_isbns:
            is_duplicate = True
        
        if is_duplicate:
            # Delete duplicate
            db_session.delete(book)
            duplicate_books_count += 1
        else:
            # Update author name to keep_author
            book.author = keep_author_obj.normalized_name
            updated_books_count += 1
            # Add to existing sets to prevent duplicates within the merge list
            if title_lower:
                existing_titles.add(title_lower)
            if book.isbn:
                existing_isbns.add(book.isbn)
    
    # Update keep_author's Open Library ID if remove_author has one and keep_author doesn't
    if not keep_author_obj.open_library_id and remove_author_obj.open_library_id:
        keep_author_obj.open_library_id = remove_author_obj.open_library_id
        print(f"  Updated Open Library ID: {keep_author_obj.open_library_id}")
    
    # Delete the remove_author
    db_session.delete(remove_author_obj)
    
    try:
        db_session.commit()
        # Get final counts
        final_catalog_count = db_session.query(AuthorCatalogBook).filter_by(author_id=keep_author_obj.id).count()
        final_books_read_count = db_session.query(Book).filter_by(author=keep_author_obj.normalized_name).count()
        
        print(f"\n✓ Merge complete!")
        print(f"  Catalog books moved: {moved_count}")
        print(f"  Duplicate catalog books removed: {duplicate_count}")
        print(f"  Final catalog books: {final_catalog_count}")
        print(f"  Books read moved: {updated_books_count}")
        print(f"  Duplicate books read removed: {duplicate_books_count}")
        print(f"  Final books read: {final_books_read_count}")
        print(f"  Removed author: {remove_author_obj.name}")
        
        return {
            'success': True,
            'keep_author': keep_author_obj.name,
            'remove_author': remove_author_obj.name,
            'catalog_books_moved': moved_count,
            'duplicate_catalog_books_removed': duplicate_count,
            'final_catalog_books': final_catalog_count,
            'books_read_moved': updated_books_count,
            'duplicate_books_read_removed': duplicate_books_count,
            'final_books_read': final_books_read_count
        }
    except Exception as e:
        db_session.rollback()
        print(f"\n✗ Error merging authors: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def extract_first_last_name(name: str) -> tuple:
    """
    Extract first and last name from author name, ignoring middle initials and punctuation.
    
    Examples:
    - "L. M. (Lucy Maud) Montgomery" -> ("L", "Montgomery")
    - "Julia R. Kelly" -> ("Julia", "Kelly")
    - "Julia Kelly" -> ("Julia", "Kelly")
    
    Returns:
        (first_name, last_name) tuple, both lowercased
    """
    if not name:
        return ("", "")
    
    import re
    # Remove content in parentheses (like "(Lucy Maud)")
    name = re.sub(r'\([^)]*\)', '', name)
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Split into parts
    parts = name.split()
    if not parts:
        return ("", "")
    
    # First name is first part (remove punctuation)
    first = re.sub(r'[^\w]', '', parts[0]).lower()
    
    # Last name is last part (remove punctuation)
    last = re.sub(r'[^\w]', '', parts[-1]).lower() if len(parts) > 1 else ""
    
    return (first, last)


def detect_duplicate_authors(db_session: Session, min_overlapping_books: int = 1) -> List[Dict]:
    """
    Detect potential duplicate authors based on name similarity and overlapping books.
    
    Args:
        db_session: Database session
        min_overlapping_books: Minimum number of overlapping book titles to consider as duplicate (default: 1)
    
    Returns:
        List of dicts with potential duplicate author pairs
    """
    from .models import Author, AuthorCatalogBook
    from sqlalchemy import func
    
    all_authors = db_session.query(Author).all()
    potential_duplicates = []
    
    # Build a map of (first, last) -> list of authors
    author_map = {}
    for author in all_authors:
        first, last = extract_first_last_name(author.name)
        if first and last:
            key = (first, last)
            if key not in author_map:
                author_map[key] = []
            author_map[key].append(author)
    
    # Check each group for potential duplicates
    for (first, last), authors in author_map.items():
        if len(authors) < 2:
            continue  # Need at least 2 authors with same first+last name
        
        # Check all pairs in this group
        for i in range(len(authors)):
            for j in range(i + 1, len(authors)):
                author1 = authors[i]
                author2 = authors[j]
                
                # Skip if they have the same Open Library ID (already handled)
                if author1.open_library_id and author2.open_library_id:
                    if author1.open_library_id == author2.open_library_id:
                        # Same Open Library ID - definitely duplicates
                        # Count overlapping books
                        books1 = {b.title.lower().strip() for b in 
                                 db_session.query(AuthorCatalogBook).filter_by(author_id=author1.id).all() 
                                 if b.title}
                        books2 = {b.title.lower().strip() for b in 
                                 db_session.query(AuthorCatalogBook).filter_by(author_id=author2.id).all() 
                                 if b.title}
                        overlapping = books1 & books2
                        
                        if len(overlapping) >= min_overlapping_books:
                            potential_duplicates.append({
                                'author1': author1,
                                'author2': author2,
                                'overlapping_books': len(overlapping),
                                'overlapping_titles': list(overlapping)[:5],  # Show first 5
                                'reason': 'same_open_library_id',
                                'confidence': 'high'
                            })
                        continue
                
                # Check for overlapping book titles (only if not already flagged by Open Library ID)
                books1 = {b.title.lower().strip() for b in 
                         db_session.query(AuthorCatalogBook).filter_by(author_id=author1.id).all() 
                         if b.title}
                books2 = {b.title.lower().strip() for b in 
                         db_session.query(AuthorCatalogBook).filter_by(author_id=author2.id).all() 
                         if b.title}
                overlapping = books1 & books2
                
                # If they have overlapping books and same first+last name (already grouped), they're likely duplicates
                if len(overlapping) >= min_overlapping_books:
                    # Determine confidence based on overlap count
                    confidence = 'high' if len(overlapping) >= 3 else 'medium'
                    # If one has Open Library ID and other doesn't, but they match, still flag it
                    if author1.open_library_id or author2.open_library_id:
                        confidence = 'high'  # Having Open Library ID increases confidence
                    
                    potential_duplicates.append({
                        'author1': author1,
                        'author2': author2,
                        'overlapping_books': len(overlapping),
                        'overlapping_titles': list(overlapping)[:5],  # Show first 5
                        'reason': 'name_match_with_overlapping_books',
                        'confidence': confidence
                    })
    
    # Sort by confidence and overlapping books
    potential_duplicates.sort(key=lambda x: (
        0 if x['confidence'] == 'high' else 1,
        -x['overlapping_books']
    ))
    
    return potential_duplicates
