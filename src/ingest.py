"""Ingest Libby CSV export into database"""
import csv
import re
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from .models import Book, Author, SystemMetadata, Recommendation, get_session


# Publishers that indicate audiobooks
AUDIOBOOK_PUBLISHERS = {
    'books on tape',
    'tantor media',
    'simon & schuster audio',
    'hachette audio',
    'penguin audio',
    'harperaudio',
    'random house audio',
    'macmillan audio',
    'recorded books',
    'blackstone audio',
    'audible',
}


def normalize_author_name(name):
    """Normalize author name for matching"""
    # Remove extra whitespace
    name = ' '.join(name.split())
    # Handle multiple authors (take first)
    if ',' in name:
        name = name.split(',')[0]
    # Remove common suffixes
    name = re.sub(r'\s+et\s+al\.?$', '', name, flags=re.IGNORECASE)
    return name.strip()


def normalize_title_for_matching(title: str) -> str:
    """
    Normalize title for matching between Libby books and recommendations.
    Removes series info, "The" prefix, and normalizes formatting.
    """
    if not title:
        return ''
    
    # Remove series info in parentheses: "Sea Before Us (Sunrise at Normandy Book #1)" -> "Sea Before Us"
    # Pattern matches: (Series Name), (Series Name #1), (Book #1), etc.
    title = re.sub(r'\s*\([^)]*(?:series|book\s*#?\s*\d+|#\s*\d+)[^)]*\)', '', title, flags=re.IGNORECASE)
    
    # Remove edition markers (including "ed." and "edition")
    title = re.sub(r'\s*\([^)]*(?:edition|version|translation|ed\.)[^)]*\)', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*\[[^\]]*(?:edition|version|translation|ed\.)[^\]]*\]', '', title, flags=re.IGNORECASE)
    
    # Remove common prefixes that don't affect content matching
    # Remove "The", "A", "An" at the start
    title = re.sub(r'^(the|a|an)\s+', '', title, flags=re.IGNORECASE)
    
    # Normalize apostrophes and possessives
    # Standardize all apostrophe types: ' ' ` → remove for comparison
    title = re.sub(r"[''`]", '', title)
    
    # Normalize whitespace and case
    title = ' '.join(title.split()).strip().lower()
    
    return title


def detect_format(publisher, title=None):
    """Detect if book is audiobook or ebook based on publisher"""
    if not publisher:
        return 'unknown'
    
    publisher_lower = publisher.lower()
    for audio_pub in AUDIOBOOK_PUBLISHERS:
        if audio_pub in publisher_lower:
            return 'audiobook'
    
    return 'ebook'


def parse_date(date_str):
    """Parse Libby date string to datetime"""
    # Format: "January 12, 2026 02:51"
    try:
        return datetime.strptime(date_str, "%B %d, %Y %H:%M")
    except ValueError:
        try:
            # Try without time
            return datetime.strptime(date_str, "%B %d, %Y")
        except ValueError:
            return None


def ingest_csv(csv_path, db_session: Session, update_existing=False):
    """
    Ingest Libby CSV export into database
    
    Args:
        csv_path: Path to CSV file
        db_session: Database session
        update_existing: If True, update existing records; if False, skip duplicates
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    books_added = 0
    authors_added = set()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Extract data
            title = row.get('title', '').strip()
            author_raw = row.get('author', '').strip()
            isbn = row.get('isbn', '').strip() or None
            publisher = row.get('publisher', '').strip() or None
            timestamp = row.get('timestamp', '').strip()
            cover_url = row.get('cover', '').strip() or None
            library = row.get('library', '').strip() or None
            details = row.get('details', '').strip() or None
            
            if not title or not author_raw:
                continue
            
            # Normalize author
            author = normalize_author_name(author_raw)
            
            # Detect format
            book_format = detect_format(publisher, title)
            
            # Parse date
            borrowed_date = parse_date(timestamp) if timestamp else None
            
            # Check if book already exists (by ISBN or title+author)
            existing_book = None
            if isbn:
                existing_book = db_session.query(Book).filter_by(isbn=isbn).first()
            if not existing_book:
                existing_book = db_session.query(Book).filter_by(
                    title=title,
                    author=author
                ).first()
            
            if existing_book and not update_existing:
                continue
            
            # Create or update book
            if existing_book and update_existing:
                book = existing_book
                book.publisher = publisher
                book.isbn = isbn or book.isbn
                book.format = book_format
                book.cover_url = cover_url or book.cover_url
                book.library = library or book.library
                book.borrowed_date = borrowed_date or book.borrowed_date
                book.loan_duration = details or book.loan_duration
            else:
                book = Book(
                    title=title,
                    author=author,
                    publisher=publisher,
                    isbn=isbn,
                    format=book_format,
                    cover_url=cover_url,
                    library=library,
                    borrowed_date=borrowed_date,
                    loan_duration=details
                )
                db_session.add(book)
                books_added += 1
            
            # Track authors
            if author not in authors_added:
                # Check if author exists
                existing_author = db_session.query(Author).filter_by(
                    normalized_name=author
                ).first()
                
                if not existing_author:
                    author_obj = Author(
                        name=author_raw,  # Keep original for display
                        normalized_name=author
                    )
                    db_session.add(author_obj)
                    authors_added.add(author)
    
    # Update system metadata: last Libby import date
    metadata = db_session.query(SystemMetadata).filter_by(key='last_libby_import').first()
    if metadata:
        metadata.value = datetime.utcnow().isoformat()
        metadata.updated_at = datetime.utcnow()
    else:
        metadata = SystemMetadata(
            key='last_libby_import',
            value=datetime.utcnow().isoformat()
        )
        db_session.add(metadata)
    
    # Mark recommendations as "already_read" if they now appear in Libby
    # This will filter them out from all views: recommendations, books to read, series analysis
    # Get all books that were just imported/updated
    all_libby_books = db_session.query(Book).all()
    # Book.author is already normalized, so we can use it directly
    # Normalize titles for matching (removes "The" prefix, series info, etc.)
    libby_titles_authors = {
        (normalize_title_for_matching(b.title), b.author.lower().strip()) 
        for b in all_libby_books if b.title and b.author
    }
    
    # Find ALL recommendations (not just thumbs_up) that match Libby books
    # We want to mark them as already_read so they're filtered from all views
    all_recs = db_session.query(Recommendation).all()
    marked_as_read_count = 0
    removed_from_books_to_read_count = 0
    
    for rec in all_recs:
        if not rec.title or not rec.author:
            continue
        # Normalize the recommendation author name to match Book.author format
        rec_author_normalized = normalize_author_name(rec.author)
        rec_author_lower = rec_author_normalized.lower().strip()
        # Normalize the recommendation title (removes series info, "The" prefix, etc.)
        rec_title_normalized = normalize_title_for_matching(rec.title)
        
        if (rec_title_normalized, rec_author_lower) in libby_titles_authors:
            # Mark as already_read (this filters from all recommendation views)
            if not rec.already_read:
                rec.already_read = True
                marked_as_read_count += 1
            
            # Also remove thumbs_up flag if it was set (removes from Books to Read)
            if rec.thumbs_up:
                rec.thumbs_up = False
                removed_from_books_to_read_count += 1
    
    db_session.commit()
    
    return {
        'books_added': books_added,
        'authors_added': len(authors_added),
        'total_books': db_session.query(Book).count(),
        'total_authors': db_session.query(Author).count(),
        'marked_as_already_read': marked_as_read_count,
        'removed_from_books_to_read': removed_from_books_to_read_count
    }


if __name__ == '__main__':
    import sys
    from .models import init_db
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.ingest <path-to-libby-export.csv>", file=sys.stderr)
        print("Or use: python scripts/bookpilot.py ingest <path-to-libby-export.csv>", file=sys.stderr)
        sys.exit(1)
    
    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    
    db_path = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = init_db(str(db_path))
    session = get_session(engine)
    
    result = ingest_csv(csv_path, session)
    print(f"Ingestion complete: {result}")
