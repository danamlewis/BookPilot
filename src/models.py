"""Database models for BookPilot"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Float, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

Base = declarative_base()


class Book(Base):
    """Books from Libby export"""
    __tablename__ = 'books'
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    author = Column(String, nullable=False)  # Normalized author name
    publisher = Column(String)
    isbn = Column(String)
    format = Column(String)  # 'audiobook' or 'ebook'
    cover_url = Column(String)
    library = Column(String)
    borrowed_date = Column(DateTime)
    loan_duration = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Future: flag if already read before Libby history
    already_read = Column(Boolean, default=False)
    
    # Relationships
    recommendations = relationship("Recommendation", back_populates="book")


class Author(Base):
    """Authors from reading history"""
    __tablename__ = 'authors'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    normalized_name = Column(String, nullable=False)  # For matching
    open_library_id = Column(String)  # Open Library author ID
    last_catalog_check = Column(DateTime)  # When we last fetched their catalog
    hidden = Column(Boolean, default=False)  # Whether author is hidden from recommendations
    hidden_at = Column(DateTime, nullable=True)  # When author was hidden
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    catalog_books = relationship("AuthorCatalogBook", back_populates="author")


class AuthorCatalogBook(Base):
    """Books in author's catalog (from API)"""
    __tablename__ = 'author_catalog_books'
    
    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.id'), nullable=False)
    title = Column(String, nullable=False)
    isbn = Column(String)
    publication_date = Column(String)
    series_name = Column(String)  # Series this book belongs to
    series_position = Column(Integer)  # Position in series (1, 2, 3...)
    format_available = Column(String)  # 'audiobook', 'ebook', 'both', 'unknown'
    open_library_key = Column(String)
    google_books_id = Column(String)
    description = Column(Text)
    categories = Column(String)  # Comma-separated genres
    fetched_at = Column(DateTime, default=datetime.utcnow)
    
    # Match to your reading history
    is_read = Column(Boolean, default=False)  # Matched to your Libby history
    matched_book_id = Column(Integer, ForeignKey('books.id'), nullable=True)
    
    # Relationships
    author = relationship("Author", back_populates="catalog_books")


class Series(Base):
    """Book series"""
    __tablename__ = 'series'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    author_id = Column(Integer, ForeignKey('authors.id'), nullable=False)
    total_books = Column(Integer)
    books_read = Column(Integer, default=0)
    status = Column(String)  # 'complete', 'partial', 'not_started'
    created_at = Column(DateTime, default=datetime.utcnow)


class Recommendation(Base):
    """Recommendations for books"""
    __tablename__ = 'recommendations'
    
    id = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey('books.id'), nullable=True)  # If from catalog
    catalog_book_id = Column(Integer, ForeignKey('author_catalog_books.id'), nullable=True)
    title = Column(String, nullable=False)
    author = Column(String, nullable=False)
    isbn = Column(String)
    format = Column(String)  # 'audiobook' or 'ebook'
    category = Column(String)  # Genre/category
    recommendation_type = Column(String)  # 'same_author', 'series_continuation', 'similar_content', 'genre_match'
    similarity_score = Column(Float)  # 0.0 to 1.0
    reason = Column(Text)  # Why this was recommended
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # User feedback
    thumbs_up = Column(Boolean, nullable=True)
    thumbs_down = Column(Boolean, nullable=True)
    non_english = Column(Boolean, default=False)  # Flag for non-English books
    language_flag_source = Column(String, nullable=True)  # manual, personalized_high, review_approved
    already_read = Column(Boolean, default=False)  # Flag for books already read
    duplicate = Column(Boolean, default=False)  # Flag for duplicate recommendations
    feedback_date = Column(DateTime, nullable=True)
    
    # Relationships
    book = relationship("Book", back_populates="recommendations")


class SystemMetadata(Base):
    """System-level metadata and tracking"""
    __tablename__ = 'system_metadata'
    
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db(db_path='data/bookpilot.db'):
    """Initialize database"""
    from pathlib import Path
    Path(db_path).resolve().parent.mkdir(parents=True, exist_ok=True)
    # Use check_same_thread=False for Flask's multi-threaded environment
    engine = create_engine(f'sqlite:///{db_path}', connect_args={'check_same_thread': False})
    Base.metadata.create_all(engine)
    # Run migrations to add any missing columns
    migrate_database(engine)
    return engine


def get_session(engine):
    """Get database session"""
    Session = sessionmaker(bind=engine)
    return Session()


def migrate_database(engine):
    """Add missing columns to existing database tables"""
    from sqlalchemy import inspect
    import sqlite3
    from pathlib import Path
    
    try:
        inspector = inspect(engine)
        
        # Get database path from engine URL (needed for both recommendations and authors migrations)
        db_url = str(engine.url)
        if db_url.startswith('sqlite:///'):
            db_path = db_url.replace('sqlite:///', '')
        else:
            # Fallback: try to get from engine
            db_path = db_url.split('///')[-1] if '///' in db_url else db_url.split('//')[-1]
        
        # Ensure absolute path
        if not Path(db_path).is_absolute():
            # Try relative to current directory
            db_path = str(Path(db_path).resolve())
        
        # Check if authors table exists and add hidden column if needed
        if 'authors' in inspector.get_table_names():
            author_columns = [col['name'] for col in inspector.get_columns('authors')]
            if 'hidden' not in author_columns:
                try:
                    conn = sqlite3.connect(db_path, timeout=30.0)
                    cursor = conn.cursor()
                    cursor.execute("ALTER TABLE authors ADD COLUMN hidden BOOLEAN DEFAULT 0")
                    conn.commit()
                    conn.close()
                    print(f"✓ Added hidden column to authors table")
                except sqlite3.OperationalError as e:
                    if 'duplicate column' not in str(e).lower():
                        print(f"  Warning: Could not add hidden column to authors table: {e}")
            if 'hidden_at' not in author_columns:
                try:
                    conn = sqlite3.connect(db_path, timeout=30.0)
                    cursor = conn.cursor()
                    cursor.execute("ALTER TABLE authors ADD COLUMN hidden_at DATETIME")
                    conn.commit()
                    conn.close()
                    print(f"✓ Added hidden_at column to authors table")
                except sqlite3.OperationalError as e:
                    if 'duplicate column' not in str(e).lower():
                        print(f"  Warning: Could not add hidden_at column to authors table: {e}")
        
        # Check if recommendations table exists
        if 'recommendations' in inspector.get_table_names():
            # Check which columns exist
            columns = [col['name'] for col in inspector.get_columns('recommendations')]
            
            # Add missing columns
            columns_to_add = [
                ('non_english', 'BOOLEAN DEFAULT 0'),
                ('language_flag_source', 'VARCHAR'),
                ('already_read', 'BOOLEAN DEFAULT 0'),
                ('duplicate', 'BOOLEAN DEFAULT 0')
            ]
            
            for col_name, col_def in columns_to_add:
                if col_name not in columns:
                    try:
                        # Use direct SQLite connection for migration with retry
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                conn = sqlite3.connect(db_path, timeout=30.0)
                                cursor = conn.cursor()
                                cursor.execute(f"ALTER TABLE recommendations ADD COLUMN {col_name} {col_def}")
                                conn.commit()
                                conn.close()
                                print(f"✓ Added {col_name} column to recommendations table")
                                break
                            except sqlite3.OperationalError as e:
                                error_msg = str(e).lower()
                                if 'duplicate column' in error_msg or 'already exists' in error_msg:
                                    print(f"  {col_name} column already exists")
                                    break
                                elif 'locked' in error_msg and attempt < max_retries - 1:
                                    import time
                                    time.sleep(1)  # Wait 1 second before retry
                                    continue
                                else:
                                    print(f"  Warning: Could not add {col_name} column (attempt {attempt + 1}/{max_retries}): {e}")
                                    if attempt == max_retries - 1:
                                        print("  Database may be locked by another process. Please wait and try again.")
                    except Exception as e:
                        print(f"  Warning: Migration error for {col_name}: {e}")

        # Add the indexes used by the interactive views and catalog refreshes.
        # The recommendation identity index also closes the race where repeated
        # UI handlers could insert the same feedback row concurrently.
        if {'books', 'authors', 'author_catalog_books', 'recommendations'}.issubset(inspector.get_table_names()):
            conn = sqlite3.connect(db_path, timeout=30.0)
            try:
                cursor = conn.cursor()
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_books_author ON books(author)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_books_isbn ON books(isbn)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_authors_normalized_name ON authors(normalized_name)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_catalog_author ON author_catalog_books(author_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_catalog_work ON author_catalog_books(open_library_key)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_catalog_isbn ON author_catalog_books(isbn)")
                cursor.execute("CREATE INDEX IF NOT EXISTS ix_recommendations_format ON recommendations(format)")
                cursor.execute("UPDATE recommendations SET title = trim(title), author = trim(author)")
                cursor.execute("""
                    UPDATE recommendations AS kept
                    SET title = trim(kept.title),
                        author = trim(kept.author),
                        thumbs_up = (SELECT dupe.thumbs_up FROM recommendations dupe
                                     WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                       AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                       AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                       AND (dupe.thumbs_up IS NOT NULL OR dupe.thumbs_down IS NOT NULL)
                                     ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC, dupe.id DESC LIMIT 1),
                        thumbs_down = (SELECT dupe.thumbs_down FROM recommendations dupe
                                       WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                         AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                         AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                         AND (dupe.thumbs_up IS NOT NULL OR dupe.thumbs_down IS NOT NULL)
                                       ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC, dupe.id DESC LIMIT 1),
                        non_english = (SELECT MAX(COALESCE(dupe.non_english, 0)) FROM recommendations dupe
                                       WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                         AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                         AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')),
                        already_read = (SELECT MAX(COALESCE(dupe.already_read, 0)) FROM recommendations dupe
                                        WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                          AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                          AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')),
                        duplicate = (SELECT MAX(COALESCE(dupe.duplicate, 0)) FROM recommendations dupe
                                     WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                       AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                       AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')),
                        language_flag_source = (SELECT dupe.language_flag_source FROM recommendations dupe
                                                WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                                  AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                                  AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                                  AND NULLIF(trim(dupe.language_flag_source), '') IS NOT NULL
                                                ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                                         dupe.id DESC LIMIT 1),
                        book_id = (SELECT dupe.book_id FROM recommendations dupe
                                   WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                     AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                     AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                     AND dupe.book_id IS NOT NULL
                                   ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                            dupe.id DESC LIMIT 1),
                        catalog_book_id = (SELECT dupe.catalog_book_id FROM recommendations dupe
                                           WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                             AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                             AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                             AND dupe.catalog_book_id IS NOT NULL
                                           ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                                    dupe.id DESC LIMIT 1),
                        isbn = (SELECT dupe.isbn FROM recommendations dupe
                                WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                  AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                  AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                  AND NULLIF(trim(dupe.isbn), '') IS NOT NULL
                                ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                         dupe.id DESC LIMIT 1),
                        category = (SELECT dupe.category FROM recommendations dupe
                                    WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                      AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                      AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                      AND NULLIF(trim(dupe.category), '') IS NOT NULL
                                    ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                             dupe.id DESC LIMIT 1),
                        recommendation_type = (SELECT dupe.recommendation_type FROM recommendations dupe
                                               WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                                 AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                                 AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                                 AND NULLIF(trim(dupe.recommendation_type), '') IS NOT NULL
                                               ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                                        dupe.id DESC LIMIT 1),
                        similarity_score = (SELECT dupe.similarity_score FROM recommendations dupe
                                            WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                              AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                              AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                              AND dupe.similarity_score IS NOT NULL
                                            ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                                     dupe.id DESC LIMIT 1),
                        reason = (SELECT dupe.reason FROM recommendations dupe
                                  WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                    AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                    AND COALESCE(dupe.format, '') = COALESCE(kept.format, '')
                                    AND NULLIF(trim(dupe.reason), '') IS NOT NULL
                                  ORDER BY COALESCE(dupe.feedback_date, dupe.created_at) DESC,
                                           dupe.id DESC LIMIT 1),
                        feedback_date = (SELECT MAX(dupe.feedback_date) FROM recommendations dupe
                                         WHERE lower(trim(dupe.title)) = lower(trim(kept.title))
                                           AND lower(trim(dupe.author)) = lower(trim(kept.author))
                                           AND COALESCE(dupe.format, '') = COALESCE(kept.format, ''))
                    WHERE kept.id IN (
                        SELECT MIN(id) FROM recommendations
                        GROUP BY lower(trim(title)), lower(trim(author)), COALESCE(format, '')
                        HAVING COUNT(*) > 1
                    )
                """)
                cursor.execute("""
                    DELETE FROM recommendations
                    WHERE id NOT IN (
                        SELECT MIN(id) FROM recommendations
                        GROUP BY lower(trim(title)), lower(trim(author)), COALESCE(format, '')
                    )
                """)
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_recommendation_identity
                    ON recommendations(lower(trim(title)), lower(trim(author)), COALESCE(format, ''))
                """)
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        # Migration failed, but don't crash - the app can still work
        print(f"Warning: Database migration check failed: {e}")
