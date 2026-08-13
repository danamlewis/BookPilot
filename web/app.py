"""BookPilot Web Interface - MVP"""
import csv
from flask import Flask, render_template, jsonify, request, g
from pathlib import Path
import sys
import tempfile
import threading
import time
from sqlalchemy import create_engine, func, literal_column
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import init_db, get_session, SystemMetadata, Book, Author, Recommendation, migrate_database
from src.ingest import ingest_csv
from src.catalog import fetch_all_author_catalogs
from src.series import analyze_all_series
from src.recommend import recommend_audiobooks, recommend_new_books
from datetime import datetime
from werkzeug.utils import secure_filename
from web.jobs import JobAlreadyRunning, JobManager


app = Flask(__name__)
app.config['SECRET_KEY'] = 'bookpilot-dev'  # Flask expects this; this app does not use Flask sessions or login
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

DB_PATH = Path(__file__).parent.parent / 'data' / 'bookpilot.db'
JOB_MANAGER = JobManager()
_INITIALIZED_DATABASES = set()
_DATABASE_INIT_LOCK = threading.Lock()


def _close_database(session, engine):
    if session is not None:
        session.close()
    if engine is not None:
        engine.dispose()


def _database_has_recommendation_identity_index(engine):
    """Confirm the migration required by atomic recommendation upserts succeeded."""
    try:
        with engine.connect() as connection:
            indexes = connection.exec_driver_sql(
                "PRAGMA index_list('recommendations')"
            ).fetchall()
    except Exception:
        return False
    return any(row[1] == 'ux_recommendation_identity' and bool(row[2]) for row in indexes)


def _request_database():
    """Return one database session for the current HTTP request."""
    if 'db_session' not in g:
        database_key = str(DB_PATH.resolve())
        with _DATABASE_INIT_LOCK:
            if database_key not in _INITIALIZED_DATABASES:
                g.db_engine = init_db(str(DB_PATH))
                # migrate_database intentionally logs rather than raising. Do
                # not cache a silently failed initialization: the next request
                # must be allowed to retry the migration, particularly because
                # feedback upserts depend on its expression-based unique index.
                if _database_has_recommendation_identity_index(g.db_engine):
                    _INITIALIZED_DATABASES.add(database_key)
            else:
                g.db_engine = create_engine(
                    f'sqlite:///{DB_PATH}',
                    connect_args={'check_same_thread': False},
                )
        g.db_session = get_session(g.db_engine)
    return g.db_session


class _RecommendationDatabaseBusy(Exception):
    """Raised after a recommendation write exhausts SQLite lock retries."""


def _recommendation_upsert_statement(
    *,
    title,
    author,
    format_type,
    insert_values,
    update_values,
):
    """Build an upsert matching the recommendation identity expression index."""
    statement = sqlite_insert(Recommendation).values(
        title=title,
        author=author,
        format=format_type,
        **insert_values,
    )
    return statement.on_conflict_do_update(
        index_elements=[
            func.lower(func.trim(Recommendation.title)),
            func.lower(func.trim(Recommendation.author)),
            func.coalesce(Recommendation.format, literal_column("''")),
        ],
        set_=update_values,
    )


def _upsert_recommendation(
    session,
    *,
    title,
    author,
    format_type,
    insert_values,
    update_values,
):
    """Atomically create or update one case-insensitive recommendation identity.

    The expression list deliberately matches ``ux_recommendation_identity`` in
    ``migrate_database``.  Using one SQLite upsert removes the select-then-insert
    race that previously returned a unique-constraint 500 when two identical UI
    actions arrived together.
    """
    now = datetime.utcnow()
    statement = _recommendation_upsert_statement(
        title=title,
        author=author,
        format_type=format_type,
        insert_values={'feedback_date': now, **insert_values},
        update_values={'feedback_date': now, **update_values},
    )

    # Preserve the existing initial attempt plus three exponential-backoff
    # retries for transient SQLite writer locks.
    for attempt in range(4):
        try:
            session.execute(statement)
            session.commit()
            return attempt > 0
        except OperationalError as exc:
            session.rollback()
            if 'locked' not in str(exc).lower():
                raise
            if attempt == 3:
                raise _RecommendationDatabaseBusy from exc
            time.sleep(0.1 * (2 ** attempt))


def _recommendation_action_response(
    format_type,
    *,
    insert_values,
    update_values,
    message,
):
    """Validate and persist a recommendation action shared by UI endpoints."""
    data = request.get_json(silent=True) or {}
    title = str(data.get('title') or '').strip()
    author = str(data.get('author') or '').strip()
    if not title or not author:
        return jsonify({'success': False, 'error': 'Missing title or author'}), 400

    session = _request_database()
    try:
        retried = _upsert_recommendation(
            session,
            title=title,
            author=author,
            format_type=format_type,
            insert_values=insert_values,
            update_values=update_values,
        )
    except _RecommendationDatabaseBusy:
        return jsonify({
            'success': False,
            'error': 'Database is currently locked. Please try again in a moment.',
            'retry': True,
        }), 503
    except Exception as exc:
        session.rollback()
        return jsonify({'success': False, 'error': str(exc)}), 500

    suffix = ' (retried)' if retried else ''
    return jsonify({
        'success': True,
        'message': f'{message.format(title=title, author=author)}{suffix}',
    })


@app.teardown_appcontext
def _close_request_database(_error=None):
    session = g.pop('db_session', None)
    engine = g.pop('db_engine', None)
    _close_database(session, engine)


def _validate_libby_csv(csv_path):
    """Reject empty or unrelated uploads before starting an import job."""
    try:
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            fields = {field.strip() for field in (reader.fieldnames or []) if field}
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ValueError("The selected file could not be read as a CSV.") from exc

    missing = {'title', 'author'} - fields
    if missing:
        raise ValueError("This does not look like a Libby history CSV (title and author columns are required).")


def _compact_recommendations(recommendations):
    """Remove fields the browser does not render from recommendation payloads."""
    for recommendation in recommendations:
        recommendation.pop('description', None)
        if recommendation.get('reason') == recommendation.get('score_reason'):
            recommendation.pop('reason', None)


def _run_import_job(csv_path, original_name, progress):
    engine = None
    session = None
    try:
        progress(message=f"Importing {original_name}…", current=0, total=1)
        engine = init_db(str(DB_PATH))
        session = get_session(engine)
        result = ingest_csv(
            csv_path,
            session,
            update_existing=False,
            progress_callback=progress,
        )
        result.update({
            'kind': 'import',
            'filename': original_name,
            'message': (
                f"Import complete — {result['books_added']} book"
                f"{'s' if result['books_added'] != 1 else ''} and "
                f"{result['authors_added']} author"
                f"{'s' if result['authors_added'] != 1 else ''} added."
            ),
        })
        return result
    finally:
        _close_database(session, engine)
        Path(csv_path).unlink(missing_ok=True)


def _run_catalog_job(*, force_refresh, only_recent, recent_years, progress):
    engine = None
    session = None
    try:
        engine = init_db(str(DB_PATH))
        session = get_session(engine)
        result = fetch_all_author_catalogs(
            session,
            force_refresh=force_refresh,
            only_recent=only_recent,
            recent_years=recent_years,
            # Deterministic catalog cleanup is safe for unattended local jobs;
            # fuzzy candidates and author merging remain review-first tools.
            auto_cleanup=True,
            progress_callback=progress,
        )
        stopped_early = bool(result.get('stopped_early'))
        result.update({
            'kind': 'catalog',
            'warning': stopped_early,
            'message': (
                "Catalog update stopped early. Completed progress was saved."
                if stopped_early else
                f"Catalog update complete — {result['total_books_added']} book"
                f"{'s' if result['total_books_added'] != 1 else ''} added across "
                f"{result['catalogs_fetched']} author"
                f"{'s' if result['catalogs_fetched'] != 1 else ''}."
            ),
        })
        return result
    finally:
        _close_database(session, engine)


def _job_response(job, status_code=202):
    return jsonify({'job': job}), status_code


def _job_conflict_response(exc):
    return jsonify({
        'error': 'A library update is already running.',
        'job': exc.job,
    }), 409


@app.errorhandler(413)
def upload_too_large(_error):
    return jsonify({'error': 'The selected CSV is larger than the 50 MB upload limit.'}), 413


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


def get_status(session=None):
    """Get system status"""
    # Ensure database exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    session = session or _request_database()
    total_books = session.query(Book).count()
    total_authors = session.query(Author).count()
    audiobooks = session.query(Book).filter_by(format='audiobook').count()
    ebooks = session.query(Book).filter_by(format='ebook').count()

    libby_import = session.query(SystemMetadata).filter_by(key='last_libby_import').first()
    catalog_check = session.query(SystemMetadata).filter_by(key='last_catalog_check').first()

    return {
        'total_books': total_books,
        'total_authors': total_authors,
        'audiobooks': audiobooks,
        'ebooks': ebooks,
        'last_libby_import': format_date_delta(libby_import.value if libby_import else None),
        'last_catalog_check': format_date_delta(catalog_check.value if catalog_check else None)
    }


@app.route('/')
def index():
    """Home page"""
    status = get_status(_request_database())
    return render_template('index.html', status=status)


@app.route('/api/series')
def api_series():
    """Get series analysis"""
    session = _request_database()
    format_filter = request.args.get('format', None)
    result = analyze_all_series(session, format_filter=format_filter)
    return jsonify(result)


@app.route('/api/recommendations/audiobook')
def api_recommendations_audiobook():
    """Get audiobook recommendations, organized by Fiction/Non-Fiction"""
    session = None
    try:
        session = _request_database()
        
        all_recommendations = recommend_audiobooks(session)
        _compact_recommendations(all_recommendations)
        recommendations = list(all_recommendations)
        
        # Filter out thumbs down, already_read, non_english, and duplicate recommendations
        # Use case-insensitive matching to match how feedback is saved
        filtered_recs = session.query(Recommendation).filter(
            (Recommendation.format == 'audiobook') &
            (
                (Recommendation.thumbs_down == True) |
                (Recommendation.already_read == True) |
                (Recommendation.non_english == True) |
                (Recommendation.duplicate == True)
            )
        ).all()
        # Create normalized set for case-insensitive matching
        filtered_set = {(rec.title.lower().strip() if rec.title else '', rec.author.lower().strip() if rec.author else '') for rec in filtered_recs}
        recommendations = [r for r in recommendations if (r.get('title', '').lower().strip(), r.get('author', '').lower().strip()) not in filtered_set]
        
        # Filter out hidden authors
        from sqlalchemy import func
        hidden_authors = session.query(Author).filter_by(hidden=True).all()
        hidden_author_names = {author.name.lower().strip() for author in hidden_authors}
        recommendations = [r for r in recommendations if r.get('author', '').lower().strip() not in hidden_author_names]
        
        # Get thumbs_up status for all recommendations (case-insensitive matching)
        thumbs_up_recs = session.query(Recommendation).filter_by(
            format='audiobook',
            thumbs_up=True
        ).all()
        thumbs_up_set = {(rec.title.lower().strip(), rec.author.lower().strip()) for rec in thumbs_up_recs}
        
        # Add thumbs_up status to each recommendation
        for rec in recommendations:
            rec['thumbs_up'] = (rec['title'].lower().strip(), rec['author'].lower().strip()) in thumbs_up_set
        
        # Get hidden authors for the hidden section (before filtering them out)
        from sqlalchemy import func
        hidden_authors_data = []
        for author in hidden_authors:
            author_recs = [r for r in all_recommendations if r.get('author', '').lower().strip() == author.name.lower().strip()]
            if author_recs:
                hidden_authors_data.append({
                    'name': author.name,
                    'book_count': len(author_recs),
                    'recommendations': author_recs
                })
        
        return jsonify({
            'recommendations': recommendations,
            'total': len(recommendations),
            'hidden_authors': hidden_authors_data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recommendations/ebook')
def api_recommendations_ebook():
    """Get ebook recommendations, organized by Fiction/Non-Fiction"""
    session = None
    try:
        session = _request_database()
        
        generated_recommendations = recommend_new_books(session, category=None)
        
        # Flatten if grouped by category (recommend_new_books returns dict when category is None)
        if isinstance(generated_recommendations, dict):
            flattened = [
                recommendation
                for category_recommendations in generated_recommendations.values()
                for recommendation in category_recommendations
            ]
        else:
            flattened = list(generated_recommendations)

        # A recommendation can appear in more than one category. Return each
        # catalog book once so the browser has less JSON and fewer cards to build.
        all_recommendations = []
        seen_recommendations = set()
        for recommendation in flattened:
            identity = recommendation.get('catalog_book_id') or (
                recommendation.get('title', '').casefold().strip(),
                recommendation.get('author', '').casefold().strip(),
            )
            if identity in seen_recommendations:
                continue
            seen_recommendations.add(identity)
            all_recommendations.append(recommendation)
        _compact_recommendations(all_recommendations)
        recommendations = list(all_recommendations)
        
        # Filter out thumbs down, already_read, non_english, and duplicate recommendations
        # Use case-insensitive matching to match how feedback is saved
        filtered_recs = session.query(Recommendation).filter(
            (Recommendation.format == 'ebook') &
            (
                (Recommendation.thumbs_down == True) |
                (Recommendation.already_read == True) |
                (Recommendation.non_english == True) |
                (Recommendation.duplicate == True)
            )
        ).all()
        # Create normalized set for case-insensitive matching
        filtered_set = {(rec.title.lower().strip() if rec.title else '', rec.author.lower().strip() if rec.author else '') for rec in filtered_recs}
        recommendations = [r for r in recommendations if (r.get('title', '').lower().strip(), r.get('author', '').lower().strip()) not in filtered_set]
        
        # Filter out hidden authors
        from sqlalchemy import func
        hidden_authors = session.query(Author).filter_by(hidden=True).all()
        hidden_author_names = {author.name.lower().strip() for author in hidden_authors}
        recommendations = [r for r in recommendations if r.get('author', '').lower().strip() not in hidden_author_names]
        
        # Get thumbs_up status for all recommendations (case-insensitive matching)
        thumbs_up_recs = session.query(Recommendation).filter_by(
            format='ebook',
            thumbs_up=True
        ).all()
        thumbs_up_set = {(rec.title.lower().strip() if rec.title else '', rec.author.lower().strip() if rec.author else '') for rec in thumbs_up_recs}
        
        # Add thumbs_up status to each recommendation
        for rec in recommendations:
            rec['thumbs_up'] = (rec.get('title', '').lower().strip(), rec.get('author', '').lower().strip()) in thumbs_up_set
        
        # Get hidden authors for the hidden section (before filtering them out)
        from sqlalchemy import func
        hidden_authors_data = []
        for author in hidden_authors:
            author_recs = [r for r in all_recommendations if r.get('author', '').lower().strip() == author.name.lower().strip()]
            if author_recs:
                hidden_authors_data.append({
                    'name': author.name,
                    'book_count': len(author_recs),
                    'recommendations': author_recs
                })
        
        return jsonify({
            'recommendations': recommendations,
            'total': len(recommendations),
            'hidden_authors': hidden_authors_data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/status')
def api_status():
    """Get system status"""
    return jsonify(get_status(_request_database()))


@app.route('/api/jobs/status')
def api_job_status():
    """Return the current or most recently completed library-update job."""
    job = JOB_MANAGER.snapshot()
    return jsonify({
        'job': job,
        'active': bool(job and job['state'] in {'queued', 'running'}),
    })


@app.route('/api/jobs/import', methods=['POST'])
def api_start_import_job():
    """Upload and import one Libby reading-history CSV."""
    active_job = JOB_MANAGER.snapshot()
    if active_job and active_job['state'] in {'queued', 'running'}:
        return jsonify({
            'error': 'A library update is already running.',
            'job': active_job,
        }), 409

    uploaded_file = request.files.get('file')
    if not uploaded_file or not uploaded_file.filename:
        return jsonify({'error': 'Choose a Libby history CSV to import.'}), 400

    original_name = secure_filename(uploaded_file.filename) or 'libby-history.csv'
    if Path(original_name).suffix.lower() != '.csv':
        return jsonify({'error': 'Choose a .csv file exported from Libby.'}), 400

    temp_file = tempfile.NamedTemporaryFile(
        prefix='bookpilot-libby-',
        suffix='.csv',
        delete=False,
    )
    temp_path = Path(temp_file.name)
    temp_file.close()

    try:
        uploaded_file.save(temp_path)
        _validate_libby_csv(temp_path)
        job = JOB_MANAGER.start(
            'import',
            'Import CSV',
            lambda progress: _run_import_job(temp_path, original_name, progress),
        )
        return _job_response(job)
    except JobAlreadyRunning as exc:
        temp_path.unlink(missing_ok=True)
        return _job_conflict_response(exc)
    except ValueError as exc:
        temp_path.unlink(missing_ok=True)
        return jsonify({'error': str(exc)}), 400
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


@app.route('/api/jobs/catalog/recent', methods=['POST'])
def api_start_recent_catalog_job():
    """Check the last year of history-backed author catalogs."""
    try:
        job = JOB_MANAGER.start(
            'catalog_recent',
            'Check for New Books',
            lambda progress: _run_catalog_job(
                force_refresh=False,
                only_recent=True,
                recent_years=1,
                progress=progress,
            ),
        )
        return _job_response(job)
    except JobAlreadyRunning as exc:
        return _job_conflict_response(exc)


@app.route('/api/jobs/catalog/full', methods=['POST'])
def api_start_full_catalog_job():
    """Force a complete refresh of history-backed author catalogs."""
    try:
        job = JOB_MANAGER.start(
            'catalog_full',
            'Refresh All Author Catalogs',
            lambda progress: _run_catalog_job(
                force_refresh=True,
                only_recent=False,
                recent_years=3,
                progress=progress,
            ),
        )
        return _job_response(job)
    except JobAlreadyRunning as exc:
        return _job_conflict_response(exc)


@app.route('/api/recommendations/<format_type>/feedback', methods=['POST'])
def api_recommendation_feedback(format_type):
    """Handle thumbs up/down feedback for recommendations"""
    data = request.get_json(silent=True) or {}
    thumbs_up = bool(data.get('thumbs_up', False))
    thumbs_down = bool(data.get('thumbs_down', False))

    insert_values = {
        'thumbs_up': True if thumbs_up else None,
        'thumbs_down': True if thumbs_down else None,
    }
    update_values = {}
    if thumbs_up:
        update_values.update(thumbs_up=True, thumbs_down=False)
    elif thumbs_down:
        update_values.update(thumbs_up=False, thumbs_down=True)

    return _recommendation_action_response(
        format_type,
        insert_values=insert_values,
        update_values=update_values,
        message='Feedback saved for "{title}" by {author}',
    )


@app.route('/api/recommendations/<format_type>/flag-non-english', methods=['POST'])
def api_flag_non_english(format_type):
    """Flag a recommendation as non-English"""
    return _recommendation_action_response(
        format_type,
        insert_values={
            'non_english': True,
            'language_flag_source': 'manual',
        },
        update_values={
            'non_english': True,
            'language_flag_source': 'manual',
        },
        message='Flagged "{title}" by {author} as non-English',
    )


@app.route('/api/recommendations/<format_type>/flag-already-read', methods=['POST'])
def api_flag_already_read(format_type):
    """Flag a recommendation as already read"""
    return _recommendation_action_response(
        format_type,
        insert_values={'already_read': True},
        update_values={'already_read': True},
        message='Flagged "{title}" by {author} as already read',
    )


@app.route('/api/recommendations/<format_type>/flag-duplicate', methods=['POST'])
def api_flag_duplicate(format_type):
    """Flag a recommendation as duplicate"""
    return _recommendation_action_response(
        format_type,
        insert_values={'duplicate': True},
        update_values={'duplicate': True},
        message='Flagged "{title}" by {author} as duplicate',
    )


@app.route('/api/authors/<author_name>/hide', methods=['POST'])
def api_hide_author(author_name):
    """Hide an author from recommendations"""
    try:
        session = _request_database()
        
        from sqlalchemy import func
        author = session.query(Author).filter(
            func.lower(Author.name) == author_name.lower()
        ).first()
        
        if not author:
            return jsonify({'success': False, 'error': 'Author not found'}), 404
        
        author.hidden = True
        author.hidden_at = datetime.utcnow()
        
        try:
            session.commit()
            return jsonify({'success': True, 'message': f'Hidden author "{author.name}"'})
        except Exception as db_error:
            session.rollback()
            return jsonify({'success': False, 'error': str(db_error)}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/authors/<author_name>/unhide', methods=['POST'])
def api_unhide_author(author_name):
    """Unhide an author from recommendations"""
    try:
        session = _request_database()
        
        from sqlalchemy import func
        author = session.query(Author).filter(
            func.lower(Author.name) == author_name.lower()
        ).first()
        
        if not author:
            return jsonify({'success': False, 'error': 'Author not found'}), 404
        
        author.hidden = False
        author.hidden_at = None
        
        try:
            session.commit()
            return jsonify({'success': True, 'message': f'Unhidden author "{author.name}"'})
        except Exception as db_error:
            session.rollback()
            return jsonify({'success': False, 'error': str(db_error)}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/recommendations/<format_type>/recategorize', methods=['POST'])
def api_recategorize(format_type):
    """Recategorize a book - toggle between Fiction and Non-Fiction"""
    try:
        session = _request_database()

        data = request.get_json(silent=True) or {}
        title = str(data.get('title') or '').strip()
        author = str(data.get('author') or '').strip()
        if not title or not author:
            return jsonify({'success': False, 'error': 'Missing title or author'}), 400

        def save_recommendation_category(category):
            """Upsert the category in the same transaction as any catalog edit."""
            statement = _recommendation_upsert_statement(
                title=title,
                author=author,
                format_type=format_type,
                insert_values={'category': category},
                update_values={'category': category},
            )
            try:
                session.execute(statement)
                session.commit()
                return None
            except OperationalError as exc:
                session.rollback()
                if 'locked' in str(exc).lower():
                    return jsonify({
                        'success': False,
                        'error': 'Database is currently locked. Your action will be saved when the database is available.',
                        'retry': True,
                    }), 503
                raise
            except Exception:
                session.rollback()
                raise

        # Try to find the catalog book to update its category
        from src.models import AuthorCatalogBook, Author
        from src.recommend import is_fiction

        # Try to find author by name first (case-insensitive), then by normalized_name
        author_obj = session.query(Author).filter(func.lower(Author.name) == author.lower()).first()
        if not author_obj:
            # Try normalized_name (case-insensitive)
            author_obj = session.query(Author).filter(func.lower(Author.normalized_name) == author.lower()).first()

        # If still not found, try to find by partial match on name
        if not author_obj:
            author_obj = session.query(Author).filter(
                func.lower(Author.name).contains(author.lower())
            ).first()

        catalog_book = None
        if author_obj:
            # Try exact title match first
            catalog_book = session.query(AuthorCatalogBook).filter_by(
                author_id=author_obj.id,
                title=title
            ).first()

            # If not found, try case-insensitive match
            if not catalog_book:
                catalog_book = session.query(AuthorCatalogBook).filter(
                    AuthorCatalogBook.author_id == author_obj.id,
                    func.lower(AuthorCatalogBook.title) == title.lower()
                ).first()

        if not catalog_book:
            # If no catalog book found, we can't recategorize
            # But we should still atomically create/update the recommendation.
            error_response = save_recommendation_category('Uncategorized')
            if error_response:
                return error_response
            return jsonify({'success': True, 'message': 'Book moved to Uncategorized (catalog book not found)'})

        # Get current categories
        current_categories = catalog_book.categories.split(', ') if catalog_book.categories else []

        # Determine if currently Fiction or Non-Fiction
        currently_fiction = is_fiction(current_categories)

        # Toggle to the opposite category
        if currently_fiction:
            # Move to Non-Fiction: add a non-fiction keyword
            # Keep existing categories but add "Non-Fiction" if not present
            if 'Non-Fiction' not in current_categories:
                current_categories.append('Non-Fiction')
            # Also ensure we have a non-fiction keyword
            if not any('nonfiction' in cat.lower() or 'non-fiction' in cat.lower() or 
                      'biography' in cat.lower() or 'history' in cat.lower() 
                      for cat in current_categories):
                current_categories.append('History')  # Add a generic non-fiction category
            new_categories = ', '.join(current_categories)
        else:
            # Move to Fiction: remove non-fiction keywords
            # Remove non-fiction keywords
            non_fiction_keywords = ['nonfiction', 'non-fiction', 'biography', 'autobiography', 
                                   'memoir', 'history', 'historical', 'science', 'philosophy',
                                   'psychology', 'sociology', 'economics', 'business', 'self-help',
                                   'health', 'medicine', 'education', 'reference', 'travel',
                                   'cooking', 'crafts', 'hobbies', 'religion', 'spirituality',
                                   'politics', 'government', 'law', 'true crime', 'essays',
                                   'journalism']
            
            filtered_categories = []
            for cat in current_categories:
                cat_lower = cat.lower()
                is_non_fiction = any(keyword in cat_lower for keyword in non_fiction_keywords)
                if not is_non_fiction and cat != 'Non-Fiction':
                    filtered_categories.append(cat)

            # If no categories left, set to a generic fiction category
            if not filtered_categories:
                filtered_categories = ['Fiction']
            else:
                # Ensure we have "Fiction" in there
                if 'Fiction' not in filtered_categories:
                    filtered_categories.insert(0, 'Fiction')

            new_categories = ', '.join(filtered_categories)

        # Update catalog book categories
        catalog_book.categories = new_categories

        # The recommendation upsert and catalog edit commit together. This
        # remains safe if another identical request creates the recommendation
        # after this request has read the catalog row.
        error_response = save_recommendation_category(new_categories)
        if error_response:
            return error_response
        target_category = 'Non-Fiction' if currently_fiction else 'Fiction'
        return jsonify({'success': True, 'message': f'Book moved to {target_category}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/books-to-read')
def api_books_to_read():
    """Get all books marked with thumbs up, grouped by author (excluding books already in Libby)"""
    try:
        session = _request_database()
        
        # Get all recommendations with thumbs up that are NOT already read and NOT duplicates
        # Use filter() instead of filter_by() for better boolean handling
        from sqlalchemy import and_, or_
        try:
            recs = session.query(Recommendation).filter(
                Recommendation.thumbs_up.is_(True),
                or_(
                    Recommendation.already_read.is_(False),
                    Recommendation.already_read.is_(None)
                ),
                or_(
                    Recommendation.duplicate.is_(False),
                    Recommendation.duplicate.is_(None)
                )
            ).all()
        except Exception as db_error:
            error_str = str(db_error).lower()
            # If column doesn't exist yet, try migration again
            if 'no such column' in error_str and 'non_english' in error_str:
                # Try to migrate
                migrate_database(g.db_engine)
                # Try the query again
                try:
                    recs = session.query(Recommendation).filter(
                        Recommendation.thumbs_up.is_(True),
                        or_(
                            Recommendation.already_read.is_(False),
                            Recommendation.already_read.is_(None)
                        ),
                        or_(
                            Recommendation.duplicate.is_(False),
                            Recommendation.duplicate.is_(None)
                        )
                    ).all()
                except Exception as retry_error:
                    retry_error_str = str(retry_error).lower()
                    # If still failing due to missing column or locked database
                    if 'no such column' in retry_error_str or 'locked' in retry_error_str:
                        # Return empty result with helpful message
                        return jsonify({
                            'books': {},
                            'total': 0,
                            'message': 'Database is currently being updated. Please wait for the catalog command to finish, then refresh this page.'
                        })
                    else:
                        raise
            elif 'locked' in error_str:
                # Database is locked by catalog command
                return jsonify({
                    'books': {},
                    'total': 0,
                    'message': 'Database is currently locked (catalog command may be running). Please wait and refresh.'
                })
            else:
                raise
        
        # Get all books from Libby to check against
        libby_books = session.query(Book).all()
        libby_set = {(b.title.lower() if b.title else '', b.author.lower() if b.author else '') 
                     for b in libby_books if b.title and b.author}
        
        # Filter out books that are already in Libby
        filtered_recs = []
        for rec in recs:
            if not rec.title or not rec.author:
                continue
            title_lower = rec.title.lower()
            author_lower = rec.author.lower()
            if (title_lower, author_lower) not in libby_set:
                filtered_recs.append(rec)
        
        # Group by author
        by_author = {}
        for rec in filtered_recs:
            if not rec.author:
                continue
            if rec.author not in by_author:
                by_author[rec.author] = []
            by_author[rec.author].append({
                'title': rec.title or '',
                'author': rec.author or '',
                'format': rec.format or '',
                'isbn': rec.isbn or '',
                'category': rec.category or '',
                'reason': rec.reason or '',
                'similarity_score': rec.similarity_score if rec.similarity_score is not None else 0.0
            })
        
        # Sort authors alphabetically, books by score within each author
        for author in by_author:
            by_author[author].sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
        
        sorted_authors = dict(sorted(by_author.items()))
        
        return jsonify({
            'books': sorted_authors,
            'total': len(filtered_recs)
        })
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        app.logger.error(f"Error in api_books_to_read: {error_msg}")
        return jsonify({'error': str(e), 'details': traceback.format_exc()}), 500


if __name__ == '__main__':
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Run migrations on startup
    try:
        engine = init_db(str(DB_PATH))
        print("Database migrations completed")
    except Exception as e:
        print(f"Warning: Database migration failed: {e}")
    finally:
        if 'engine' in locals():
            engine.dispose()
    
    # Library update endpoints modify local data, so keep the development app
    # reachable only from this computer.
    app.run(debug=True, host='127.0.0.1', port=5000)
