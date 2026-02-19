# BookPilot

## Personal Reading Recommender 

BookPilot is a tool to help you analyze your Libby reading history and generate personalized book recommendations, based on authors you've already read.

## Quick start (new users)

1. **Clone and install**
   ```bash
   git clone https://github.com/danamlewis/BookPilot.git
   cd BookPilot
   pip install -r requirements.txt
   ```

2. **Get your Libby export CSV**  
   In the Libby app or at [libbyapp.com](https://libbyapp.com): go to **Account** → **Reading History** → **Export**. Download the CSV (often named something like `libbytimeline-all-loans,all.csv`).  
   If you have multiple library cards: exporting from the **phone app** exports everything in one file; exporting from the web may not link all of your library card data together.

3. **Put the CSV anywhere you like** (e.g. in the project folder or your Downloads folder).

4. **Run these three commands in order:**
   ```bash
   # 1. Import your reading history (use your file path; if the CSV is in the project folder, use its filename)
   python scripts/bookpilot.py ingest path/to/your-export.csv

   # 2. Fetch author catalogs (uses Open Library). Note: slow! May take 10–30 minutes if you have a few dozen authors, or over an hour if you have hundreds.
   python scripts/bookpilot.py catalog

   # 3. Start the web interface
   python web/app.py
   ```
   Then open **http://localhost:5000** in your browser. Recommendations are generated when you open the Audiobook or Ebook recommendations tabs.

That’s it. For updates when you have new loans, see [Regular Updates (Existing User)](#regular-updates-existing-user) below.

---

## Features

### v0.1 (Current)
- **CSV Import**: Ingest Libby export CSV files
- **Format Detection**: Automatically detect audiobooks vs ebooks
- **Author Catalog Fetching**: Get full catalogs from Open Library (Google Books used only for language checks during cleanup)
- **Series Analysis**: Identify partially read series, missing series, and standalone books
- **Audiobook Recommendations**: Recommendations based on authors you've listened to
- **Ebook Recommendations**: New books by favorite authors, grouped by author. Sort by number of books per author or A–Z.
- **Web Interface**: Interactive web UI for exploring recommendations
- **Command-line Tools**: Command-line tools for core operations
- **Tracking**: Track last Libby import and catalog check dates
- **User Feedback**: Thumbs up/down on recommendations, flag books as already read/duplicate/non-English. Thumbs down/flagged won't show in your to-read section.
- **Books to Read List**: Curated list of books you've given thumbs up
- **Smart Matching**: Handles title variations ("The" prefix, series info) and author name differences
- **Automatic Cleanup**: Auto-removes non-English books and duplicates during catalog fetch
- **Duplicate Author Detection**: Automatically detects and helps merge duplicate author records
- **Optimized Catalog Fetching**: Skips existing books and old publications to reduce API calls by 50-80%

## Setup

### Prerequisites
- Python 3.9+
- pip

### Installation

1. Clone the repository (or download and extract the code), then go into the project directory:
   ```bash
   git clone https://github.com/danamlewis/BookPilot.git
   cd BookPilot
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. **Data directory**: The repo includes a `data/` directory (via `data/.gitkeep`) so the project runs out of the box. Your database and API cache are created on first run and are **gitignored**, so your personal data is never committed. You do not need to create `data/` or `data/cache/` manually.

**Your data stays local:** CSV files, the SQLite database (`data/bookpilot.db`), the API cache (`data/cache/`), and any `.env` or backup files are ignored by git. Nothing you ingest or generate is ever committed when you push or share the repo.

### If something is missing after clone

All source files (including `src/catalog.py`, `src/ingest.py`, etc.) and scripts are intended to be in the repo. If a file is missing (e.g. you had to restore it from git history), check that nothing in `.gitignore` is excluding it (e.g. we do not ignore `src/` or `*.py`) and run `git status` to see untracked or missing files.

## Usage

### Common Workflows

#### First Time Setup (New User)

If you haven’t already, follow the [Quick start](#quick-start-new-users) above. Summary:

**Step 1: Ingest your Libby CSV export**  
Use the path to your exported CSV (e.g. if it’s in the project folder: `my-export.csv`, or a full path: `/Users/you/Downloads/libbytimeline-all-loans,all.csv`).
```bash
python scripts/bookpilot.py ingest path/to/your-export.csv
```
This imports your reading history, detects formats (audiobook/ebook), and extracts authors.

**Step 2: Fetch author catalogs**  
Note: slow! May take 10–30 minutes if you have a few dozen authors, or over an hour if you have hundreds.
```bash
python scripts/bookpilot.py catalog
```
Fetches full catalogs for all authors (rate-limited APIs). You can run it in the background.

**Step 3: Start web interface**
```bash
python web/app.py
```
Then open http://localhost:5000 in your browser. The web interface will automatically generate and display recommendations when you click on the "Audiobook Recommendations" or "Ebook Recommendations" tabs.

**Step 4 (Optional): Generate recommendations from command line**
If you prefer to generate recommendations from the command line instead of using the web UI:
```bash
python scripts/bookpilot.py recommend audiobook --save
python scripts/bookpilot.py recommend ebook --save
```
Note: This is optional - the web UI generates recommendations automatically when you view the recommendations tabs.

---

#### Regular Updates (Existing User)

**Scenario A: You've read new books and want to update everything**

1. **Ingest new CSV export:**
   ```bash
   python scripts/bookpilot.py ingest "*.csv"
   ```
   Or specify the exact file:
   ```bash
   python scripts/bookpilot.py ingest "libbytimeline-all-loans,all 2.csv"
   ```
   - Adds new books and authors
   - Automatically marks matching recommendations as "already read"
   - Removes books from "Books to Read" if they're now in your Libby history

2. **Check for recent catalog updates (fast, only last 1-3 years):**
   ```bash
   python scripts/bookpilot.py catalog --only-recent --recent-years 1 --auto-cleanup
   ```
   - Only fetches books published in the last year for existing authors
   - Skips authors checked within 7 days (saves time)
   - Automatically removes non-English books and duplicates
   - Detects and prompts to merge duplicate authors

3. **View updated recommendations in web UI:**
   The web interface automatically generates fresh recommendations when you click on the recommendations tabs. No need to run commands separately.

**Scenario B: Quick update - just check for new books by your authors**

```bash
python scripts/bookpilot.py catalog --only-recent --recent-years 3 --auto-cleanup
```
- Checks for books from last 3 years
- Skips authors checked <7 days ago
- Auto-cleans duplicates and non-English books
- Shows duplicate authors for review

**Scenario C: Full refresh (force update all authors)**

```bash
python scripts/bookpilot.py catalog --force --auto-cleanup
```
- Forces refresh of all authors (ignores 7-day check)
- Useful if you want to ensure everything is up to date

---

### Command Reference

#### Ingest Libby CSV Export
```bash
# Basic ingest (use glob pattern; quote it so the script receives "*.csv" and picks the most recent)
python scripts/bookpilot.py ingest "*.csv"

# Or specify exact file
python scripts/bookpilot.py ingest libbytimeline-all-loans,all.csv

# Update existing records
python scripts/bookpilot.py ingest "*.csv" --update
```

**What it does:**
- Parses CSV, detects audiobooks vs ebooks
- Extracts and normalizes author names
- Marks recommendations as "already read" if books match (handles "The" prefix and series info in titles)
- Removes books from "Books to Read" if they're now in Libby history

#### Fetch Author Catalogs
```bash
# Basic catalog fetch (respects 7-day check)
python scripts/bookpilot.py catalog

# Only fetch recent books (last 1 year) for existing authors
python scripts/bookpilot.py catalog --only-recent --recent-years 1

# Only fetch recent books (last 3 years) with auto-cleanup
python scripts/bookpilot.py catalog --only-recent --recent-years 3 --auto-cleanup

# Force refresh all authors (ignore 7-day check)
python scripts/bookpilot.py catalog --force

# Full refresh with cleanup and auto-merge duplicates
python scripts/bookpilot.py catalog --force --auto-cleanup --yes
```

**Options:**
- `--only-recent`: Only fetch books from last N years (for authors that already have catalogs)
- `--recent-years N`: Number of years to look back (default: 3)
- `--force`: Force refresh even if checked within 7 days
- `--auto-cleanup`: Automatically remove non-English books and duplicates after fetch
- `--yes`: Auto-merge duplicate authors without prompting

**Optimizations:**
- Authors checked <7 days ago are automatically skipped (unless `--force`)
- Existing books are skipped (saves ~2 API calls per book)
- Old books are skipped early when using `--only-recent` (saves ~1 API call per old book)

#### Analyze Series
```bash
# Analyze all series
python scripts/bookpilot.py series

# Filter by format
python scripts/bookpilot.py series --format ebook
python scripts/bookpilot.py series --format audiobook
```

#### Generate Recommendations
**Note:** Recommendations are automatically generated when you view them in the web UI. These commands are optional if you prefer command-line access.

```bash
# Preview recommendations (doesn't save)
python scripts/bookpilot.py recommend audiobook
python scripts/bookpilot.py recommend ebook

# Generate and save to database (for web UI display)
python scripts/bookpilot.py recommend audiobook --save
python scripts/bookpilot.py recommend ebook --save

# Filter by category
python scripts/bookpilot.py recommend ebook --category "Fiction"
```

#### Check Status
```bash
python scripts/bookpilot.py status
```
Shows: total books, authors, last import date, last catalog check date

#### Merge Duplicate Authors
```bash
# Using author names
python scripts/bookpilot.py merge-authors \
  --author1 "L. M. (Lucy Maud) Montgomery" \
  --author2 "L. M. Montgomery" \
  --yes

# Using author IDs
python scripts/bookpilot.py merge-authors \
  --author1-id 1 \
  --author2-id 2 \
  --yes

# Preview first (dry run)
python scripts/bookpilot.py merge-authors \
  --author1-id 1 \
  --author2-id 2 \
  --dry-run
```

#### List Authors
```bash
# List all authors
python scripts/bookpilot.py list-authors

# Search for specific author
python scripts/bookpilot.py list-authors --search "Montgomery"
```

#### Cleanup Commands
```bash
# Remove non-English books
python scripts/bookpilot.py cleanup --yes

# Remove duplicate titles
python scripts/bookpilot.py remove-duplicates --yes

# Preview first (dry run)
python scripts/bookpilot.py remove-duplicates --dry-run
```

---

### What Happens During Each Command

#### `ingest` - What It Does
1. Parses your Libby CSV file
2. Detects format (audiobook vs ebook) from publisher names
3. Normalizes author names
4. Adds new books and authors to database
5. **Automatically marks recommendations as "already read"** if they match books in your CSV
   - Handles title variations: "The Sea Before Us" matches "Sea Before Us (Sunrise at Normandy Book #1)"
   - Normalizes author names for matching
6. **Removes books from "Books to Read"** if they're now in your Libby history
7. Updates last import date

#### `catalog` - What It Does
1. Checks each author's `last_catalog_check` date
   - Skips if checked <7 days ago (unless `--force`)
   - Fetches if never checked or >7 days ago
2. For each author to fetch:
   - **Skips existing books** (by Open Library work key) - saves ~2 API calls per book
   - **Skips old books early** (if using `--only-recent`) - saves ~1 API call per old book
   - Fetches new books from Open Library
   - Matches catalog books to your reading history
3. If `--auto-cleanup` is used:
   - Removes non-English books
   - Removes duplicate titles
4. **Automatically detects duplicate authors** and prompts to merge them
5. Updates last catalog check date

#### `recommend` - What It Does
**Note:** This is automatically run when you view recommendations in the web UI. The command-line version is optional.

1. Analyzes your reading history
2. Finds unread books by your authors
3. Filters out:
   - Books you've already read (from Libby or marked as "already read")
   - Books marked as thumbs down, duplicate, or non-English
   - Hidden authors
4. Scores and ranks recommendations
5. If `--save`, stores in database for web UI display

### Web Interface

1. Start the web server:
```bash
python web/app.py
```

2. Open your browser to:
   ```
   http://localhost:5000
   ```

The web interface provides:
- **Status**: Dashboard with book counts, last Libby import, and last catalog check
- **Series**: Partially read and not-started series (ebooks), with collapse/expand and sort (by count or A–Z)
- **Audiobook / Ebook recommendations**: Grouped by author. **Sort** by number of books per author or A–Z; **Collapse All / Expand All** to show or hide author sections. **Hide** an author (✕) to remove them from the main list; hidden authors appear in a “Hidden authors” section at the bottom where you can **Show** them again. Generated automatically when you open the tab.
- **Books to Read**: Thumbs-up recommendations in one list
- **Feedback** on each recommendation: thumbs up/down, already read, duplicate, non-English, recategorize

**Note:** Recommendations are generated on-demand when you click the recommendations tabs. No need to run `recommend` commands separately unless you prefer command-line access.

## Project Structure

```
BookPilot/
├── data/                    # Database and cache (not in repo)
│   ├── bookpilot.db        # SQLite database
│   └── cache/              # API response cache
│       ├── googlebooks/    # Cached Google Books API responses
│       └── openlibrary/    # Cached Open Library API responses
├── src/                     # Core modules
│   ├── models.py           # Database models
│   ├── ingest.py           # CSV ingestion
│   ├── catalog.py          # Author catalog fetching
│   ├── series.py           # Series analysis
│   ├── recommend.py       # Recommendation engine
│   ├── api/                # API clients
│   │   ├── openlibrary.py
│   │   └── googlebooks.py
│   └── deduplication/      # Deduplication utilities
│       └── language_detection.py
├── scripts/                 # Command-line tools and utility scripts
│   ├── bookpilot.py        # Main command-line tool for core operations
│   ├── analyze_*.py        # Analysis scripts
│   ├── detect_*.py         # Detection scripts
│   ├── remove_*.py         # Cleanup scripts
│   ├── review_*.py         # Review scripts
│   └── *.md                # Script documentation
├── web/                     # Web interface
│   ├── app.py              # Flask application
│   └── templates/
│       └── index.html       # Web UI template
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Workflow Examples

### First Time Setup
```bash
# 1. Import your reading history
python scripts/bookpilot.py ingest libbytimeline-all-loans,all.csv

# 2. Fetch all author catalogs. Note: slow! May take 10–30 min (few dozen authors) or over an hour (hundreds of authors)
python scripts/bookpilot.py catalog

# 3. Start web interface
python web/app.py
# Then open http://localhost:5000
# Recommendations are generated automatically when you view the tabs

# (Optional) Generate recommendations from command line instead
python scripts/bookpilot.py recommend audiobook --save
python scripts/bookpilot.py recommend ebook --save
```

### Regular Update Workflow (After Reading New Books)

**When you have a new Libby CSV export:**

```bash
# 1. Ingest new books (automatically marks matching recommendations as "already read")
python scripts/bookpilot.py ingest "libbytimeline-all-loans,all 2.csv"

# 2. Check for recent catalog updates (only last year, with cleanup)
python scripts/bookpilot.py catalog --only-recent --recent-years 1 --auto-cleanup

# 3. Review and merge any duplicate authors (prompted automatically)
# Type 'all' to merge all, 'none' to skip, or specific numbers like '1 3'

# 4. Regenerate recommendations
python scripts/bookpilot.py recommend audiobook --save
python scripts/bookpilot.py recommend ebook --save
```

**Quick check for new books (no new CSV):**

```bash
# Just check for recent books by your authors (last 3 years)
python scripts/bookpilot.py catalog --only-recent --recent-years 3 --auto-cleanup
```
Then refresh the web UI to see updated recommendations.

### Weekly/Monthly Maintenance

```bash
# Check status
python scripts/bookpilot.py status

# Update catalogs for recent books only (fast)
python scripts/bookpilot.py catalog --only-recent --recent-years 1 --auto-cleanup --yes

# View recommendations in web UI (generated automatically)
# Or optionally generate from command line:
python scripts/bookpilot.py recommend audiobook --save
python scripts/bookpilot.py recommend ebook --save
```

## Scripts

BookPilot includes various scripts in the `scripts/` directory:
- **Core operations**: `bookpilot.py` - Main command-line tool for ingest, catalog, series, recommend, and status
- **Maintenance scripts**: Data cleanup, analysis, and management utilities

⚠️ **Warning**: Many scripts modify your database. Always use `--dry-run` or preview modes first!

### Core Operations
- `bookpilot.py` - Main command-line tool for core operations (ingest, catalog, series, recommend, status)

### Analysis Scripts
- `analyze_author_catalog.py` - Analyze an author's catalog for duplicates and non-English editions
- `analyze_catalog_duplicates.py` - Analyze duplicate books across author catalogs
- `analyze_prolific_duplicates.py` - Analyze duplicates for authors with large catalogs
- `analyze_publisher_authors.py` - Detect and analyze publisher/company authors
- `check_author_duplicates.py` - Check for duplicate author records
- `check_deleted_books.py` - Verify books marked as deleted
- `check_duplicate_recommendations.py` - Check for duplicate recommendations
- `scan_non_english_titles.py` - Scan catalog for non-English book titles

### Cleanup Scripts
- `filter_author_books.py` - Filter books from an author's catalog by title patterns (e.g., remove textbooks)
- `preview_and_delete_non_english.py` - Preview and delete non-English books from catalog
- `remove_credential_authors.py` - Remove authors that are only credentials (e.g., "PhD", "MD")
- `remove_publisher_authors.py` - Remove author(s) by name and all associated books
- `reassign_author_books.py` - Reassign books from one author to another (fixes incorrect assignments)

### Series Management
- `consolidate_series.py` - Consolidate series information
- `extract_series_from_titles.py` - Extract series information from book titles
- `review_and_consolidate_series.py` - Review and consolidate series data
- `split_author_group.py` - Split author groups into individual authors

### Specialized Cleanup
- `detect_childrens_books.py` - Detect children's books in catalog
- `detect_composite_volumes.py` - Detect composite/omnibus volumes
- `review_and_delete_childrens_books.py` - Review and delete children's books
- `review_and_delete_composites.py` - Review and delete composite volumes

### Data Quality
- `bulk_dedupe_approval.py` - Bulk duplicate removal with approval workflow
- `fix_mismatched_normalized_names.py` - Fix mismatched author normalized names
- `review_cleanup.py` - Review cleanup results before execution
- `verify_cleanup.py` - Verify cleanup operation results

### Testing & Utilities
- `test_language_detection.py` - Test language detection functionality

For detailed documentation on specific scripts, see the `scripts/` directory for individual markdown files.

## Data Sources

- **Open Library API**: Author catalogs, series information, book metadata
- **Google Books API**: Language checks only (during non-English cleanup), not for categories/descriptions

Both APIs are free and don't require authentication. Responses are cached locally to minimize API calls.

## Database Schema

- `books`: Your reading history from Libby
- `authors`: Authors you've read
- `author_catalog_books`: Full catalogs from APIs
- `series`: Series information
- `recommendations`: Generated recommendations
- `system_metadata`: Tracking dates (last import, last catalog check)

## Key Features & Optimizations

### Smart Catalog Fetching
- **7-day skip**: Authors checked <7 days ago are automatically skipped (saves time)
- **Existing book detection**: Books you already have are skipped (saves ~2 API calls per book)
- **Early date filtering**: Old books are skipped before expensive API calls when using `--only-recent`
- **Hybrid optimization**: Can reduce API calls by 50-80% for prolific authors with existing catalogs

### Automatic Cleanup
- **Non-English detection**: Automatically filters out non-English books during catalog fetch (with `--auto-cleanup`)
- **Duplicate removal**: Removes duplicate titles within same author (keeps most complete version)
- **Duplicate author detection**: Automatically detects and prompts to merge duplicate authors after catalog fetch

### Smart Matching
- **Title normalization**: Handles "The" prefix differences and series info in titles
- **Author normalization**: Matches authors even with middle initial differences (e.g., "Julia Kelly" vs "Julia R. Kelly")
- **Already-read detection**: Books in your Libby history automatically mark recommendations as "already read"

## Notes

- API calls are rate-limited (0.5s delay) and cached to avoid hitting limits
- Author catalogs are checked every 7 days by default (use `--force` to override) when you run the command.
- Format detection uses publisher names (audiobook publishers like "Books on Tape")
- Series detection relies on Open Library metadata primarily
- Books marked as "already read" are automatically filtered from all recommendation views

## Quick Reference

### Most Common Commands

**First time setup:**
```bash
python scripts/bookpilot.py ingest libbytimeline-all-loans,all.csv
python scripts/bookpilot.py catalog
python web/app.py
# Recommendations are generated automatically in the web UI
```

**Regular update (new books read):**
```bash
python scripts/bookpilot.py ingest "*.csv"
python scripts/bookpilot.py catalog --only-recent --recent-years 1 --auto-cleanup
# Then refresh web UI - recommendations update automatically
```

**Quick check for new books (no new CSV):**
```bash
python scripts/bookpilot.py catalog --only-recent --recent-years 3 --auto-cleanup
# Then refresh web UI to see updated recommendations
```

**Check status:**
```bash
python scripts/bookpilot.py status
```

**Merge duplicate authors:**
```bash
python scripts/bookpilot.py merge-authors --author1-id 1 --author2-id 2 --yes
```

### Command Flags Reference

**Catalog command:**
- `--only-recent`: Only fetch books from last N years (for existing authors)
- `--recent-years N`: Number of years (default: 3)
- `--force`: Force refresh even if checked <7 days ago
- `--auto-cleanup`: Auto-remove non-English books and duplicates
- `--yes`: Auto-merge duplicate authors without prompting

**Ingest command:**
- `--update`: Update existing records instead of skipping

**Recommend command:**
- `--save`: Save recommendations to database (for web UI)
- `--category`: Filter by category/genre

## License

MIT — see [LICENSE](LICENSE).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to contribute and which kinds of features and PRs we encourage (e.g. new data sources like StoryGraph, similar-authors recommendations) and which are out of scope (e.g. social features).
