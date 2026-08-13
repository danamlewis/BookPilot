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

3. **Start the web interface:**
   ```bash
   python web/app.py
   ```
   Then open **http://localhost:5000** in your browser.

4. **Use the Update Library menu in the top-right:**
   - Choose **Import CSV…** and select your export CSV.
   - When the import finishes, choose **Refresh All Author Catalogs…**. The first catalog fetch may take 10–30 minutes for a few dozen authors or over an hour for hundreds.
   - Follow progress in the **Library update** row at the top of the page. It is safe to refresh the page while a job is running.

Recommendations are generated when you open the Audiobook or Ebook tabs. The command-line workflows remain available for advanced maintenance and automation.

The optional series tools use Hardcover's API and require a personal Hardcover
account and API token. BookPilot does not provide a shared token. Follow
[Connect Hardcover for series data](#connect-hardcover-for-series-data-optional)
before using **Series Reconciliation** or Hardcover-backed series enrichment.

That’s it. For updates when you have new loans, see [Regular Updates (Existing User)](#regular-updates-existing-user) below.

---

## Features

### v0.3 (Current)
- **Libby History Import**: Import a Libby reading-history CSV, recognize ebook and audiobook loans, add new reads without duplicating prior imports, and track the most recent import.
- **Guided Library Updates**: Use **Update Library** to import history, **Check for New Books** from the last year, or **Refresh All Author Catalogs**. Long-running updates show persistent progress plus a summary of additions and errors.
- **History-backed Author Catalogs**: Fetch Open Library catalogs only for authors represented in reading history. Publisher and organization credits are retained as reads but excluded from author catalog refreshes.
- **Format-aware Recommendations**: Keep separate **Audiobooks** and **Ebooks** views while excluding a work already read in either format. Expanded title normalization handles common edition suffixes, series annotations, packages, and other title variations.
- **Personal Match Scores**: Rank likely interests using reading history, author affinity, topic overlap, series context, saved books, and prior review decisions, with a short explanation attached to each score.
- **Recommendation Browsing**: Search by author or title; filter to **Strong matches**, **Possible + strong**, **Books only**, or **Likely non-reads**; and sort by **Score High–Low**, **Score Low–High**, **Author Count**, or **Author A–Z**.
- **Recommendation Review Actions**: Use **Save** to add a title to **To Read**, **Pass** to remove an unwanted suggestion, **already read** to record a prior read, **not english** to suppress a mismatched-language edition, **recategorize** to switch Fiction/Non-Fiction classification, or **duplicate** to remove a duplicate record.
- **Author Review Controls**: **Hide** all recommendations from an author and restore them later with **Show**.
- **Books to Read**: Review titles collected with **Save**, grouped by author. Passed, duplicate, and already-read books are removed from this list.
- **Improved Series Analysis**: Identify partially read, completed, and not-started series; show missing books in reading order; and distinguish series titles from standalone books even when the original catalog metadata is incomplete.
- **Hardcover Series Reconciliation**: With the user's own optional Hardcover API token, compare visible ebook and audiobook recommendations with Hardcover's structured series data. BookPilot reviews high-volume authors in manageable batches, maps recommendations and known reads into series order, and highlights gaps or books that may have been read before Libby history was available.
- **Series Review Actions**: **Mark read** for one title, mark all matched recommendations or a full series as read, or choose **Ignore for now** for an irrelevant series. Ignoring also applies **Pass** to matched ebook and audiobook recommendations; ignored series can be restored later without undoing those passes.
- **Cached and Resumable Series Checks**: Save reconciliation progress locally, reuse cached Hardcover results, continue with the next author batch, and avoid repeating completed API work unnecessarily.
- **Likely Non-read Batching**: Detect textbooks, access cards, workbooks, loose-leaf editions, and similar catalog artifacts so they can be reviewed together with the **Likely non-reads** filter.
- **Language and Catalog Cleanup**: Learn conservative language signals from **not english** decisions, automatically remove high-confidence non-English editions and multi-book packages, and provide duplicate-title, duplicate-author, and unsupported-author cleanup tools.
- **Local-first Storage and Tracking**: Keep reading history, recommendation decisions, ignored series, update status, and API caches in the local SQLite database; personal data and secrets remain outside Git.
- **Command-line Tools**: Retain CLI workflows for imports, catalog maintenance, recommendations, series analysis, duplicate-author merging, and advanced cleanup.

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

### Connect Hardcover for series data (optional)

BookPilot uses the [Hardcover API](https://api.hardcover.app/) to look up
structured author, series, book-order, and series-membership data. These lookups
power **Series Reconciliation** and the Hardcover-backed enrichment in the
**Series** tab. The ordinary Libby import and recommendation pages still work
without Hardcover, but those series features will ask you to configure a token.

Hardcover authenticates API requests with a token tied to an individual
Hardcover account. Each BookPilot user therefore needs to create their own
Hardcover account and save their own token locally:

1. Create an account at [hardcover.app](https://hardcover.app/) or sign in to
   your existing account. You do not need to copy your Libby history into
   Hardcover for BookPilot's series lookups.
2. While signed in, open Hardcover's
   [Account API page](https://hardcover.app/account/api) and copy the API token
   shown there. Treat it like a password: do not paste it into an issue, commit,
   screenshot, or shared message.
3. In the root of your local BookPilot checkout—the same folder as this
   README—create a file named `.env.local` and add exactly one setting:

   ```dotenv
   HARDCOVER_API_TOKEN=replace-this-with-your-own-token
   ```

   A token copied with or without the `Bearer ` prefix is accepted; BookPilot
   normalizes either form. Do not add quotation marks unless they are part of
   the token.
4. From the BookPilot directory, verify the token without displaying it:

   ```bash
   python scripts/test_hardcover_api.py
   ```

   A successful check prints the Hardcover username that authenticated. If it
   says the token is missing, confirm the filename is exactly `.env.local` and
   that it is in the repository root. If authentication fails, copy a current
   token again from the Hardcover API page.
5. Start BookPilot, or restart it if it was already running, so the Python
   server loads the new environment value:

   ```bash
   python web/app.py
   ```

   Then open [http://localhost:5000](http://localhost:5000). A browser refresh
   alone is not enough after adding or changing the token.

The token is read by the local Python server and sent only to Hardcover's API
in the authorization header; it is not sent to BookPilot's browser interface.
BookPilot's Hardcover integration currently makes read-only GraphQL queries—it
does not edit the user's Hardcover library. An explicitly exported
`HARDCOVER_API_TOKEN` shell variable takes precedence over `.env.local`.

#### Confirm the token will not be committed

BookPilot's `.gitignore` explicitly ignores `.env`, `.env.local`, and
`.env.*.local`. You can confirm the protection in your own checkout:

```bash
git check-ignore -v .env.local
git status --short --ignored .env.local
```

The first command should identify the matching `.gitignore` rule, and the
second should show `!! .env.local`, meaning Git is ignoring it. Before any
commit, also review `git diff --cached` and never stage an environment or token
file. If a secret was ever committed before an ignore rule was added, ignoring
it afterward is not sufficient: revoke that token in Hardcover, remove the
file from Git tracking, and replace the token.

### If something is missing after clone

All source files (including `src/catalog.py`, `src/ingest.py`, etc.) and scripts are intended to be in the repo. If a file is missing (e.g. you had to restore it from git history), check that nothing in `.gitignore` is excluding it (e.g. we do not ignore `src/` or `*.py`) and run `git status` to see untracked or missing files.

## Usage

### Common Workflows

#### First Time Setup (New User)

If you haven’t already, follow the [Quick start](#quick-start-new-users) above. Summary:

**Recommended: use the web interface**

1. Start it with `python web/app.py` and open http://localhost:5000.
2. Choose **Update Library → Import CSV…** and select your CSV.
3. Choose **Update Library → Refresh All Author Catalogs…**.
4. Watch the Library update row for progress and open the Audiobook or Ebook tab when the refresh finishes.

**Command-line alternative**

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
Fetches catalogs for authors represented in imported reading history (rate-limited APIs). Authors created only by old catalog splits or repairs are not fetched.

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

From the web interface, choose **Update Library → Import CSV…**, then **Update Library → Check for New Books**. The latter checks the last year for established author catalogs, fully initializes newly imported authors, and skips eligible catalogs checked within the last seven days. The header shows live progress and refreshes the counts and current view when the job finishes.

Command-line alternative:

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
   - Automatically removes duplicates, multi-book packages, and high-confidence non-English titles
   - Skips catalog-only authors that have no matching imported-history book
   - Detects and prompts to merge duplicate authors

3. **View updated recommendations in web UI:**
   The web interface automatically generates fresh recommendations when you click on the recommendations tabs. No need to run commands separately.

**Scenario B: Quick update - just check for new books by your authors**

```bash
python scripts/bookpilot.py catalog --only-recent --recent-years 3 --auto-cleanup
```
- Checks for books from last 3 years
- Skips authors checked <7 days ago
- Auto-cleans duplicates, multi-book packages, and non-English titles
- Keeps catalog-only split authors from being fetched again
- Shows duplicate authors for review

**Scenario C: Full refresh (force update all history-backed authors)**

```bash
python scripts/bookpilot.py catalog --force --auto-cleanup
```
- Forces refresh of all authors represented in imported reading history (ignores the 7-day check)
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

# Force refresh all history-backed authors (ignore 7-day check)
python scripts/bookpilot.py catalog --force

# Full refresh with cleanup and auto-merge duplicates
python scripts/bookpilot.py catalog --force --auto-cleanup --yes
```

**Options:**
- `--only-recent`: Only fetch books from last N years (for authors that already have catalogs)
- `--recent-years N`: Number of years to look back (default: 3)
- `--force`: Force refresh even if checked within 7 days
- `--auto-cleanup`: Automatically remove duplicates, multi-book packages, and high-confidence non-English titles after fetch
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

#### Audit personal-fit scores

Preview the ranking for a prolific author without modifying the database:

```bash
python scripts/analyze_personal_fit.py --author "Example Author" --limit 50
```

Export an auditable CSV (score, tier, content type, and explanation):

```bash
python scripts/analyze_personal_fit.py --csv data/personal-fit-audit.csv
```

#### Review series books possibly read before Libby tracking

Create a read-only CSV from the current ebook and audiobook recommendation
sets. By default, only authors with more than five remaining recommendations
are inspected:

```bash
python scripts/review_prior_series_reads.py
```

Test specific authors or change the threshold:

```bash
python scripts/review_prior_series_reads.py \
  --author "Example Author One" \
  --author "Example Author Two" \
  --author "Example Author Three" \
  --min-unread 5 \
  --output data/series-review.csv
```

The script uses only titles already eligible for the recommendation pages,
plus known-read books used as series anchors. It does not mark books read or
change the database. Inferred series and prior-read likelihood are included as
evidence for manual review.

For a structured series-by-series comparison, open the **Series** tab and use
**Series Reconciliation**. It selects authors with more than five unique visible
ebook/audiobook recommendations, looks up their ordered series through
Hardcover, and compares the full relevant series with local reads and
recommendations. Nothing runs automatically.

Runs default to ten authors, highest recommendation count first. Use **Run next
batch** to continue without repeating authors already checked, or **Run again**
to discard the saved comparison and start from the current recommendation set.
Results are stored in the local BookPilot database and survive page refreshes.
Use **Ignore for now** on a series you do not want included in later lookups.
Ignoring also applies **Pass** to its unread recommendation titles in both
ebook and audiobook formats, preventing those works from reappearing in either
recommendation list. Restoring the series does not erase that explicit Pass
feedback.

To backfill Pass feedback for series ignored by an older BookPilot version,
preview and then apply the source-backed matches:

```bash
python scripts/backfill_ignored_series_passes.py
python scripts/backfill_ignored_series_passes.py --apply
```
Ignored Hardcover series IDs remain excluded from future runs until restored
from the **Ignored series** control.

The **Reading progress** view can also build a complete Hardcover-backed series
catalog for every locally partial or not-started series. The first build groups
series by author and can use two API calls per uncached author (author resolution
plus the complete series list). BookPilot stays below Hardcover's 60-request per
minute limit by targeting 50 requests per minute, saves progress after every
author, and shows a live call count and estimated time remaining. Cached data is
kept for 180 days and is used without API calls on normal page loads. A later
manual full refresh normally needs only one call per resolved author because the
cached Hardcover author ID is reused.

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

# Preview authors and catalog/list data unsupported by reading history
python scripts/prune_historyless_authors.py --show-authors

# Remove those unsupported author records and their catalog/list data
python scripts/prune_historyless_authors.py --execute
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
1. Restricts catalog work to authors with at least one matching imported-history book
   - Skips old catalog-only split authors without changing their stored data
   - Applies this requirement even when `--force` is used
2. Checks each eligible author's `last_catalog_check` date
   - Skips if checked <7 days ago (unless `--force`)
   - Fetches if never checked or >7 days ago
3. For each author to fetch:
   - **Skips existing books** (by Open Library work key) - saves ~2 API calls per book
   - **Skips old books early** (if using `--only-recent`) - saves ~1 API call per old book
   - Fetches new books from Open Library
   - Matches catalog books to your reading history
4. If `--auto-cleanup` is used:
   - Removes duplicate titles and multi-book packages
   - Applies conservative rule-based and personally learned non-English detection
   - Automatically removes high-confidence language matches and reports medium-confidence matches for review
5. **Automatically detects duplicate authors** and prompts to merge them
6. Updates last catalog check date

#### `recommend` - What It Does
**Note:** This is automatically run when you view recommendations in the web UI. The command-line version is optional.

1. Builds a local preference profile from reading history, already-read matches, saved books, and catalog topics
2. Finds unread books by your authors
3. Filters out:
   - Books you've already read in either ebook or audiobook format
   - Books marked **Pass**, **duplicate**, or **not english**
   - Hidden authors
4. Produces an explainable personal-fit score using author affinity, topic overlap, saved-book signals, series context, and metadata quality
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
- **Status**: Dashboard with book counts, last Libby import, last catalog check, and live Library update progress
- **Update Library**: Import a Libby CSV, check the last year for new books, or run a confirmed full catalog refresh. Only one update runs at a time; completed progress survives page refresh, and result summaries show what changed.
- **Series → Reading progress**: Review partially read and not-started series, build complete Hardcover-backed series catalogs, mark one book or a full series read, and ignore or restore a series.
- **Series → Reconciliation**: Check authors with more than five visible recommendations against structured Hardcover series order in bounded batches. Compare recommendations, reads, and surrounding gaps; use **Mark matched recommendations read** or **Ignore for now**; and manage ignored series from the same view.
- **Audiobook / Ebook recommendations**: Search by author or title; filter with **Strong matches**, **Possible + strong**, **Books only**, or **Likely non-reads**; and sort with **Score High–Low**, **Score Low–High**, **Author Count**, or **Author A–Z**. Author-grouped views can be collapsed, expanded, hidden, and restored. Recommendations are generated when the tab is opened.
- **Books to Read**: Recommendations collected with **Save**, grouped by author
- **Recommendation actions**: **Save**, **Pass**, **already read**, **not english**, **recategorize**, and **duplicate**

**Note:** Recommendations are generated on-demand when you click the recommendations tabs. No need to run `recommend` commands separately unless you prefer command-line access.

Restart a running BookPilot server after pulling code that adds or changes UI
routes. Refreshing the page is enough after reading-history or recommendation
data changes.

Database cleanup, duplicate-author merging, author repair, and other destructive or expert review scripts intentionally remain command-line-only.

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
│   ├── jobs.py             # Single-job background update coordinator
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
- **Reading-history requirement**: Only authors backed by an imported book or audiobook are refreshed automatically; unsupported records remain untouched until explicitly pruned
- **Publisher/company exclusion**: Organization-like author credits—including names that match a legally identified publisher in the Libby export—are skipped during both recent and full catalog refreshes
- **Split-author protection**: Catalog repair and group splitting cannot promote anthology contributors that have no reading-history match
- **7-day skip**: Authors checked <7 days ago are automatically skipped (saves time)
- **Existing book detection**: Books you already have are skipped (saves ~2 API calls per book)
- **Early date filtering**: Old books are skipped before expensive API calls when using `--only-recent`
- **Hybrid optimization**: Can reduce API calls by 50-80% for prolific authors with existing catalogs

### Automatic Cleanup
- **Personalized non-English detection**: Learns conservative title signals from manual tags; auto-removes high-confidence matches and reports medium-confidence matches for review
- **Duplicate removal**: Matches more title and edition variations, then removes duplicate titles while keeping the most complete record
- **Catalog containment**: Skips authors without a reading-history match during refresh; a separate dry-run-first command handles explicit pruning
- **Package cleanup**: Removes box sets, bundles, and numbered multi-book ranges
- **Duplicate author detection**: Automatically detects and prompts to merge duplicate authors after catalog fetch

### Smart Matching
- **Cross-format matching**: An ebook already read is excluded from audiobook recommendations, and vice versa
- **Title normalization**: Handles "The" prefix differences, series annotations, and common audiobook/print edition suffixes
- **Author normalization**: Matches authors even with middle initial differences (e.g., "Julia Kelly" vs "Julia R. Kelly")
- **Already-read detection**: Books in your Libby history automatically mark recommendations as "already read"

### Personal Recommendation Model
- **Local profile**: Uses reading history, already-read matches, catalog topics, saved recommendations, and existing feedback/suppression state
- **Explainable score**: Combines author affinity, topic overlap, series context, and metadata quality into a 0–100 personal-fit score
- **Feedback-aware results**: Saved books strengthen positive signals, while passed, duplicate, non-English, already-read, and hidden items are excluded from recommendation views
- **Ranking controls**: Filter by match tier or likely non-read status and sort scores from high-to-low or low-to-high

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
- `--auto-cleanup`: Auto-remove duplicates, multi-book packages, and high-confidence non-English titles
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
