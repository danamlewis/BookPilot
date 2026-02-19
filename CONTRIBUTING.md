# Contributing to BookPilot

Thanks for your interest in contributing.

## How to contribute

- **Bug reports and feature ideas**: Open an [issue](../../issues).
- **Code changes**: Open a pull request. Keep changes focused and add a short description of what and why.

## What we welcome vs. what’s out of scope

BookPilot is focused on **helping you find more things to read, anchored off authors you already know you like.** PRs and feature ideas that fit that focus are encouraged; others are out of scope. If you're not sure, feel free to open an issue and ask before you get started! 

**Examples of great PRs / feature ideas you could work on:**

- **New data sources or data targets** for reading history or metadata, e.g. pulling from **StoryGraph**, or other book/reading exports, as long as they feed into the same “your library, your next reads” workflow. Anything for exporting your data from BookPilot is also welcome.
- **Recommend similar authors** (“if you like Author A, try Author B”) using your existing author list and catalog data.
- **Content-based and similarity features**: e.g. using book descriptions/titles for similarity, genre/category matching, series continuation recommendations, or using thumbs up/down (and other feedback) to improve scoring. The genre data so far isn't great, as you'll see, with the default genres pulled.
- **Discovery improvements**: author-similarity discovery, better series detection and consolidation, reading-pattern visualizations that help you choose what to read next, export/reports (e.g. Markdown/HTML).
- **Data quality and scale**: better deduplication, language detection, and tooling for very large libraries (hundreds of authors).

**Out of scope:**

- **Social and social-like features**: social media integrations, sharing with others, comparing your reading to others, community reviews, feeds, or any feature centered on social/community rather than “your list, your next reads.”
- Anything that shifts away from **personal reading intelligence** (your history, your recommendations) toward a social or general-purpose book platform. 
- Cloud-based hosting. While being able to sync and see this on your phone would be nice, we don't want to turn this into a hosted service. (Unless you want to do so - if so, reach out and let's chat!)

When in doubt, open an issue and describe the idea; we’re happy to say whether it fits.

## Development setup

1. Clone the repo and install dependencies: `pip install -r requirements.txt`
2. Use a separate database for development so you don't touch real data:  
   `python scripts/bookpilot.py --db /tmp/bookpilot_dev.db ingest /path/to/sample.csv`
3. The web app uses `data/bookpilot.db` by default. For CLI work you can use `--db /tmp/bookpilot_dev.db` to keep dev data separate.

## Code and data

- **No personal data in the repo.** All user data (CSV, database, cache) is gitignored. Don't commit `.db`, `.csv`, `data/cache/`, or `.env`.

## License

By contributing, you agree that your contributions will be licensed under the same MIT License as the project.
