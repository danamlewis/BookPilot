#!/usr/bin/env python3
"""Rank low-personal-fit recommendations by likelihood of being non-English.

By default this uses the same generated ebook/audiobook lists and visibility
filters as the web app. This is a review-only audit: it never changes flags.
It combines the human-flag-trained personalized model with deliberately
skeptical learned-title signals, then adds a modest bonus when an author has
many low-score recommendations.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime
import math
from pathlib import Path
import re
import sys
from typing import Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.history_crosscheck import (
    get_non_english_title_keys,
    get_read_title_keys,
    normalize_work_title,
)
from src.models import Author, AuthorCatalogBook, Book, Recommendation, get_session, init_db
from src.personal_language_cleanup import get_training_examples
from src.personal_language_review import PersonalLanguageModel, normalize_language_title
from src.preference_scoring import build_preference_profile, score_catalog_item
from src.recommend import count_books_by_author, recommend_audiobooks, recommend_new_books


CONFIDENCE_ORDER = {"very_high": 4, "high": 3, "medium": 2, "low": 1, "very_low": 0}
TRANSLITERATION_RE = re.compile(r"[ʻʼʾʿṭḥḳṿṣẓḤḲṾṢẒ]")
LATIN_DIACRITIC_RE = re.compile(r"[\u00c0-\u024f]")
WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)
GENERIC_CATALOG_TOKENS = {
    "author", "book", "books", "boxed", "collection", "edition", "export",
    "hardcover", "novel", "paperback", "pocket", "published", "publisher",
    "reissue", "set", "unabridged", "volume", "written",
}

# These patterns are intentionally broader than the production filter. A match
# raises a review score; it never auto-flags or deletes a book.
LANGUAGE_PATTERNS = {
    "French-looking words": re.compile(
        r"\b(?:aux|avec|bonheur|coeur|femme|foudre|jours|maison|pour|sans|travers(?:e|é)es|vie)\b",
        re.IGNORECASE,
    ),
    "Spanish-looking words": re.compile(
        r"\b(?:azul|empezar|estrella|luz|nuevo|rescate|sangre|sue(?:n|ñ)o)\b",
        re.IGNORECASE,
    ),
    "Italian-looking words": re.compile(
        r"\b(?:amante|dello|lunga|promessa|strada|verso)\b",
        re.IGNORECASE,
    ),
    "German-looking words": re.compile(
        r"\b(?:gl(?:u|ü)ck|regenbogen|unter|vertauschte[rs]?)\b",
        re.IGNORECASE,
    ),
    "Dutch-looking words": re.compile(
        r"\b(?:adel|jezelf|trouw|vrouw)\b",
        re.IGNORECASE,
    ),
    "Polish-looking words": re.compile(
        r"\b(?:bezcenne|bezpieczna|przysta[nń]|wypadek)\b",
        re.IGNORECASE,
    ),
    "Portuguese-looking words": re.compile(
        r"\b(?:espelho|imagem|invis(?:i|í)vel)\b",
        re.IGNORECASE,
    ),
    "Hebrew transliteration": re.compile(
        r"(?:\b(?:ha|le|mi|be)-|\b(?:devarim|mifgashim|mikhtav|sodot|te.unah)\b)",
        re.IGNORECASE,
    ),
}


def title_tokens(title: str) -> set[str]:
    """Return normalized word tokens, retaining Latin diacritics."""
    return set(WORD_RE.findall(normalize_language_title(title)))


class CynicalTokenModel:
    """Learn permissive language-token evidence from human flags only."""

    def __init__(self, flagged_examples: Sequence[tuple[str, str]], english_titles: Iterable[str]):
        positive = Counter()
        negative = Counter()
        flagged_examples = list(flagged_examples)
        english_titles = list(english_titles)
        for _, title in flagged_examples:
            positive.update(title_tokens(title))
        for title in english_titles:
            negative.update(title_tokens(title))

        positive_total = max(1, len(flagged_examples))
        negative_total = max(1, len(english_titles))
        self.weights: dict[str, float] = {}
        for token, positive_count in positive.items():
            if positive_count < 2 or len(token) < 2 or token in GENERIC_CATALOG_TOKENS:
                continue
            # Smoothed document-frequency log odds. Unlike the production model,
            # this review model does not require a token to occur under two authors.
            weight = math.log((positive_count + 1) / (positive_total + 2)) - math.log(
                (negative[token] + 1) / (negative_total + 2)
            )
            if weight >= 1.25 and negative[token] <= max(2, positive_count // 3):
                self.weights[token] = min(4.0, weight)

    def assess(self, title: str) -> tuple[float, list[str]]:
        matches = sorted(
            ((token, self.weights[token]) for token in title_tokens(title) if token in self.weights),
            key=lambda item: item[1],
            reverse=True,
        )
        # One recurring proper noun is not language evidence (for example
        # "Emma"). Require a small constellation of learned tokens; explicit
        # one-word language signals are handled by LANGUAGE_PATTERNS instead.
        if len(matches) < 2:
            return 0.0, []
        raw = sum(weight for _, weight in matches)
        points = min(45.0, raw * 8.0)
        reasons = [f"flag-learned token {token!r} ({weight:.2f})" for token, weight in matches[:6]]
        return points, reasons


def title_signal_score(title: str) -> tuple[float, list[str]]:
    """Return cynical title-only evidence not covered by the production detector."""
    points = 0.0
    reasons: list[str] = []
    transliteration_count = len(TRANSLITERATION_RE.findall(title))
    if transliteration_count:
        added = min(45.0, 24.0 + transliteration_count * 6.0)
        points += added
        reasons.append(f"{transliteration_count} transliteration character(s) (+{added:.0f})")

    diacritics = LATIN_DIACRITIC_RE.findall(title)
    if diacritics:
        added = min(25.0, 6.0 + len(diacritics) * 4.0)
        points += added
        reasons.append(f"{len(diacritics)} Latin diacritic(s) (+{added:.0f})")

    for label, pattern in LANGUAGE_PATTERNS.items():
        matches = sorted({match.group(0) for match in pattern.finditer(title)}, key=str.casefold)
        if matches:
            added = min(38.0, 20.0 + 7.0 * (len(matches) - 1))
            points += added
            reasons.append(f"{label}: {', '.join(matches)} (+{added:.0f})")
    return min(60.0, points), reasons


def confidence_for_score(score: float) -> str:
    if score >= 85:
        return "very_high"
    if score >= 65:
        return "high"
    if score >= 45:
        return "medium"
    if score >= 25:
        return "low"
    return "very_low"


def personalized_component(confidence: str, score: float, reasons: Sequence[str]) -> tuple[float, list[str]]:
    """Discount known personalized-model false-positive shapes in review output."""
    if not reasons or confidence == "none":
        return 0.0, []
    if any(reason.startswith("near match to flagged title") for reason in reasons):
        # Accent-only variants can be near-identical to an English original.
        # Keep them reviewable, not decisive.
        return 38.0, [f"personalized near-match warning: {reason}" for reason in reasons]
    learned_tokens = {
        match.group(1).casefold()
        for reason in reasons
        if (match := re.search(r"learned phrase: ['\"]([^'\"]+)", reason))
    }
    if learned_tokens and learned_tokens <= GENERIC_CATALOG_TOKENS:
        return 0.0, []
    meaningful_learned = learned_tokens - GENERIC_CATALOG_TOKENS
    if learned_tokens and len(meaningful_learned) < 2 and not any(" " in token for token in meaningful_learned):
        return 0.0, []
    if confidence == "high":
        return min(82.0, 65.0 + score), [f"personalized high: {reason}" for reason in reasons]
    if confidence == "medium":
        return min(48.0, 25.0 + score * 3.0), [f"personalized medium: {reason}" for reason in reasons]
    return 0.0, []


def score_language_risk(
    *,
    match_score: int,
    personalized_confidence: str,
    personalized_score: float,
    personalized_reasons: Sequence[str],
    learned_token_points: float,
    learned_token_reasons: Sequence[str],
    title_points: float,
    title_reasons: Sequence[str],
    author_low_count: int,
    author_signal_count: int,
    prolific_threshold: int = 5,
) -> tuple[int, str, float, float, list[str]]:
    """Combine detector evidence into a transparent 0-100 review score."""
    components = []
    reasons: list[str] = []
    personal_points, personal_review_reasons = personalized_component(
        personalized_confidence, personalized_score, personalized_reasons
    )
    if personal_points:
        components.append(personal_points)
        reasons.extend(personal_review_reasons)
    if learned_token_points:
        components.append(learned_token_points)
    if title_points:
        components.append(title_points)

    # Independent checks corroborate one another, but overlapping evidence is
    # not simply summed; that made generic catalog words look definitive.
    components.sort(reverse=True)
    evidence = components[0] if components else 0.0
    if len(components) > 1:
        evidence = min(90.0, evidence + min(15.0, sum(components[1:]) * 0.15))
    reasons.extend(learned_token_reasons)
    reasons.extend(title_reasons)

    low_fit_bonus = min(10.0, max(0.0, (50 - match_score) / 5.0))
    prolific_bonus = 0.0
    if author_low_count > prolific_threshold:
        prolific_bonus = min(15.0, 5.0 + (author_low_count - prolific_threshold) * 0.5)
        reasons.append(f"author has {author_low_count} titles below 50 (+{prolific_bonus:.1f})")
    cluster_bonus = 0.0
    if author_signal_count >= 3:
        cluster_bonus = min(15.0, 5.0 + (author_signal_count - 2) * 2.0)
        reasons.append(f"{author_signal_count} low-score titles by author have language signals (+{cluster_bonus:.1f})")
    if low_fit_bonus:
        reasons.append(f"personal-fit score {match_score} (+{low_fit_bonus:.1f} skepticism)")

    score = round(min(100.0, evidence + low_fit_bonus + prolific_bonus + cluster_bonus))
    return score, confidence_for_score(score), prolific_bonus, cluster_bonus, reasons


def collect_low_score_rows(
    session,
    threshold: int,
    include_flagged: bool,
    author_filter: str | None,
    excluded_authors: set[str] | None = None,
    source: str = "recommendations",
):
    if source == "recommendations":
        return collect_recommendation_rows(session, threshold, author_filter, excluded_authors)

    profile = build_preference_profile(session)
    rows = []
    authors = session.query(Author).all()
    excluded_keys = {name.casefold() for name in excluded_authors or set()}
    if excluded_keys:
        authors = [author for author in authors if author.name.casefold() not in excluded_keys]
    if author_filter:
        needle = author_filter.casefold()
        authors = [author for author in authors if needle in author.name.casefold()]

    for author in authors:
        read_keys = get_read_title_keys(session, author)
        flagged_keys = get_non_english_title_keys(session, author)
        read_count = count_books_by_author(session, author.normalized_name, author.name)
        catalog = session.query(AuthorCatalogBook).filter_by(author_id=author.id, is_read=False).all()
        for book in catalog:
            work_key = normalize_work_title(book.title)
            if not work_key or work_key in read_keys:
                continue
            currently_flagged = work_key in flagged_keys
            if currently_flagged and not include_flagged:
                continue
            fit = score_catalog_item(profile, book, read_count)
            if fit["match_score"] >= threshold:
                continue
            rows.append({
                "book": book,
                "author": author,
                "read_count": read_count,
                "currently_flagged": currently_flagged,
                "recommendation_format": "catalog",
                **fit,
            })
    return rows


def _flatten_recommendations(items):
    if isinstance(items, dict):
        return [item for group in items.values() for item in group]
    return list(items)


def collect_recommendation_rows(
    session,
    threshold: int,
    author_filter: str | None,
    excluded_authors: set[str] | None = None,
):
    """Mirror the visible ebook/audiobook API filters before language review."""
    recommendations = [
        *_flatten_recommendations(recommend_new_books(session, category=None)),
        *recommend_audiobooks(session),
    ]
    hidden_keys = {
        author.name.casefold().strip()
        for author in session.query(Author).filter(Author.hidden.is_(True)).all()
    }
    filtered_by_format = {}
    for format_name in ("ebook", "audiobook"):
        feedback = session.query(Recommendation).filter(
            Recommendation.format == format_name,
            (
                Recommendation.thumbs_down.is_(True)
                | Recommendation.already_read.is_(True)
                | Recommendation.non_english.is_(True)
                | Recommendation.duplicate.is_(True)
            ),
        ).all()
        filtered_by_format[format_name] = {
            ((item.title or "").casefold().strip(), (item.author or "").casefold().strip())
            for item in feedback
        }

    excluded_keys = {name.casefold() for name in excluded_authors or set()}
    author_needle = author_filter.casefold() if author_filter else None
    catalog_ids = {item.get("catalog_book_id") for item in recommendations if item.get("catalog_book_id")}
    catalog_by_id = {
        book.id: book
        for book in session.query(AuthorCatalogBook).filter(AuthorCatalogBook.id.in_(catalog_ids)).all()
    }
    authors_by_id = {
        author.id: author
        for author in session.query(Author).filter(
            Author.id.in_({book.author_id for book in catalog_by_id.values()})
        ).all()
    }

    rows = []
    seen = set()
    for item in recommendations:
        format_name = item.get("format") or "unknown"
        title_key = (item.get("title") or "").casefold().strip()
        author_key = (item.get("author") or "").casefold().strip()
        catalog_id = item.get("catalog_book_id")
        unique_key = (format_name, catalog_id or (author_key, title_key))
        if unique_key in seen:
            continue
        seen.add(unique_key)
        if (title_key, author_key) in filtered_by_format.get(format_name, set()):
            continue
        if author_key in hidden_keys or author_key in excluded_keys:
            continue
        if author_needle and author_needle not in author_key:
            continue
        if item["match_score"] >= threshold:
            continue
        book = catalog_by_id.get(catalog_id)
        author = authors_by_id.get(book.author_id) if book else None
        if not book or not author:
            continue
        rows.append({
            **item,
            "book": book,
            "author": author,
            "read_count": item.get("books_by_author_count", 0),
            "currently_flagged": False,
            "recommendation_format": format_name,
        })
    return rows


def build_report_rows(
    session,
    threshold: int,
    include_flagged: bool,
    author_filter: str | None,
    prolific_threshold: int,
    excluded_authors: set[str] | None = None,
    source: str = "recommendations",
):
    low_rows = collect_low_score_rows(
        session, threshold, include_flagged, author_filter, excluded_authors, source
    )
    counts = Counter((row["recommendation_format"], row["author"].id) for row in low_rows)
    all_format_counts = Counter(row["author"].id for row in low_rows)
    flagged_examples = get_training_examples(session)
    english_titles = [book.title for book in session.query(Book).all() if book.title]
    personal_model = PersonalLanguageModel(flagged_examples, english_titles)
    token_model = CynicalTokenModel(flagged_examples, english_titles)

    assessed = []
    signal_counts = Counter()
    for row in low_rows:
        book = row["book"]
        personal = personal_model.assess(book.title)
        token_points, token_reasons = token_model.assess(book.title)
        title_points, title_reasons = title_signal_score(book.title)
        personal_points, _ = personalized_component(personal.confidence, personal.score, personal.reasons)
        base_has_signal = bool(
            personal_points >= 25
            or token_points >= 12
            or title_points >= 20
        )
        if base_has_signal:
            signal_counts[(row["recommendation_format"], row["author"].id)] += 1
        assessed.append((row, personal, token_points, token_reasons, title_points, title_reasons))

    report = []
    for row, personal, token_points, token_reasons, title_points, title_reasons in assessed:
        author = row["author"]
        book = row["book"]
        count_key = (row["recommendation_format"], author.id)
        score, confidence, prolific_bonus, cluster_bonus, reasons = score_language_risk(
            match_score=row["match_score"],
            personalized_confidence=personal.confidence,
            personalized_score=personal.score,
            personalized_reasons=personal.reasons,
            learned_token_points=token_points,
            learned_token_reasons=token_reasons,
            title_points=title_points,
            title_reasons=title_reasons,
            author_low_count=counts[count_key],
            author_signal_count=signal_counts[count_key],
            prolific_threshold=prolific_threshold,
        )
        report.append({
            "rank": 0,
            "non_english_score": score,
            "non_english_confidence": confidence,
            "personal_fit_score": row["match_score"],
            "recommendation_format": row["recommendation_format"],
            "books_read_by_author": row["read_count"],
            "author": author.name,
            "title": book.title,
            "isbn": book.isbn or "",
            "open_library_key": book.open_library_key or "",
            "catalog_book_id": book.id,
            "currently_flagged_non_english": row["currently_flagged"],
            "format_author_low_score_count": counts[count_key],
            "all_formats_author_low_score_count": all_format_counts[author.id],
            "author_language_signal_count": signal_counts[count_key],
            "prolific_author_bonus": f"{prolific_bonus:.1f}",
            "author_language_cluster_bonus": f"{cluster_bonus:.1f}",
            "personalized_confidence": personal.confidence,
            "personalized_raw_score": f"{personal.score:.3f}",
            "personalized_reasons": "; ".join(personal.reasons),
            "learned_token_points": f"{token_points:.1f}",
            "title_signal_points": f"{title_points:.1f}",
            "interest_tier": row["interest_tier"],
            "content_type": row["content_type"],
            "personal_fit_reason": row["score_reason"],
            "non_english_reasons": "; ".join(reasons),
        })

    report.sort(key=lambda item: (
        -item["non_english_score"],
        -CONFIDENCE_ORDER[item["non_english_confidence"]],
        -item["format_author_low_score_count"],
        item["personal_fit_score"],
        item["recommendation_format"],
        item["author"].casefold(),
        item["title"].casefold(),
    ))
    for rank, item in enumerate(report, 1):
        item["rank"] = rank
    return report, len(flagged_examples), len(token_model.weights)


def write_report(rows: list[dict], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0]) if rows else [
        "rank", "non_english_score", "non_english_confidence", "personal_fit_score",
        "recommendation_format", "author", "title", "isbn", "open_library_key", "catalog_book_id",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(__file__).resolve().parents[1] / "data" / "bookpilot.db"))
    parser.add_argument("--score-below", type=int, default=50, help="Review personal-fit scores below this value.")
    parser.add_argument("--prolific-threshold", type=int, default=5)
    parser.add_argument(
        "--source", choices=("recommendations", "catalog"), default="recommendations",
        help="Review visible ebook/audiobook recommendations (default) or the full catalog.",
    )
    parser.add_argument("--include-flagged", action="store_true", help="With --source catalog, include already flagged titles.")
    parser.add_argument("--author", help="Limit to an author name substring.")
    parser.add_argument(
        "--exclude-author",
        action="append",
        default=[],
        help="Exclude an exact author name; may be repeated.",
    )
    parser.add_argument("--output", help="CSV path; defaults to a timestamped file beside the database.")
    parser.add_argument("--limit", type=int, default=40, help="Number of top rows printed to the terminal.")
    args = parser.parse_args()
    if not 1 <= args.score_below <= 101:
        parser.error("--score-below must be between 1 and 101")

    session = get_session(init_db(args.db))
    try:
        excluded_authors = set(args.exclude_author)
        rows, training_count, learned_token_count = build_report_rows(
            session,
            threshold=args.score_below,
            include_flagged=args.include_flagged,
            author_filter=args.author,
            prolific_threshold=args.prolific_threshold,
            excluded_authors=excluded_authors,
            source=args.source,
        )
    finally:
        session.close()

    output = Path(args.output) if args.output else Path(args.db).resolve().parent / (
        f"low_score_non_english_review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    write_report(rows, output)
    confidence_counts = Counter(row["non_english_confidence"] for row in rows)
    print(
        f"Reviewed {len(rows)} {args.source} rows with personal-fit score < {args.score_below}; "
        f"trained from {training_count} human flags and {learned_token_count} skeptical tokens."
    )
    if excluded_authors:
        print(f"Excluded {len(excluded_authors)} named authors from this report.")
    print("Confidence: " + ", ".join(
        f"{label}={confidence_counts[label]}"
        for label in ("very_high", "high", "medium", "low", "very_low")
    ))
    for row in rows[: args.limit]:
        print(
            f"{row['rank']:>4}. NE {row['non_english_score']:>3} [{row['non_english_confidence']:<9}] "
            f"fit {row['personal_fit_score']:>2} | {row['recommendation_format']:<9} | "
            f"{row['author']} — {row['title']}"
        )
    print(f"Review CSV: {output}")
    print("Dry run only; no database flags were changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
