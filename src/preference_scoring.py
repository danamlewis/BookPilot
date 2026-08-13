"""Transparent, local preference scoring for catalog recommendations.

The scorer intentionally has no external model dependency.  It builds a small
content profile from books already read (plus catalog metadata for matched
works), then combines topic overlap with author affinity and series signals.
Course materials are classified separately so they can be batched in the UI
without teaching the preference model that an otherwise-liked author is bad.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import math
import re
from typing import Dict, Optional, Set

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import AuthorCatalogBook, Book, Recommendation


STOPWORDS = {
    "a", "an", "and", "as", "at", "be", "by", "for", "from", "how",
    "in", "into", "is", "it", "of", "on", "or", "that", "the", "their",
    "this", "to", "with", "your", "volume", "book", "novel",
}

# High precision: these are product/course artifacts rather than ordinary
# trade books.  Keeping this independent from the interest score is important.
COURSE_MATERIAL_PATTERNS = [
    r"\baccess (?:card|code)\b", r"\blaunchpad\b", r"\bsaplingplus\b",
    r"\beconportal\b", r"\biclicker\b", r"\bstudent remote\b",
    r"\bloose[- ]leaf\b", r"\binstructor(?:'s)? manual\b",
    r"\bteacher(?:'s)? edition\b", r"\btest bank\b", r"\bcoursepack\b",
    r"\bstudy guide\b", r"\bworkbook\b", r"\btextbook\b",
    r"\b(?:principles of )?(?:micro|macro)economics\b",
    r"\bmodern principles(?: of economics)?\b",
]

EDITION_PATTERNS = [
    r"\bunabridged selections?\b", r"\babridged edition\b",
    r"\bsummary (?:of|and analysis)\b", r"\bcollector'?s edition\b",
    r"\bbox(?:ed)? set\b", r"\bbundle\b", r"\b\d+(?:st|nd|rd|th) edition\b",
]


def tokenize(text: Optional[str]) -> Set[str]:
    """Return conservative content tokens suitable for sparse metadata."""
    if not text:
        return set()
    words = re.findall(r"[a-z][a-z'-]{2,}", text.lower())
    return {word.strip("'-") for word in words if word not in STOPWORDS}


def classify_catalog_item(title: str, categories: str = "", description: str = "") -> Dict[str, str]:
    """Classify artifacts that should be reviewable as a separate batch."""
    haystack = " ".join(part for part in (title, categories, description) if part).lower()
    for pattern in COURSE_MATERIAL_PATTERNS:
        if re.search(pattern, haystack):
            return {
                "content_type": "course_material",
                "content_label": "Textbook / course material",
                "content_reason": "Title or metadata looks like a textbook, study aid, or course-access product.",
            }
    for pattern in EDITION_PATTERNS:
        if re.search(pattern, haystack):
            return {
                "content_type": "edition_or_bundle",
                "content_label": "Edition / bundle",
                "content_reason": "Title looks like a special edition, summary, or multi-book bundle.",
            }
    return {
        "content_type": "trade_book",
        "content_label": "Book",
        "content_reason": "No textbook or product-edition signals detected.",
    }


@dataclass
class PreferenceProfile:
    token_weights: Counter
    max_author_count: int
    saved_title_tokens: Set[str]
    max_token_weight: float


def build_preference_profile(db_session: Session) -> PreferenceProfile:
    """Build a reusable profile from local reading history and explicit saves."""
    weights: Counter = Counter()

    for book in db_session.query(Book).all():
        for token in tokenize(book.title):
            weights[token] += 1.0

    # Matched catalog rows add the categories/descriptions missing from Libby.
    matched = db_session.query(AuthorCatalogBook).filter(AuthorCatalogBook.is_read == True).all()
    for item in matched:
        for token in tokenize(item.categories):
            weights[token] += 2.0
        for token in tokenize(item.description):
            weights[token] += 0.15

    saved_tokens: Set[str] = set()
    saved = db_session.query(Recommendation).filter(Recommendation.thumbs_up == True).all()
    for item in saved:
        item_tokens = tokenize(item.title) | tokenize(item.category)
        saved_tokens.update(item_tokens)
        for token in item_tokens:
            weights[token] += 4.0

    counts = [count for _, count in db_session.query(Book.author, func.count(Book.id)).group_by(Book.author).all()]
    return PreferenceProfile(
        weights,
        max(counts, default=1),
        saved_tokens,
        max(weights.values(), default=0.0),
    )


def _topic_match(profile: PreferenceProfile, item: AuthorCatalogBook) -> float:
    title_tokens = tokenize(item.title)
    category_tokens = tokenize(item.categories)
    description_tokens = tokenize(item.description)
    weighted_tokens = [(token, 2.0) for token in category_tokens]
    weighted_tokens += [(token, 1.0) for token in title_tokens]
    weighted_tokens += [(token, 0.25) for token in description_tokens]
    if not weighted_tokens or not profile.token_weights:
        return 0.0

    numerator = sum(math.log1p(profile.token_weights.get(token, 0.0)) * weight for token, weight in weighted_tokens)
    denominator = sum(math.log1p(profile.max_token_weight) * weight for _, weight in weighted_tokens)
    score = numerator / denominator if denominator else 0.0
    if title_tokens & profile.saved_title_tokens:
        score = min(1.0, score + 0.15)
    return score


def score_catalog_item(
    profile: PreferenceProfile,
    item: AuthorCatalogBook,
    books_by_author_count: int,
) -> Dict[str, object]:
    """Return a 0-100 personal-fit score plus explainable components."""
    classification = classify_catalog_item(item.title, item.categories or "", item.description or "")
    # Reading even one book is meaningful enough to place an author in this
    # catalog.  Additional reads increase confidence but must not swamp the
    # within-author content signals (especially for authors with only one read).
    author_affinity = 0.40 + 0.60 * (
        math.log1p(books_by_author_count) / math.log1p(max(profile.max_author_count, 1))
    )
    topic_match = _topic_match(profile, item)
    metadata_quality = min(1.0, (bool(item.categories) + bool(item.description) + bool(item.isbn)) / 3.0)
    series_bonus = 1.0 if item.series_name else 0.0

    raw = 100.0 * (
        0.48 * author_affinity
        + 0.34 * topic_match
        + 0.10 * metadata_quality
        + 0.08 * series_bonus
    )
    if classification["content_type"] == "course_material":
        raw -= 45.0
    elif classification["content_type"] == "edition_or_bundle":
        raw -= 18.0
    score = max(0, min(100, round(raw)))

    if classification["content_type"] == "course_material":
        tier = "batch"
        label = "Likely non-read"
    elif score >= 62:
        tier = "strong"
        label = "Strong match"
    elif score >= 38:
        tier = "possible"
        label = "Possible match"
    else:
        tier = "low"
        label = "Low confidence"

    reasons = [f"{books_by_author_count} book{'s' if books_by_author_count != 1 else ''} read by this author"]
    if topic_match >= 0.45:
        reasons.append("topics overlap with your reading history")
    elif not item.categories and not item.description:
        reasons.append("limited catalog metadata")
    if item.series_name:
        reasons.append("part of a known series")
    if classification["content_type"] != "trade_book":
        reasons.append(classification["content_reason"])

    return {
        "similarity_score": score / 100.0,
        "match_score": score,
        "interest_tier": tier,
        "interest_label": label,
        "score_reason": "; ".join(reasons),
        **classification,
    }
