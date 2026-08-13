"""Build a conservative review queue for books likely read before Libby tracking.

Only books already present in the ebook/audiobook recommendation sets are
eligible for output. Catalog metadata is inspected for those books and for
known-read anchors, but this module never changes the database.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
import json
from pathlib import Path
import re
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from dateutil import parser as date_parser
from sqlalchemy import func
from sqlalchemy.orm import Session

from .history_crosscheck import get_read_title_keys, normalize_work_title
from .models import Author, AuthorCatalogBook, Book, Recommendation
from .recommend import recommend_audiobooks, recommend_new_books


GENERIC_SERIES_PREFIXES = re.compile(
    r"^(?:the\s+|this\s+)?(?:new\s+)?(?:york\s+times\s+)?(?:national\s+)?"
    r"(?:beloved\s+)?(?:bestselling\s+)?(?:acclaimed\s+)?",
    re.IGNORECASE,
)
SERIES_PATTERNS = [
    re.compile(
        r"\b([A-Z][A-Za-z'’&.-]+(?:\s+(?:of\s+|the\s+|and\s+)?[A-Z][A-Za-z'’&.-]+){0,5})"
        r"\s+(?:[Mm]ystery|[Mm]ysteries|[Ss]eries|[Bb]ooks|[Nn]ovels|[Ss]aga|[Aa]dventures?)\b"
    ),
    re.compile(r"\bseries,?\s+(?:THE\s+)?([A-Z][A-Z'’& -]{3,60})(?:,|\.|\s+a\s)", re.MULTILINE),
    re.compile(r"\b(Casebook of [A-Z][A-Za-z'’.-]+(?:\s+[A-Z][A-Za-z'’.-]+){0,3})\b"),
]
GENERIC_LABELS = {
    "new york times", "new york times bestselling", "bestselling",
    "national bestselling", "all new", "latest", "mystery", "series",
}
NUMBER_WORDS = {
    "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
    "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
    "eleventh": 11, "twelfth": 12, "thirteenth": 13, "fourteenth": 14,
    "fifteenth": 15, "sixteenth": 16, "seventeenth": 17,
    "eighteenth": 18, "nineteenth": 19, "twentieth": 20,
}
NAME_PATTERN = re.compile(
    r"\b(?:(?:Lady|Lord|Miss|Mr|Mrs|Captain|Constable|Detective)\s+)?"
    r"[A-Z][a-z'’.-]+(?:\s+[A-Z][a-z'’.-]+){1,2}\b"
)
NAME_STOPWORDS = {"New York", "New York Times", "United States", "Justice Department"}


@dataclass
class ReviewCandidate:
    author: str
    title: str
    formats: Set[str]
    catalog_book_id: Optional[int]


def _canonical_series(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    value = re.sub(r"\s+", " ", label).strip(" .,:;-'\"")
    # Blurbs often say "Author's Series Name series". The possessive author
    # is attribution, not part of the series label.
    if re.search(r"['’]s\s+", value):
        value = re.split(r"['’]s\s+", value)[-1]
    value = GENERIC_SERIES_PREFIXES.sub("", value).strip()
    value = re.sub(r"\s+(?:mystery|mysteries|series|books|novels|saga)$", "", value, flags=re.I)
    if not value or value.casefold() in GENERIC_LABELS or len(value) < 3:
        return None
    return value.title() if value.isupper() else value


def infer_explicit_series(book: AuthorCatalogBook) -> Tuple[Optional[str], str]:
    """Return a series label and the local evidence used to infer it."""
    if book.series_name:
        return _canonical_series(book.series_name), "catalog series metadata"
    text = " ".join(part for part in (book.title, book.description) if part)
    for pattern in SERIES_PATTERNS:
        for match in pattern.finditer(text):
            label = _canonical_series(match.group(1))
            if label:
                return label, f'text says "{match.group(0).strip()}"'
    return None, ""


def infer_position(book: AuthorCatalogBook) -> Optional[int]:
    if book.series_position:
        return book.series_position
    text = " ".join(part for part in (book.title, book.description) if part)
    for pattern in (
        r"\b(?:book|volume|installment|mystery)\s*(?:number|no\.?|#)?\s*(\d{1,2})\b",
        r"\b(\d{1,2})(?:st|nd|rd|th)\s+(?:book|installment|mystery)\b",
    ):
        match = re.search(pattern, text, re.I)
        if match:
            return int(match.group(1))
    match = re.search(
        r"\b(" + "|".join(NUMBER_WORDS) + r")\s+(?:book|installment|mystery)\b",
        text, re.I,
    )
    return NUMBER_WORDS.get(match.group(1).casefold()) if match else None


def publication_year(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"\b(?:18|19|20)\d{2}\b", value)
    if match:
        return int(match.group(0))
    try:
        return date_parser.parse(value, fuzzy=True).year
    except (ValueError, TypeError, OverflowError):
        return None


def _candidate_key(author: str, title: str) -> Tuple[str, str]:
    return author.casefold().strip(), normalize_work_title(title)


def _flatten_ebooks(value) -> List[Dict]:
    if not isinstance(value, dict):
        return list(value)
    flattened = []
    for group in value.values():
        flattened.extend(group)
    return flattened


def load_visible_recommendation_candidates(db_session: Session) -> List[ReviewCandidate]:
    """Load the recommender output and apply the same user suppression flags."""
    raw = [*_flatten_ebooks(recommend_new_books(db_session)), *recommend_audiobooks(db_session)]
    suppressed = db_session.query(Recommendation).filter(
        Recommendation.thumbs_down.is_(True)
        | Recommendation.already_read.is_(True)
        | Recommendation.non_english.is_(True)
        | Recommendation.duplicate.is_(True)
    ).all()
    suppressed_keys = {
        (rec.format or "", *_candidate_key(rec.author or "", rec.title or ""))
        for rec in suppressed
    }
    hidden = {
        author.name.casefold().strip()
        for author in db_session.query(Author).filter(Author.hidden.is_(True)).all()
    }

    combined: Dict[Tuple[str, str], ReviewCandidate] = {}
    for item in raw:
        author = item.get("author", "").strip()
        title = item.get("title", "").strip()
        format_type = item.get("format", "").strip()
        if not author or not title or author.casefold() in hidden:
            continue
        key = _candidate_key(author, title)
        if (format_type, *key) in suppressed_keys:
            continue
        candidate = combined.setdefault(key, ReviewCandidate(
            author=author, title=title, formats=set(),
            catalog_book_id=item.get("catalog_book_id"),
        ))
        candidate.formats.add(format_type)
        candidate.catalog_book_id = candidate.catalog_book_id or item.get("catalog_book_id")
    return list(combined.values())


def _proper_names(text: str) -> Set[str]:
    names = set()
    for match in NAME_PATTERN.finditer((text or "")[:400]):
        name = match.group(0).strip()
        if name not in NAME_STOPWORDS and not name.startswith(("The ", "This ")):
            names.add(name)
    return names


def _assign_series(items: Sequence[AuthorCatalogBook]) -> Dict[int, Tuple[Optional[str], str]]:
    """Infer explicit labels, then propagate through recurring character names."""
    assignments: Dict[int, Tuple[Optional[str], str]] = {}
    explicit_by_label: Dict[str, List[AuthorCatalogBook]] = defaultdict(list)
    for book in items:
        label, evidence = infer_explicit_series(book)
        assignments[book.id] = (label, evidence)
        if label:
            explicit_by_label[label].append(book)

    name_occurrences: Counter = Counter()
    for book in items:
        for name in _proper_names(book.description or ""):
            name_occurrences[name.casefold()] += 1
    name_labels: Dict[str, Set[str]] = defaultdict(set)
    for label, books in explicit_by_label.items():
        name_labels[label.casefold()].add(label)
        for book in books:
            for name in _proper_names(book.description or ""):
                key = name.casefold()
                if name_occurrences[key] >= 2:
                    name_labels[key].add(label)
    signatures = {
        signature: next(iter(labels)) for signature, labels in name_labels.items()
        if len(labels) == 1
    }
    for book in items:
        if assignments[book.id][0]:
            continue
        text = (book.description or "").casefold()
        matches = [(sig, label) for sig, label in signatures.items() if len(sig) >= 5 and sig in text]
        labels = {label for _, label in matches}
        if len(labels) == 1:
            label = next(iter(labels))
            signature = sorted(
                (sig for sig, _ in matches),
                key=lambda sig: (sig == label.casefold(), len(sig)), reverse=True,
            )[0]
            assignments[book.id] = (label, f'recurring character/name "{signature}"')
    return assignments


def build_series_review_rows(
    db_session: Session,
    candidates: Sequence[ReviewCandidate],
    min_unread: int = 5,
    author_names: Optional[Iterable[str]] = None,
) -> List[Dict[str, object]]:
    """Return review-only rows; no database changes are made."""
    requested = {name.casefold().strip() for name in author_names or []}
    by_author: Dict[str, List[ReviewCandidate]] = defaultdict(list)
    for candidate in candidates:
        by_author[candidate.author].append(candidate)
    eligible_names = {
        name for name, values in by_author.items()
        if len(values) > min_unread and (not requested or name.casefold() in requested)
    }
    if not eligible_names:
        return []

    authors = db_session.query(Author).filter(
        func.lower(Author.name).in_([name.casefold() for name in eligible_names])
    ).all()
    results = []
    for author in authors:
        author_candidates = by_author.get(author.name, [])
        candidate_ids = {item.catalog_book_id for item in author_candidates if item.catalog_book_id}
        read_keys = get_read_title_keys(db_session, author)
        catalog_items = db_session.query(AuthorCatalogBook).filter(
            AuthorCatalogBook.author_id == author.id,
            AuthorCatalogBook.id.in_(candidate_ids) | AuthorCatalogBook.is_read.is_(True),
        ).all()
        candidate_by_id = {item.catalog_book_id: item for item in author_candidates if item.catalog_book_id}
        candidate_by_title = {normalize_work_title(item.title): item for item in author_candidates}
        assignments = _assign_series(catalog_items)
        author_keys = {
            value.casefold().strip() for value in (author.name, author.normalized_name) if value
        }
        known_read_titles = {
            item.title for item in db_session.query(Book).filter(func.lower(Book.author).in_(author_keys)).all()
            if item.title
        }
        known_read_titles.update(
            item.title for item in db_session.query(Recommendation).filter(
                func.lower(Recommendation.author).in_(author_keys),
                Recommendation.already_read.is_(True),
            ).all() if item.title
        )

        groups: Dict[str, Dict[str, list]] = defaultdict(lambda: {"candidates": [], "anchors": []})
        for book in catalog_items:
            label, evidence = assignments.get(book.id, (None, ""))
            if not label:
                continue
            is_anchor = book.is_read or normalize_work_title(book.title) in read_keys
            candidate = candidate_by_id.get(book.id) or candidate_by_title.get(normalize_work_title(book.title))
            entry = (book, evidence, candidate)
            if is_anchor:
                groups[label]["anchors"].append(entry)
            elif candidate:
                groups[label]["candidates"].append(entry)

        anchored_labels = {label for label, group in groups.items() if group["anchors"]}
        for label, group in groups.items():
            if not group["candidates"]:
                continue
            anchors = group["anchors"]
            fallback_cluster = not anchored_labels and len(read_keys) >= 2 and len(group["candidates"]) >= 3
            if not anchors and not fallback_cluster:
                continue
            anchor_titles = sorted({entry[0].title for entry in anchors})
            displayed_anchors = anchor_titles if anchors else sorted(known_read_titles)
            anchor_positions = [infer_position(entry[0]) for entry in anchors]
            anchor_positions = [value for value in anchor_positions if value is not None]
            anchor_years = [publication_year(entry[0].publication_date) for entry in anchors]
            anchor_years = [value for value in anchor_years if value is not None]

            for book, evidence, candidate in group["candidates"]:
                position = infer_position(book)
                year = publication_year(book.publication_date)
                if anchors and position is not None and anchor_positions and position <= max(anchor_positions):
                    likelihood, action = "high", "likely read before tracking — confirm"
                    boundary = f"position {position} is at/before known read position {max(anchor_positions)}"
                elif anchors and year is not None and anchor_years and year <= max(anchor_years):
                    likelihood, action = "medium", "possible earlier series read — review"
                    boundary = f"published {year}, at/before known read year {max(anchor_years)}"
                elif anchors:
                    likelihood, action = "medium", "same series as a known read — review"
                    boundary = "series matches known read; position/date boundary unavailable"
                else:
                    likelihood, action = "low", "confirm this is the previously read series"
                    boundary = "coherent series cluster, but known-read title lacks series metadata"
                results.append({
                    "author": author.name,
                    "author_remaining_recommendations": len(author_candidates),
                    "title": book.title,
                    "recommendation_formats": "; ".join(sorted(candidate.formats)),
                    "inferred_series": label,
                    "inferred_position": position or "",
                    "publication_date": book.publication_date or "",
                    "prior_read_likelihood": likelihood,
                    "known_read_anchors": "; ".join(displayed_anchors),
                    "series_evidence": evidence,
                    "boundary_evidence": boundary,
                    "suggested_action": action,
                    "your_decision": "",
                })

    likelihood_order = {"high": 0, "medium": 1, "low": 2}
    results.sort(key=lambda row: (
        row["author"].casefold(), likelihood_order.get(row["prior_read_likelihood"], 9),
        row["inferred_series"].casefold(),
        row["inferred_position"] if isinstance(row["inferred_position"], int) else 999,
        publication_year(row["publication_date"]) or 9999, row["title"].casefold(),
    ))
    return results


def load_official_series_reference(path: str) -> Dict[str, list]:
    """Load ordered, source-attributed series lists researched online."""
    reference_path = Path(path)
    with reference_path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _title_similarity(first: str, second: str) -> Tuple[float, str]:
    first_key = " ".join(re.sub(r"[^a-z0-9]+", " ", normalize_work_title(first)).split())
    second_key = " ".join(re.sub(r"[^a-z0-9]+", " ", normalize_work_title(second)).split())
    if not first_key or not second_key:
        return 0.0, ""
    if first_key == second_key:
        return 1.0, "exact"
    # Retail subtitles and bonus-material suffixes often extend the canonical
    # series title without changing the underlying work.
    if min(len(first_key), len(second_key)) >= 8 and (
        first_key.startswith(second_key + " ") or second_key.startswith(first_key + " ")
    ):
        return 0.97, "title/subtitle"
    ratio = SequenceMatcher(None, first_key, second_key).ratio()
    first_tokens = set(re.findall(r"[a-z0-9]+", first_key))
    second_tokens = set(re.findall(r"[a-z0-9]+", second_key))
    overlap = len(first_tokens & second_tokens) / len(first_tokens | second_tokens)
    return max(ratio, overlap), "fuzzy"


def _best_local_match(reference_title: str, records: Sequence[Dict]) -> Tuple[Optional[Dict], float, str]:
    best_record = None
    best_score = 0.0
    best_method = ""
    for record in records:
        score, method = _title_similarity(reference_title, record["title"])
        if score > best_score:
            best_record, best_score, best_method = record, score, method
    if best_score < 0.86:
        return None, best_score, best_method
    return best_record, best_score, best_method


def build_official_series_rows(
    db_session: Session,
    candidates: Sequence[ReviewCandidate],
    reference: Dict[str, list],
    author_names: Optional[Iterable[str]] = None,
) -> Dict[str, List[Dict[str, object]]]:
    """Map authoritative ordered series lists to reads and recommendations.

    Only reference titles that match a known read or a current recommendation
    are returned. Consequently, a blank ``already_read`` cell means the title
    is present in the current recommendation set, as requested by the review
    workflow.
    """
    requested = {name.casefold().strip() for name in author_names or reference.keys()}
    candidates_by_author: Dict[str, List[ReviewCandidate]] = defaultdict(list)
    for candidate in candidates:
        candidates_by_author[candidate.author.casefold().strip()].append(candidate)

    output: Dict[str, List[Dict[str, object]]] = {}
    for author_name, series_groups in reference.items():
        if author_name.casefold() not in requested:
            continue
        author = db_session.query(Author).filter(func.lower(Author.name) == author_name.casefold()).first()
        if not author:
            output[author_name] = []
            continue
        author_keys = {
            value.casefold().strip() for value in (author.name, author.normalized_name) if value
        }
        read_records = []
        for book in db_session.query(Book).filter(func.lower(Book.author).in_(author_keys)).all():
            read_records.append({"title": book.title, "formats": {book.format or ""}, "kind": "read"})
        for rec in db_session.query(Recommendation).filter(
            func.lower(Recommendation.author).in_(author_keys), Recommendation.already_read.is_(True),
        ).all():
            read_records.append({"title": rec.title, "formats": {rec.format or ""}, "kind": "read"})
        for book in db_session.query(AuthorCatalogBook).filter_by(author_id=author.id, is_read=True).all():
            read_records.append({"title": book.title, "formats": set(), "kind": "read"})

        recommendation_records = [
            {"title": candidate.title, "formats": candidate.formats, "kind": "recommendation"}
            for candidate in candidates_by_author.get(author_name.casefold(), [])
        ]
        rows = []
        for group in series_groups:
            for position, title in group["books"]:
                read_match, read_score, read_method = _best_local_match(title, read_records)
                rec_match, rec_score, rec_method = _best_local_match(title, recommendation_records)
                match = read_match or rec_match
                if not match:
                    continue
                already_read = "Already read" if read_match else ""
                score = read_score if read_match else rec_score
                method = read_method if read_match else rec_method
                rows.append({
                    "series": group["series"],
                    "series_number": position,
                    "book": title,
                    "already_read": already_read,
                    "recommendation_formats": "" if read_match else "; ".join(sorted(rec_match["formats"])),
                    "local_title": match["title"],
                    "match": method if score == 1.0 else f"{method} ({score:.0%})",
                    "source_url": group["source_url"],
                })
        output[author_name] = rows
    return output
