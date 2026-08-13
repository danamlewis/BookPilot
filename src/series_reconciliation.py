"""User-triggered comparison of recommendations with structured series order."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from typing import Dict, List, Optional, Sequence

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import Author, AuthorCatalogBook, Book, Recommendation, SystemMetadata
from .series_review import ReviewCandidate, _best_local_match, _title_similarity


RESULT_METADATA_KEY = "series_reconciliation_result"
IGNORED_METADATA_KEY = "series_reconciliation_ignored"
MIN_RECOMMENDATIONS = 5
DEFAULT_BATCH_SIZE = 10
MAX_BATCH_SIZE = 25


def _legacy_hardcover_book_id(series_id: object, position: object, title: object) -> int:
    """Create a stable, non-provider identity for saved rows from before IDs were cached."""
    value = f"{series_id}|{position}|{str(title or '').casefold().strip()}"
    digest = int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:12], 16)
    return 9_000_000_000_000 + digest


def eligible_reconciliation_authors(
    candidates: Sequence[ReviewCandidate],
    *,
    min_recommendations: int = MIN_RECOMMENDATIONS,
) -> List[Dict]:
    """Return authors with strictly more than the requested unique-title count."""
    counts = Counter(candidate.author.strip() for candidate in candidates if candidate.author.strip())
    return [
        {"author": author, "recommendations": count}
        for author, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].casefold()))
        if count > min_recommendations
    ]


def load_reconciliation_result(session: Session) -> Optional[Dict]:
    row = session.query(SystemMetadata).filter_by(key=RESULT_METADATA_KEY).first()
    if not row or not row.value:
        return None
    try:
        value = json.loads(row.value)
    except (TypeError, ValueError):
        return None
    if not isinstance(value, dict):
        return None
    from .series_enrichment import _book_action_key, load_hardcover_book_actions
    actions = load_hardcover_book_actions(session)
    status_rank = {"other": 0, "recommendation": 1, "read": 2}

    def visible_book_rank(book: Dict) -> tuple:
        external_title = str(book.get("book") or "").casefold().strip()
        local_title = str(book.get("local_title") or "").casefold().strip()
        return (
            external_title == local_title and bool(local_title),
            status_rank.get(book.get("status"), 0),
            bool(book.get("hardcover_book_id")),
            -len(external_title),
        )
    for author_result in value.get("authors") or []:
        merged_series = {}
        for series in author_result.get("series") or []:
            series_id = series.get("hardcover_series_id")
            series_key = int(series_id) if series_id else str(series.get("name") or "").casefold().strip()
            existing = merged_series.get(series_key)
            if existing is None:
                existing = {**series, "books": []}
                merged_series[series_key] = existing
            by_book = {}
            for book in existing.get("books") or []:
                position = book.get("series_number")
                identity = (
                    "position", position,
                ) if position is not None else (
                    "title", str(book.get("book") or "").casefold().strip(),
                )
                current = by_book.get(identity)
                if current is None or visible_book_rank(book) > visible_book_rank(current):
                    by_book[identity] = book
            for book in series.get("books") or []:
                book_id = book.get("hardcover_book_id")
                if series_id and not book_id:
                    book_id = _legacy_hardcover_book_id(
                        series_id, book.get("series_number"), book.get("book"),
                    )
                    book["hardcover_book_id"] = book_id
                    book["hardcover_identity_source"] = "legacy_saved_result"
                if series_id and book_id:
                    action = actions.get(_book_action_key(series_id, book_id, book.get("series_number")))
                    if (action or {}).get("action") in {"duplicate", "non_english"}:
                        continue
                position = book.get("series_number")
                identity = (
                    "position", position,
                ) if position is not None else (
                    "title", str(book.get("book") or "").casefold().strip(),
                )
                current = by_book.get(identity)
                if current is None or visible_book_rank(book) > visible_book_rank(current):
                    by_book[identity] = book
            existing["books"] = sorted(by_book.values(), key=lambda book: (
                book.get("series_number") is None,
                book.get("series_number") if book.get("series_number") is not None else 0,
                str(book.get("book") or "").casefold(),
            ))
            existing["recommended_matches"] = len({
                str(book.get("local_title") or "").casefold().strip()
                for book in existing["books"]
                if book.get("status") == "recommendation" and book.get("local_title")
            })
        author_result["series"] = list(merged_series.values())
        author_result["matched_recommendations"] = len({
            str(book.get("local_title") or "").casefold().strip()
            for series in author_result["series"]
            for book in series.get("books") or []
            if book.get("status") == "recommendation" and book.get("local_title")
        })
    return value


def save_reconciliation_result(session: Session, result: Dict) -> None:
    row = session.query(SystemMetadata).filter_by(key=RESULT_METADATA_KEY).first()
    if row is None:
        row = SystemMetadata(key=RESULT_METADATA_KEY)
        session.add(row)
    row.value = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    row.updated_at = datetime.utcnow()
    session.commit()


def load_ignored_series(session: Session) -> List[Dict]:
    row = session.query(SystemMetadata).filter_by(key=IGNORED_METADATA_KEY).first()
    if not row or not row.value:
        return []
    try:
        values = json.loads(row.value)
    except (TypeError, ValueError):
        return []
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, dict) and value.get("series_id")]


def save_ignored_series(session: Session, ignored: Sequence[Dict]) -> None:
    row = session.query(SystemMetadata).filter_by(key=IGNORED_METADATA_KEY).first()
    if row is None:
        row = SystemMetadata(key=IGNORED_METADATA_KEY)
        session.add(row)
    row.value = json.dumps(list(ignored), ensure_ascii=False, separators=(",", ":"))
    row.updated_at = datetime.utcnow()
    session.commit()


def ignore_series(session: Session, *, series_id: int, name: str, author: str) -> List[Dict]:
    ignored = load_ignored_series(session)
    ignored_by_id = {int(row["series_id"]): row for row in ignored}
    ignored_by_id[int(series_id)] = {
        "series_id": int(series_id),
        "name": str(name or "Unnamed series").strip(),
        "author": str(author or "").strip(),
        "ignored_at": datetime.now(timezone.utc).isoformat(),
    }
    output = sorted(ignored_by_id.values(), key=lambda row: (
        str(row.get("author") or "").casefold(), str(row.get("name") or "").casefold(),
    ))
    save_ignored_series(session, output)

    result = load_reconciliation_result(session)
    if result:
        for author_result in result.get("authors") or []:
            author_result["series"] = [
                series for series in author_result.get("series") or []
                if int(series.get("hardcover_series_id") or 0) != int(series_id)
            ]
            author_result["matched_recommendations"] = sum(
                int(series.get("recommended_matches") or 0)
                for series in author_result["series"]
            )
        save_reconciliation_result(session, result)
    return output


def record_ignored_series_passes(
    session: Session,
    *,
    series_id: int,
    titles: Sequence[str],
) -> List[Dict]:
    """Record which recommendation titles were passed for an ignored series."""
    clean_titles = {str(title).strip() for title in titles if str(title).strip()}
    ignored = load_ignored_series(session)
    for row in ignored:
        if int(row.get("series_id") or 0) == int(series_id):
            clean_titles.update(
                str(title).strip() for title in row.get("passed_titles") or [] if str(title).strip()
            )
            ordered_titles = sorted(clean_titles, key=str.casefold)
            row["passed_titles"] = ordered_titles
            row["passed_count"] = len(ordered_titles)
            row["passed_at"] = datetime.now(timezone.utc).isoformat()
            break
    save_ignored_series(session, ignored)
    return ignored


def match_series_recommendations(
    external_series: Dict,
    candidates: Sequence[ReviewCandidate],
    author_name: str,
) -> List[Dict]:
    """Match one external ordered series to current local recommendations."""
    records = [
        {"title": candidate.title, "formats": set(candidate.formats)}
        for candidate in candidates
        if candidate.author.casefold().strip() == author_name.casefold().strip()
    ]
    matches = {}
    for book in external_series.get("books") or []:
        external_title = str(book.get("title") or "")
        for record in records:
            score, method = _title_similarity(external_title, record["title"])
            if score < 0.86:
                continue
            key = record["title"].casefold().strip()
            matches[key] = {
                "title": record["title"],
                "formats": sorted(record["formats"]),
                "match": method if score == 1 else f"{method} ({score:.0%})",
            }
    return sorted(matches.values(), key=lambda row: row["title"].casefold())


def pass_titles_for_both_formats(
    session: Session,
    *,
    author: str,
    titles: Sequence[str],
) -> List[str]:
    """Apply persistent Pass feedback to works in both recommendation formats."""
    clean_titles = sorted({str(title).strip() for title in titles if str(title).strip()}, key=str.casefold)
    author_key = author.casefold().strip()
    now = datetime.utcnow()
    for title in clean_titles:
        title_key = title.casefold().strip()
        for format_type in ("audiobook", "ebook"):
            row = session.query(Recommendation).filter(
                func.lower(func.trim(Recommendation.title)) == title_key,
                func.lower(func.trim(Recommendation.author)) == author_key,
                func.coalesce(Recommendation.format, "") == format_type,
            ).first()
            if row is None:
                row = Recommendation(title=title, author=author, format=format_type)
                session.add(row)
            row.thumbs_up = False
            row.thumbs_down = True
            row.feedback_date = now
    session.commit()
    return clean_titles


def restore_series(session: Session, series_id: int) -> List[Dict]:
    output = [
        row for row in load_ignored_series(session)
        if int(row.get("series_id") or 0) != int(series_id)
    ]
    save_ignored_series(session, output)
    return output


def _local_records(session: Session, author_name: str, candidates: Sequence[ReviewCandidate]):
    author_key = author_name.casefold().strip()
    author = session.query(Author).filter(
        (func.lower(Author.name) == author_key)
        | (func.lower(Author.normalized_name) == author_key)
    ).first()
    author_names = {author_key}
    if author:
        author_names.update(
            value.casefold().strip() for value in (author.name, author.normalized_name) if value
        )

    reads = [
        {"title": book.title, "formats": {book.format} if book.format else set(), "kind": "read"}
        for book in session.query(Book).filter(func.lower(Book.author).in_(author_names)).all()
    ]
    reads.extend({"title": row.title, "formats": {row.format} if row.format else set(), "kind": "read"}
                 for row in session.query(Recommendation).filter(
                     func.lower(Recommendation.author).in_(author_names),
                     Recommendation.already_read.is_(True),
                 ).all())
    if author:
        reads.extend({"title": row.title, "formats": set(), "kind": "read"}
                     for row in session.query(AuthorCatalogBook).filter_by(
                         author_id=author.id, is_read=True,
                     ).all())
    recommendations = [
        {"title": candidate.title, "formats": set(candidate.formats), "kind": "recommendation"}
        for candidate in candidates if candidate.author.casefold().strip() == author_key
    ]
    return reads, recommendations


def reconcile_author(
    session: Session,
    provider,
    author_name: str,
    recommendation_count: int,
    candidates: Sequence[ReviewCandidate],
    ignored_series_ids: Sequence[int] = (),
) -> Dict:
    reads, recommendations = _local_records(session, author_name, candidates)
    lookup = provider.lookup_author_series(
        author_name,
        [record["title"] for record in [*reads, *recommendations]],
        excluded_series_ids=ignored_series_ids,
    )
    provider_author = lookup.get("author")
    series_output = []
    matched_recommendation_titles = set()

    for series in lookup.get("series") or []:
        books = []
        series_recommendations = 0
        for external_book in series.get("books") or []:
            title = str(external_book.get("title") or "").strip()
            if not title:
                continue
            read_match, read_score, read_method = _best_local_match(title, reads)
            rec_match, rec_score, rec_method = _best_local_match(title, recommendations)
            if read_match:
                status, match, score, method = "read", read_match, read_score, read_method
            elif rec_match:
                status, match, score, method = "recommendation", rec_match, rec_score, rec_method
                series_recommendations += 1
                matched_recommendation_titles.add(rec_match["title"].casefold().strip())
            else:
                status, match, score, method = "other", None, 0, ""
            books.append({
                "book": title,
                "hardcover_book_id": external_book.get("hardcover_book_id"),
                "series_number": external_book.get("position"),
                "status": status,
                "local_title": match["title"] if match else "",
                "formats": sorted(match["formats"]) if match and status == "recommendation" else [],
                "match": method if score == 1 else f"{method} ({score:.0%})" if match else "",
            })

        deduped_books = {}
        status_rank = {"other": 0, "recommendation": 1, "read": 2}
        for book in books:
            position = book.get("series_number")
            key = (
                "position", position,
            ) if position is not None else (
                "title", book.get("book", "").casefold().strip(),
            )
            current = deduped_books.get(key)
            rank = (
                str(book.get("book") or "").casefold().strip()
                == str(book.get("local_title") or "").casefold().strip()
                and bool(book.get("local_title")),
                status_rank.get(book.get("status"), 0),
                bool(book.get("hardcover_book_id")),
                -len(str(book.get("book") or "")),
            )
            current_rank = (
                str(current.get("book") or "").casefold().strip()
                == str(current.get("local_title") or "").casefold().strip()
                and bool(current.get("local_title")),
                status_rank.get(current.get("status"), 0),
                bool(current.get("hardcover_book_id")),
                -len(str(current.get("book") or "")),
            ) if current else None
            if current is None or rank > current_rank:
                deduped_books[key] = book
        books = list(deduped_books.values())
        series_recommendations = len({
            book["local_title"].casefold().strip()
            for book in books
            if book.get("status") == "recommendation" and book.get("local_title")
        })

        # This workflow is about reconciling current recommendations. Known
        # series with no recommendation match are not actionable and stay out.
        if not series_recommendations:
            continue
        slug = series.get("slug")
        series_output.append({
            "name": series.get("name") or "Unnamed series",
            "hardcover_series_id": series.get("hardcover_series_id"),
            "source_url": f"https://hardcover.app/series/{slug}" if slug else "https://hardcover.app",
            "recommended_matches": series_recommendations,
            "books": books,
        })

    unmatched = [
        record["title"] for record in recommendations
        if record["title"].casefold().strip() not in matched_recommendation_titles
    ]
    return {
        "author": author_name,
        "provider_author": provider_author.get("name") if provider_author else None,
        "recommendation_count": recommendation_count,
        "matched_recommendations": len(matched_recommendation_titles),
        "unmatched_recommendations": unmatched,
        "series": series_output,
        "lookup_status": "matched" if provider_author else "author_not_found",
    }


def run_series_reconciliation(
    session: Session,
    provider,
    candidates: Sequence[ReviewCandidate],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    reset: bool = True,
    progress_callback=None,
) -> Dict:
    batch_size = min(MAX_BATCH_SIZE, max(1, int(batch_size)))
    eligible = eligible_reconciliation_authors(candidates)
    previous = None if reset else load_reconciliation_result(session)
    existing_authors = list((previous or {}).get("authors") or [])
    processed = {row.get("author", "").casefold() for row in existing_authors}
    pending = [row for row in eligible if row["author"].casefold() not in processed]
    batch = pending[:batch_size]
    errors = [] if reset else list((previous or {}).get("errors") or [])
    ignored_series_ids = [int(row["series_id"]) for row in load_ignored_series(session)]
    by_author = {row.get("author", "").casefold(): row for row in existing_authors}

    total = len(batch)
    for index, item in enumerate(batch, 1):
        author_name = item["author"]
        if progress_callback:
            progress_callback(
                message=f"Looking up series for {author_name} — {index} of {total}",
                current=index - 1,
                total=total,
            )
        try:
            by_author[author_name.casefold()] = reconcile_author(
                session, provider, author_name, item["recommendations"], candidates,
                ignored_series_ids,
            )
        except Exception as exc:
            errors.append({"author": author_name, "error": str(exc)})
            by_author[author_name.casefold()] = {
                "author": author_name,
                "recommendation_count": item["recommendations"],
                "matched_recommendations": 0,
                "unmatched_recommendations": [],
                "series": [],
                "lookup_status": "error",
            }
        if progress_callback:
            progress_callback(current=index, total=total)

    eligible_names = {row["author"].casefold() for row in eligible}
    authors = [
        by_author[row["author"].casefold()] for row in eligible
        if row["author"].casefold() in by_author
    ]
    processed_count = len({row.get("author", "").casefold() for row in authors} & eligible_names)
    result = {
        "provider": "Hardcover",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "threshold": MIN_RECOMMENDATIONS,
        "batch_size": batch_size,
        "eligible_authors": len(eligible),
        "processed_authors": processed_count,
        "remaining_authors": max(0, len(eligible) - processed_count),
        "authors": authors,
        "errors": errors,
    }
    save_reconciliation_result(session, result)
    return result
