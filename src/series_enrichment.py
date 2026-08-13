"""Persistent Hardcover enrichment for locally identified reading-progress series."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import hashlib
import json
import re
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import or_

from .models import SystemMetadata
from .series_review import _best_local_match


CACHE_PREFIX = "hardcover_series_cache:"
BOOK_ACTIONS_METADATA_KEY = "hardcover_series_book_actions"
CACHE_DAYS = 180
CACHE_SCHEMA_VERSION = 2
SAFE_REQUESTS_PER_MINUTE = 50
REQUEST_INTERVAL_SECONDS = 60 / SAFE_REQUESTS_PER_MINUTE


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: object) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _author_key(author: str) -> str:
    digest = hashlib.sha256(author.casefold().strip().encode("utf-8")).hexdigest()[:24]
    return f"{CACHE_PREFIX}{digest}"


def load_enrichment_cache(session: Session, metadata_rows=None) -> Dict[str, Dict]:
    output = {}
    rows = metadata_rows if metadata_rows is not None else session.query(SystemMetadata).filter(
        SystemMetadata.key.like(f"{CACHE_PREFIX}%")
    ).all()
    for row in rows:
        if not str(row.key or "").startswith(CACHE_PREFIX):
            continue
        try:
            value = json.loads(row.value or "")
        except (TypeError, ValueError):
            continue
        if isinstance(value, dict) and value.get("author"):
            output[str(value["author"]).casefold().strip()] = value
    return output


def save_author_cache(session: Session, value: Dict) -> None:
    key = _author_key(value["author"])
    row = session.query(SystemMetadata).filter_by(key=key).first()
    if row is None:
        row = SystemMetadata(key=key)
        session.add(row)
    row.value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    row.updated_at = datetime.utcnow()
    session.commit()


def _book_action_key(series_id: object, book_id: object, position: object) -> str:
    if position in (None, ""):
        position_key = ""
    else:
        try:
            numeric_position = float(position)
            position_key = (
                str(int(numeric_position)) if numeric_position.is_integer()
                else str(numeric_position)
            )
        except (TypeError, ValueError):
            position_key = str(position).strip()
    return f"{int(series_id)}:{int(book_id)}:{position_key}"


def load_hardcover_book_actions(session: Session, metadata_rows=None) -> Dict[str, Dict]:
    row = next((
        item for item in metadata_rows or [] if item.key == BOOK_ACTIONS_METADATA_KEY
    ), None) if metadata_rows is not None else session.query(SystemMetadata).filter_by(
        key=BOOK_ACTIONS_METADATA_KEY
    ).first()
    if not row or not row.value:
        return {}
    try:
        values = json.loads(row.value)
    except (TypeError, ValueError):
        return {}
    if not isinstance(values, list):
        return {}
    return {
        _book_action_key(value["series_id"], value["book_id"], value.get("position")): value
        for value in values
        if isinstance(value, dict) and value.get("series_id") and value.get("book_id")
    }


def load_enrichment_state(session: Session) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """Load cache entries and row actions with one metadata query."""
    rows = session.query(SystemMetadata).filter(or_(
        SystemMetadata.key.like(f"{CACHE_PREFIX}%"),
        SystemMetadata.key == BOOK_ACTIONS_METADATA_KEY,
    )).all()
    return (
        load_enrichment_cache(session, rows),
        load_hardcover_book_actions(session, rows),
    )


def record_hardcover_book_action(
    session: Session,
    *,
    action: str,
    series_id: int,
    book_id: int,
    position: object,
    title: str,
    author: str,
) -> Dict[str, Dict]:
    if action not in {"duplicate", "non_english"}:
        raise ValueError("Unsupported Hardcover book action")
    actions = load_hardcover_book_actions(session)
    key = _book_action_key(series_id, book_id, position)
    actions[key] = {
        "action": action,
        "series_id": int(series_id),
        "book_id": int(book_id),
        "position": position,
        "title": title.strip(),
        "author": author.strip(),
        "recorded_at": _now().isoformat(),
    }
    row = session.query(SystemMetadata).filter_by(key=BOOK_ACTIONS_METADATA_KEY).first()
    if row is None:
        row = SystemMetadata(key=BOOK_ACTIONS_METADATA_KEY)
        session.add(row)
    row.value = json.dumps(list(actions.values()), ensure_ascii=False, separators=(",", ":"))
    row.updated_at = datetime.utcnow()
    session.commit()
    return actions


def candidate_authors(series_rows: Sequence[Dict]) -> List[Dict]:
    grouped: Dict[str, Dict] = {}
    for series in series_rows:
        if series.get("status") not in {"partial", "not_started"}:
            continue
        author = str(series.get("author") or "").strip()
        if not author:
            continue
        entry = grouped.setdefault(author.casefold(), {"author": author, "series": []})
        entry["series"].append(series)
    return sorted(grouped.values(), key=lambda row: (
        0 if any(series.get("status") == "partial" for series in row["series"]) else 1,
        -len(row["series"]), row["author"].casefold(),
    ))


def cache_is_fresh(value: Optional[Dict], now: Optional[datetime] = None) -> bool:
    if (value or {}).get("schema_version") != CACHE_SCHEMA_VERSION:
        return False
    fetched = _parse_timestamp((value or {}).get("fetched_at"))
    return bool(fetched and (now or _now()) - fetched < timedelta(days=CACHE_DAYS))


def enrichment_status(series_rows: Sequence[Dict], cache: Dict[str, Dict]) -> Dict:
    authors = candidate_authors(series_rows)
    fresh = stale = missing = resolved = 0
    for item in authors:
        value = cache.get(item["author"].casefold())
        if value:
            resolved += int(bool((value.get("hardcover_author") or {}).get("id")))
            if cache_is_fresh(value):
                fresh += 1
            else:
                stale += 1
        else:
            missing += 1
    build_calls = sum(
        0 if cache_is_fresh(cache.get(item["author"].casefold())) else
        1 if (cache.get(item["author"].casefold(), {}).get("hardcover_author") or {}).get("id") else 2
        for item in authors
    )
    refresh_calls = sum(
        1 if (cache.get(item["author"].casefold(), {}).get("hardcover_author") or {}).get("id") else 2
        for item in authors
    )
    return {
        "candidate_series": sum(len(item["series"]) for item in authors),
        "candidate_authors": len(authors),
        "fresh_authors": fresh,
        "stale_authors": stale,
        "missing_authors": missing,
        "resolved_authors": resolved,
        "errored_authors": sum(bool(value.get("last_error")) for value in cache.values()),
        "build_api_calls": build_calls,
        "refresh_api_calls": refresh_calls,
        "build_estimated_seconds": round(build_calls * REQUEST_INTERVAL_SECONDS),
        "refresh_estimated_seconds": round(refresh_calls * REQUEST_INTERVAL_SECONDS),
        "cache_days": CACHE_DAYS,
        "safe_requests_per_minute": SAFE_REQUESTS_PER_MINUTE,
    }


def run_enrichment(
    session: Session,
    provider,
    series_rows: Sequence[Dict],
    *,
    force_refresh: bool = False,
    progress_callback=None,
) -> Dict:
    authors = candidate_authors(series_rows)
    cache = load_enrichment_cache(session)
    pending = [
        item for item in authors
        if force_refresh or not cache_is_fresh(cache.get(item["author"].casefold()))
    ]
    calls_total = sum(
        1 if (cache.get(item["author"].casefold(), {}).get("hardcover_author") or {}).get("id") else 2
        for item in pending
    )
    started = time.monotonic()
    calls_completed = 0
    errors = []

    def request_completed():
        nonlocal calls_completed
        calls_completed += 1

    provider.request_callback = request_completed
    total = len(pending)
    for index, item in enumerate(pending, 1):
        author = item["author"]
        previous = cache.get(author.casefold()) or {}
        remaining_calls = max(0, calls_total - calls_completed)
        if progress_callback:
            progress_callback(
                message=f"Enriching {author} — author {index} of {total}",
                current=index - 1,
                total=total,
                api_calls_completed=calls_completed,
                api_calls_total=calls_total,
                eta_seconds=round(remaining_calls * REQUEST_INTERVAL_SECONDS),
                cache_mode="refresh" if force_refresh else "build",
            )
        known_titles = [
            book["title"]
            for series in item["series"]
            for book in [*(series.get("read_books") or []), *(series.get("unread_books") or [])]
            if book.get("title")
        ]
        try:
            hardcover_author = previous.get("hardcover_author") or None
            if hardcover_author and hardcover_author.get("id"):
                external_series = provider.get_author_series(int(hardcover_author["id"]))
            else:
                lookup = provider.lookup_author_series(author, known_titles)
                hardcover_author = lookup.get("author")
                external_series = lookup.get("series") or []
            value = {
                "author": author,
                "hardcover_author": hardcover_author,
                "series": external_series,
                "fetched_at": _now().isoformat(),
                "source": "Hardcover",
                "schema_version": CACHE_SCHEMA_VERSION,
                "error": None,
            }
            save_author_cache(session, value)
            cache[author.casefold()] = value
        except Exception as exc:
            errors.append({"author": author, "error": str(exc)})
            value = {
                **previous,
                "author": author,
                "last_error": str(exc),
                "last_attempted_at": _now().isoformat(),
            }
            save_author_cache(session, value)
        if progress_callback:
            elapsed = max(0.01, time.monotonic() - started)
            completed = index
            eta = round((elapsed / completed) * (total - completed)) if completed else 0
            progress_callback(
                current=completed,
                total=total,
                api_calls_completed=calls_completed,
                api_calls_total=calls_total,
                eta_seconds=eta,
            )

    refreshed = len(pending) - len(errors)
    return {
        "kind": "series_enrichment",
        "mode": "refresh" if force_refresh else "build",
        "authors_considered": len(authors),
        "authors_refreshed": refreshed,
        "api_calls": calls_completed,
        "errors": errors,
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "message": (
            f"Series catalog {'refresh' if force_refresh else 'build'} complete — "
            f"{refreshed} author{'s' if refreshed != 1 else ''} updated using "
            f"{calls_completed} Hardcover call{'s' if calls_completed != 1 else ''}."
        ),
    }


def _name_key(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", str(value or "").casefold()))


def _series_match(local: Dict, external_series: Sequence[Dict]) -> Tuple[Optional[Dict], float, str]:
    local_books = [*(local.get("read_books") or []), *(local.get("unread_books") or [])]
    local_records = [{"title": book["title"]} for book in local_books if book.get("title")]
    local_name = _name_key(local.get("series_name"))
    scored = []
    for external in external_series:
        name_score = SequenceMatcher(None, local_name, _name_key(external.get("name"))).ratio()
        title_matches = 0
        best_title_score = 0.0
        for book in external.get("books") or []:
            match, score, _method = _best_local_match(book.get("title") or "", local_records)
            if match:
                title_matches += 1
                best_title_score = max(best_title_score, score)
        score = (title_matches * 2.0) + best_title_score + name_score
        scored.append((score, title_matches, name_score, external))
    if not scored:
        return None, 0.0, "no Hardcover series returned"
    scored.sort(key=lambda row: row[0], reverse=True)
    best = scored[0]
    runner_up = scored[1][0] if len(scored) > 1 else 0.0
    confident = best[1] >= 1 and (best[2] >= 0.45 or best[1] >= 2) and best[0] - runner_up >= 0.35
    if not confident:
        return None, best[0], "ambiguous Hardcover series match"
    return best[3], best[0], f"{best[1]} title anchor{'s' if best[1] != 1 else ''}"


def apply_enrichment(
    series_rows: Sequence[Dict],
    cache: Dict[str, Dict],
    recommendation_rows: Sequence[object] = (),
    hardcover_book_actions: Optional[Dict[str, Dict]] = None,
) -> List[Dict]:
    hardcover_book_actions = hardcover_book_actions or {}
    feedback = {}
    for row in recommendation_rows:
        author = str(getattr(row, "author", "") or "").casefold().strip()
        title = str(getattr(row, "title", "") or "").casefold().strip()
        if not author or not title:
            continue
        state = feedback.setdefault((author, title), {
            "read": False, "saved": False, "suppressed": False,
        })
        state["read"] = state["read"] or bool(getattr(row, "already_read", False))
        state["saved"] = state["saved"] or bool(getattr(row, "thumbs_up", False))
        state["suppressed"] = state["suppressed"] or any(bool(getattr(row, field, False)) for field in (
            "thumbs_down", "duplicate", "non_english",
        ))
    output = []
    for local in series_rows:
        cached = cache.get(str(local.get("author") or "").casefold().strip())
        if not cached or not cache_is_fresh(cached):
            output.append(local)
            continue
        external, score, evidence = _series_match(local, cached.get("series") or [])
        if not external:
            output.append({**local, "enrichment": {"status": "unmatched", "evidence": evidence}})
            continue
        read_records = [{"title": book["title"], **book} for book in local.get("read_books") or []]
        unread_records = [{"title": book["title"], **book} for book in local.get("unread_books") or []]
        books = []
        for external_book in external.get("books") or []:
            external_action = hardcover_book_actions.get(_book_action_key(
                external.get("hardcover_series_id"),
                external_book.get("hardcover_book_id"),
                external_book.get("position"),
            )) if external.get("hardcover_series_id") and external_book.get("hardcover_book_id") else None
            if (external_action or {}).get("action") in {"duplicate", "non_english"}:
                continue
            manual = feedback.get((
                str(local.get("author") or "").casefold().strip(),
                str(external_book.get("title") or "").casefold().strip(),
            ), {})
            if manual.get("suppressed"):
                continue
            read_match, _, _ = _best_local_match(external_book.get("title") or "", read_records)
            unread_match, _, _ = _best_local_match(external_book.get("title") or "", unread_records)
            status = (
                "read" if read_match or manual.get("read") else
                "unread" if unread_match or manual.get("saved") else
                "other"
            )
            books.append({
                "title": external_book.get("title"),
                "position": external_book.get("position"),
                "status": status,
                "local_title": (read_match or unread_match or {}).get("title", ""),
                "hardcover_book_id": external_book.get("hardcover_book_id"),
            })
        output.append({
            **local,
            "series_name": external.get("name") or local.get("series_name"),
            "series_books": books,
            "total_books": len(books),
            "books_read": sum(book["status"] == "read" for book in books),
            "completion_pct": (
                sum(book["status"] == "read" for book in books) / len(books) * 100 if books else 0
            ),
            "status": (
                "not_started" if not any(book["status"] == "read" for book in books)
                else "complete" if books and all(book["status"] == "read" for book in books)
                else "partial"
            ),
            "enrichment": {
                "status": "matched",
                "provider": "Hardcover",
                "hardcover_series_id": external.get("hardcover_series_id"),
                "source_url": (
                    f"https://hardcover.app/series/{external['slug']}"
                    if external.get("slug") else "https://hardcover.app"
                ),
                "fetched_at": cached.get("fetched_at"),
                "evidence": evidence,
                "score": score,
            },
        })
    return _merge_duplicate_enriched_series(output)


def _merge_duplicate_enriched_series(series_rows: Sequence[Dict]) -> List[Dict]:
    """Return one card when multiple local records resolve to one Hardcover series."""
    output = []
    by_hardcover_series = {}
    status_rank = {"other": 0, "unread": 1, "read": 2}
    for series in series_rows:
        series_id = (series.get("enrichment") or {}).get("hardcover_series_id")
        if not series_id:
            output.append(series)
            continue
        key = (str(series.get("author") or "").casefold().strip(), int(series_id))
        existing = by_hardcover_series.get(key)
        if existing is None:
            clone = {
                **series,
                "series_books": [dict(book) for book in series.get("series_books") or []],
            }
            by_hardcover_series[key] = clone
            output.append(clone)
            continue

        merged_books = {}
        for book in [*(existing.get("series_books") or []), *(series.get("series_books") or [])]:
            # The same work can arrive with two Hardcover IDs when a stale
            # membership survives upstream deduplication. The visible identity
            # is series position + normalized title, not provider row ID.
            position = book.get("position")
            book_key = (
                "position", position,
            ) if position is not None else (
                "title", _name_key(book.get("title")),
            )
            current = merged_books.get(book_key)
            if current is None or status_rank.get(book.get("status"), 0) > status_rank.get(current.get("status"), 0):
                merged_books[book_key] = dict(book)
        existing["series_books"] = sorted(merged_books.values(), key=lambda book: (
            book.get("position") is None,
            book.get("position") if book.get("position") is not None else 0,
            str(book.get("title") or "").casefold(),
        ))
        existing["read_books"] = _dedupe_local_books([
            *(existing.get("read_books") or []), *(series.get("read_books") or []),
        ])
        existing["unread_books"] = _dedupe_local_books([
            *(existing.get("unread_books") or []), *(series.get("unread_books") or []),
        ])
        existing["enrichment"]["merged_local_records"] = (
            int(existing["enrichment"].get("merged_local_records") or 1) + 1
        )
        _recalculate_enriched_progress(existing)
    return output


def _dedupe_local_books(books: Sequence[Dict]) -> List[Dict]:
    by_key = {}
    for book in books:
        key = (book.get("position"), _name_key(book.get("title")))
        by_key.setdefault(key, book)
    return list(by_key.values())


def _recalculate_enriched_progress(series: Dict) -> None:
    books = series.get("series_books") or []
    read_count = sum(book.get("status") == "read" for book in books)
    series["total_books"] = len(books)
    series["books_read"] = read_count
    series["completion_pct"] = read_count / len(books) * 100 if books else 0
    series["status"] = (
        "not_started" if not read_count
        else "complete" if books and read_count == len(books)
        else "partial"
    )
