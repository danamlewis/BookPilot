"""Database workflow for personalized catalog language cleanup."""

import csv
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, or_

from .models import Author, AuthorCatalogBook, Book, Recommendation
from .personal_language_review import PersonalLanguageModel, normalize_language_title


CONFIDENCE_ORDER = {"none": 0, "medium": 1, "high": 2}
TRAINING_SOURCES = ("manual", "review_approved")


def get_training_examples(session):
    """Use human decisions only; never train on automated predictions."""
    flagged = session.query(Recommendation).filter(
        Recommendation.non_english.is_(True),
        or_(
            Recommendation.language_flag_source.is_(None),  # legacy manual flags
            Recommendation.language_flag_source.in_(TRAINING_SOURCES),
        ),
    ).all()
    return [(rec.author, rec.title) for rec in flagged if rec.author and rec.title]


def collect_candidates(session, model, author_filter=None, catalog_book_ids=None):
    flagged_keys = {
        (rec.author.casefold(), normalize_language_title(rec.title))
        for rec in session.query(Recommendation).filter(Recommendation.non_english.is_(True)).all()
        if rec.author and rec.title
    }
    query = session.query(AuthorCatalogBook, Author).join(
        Author, Author.id == AuthorCatalogBook.author_id
    ).filter(AuthorCatalogBook.is_read.is_(False))
    if author_filter:
        query = query.filter(func.lower(Author.name) == author_filter.casefold())
    if catalog_book_ids is not None:
        query = query.filter(AuthorCatalogBook.id.in_(catalog_book_ids))

    candidates = []
    seen = set()
    for catalog_book, author in query.all():
        key = (author.name.casefold(), normalize_language_title(catalog_book.title))
        if not key[1] or key in seen or key in flagged_keys:
            continue
        seen.add(key)
        assessment = model.assess(catalog_book.title)
        if assessment.confidence == "none":
            continue
        candidates.append({
            "catalog_book_id": catalog_book.id,
            "author": author.name,
            "title": catalog_book.title,
            "isbn": catalog_book.isbn or "",
            "confidence": assessment.confidence,
            "score": assessment.score,
            "reasons": "; ".join(assessment.reasons),
        })
    return sorted(
        candidates,
        key=lambda item: (CONFIDENCE_ORDER[item["confidence"]], item["score"]),
        reverse=True,
    )


def apply_flags(session, candidates, minimum_confidence="high", source="personalized_high"):
    minimum = CONFIDENCE_ORDER[minimum_confidence]
    recommendations_by_key = {}
    for recommendation in session.query(Recommendation).all():
        key = (
            (recommendation.author or "").casefold(),
            normalize_language_title(recommendation.title),
        )
        recommendations_by_key.setdefault(key, []).append(recommendation)

    rows_flagged = 0
    suppression_rows_created = 0
    for candidate in candidates:
        if CONFIDENCE_ORDER[candidate["confidence"]] < minimum:
            continue
        key = (candidate["author"].casefold(), normalize_language_title(candidate["title"]))
        matching = recommendations_by_key.get(key, [])
        if matching:
            for recommendation in matching:
                if not recommendation.non_english:
                    recommendation.non_english = True
                    recommendation.thumbs_up = False
                    rows_flagged += 1
                # A human-reviewed result should replace older/automated provenance.
                # Automated passes must never overwrite a human provenance marker.
                if source == "review_approved" or not recommendation.language_flag_source:
                    recommendation.language_flag_source = source
        else:
            recommendation = Recommendation(
                title=candidate["title"],
                author=candidate["author"],
                isbn=candidate["isbn"] or None,
                format="catalog",
                recommendation_type="language_filter",
                similarity_score=0.0,
                reason=f"Personal language review: {candidate['reasons']}",
                non_english=True,
                language_flag_source=source,
            )
            session.add(recommendation)
            recommendations_by_key[key] = [recommendation]
            suppression_rows_created += 1
    session.commit()
    return rows_flagged, suppression_rows_created


def delete_catalog_candidates(session, candidates, minimum_confidence="high"):
    minimum = CONFIDENCE_ORDER[minimum_confidence]
    candidate_ids = {
        candidate["catalog_book_id"]
        for candidate in candidates
        if CONFIDENCE_ORDER[candidate["confidence"]] >= minimum
    }
    if not candidate_ids:
        return 0
    rows = session.query(AuthorCatalogBook).filter(AuthorCatalogBook.id.in_(candidate_ids)).all()
    for row in rows:
        session.delete(row)
    session.commit()
    return len(rows)


def write_report(candidates, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=candidates[0].keys() if candidates else [
            "catalog_book_id", "author", "title", "isbn", "confidence", "score", "reasons"
        ])
        writer.writeheader()
        writer.writerows(candidates)
    return output_path


def run_personalized_language_cleanup(session, catalog_book_ids=None, report_directory=None):
    """Apply only high-confidence predictions and report medium candidates."""
    flagged_examples = get_training_examples(session)
    english_titles = [book.title for book in session.query(Book).all() if book.title]
    model = PersonalLanguageModel(flagged_examples, english_titles)
    candidates = collect_candidates(
        session,
        model,
        catalog_book_ids=catalog_book_ids,
    )
    high_count = sum(item["confidence"] == "high" for item in candidates)
    medium_count = sum(item["confidence"] == "medium" for item in candidates)

    report_path = None
    if candidates:
        if report_directory is None:
            database_path = getattr(getattr(session, "bind", None), "url", None)
            database_path = getattr(database_path, "database", None)
            report_directory = Path(database_path).parent if database_path else Path("data")
        report_path = write_report(
            candidates,
            Path(report_directory) / f"non_english_auto_review_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )

    rows_flagged, rows_created = apply_flags(
        session,
        candidates,
        minimum_confidence="high",
        source="personalized_high",
    )
    rows_deleted = delete_catalog_candidates(session, candidates, minimum_confidence="high")
    return {
        "training_examples": len(flagged_examples),
        "learned_features": len(model.learned_features),
        "high_count": high_count,
        "medium_count": medium_count,
        "recommendations_flagged": rows_flagged,
        "suppression_rows_created": rows_created,
        "catalog_rows_deleted": rows_deleted,
        "report_path": str(report_path) if report_path else None,
    }
