"""Safe automatic merging of deterministic within-author catalog duplicates."""
from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Iterable, Optional, Sequence

from ..models import AuthorCatalogBook, Recommendation
from .within_author import assess_duplicate_pair, choose_display_title, choose_keeper


# These review-tier reasons are exact, explainable transformations. Fuzzy and
# identifier-conflict candidates stay in the manual review workflow.
AUTO_CLEANUP_REVIEW_REASONS = frozenset({
    "leading_article_only",
    "regional_spelling_variant",
    "retail_boilerplate_or_date_only",
})


def is_automatic_cleanup_match(assessment) -> bool:
    if assessment.tier == "auto":
        return True
    return (
        assessment.tier == "review"
        and bool(assessment.reason_codes)
        and set(assessment.reason_codes).issubset(AUTO_CLEANUP_REVIEW_REASONS)
    )


def _assessment(first, second):
    return assess_duplicate_pair(
        first.title,
        second.title,
        isbn_a=first.isbn,
        isbn_b=second.isbn,
        work_key_a=first.open_library_key,
        work_key_b=second.open_library_key,
    )


def _connected_components(edges: Iterable[tuple[int, int]]) -> list[set[int]]:
    neighbors: dict[int, set[int]] = defaultdict(set)
    for first, second in edges:
        neighbors[first].add(second)
        neighbors[second].add(first)
    unseen = set(neighbors)
    components = []
    while unseen:
        seed = min(unseen)
        stack = [seed]
        component = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(neighbors[current] - component)
        unseen -= component
        components.append(component)
    return components


def find_automatic_components(
    books: Sequence[AuthorCatalogBook],
    scoped_ids: Optional[set[int]] = None,
) -> tuple[list[list[AuthorCatalogBook]], int, int]:
    """Return safe components plus counts of manual and protected candidates."""
    by_author: dict[int, list[AuthorCatalogBook]] = defaultdict(list)
    for book in books:
        by_author[book.author_id].append(book)

    components = []
    manual_candidates = 0
    protected_candidates = 0
    for author_books in by_author.values():
        by_id = {book.id: book for book in author_books}
        assessments = {}
        edges = []
        for first, second in combinations(author_books, 2):
            assessment = _assessment(first, second)
            assessments[frozenset((first.id, second.id))] = assessment
            if is_automatic_cleanup_match(assessment):
                edges.append((first.id, second.id))
            elif assessment.tier == "review":
                manual_candidates += 1
            elif assessment.tier == "never":
                protected_candidates += 1

        for component_ids in _connected_components(edges):
            if scoped_ids is not None and component_ids.isdisjoint(scoped_ids):
                continue
            component = [by_id[book_id] for book_id in sorted(component_ids)]
            # A deterministic edge chain must never bridge an explicitly
            # protected numbered work or collection relationship.
            if any(
                assessments[frozenset((first.id, second.id))].tier == "never"
                for first, second in combinations(component, 2)
            ):
                protected_candidates += 1
                continue
            components.append(component)
    return components, manual_candidates, protected_candidates


def _merge_component(session, books: Sequence[AuthorCatalogBook]) -> dict:
    keeper = choose_keeper(books)
    keeper.title = choose_display_title(books)
    removed = [book for book in books if book.id != keeper.id]

    for field in (
        "isbn", "publication_date", "series_name", "series_position",
        "open_library_key", "google_books_id", "description", "categories",
        "matched_book_id",
    ):
        if not getattr(keeper, field):
            donor = next((getattr(book, field) for book in books if getattr(book, field)), None)
            if donor is not None:
                setattr(keeper, field, donor)

    formats = {
        book.format_available
        for book in books
        if book.format_available not in (None, "unknown")
    }
    if "both" in formats or formats == {"ebook", "audiobook"}:
        keeper.format_available = "both"
    elif keeper.format_available in (None, "unknown") and formats:
        keeper.format_available = sorted(formats)[0]
    keeper.is_read = any(bool(book.is_read) for book in books)

    removed_ids = [book.id for book in removed]
    if removed_ids:
        session.query(Recommendation).filter(
            Recommendation.catalog_book_id.in_(removed_ids)
        ).update(
            {
                Recommendation.catalog_book_id: keeper.id,
                Recommendation.title: keeper.title,
                Recommendation.isbn: keeper.isbn,
            },
            synchronize_session=False,
        )
        for book in removed:
            session.delete(book)
    return {
        "keeper_id": keeper.id,
        "title": keeper.title,
        "removed_ids": removed_ids,
        "cluster_size": len(books),
    }


def cleanup_within_author_catalog(
    session,
    books: Sequence[AuthorCatalogBook],
    *,
    scoped_ids: Optional[set[int]] = None,
    dry_run: bool = True,
) -> dict:
    """Find and optionally merge deterministic duplicate catalog components."""
    components, manual_candidates, protected_candidates = find_automatic_components(
        books,
        scoped_ids=scoped_ids,
    )
    planned_removals = sum(len(component) - 1 for component in components)
    audit = []
    if not dry_run:
        audit = [_merge_component(session, component) for component in components]
    return {
        "clusters_found": len(components),
        "rows_planned": planned_removals,
        "rows_removed": 0 if dry_run else planned_removals,
        "manual_candidates": manual_candidates,
        "protected_candidates": protected_candidates,
        "audit": audit,
    }
