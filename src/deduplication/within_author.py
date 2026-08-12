"""Conservative, explainable within-author duplicate assessment."""
from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import html
import re
import unicodedata
from typing import Optional, Sequence, Tuple


LEADING_ARTICLE_RE = re.compile(r"^(?:the|a|an)\s+", re.IGNORECASE)
SAFE_PACKAGING_RE = re.compile(
    r"\s*[\[(](?:unabridged|abridged|large\s+print|reissue|revised|"
    r"anniversary|illustrated|audio\s*cd|compact\s+disc|hardcover)"
    r"(?:\s+edition)?[\])]\s*$",
    re.IGNORECASE,
)
SPLIT_MARKER_RE = re.compile(r"\s*\[\d+/\d+\]\s*$")
STRUCTURED_NUMBER_RE = re.compile(
    r"\b(?P<label>volume|vols?\.?|book|part|tome|novella|collection)\s*#?\s*"
    r"(?P<number>\d+|one|two|three|four|five|six|seven|eight|nine|ten|[ivxlcdm]+)\b",
    re.IGNORECASE,
)
TRAILING_NUMBER_RE = re.compile(r"\b(?P<number>\d+)\s*$")
DATE_RE = re.compile(r"\(\d{4}-\d{2}-\d{2}\)")
AUTHOR_DATE_SUFFIX_RE = re.compile(
    r"\s+by\s+[\w .'-]+\s+\(\d{4}-\d{2}-\d{2}\)\s*$",
    re.IGNORECASE,
)
RETAIL_BOILERPLATE_RE = re.compile(
    r"\s*:\s*written\s+by\s+.+?\b\d{4}\s+edition\s*,?\s*publisher\s*$",
    re.IGNORECASE,
)
GENERIC_TOKENS = {
    "a", "an", "and", "author", "book", "by", "edition", "hardcover",
    "large", "print", "publisher", "reissue", "the", "unabridged", "written",
}
COMPOSITE_RE = re.compile(
    r"\b(?:boxed?\s+set|collection|omnibus|anthology|\d+\s+(?:books?|novels?)|"
    r"books?\s+included|complete\s+series)\b",
    re.IGNORECASE,
)
SPELLING_EQUIVALENTS = {
    "colour": "color", "honour": "honor", "favour": "favor",
    "traveller": "traveler", "centre": "center", "theatre": "theater",
}
CONTRAST_TOKEN_GROUPS = (
    {"macroeconomics", "microeconomics"},
    {"macro", "micro"},
)


@dataclass(frozen=True)
class DuplicateAssessment:
    tier: str
    confidence: int
    reason_codes: Tuple[str, ...]
    explanation: str
    title_similarity: float
    token_jaccard: float
    normalized_title_a: str
    normalized_title_b: str


def normalize_title(title: str, *, remove_article: bool = False, remove_boilerplate: bool = False) -> str:
    text = unicodedata.normalize("NFKC", html.unescape(title or "")).casefold()
    text = text.replace("’", "'").replace("‘", "'")
    previous = None
    while text != previous:
        previous = text
        text = SAFE_PACKAGING_RE.sub("", text)
        text = SPLIT_MARKER_RE.sub("", text)
    if remove_boilerplate:
        text = RETAIL_BOILERPLATE_RE.sub("", text)
        text = AUTHOR_DATE_SUFFIX_RE.sub("", text)
        text = DATE_RE.sub("", text)
    text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    text = " ".join(text.split())
    if remove_article:
        text = LEADING_ARTICLE_RE.sub("", text)
    return text


def title_tokens(title: str) -> set[str]:
    return {token for token in normalize_title(title).split() if token not in GENERIC_TOKENS}


def structured_numbers(title: str) -> set[tuple[str, str]]:
    values = {
        (match.group("label").rstrip(".").casefold(), match.group("number").casefold())
        for match in STRUCTURED_NUMBER_RE.finditer(title or "")
    }
    if re.search(r"\buntitled\b", title or "", re.IGNORECASE):
        match = TRAILING_NUMBER_RE.search(title or "")
        if match:
            values.add(("untitled", match.group("number")))
    return values


def _number_conflict(first: str, second: str) -> bool:
    first_numbers = structured_numbers(first)
    second_numbers = structured_numbers(second)
    labels = {label for label, _ in first_numbers} & {label for label, _ in second_numbers}
    for label in labels:
        if {number for item_label, number in first_numbers if item_label == label} != {
            number for item_label, number in second_numbers if item_label == label
        }:
            return True
    return False


def _without_structured_numbers(title: str) -> str:
    text = STRUCTURED_NUMBER_RE.sub(lambda match: match.group("label"), title or "")
    if re.search(r"\buntitled\b", text, re.IGNORECASE):
        text = TRAILING_NUMBER_RE.sub("", text)
    return normalize_title(text, remove_article=True)


def _normalized_isbn(isbn: Optional[str]) -> str:
    return re.sub(r"[^0-9Xx]", "", isbn or "").upper()


def _similarity(first: str, second: str) -> tuple[float, float]:
    sequence = SequenceMatcher(None, first, second).ratio() if first and second else 0.0
    first_tokens = set(first.split()) - GENERIC_TOKENS
    second_tokens = set(second.split()) - GENERIC_TOKENS
    union = first_tokens | second_tokens
    jaccard = len(first_tokens & second_tokens) / len(union) if union else 0.0
    return sequence, jaccard


def _canonical_spelling(title: str) -> str:
    return " ".join(SPELLING_EQUIVALENTS.get(token, token) for token in title.split())


def _contrast_conflict(first: str, second: str) -> bool:
    first_tokens = set(first.split())
    second_tokens = set(second.split())
    for group in CONTRAST_TOKEN_GROUPS:
        if (first_tokens & group) and (second_tokens & group) and (first_tokens & group) != (second_tokens & group):
            return True
    return False


def assess_duplicate_pair(
    title_a: str,
    title_b: str,
    *,
    isbn_a: Optional[str] = None,
    isbn_b: Optional[str] = None,
    work_key_a: Optional[str] = None,
    work_key_b: Optional[str] = None,
) -> DuplicateAssessment:
    """Classify one same-author pair as auto, review, never, or unrelated."""
    normal_a = normalize_title(title_a)
    normal_b = normalize_title(title_b)
    article_a = normalize_title(title_a, remove_article=True)
    article_b = normalize_title(title_b, remove_article=True)
    boiler_a = normalize_title(title_a, remove_article=True, remove_boilerplate=True)
    boiler_b = normalize_title(title_b, remove_article=True, remove_boilerplate=True)
    similarity, jaccard = _similarity(normal_a, normal_b)
    reasons = []

    if _number_conflict(title_a, title_b):
        stripped_a = _without_structured_numbers(title_a)
        stripped_b = _without_structured_numbers(title_b)
        stripped_similarity, stripped_jaccard = _similarity(stripped_a, stripped_b)
        if stripped_a == stripped_b or stripped_similarity >= 0.86 or stripped_jaccard >= 0.70:
            return DuplicateAssessment(
                "never", 99, ("conflicting_numbered_parts",),
                "Different volume, part, book, tome, collection, or placeholder numbers must remain separate.",
                similarity, jaccard, normal_a, normal_b,
            )
        return DuplicateAssessment(
            "unrelated", 0, (), "Different numbered works with no duplicate-title evidence.",
            similarity, jaccard, normal_a, normal_b,
        )
    if bool(COMPOSITE_RE.search(title_a or "")) != bool(COMPOSITE_RE.search(title_b or "")):
        if similarity >= 0.78 or jaccard >= 0.65:
            return DuplicateAssessment(
                "never", 95, ("composite_vs_standalone",),
                "A collection or omnibus must not be collapsed into a standalone work.",
                similarity, jaccard, normal_a, normal_b,
            )
        return DuplicateAssessment(
            "unrelated", 0, (), "Collection and standalone titles have no duplicate-title evidence.",
            similarity, jaccard, normal_a, normal_b,
        )
    if _contrast_conflict(normal_a, normal_b):
        return DuplicateAssessment(
            "never", 98, ("contrasting_substantive_terms",),
            "The titles contain contrasting substantive terms and represent different works.",
            similarity, jaccard, normal_a, normal_b,
        )

    same_work_key = bool(work_key_a and work_key_b and work_key_a == work_key_b)
    isbn_one = _normalized_isbn(isbn_a)
    isbn_two = _normalized_isbn(isbn_b)
    same_isbn = bool(isbn_one and isbn_two and isbn_one == isbn_two)
    if same_work_key and (normal_a == normal_b or article_a == article_b or jaccard >= 0.65):
        reasons.append("same_open_library_work")
    if same_isbn and (normal_a == normal_b or article_a == article_b or jaccard >= 0.80):
        reasons.append("same_isbn_with_title_agreement")
    if normal_a == normal_b:
        reasons.append("same_conservative_title")
    if reasons:
        return DuplicateAssessment(
            "auto", 99 if same_work_key or same_isbn else 96, tuple(reasons),
            "Identifiers or conservative title normalization agree without structural conflicts.",
            similarity, jaccard, normal_a, normal_b,
        )

    if article_a == article_b:
        return DuplicateAssessment(
            "review", 92, ("leading_article_only",),
            "Titles differ only by a leading article; likely duplicate but retained for review.",
            similarity, jaccard, normal_a, normal_b,
        )
    if _canonical_spelling(article_a) == _canonical_spelling(article_b):
        return DuplicateAssessment(
            "review", 90, ("regional_spelling_variant",),
            "Titles differ only by a recognized regional spelling variant.",
            similarity, jaccard, normal_a, normal_b,
        )
    if boiler_a and boiler_a == boiler_b:
        return DuplicateAssessment(
            "review", 90, ("retail_boilerplate_or_date_only",),
            "Titles reduce to the same work after removing author/date or retail boilerplate.",
            similarity, jaccard, normal_a, normal_b,
        )
    if same_work_key:
        return DuplicateAssessment(
            "review", 88, ("same_open_library_work_title_conflict",),
            "Open Library work keys agree, but titles need human confirmation.",
            similarity, jaccard, normal_a, normal_b,
        )
    if same_isbn:
        return DuplicateAssessment(
            "review", 82, ("same_isbn_title_conflict",),
            "ISBNs agree, but title disagreement makes automatic deletion unsafe.",
            similarity, jaccard, normal_a, normal_b,
        )
    if RETAIL_BOILERPLATE_RE.search(title_a or "") and RETAIL_BOILERPLATE_RE.search(title_b or ""):
        return DuplicateAssessment(
            "unrelated", 0, (),
            "Shared retail boilerplate is not duplicate evidence when the work titles differ.",
            similarity, jaccard, normal_a, normal_b,
        )
    if similarity >= 0.94 and jaccard >= 0.70:
        return DuplicateAssessment(
            "review", round(70 + 20 * similarity), ("probable_typo_or_spelling_variant",),
            "Very high character similarity and strong token overlap suggest a typo or spelling variant.",
            similarity, jaccard, normal_a, normal_b,
        )
    if similarity >= 0.86 and jaccard >= 0.55:
        return DuplicateAssessment(
            "review", round(45 + 25 * similarity), ("fuzzy_title_candidate",),
            "Similar wording suggests a possible duplicate, but substantive differences may remain.",
            similarity, jaccard, normal_a, normal_b,
        )
    shared_tokens = (set(normal_a.split()) & set(normal_b.split())) - GENERIC_TOKENS
    if similarity >= 0.91 and shared_tokens:
        return DuplicateAssessment(
            "review", round(45 + 25 * similarity), ("short_title_typo_candidate",),
            "High character similarity plus a shared substantive word suggests a short-title typo.",
            similarity, jaccard, normal_a, normal_b,
        )
    return DuplicateAssessment(
        "unrelated", 0, (), "Insufficient evidence of duplication.",
        similarity, jaccard, normal_a, normal_b,
    )


def choose_keeper(books: Sequence) -> object:
    """Prefer clean titles and complete metadata without treating ISBN as truth."""
    def score(book):
        title = book.title or ""
        metadata = (
            5 * bool(book.description)
            + 3 * bool(book.categories)
            + 3 * bool(book.open_library_key)
            + 2 * bool(book.isbn)
            + bool(book.publication_date)
        )
        title_penalty = (
            8 * bool(RETAIL_BOILERPLATE_RE.search(title))
            + 5 * bool(DATE_RE.search(title))
            + 4 * bool(re.search(r"\buntitled\b", title, re.IGNORECASE))
            + min(6, max(0, len(title) - 100) // 20)
        )
        return metadata - title_penalty, -len(title), -book.id
    return max(books, key=score)
