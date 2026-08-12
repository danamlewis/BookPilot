"""Conservative classification of organization-like author credits."""
from __future__ import annotations

import re
from typing import Iterable, Optional


LEGAL_ENTITY_SUFFIX = re.compile(
    r"(?:,?\s+|\b)(?:l\.?l\.?c\.?|incorporated|inc\.?|corporation|corp\.?|"
    r"limited|ltd\.?|l\.?l\.?p\.?|p\.?l\.?c\.?)\s*$",
    re.IGNORECASE,
)

ORGANIZATION_NAME_PATTERNS = (
    (re.compile(r"\b(?:press|publishing|publications?|publishers?)\b", re.I), "publisher term in author name"),
    (re.compile(r"\b(?:staff|editorial team|editors?|contributors?)\b", re.I), "collective credit in author name"),
    (re.compile(r"\b(?:corporation|corp\.?|company|incorporated|inc\.?|l\.?l\.?c\.?)$", re.I), "legal entity in author name"),
    (re.compile(r"\b(?:university|institute|association|foundation|society|department)\b", re.I), "institution term in author name"),
    (re.compile(r"(?:\.com|\.org|\.net)$", re.I), "website used as author name"),
)


def _canonical_name(value: str) -> str:
    value = LEGAL_ENTITY_SUFFIX.sub("", value or "")
    return " ".join(re.findall(r"[a-z0-9]+", value.casefold()))


def organization_name_reason(name: str) -> Optional[str]:
    """Return a reason when the author credit itself clearly names an organization."""
    candidate = (name or "").strip()
    if not candidate:
        return None
    for pattern, reason in ORGANIZATION_NAME_PATTERNS:
        if pattern.search(candidate):
            return reason
    return None


def organization_author_reason(author_name: str, publishers: Iterable[str]) -> Optional[str]:
    """Return why an author credit is company-like, or ``None`` when uncertain.

    Publisher evidence is deliberately strict: the publisher must carry a legal
    entity suffix and reduce to the same canonical words as the author credit.
    This catches Libby rows such as author ``Innovative Language Learning`` and
    publisher ``Innovative Language Learning, LLC`` without excluding ordinary
    self-published authors merely because their names appear in both fields.
    """
    name_reason = organization_name_reason(author_name)
    if name_reason:
        return name_reason

    author_key = _canonical_name(author_name)
    if not author_key:
        return None

    for publisher in publishers:
        publisher = (publisher or "").strip()
        if not publisher or not LEGAL_ENTITY_SUFFIX.search(publisher):
            continue
        if _canonical_name(publisher) == author_key:
            return "author credit matches a publisher with a legal entity suffix"
    return None
