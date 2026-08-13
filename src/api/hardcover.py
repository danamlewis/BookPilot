"""Small, read-only client for Hardcover's public bibliographic GraphQL data."""
from __future__ import annotations

import json
import time
from typing import Dict, Iterable, List, Optional

import requests


class HardcoverAPIError(requests.RequestException):
    """Raised when Hardcover cannot provide a safe, complete lookup result."""


class HardcoverClient:
    """Fetch author series and ordered books without exposing the token to the UI."""

    ENDPOINT = "https://api.hardcover.app/v1/graphql"

    CURRENT_USER = """
    query Test {
      me {
        username
      }
    }
    """

    AUTHOR_SEARCH = """
    query SearchAuthor($query: String!) {
      search(query: $query, query_type: "Author", per_page: 5, page: 1) {
        ids
        results
      }
    }
    """

    AUTHOR_SERIES = """
    query AuthorSeries($authorId: Int!, $excludedSeriesIds: [Int!]!) {
      series(
        where: {
          author_id: {_eq: $authorId}
          canonical_id: {_is_null: true}
          id: {_nin: $excludedSeriesIds}
        }
        order_by: [{books_count: desc}, {name: asc}]
      ) {
        id
        name
        slug
        books_count
        primary_books_count
        book_series(
          where: {
            book: {
              canonical_id: {_is_null: true}
              compilation: {_eq: false}
              book_status_id: {_eq: 1}
            }
          }
          order_by: [{position: asc}, {book_id: asc}]
        ) {
          position
          featured
          compilation
          details
          book {
            id
            title
            slug
            alternative_titles
            canonical_id
            compilation
            book_status_id
            users_count
            editions_count
            english_editions: editions(where: {language_id: {_eq: 1}}, limit: 1) {
              id
            }
            known_language_editions: editions(
              where: {language_id: {_is_null: false}}
              limit: 1
            ) {
              language_id
            }
          }
        }
      }
    }
    """

    SERIES_BY_IDS = """
    query SeriesByIds($seriesIds: [Int!]!) {
      series(where: {id: {_in: $seriesIds}}, order_by: [{name: asc}]) {
        id
        name
        slug
        book_series(
          where: {
            book: {
              canonical_id: {_is_null: true}
              compilation: {_eq: false}
              book_status_id: {_eq: 1}
            }
          }
          order_by: [{position: asc}, {book_id: asc}]
        ) {
          position
          featured
          compilation
          details
          book {
            id
            title
            slug
            alternative_titles
            canonical_id
            compilation
            book_status_id
            users_count
            editions_count
            english_editions: editions(where: {language_id: {_eq: 1}}, limit: 1) {
              id
            }
            known_language_editions: editions(
              where: {language_id: {_is_null: false}}
              limit: 1
            ) {
              language_id
            }
          }
        }
      }
    }
    """

    def __init__(
        self,
        token: str,
        *,
        timeout: float = 30,
        rate_limit_delay: float = 0.15,
        session=None,
        request_callback=None,
    ) -> None:
        clean_token = str(token or "").strip()
        if not clean_token:
            raise ValueError("HARDCOVER_API_TOKEN is not configured")
        self.token = clean_token
        self.timeout = timeout
        self.rate_limit_delay = max(0, rate_limit_delay)
        self.session = session or requests.Session()
        self.request_callback = request_callback

    @property
    def authorization(self) -> str:
        return self.token if self.token.lower().startswith("bearer ") else f"Bearer {self.token}"

    def _request(self, query: str, variables: Dict) -> Dict:
        headers = {
            "Authorization": self.authorization,
            "Content-Type": "application/json",
            "User-Agent": "BookPilot local series reconciliation",
        }
        for attempt in range(3):
            if self.rate_limit_delay:
                time.sleep(self.rate_limit_delay)
            try:
                response = self.session.post(
                    self.ENDPOINT,
                    headers=headers,
                    json={"query": query, "variables": variables},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                raise HardcoverAPIError(f"Hardcover request failed: {exc}") from exc

            if response.status_code == 429 and attempt < 2:
                try:
                    delay = min(5, max(1, int(response.headers.get("Retry-After", "1"))))
                except (TypeError, ValueError):
                    delay = 1
                time.sleep(delay)
                continue
            if response.status_code in {401, 403}:
                raise HardcoverAPIError("Hardcover rejected the API token; create a fresh token in Hardcover settings")
            try:
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                raise HardcoverAPIError(f"Hardcover returned an invalid response ({response.status_code})") from exc
            if payload.get("errors"):
                messages = "; ".join(
                    str(error.get("message") or error) for error in payload["errors"][:3]
                )
                raise HardcoverAPIError(f"Hardcover query failed: {messages}")
            if self.request_callback:
                self.request_callback()
            return payload.get("data") or {}
        raise HardcoverAPIError("Hardcover rate limit did not clear after retries")

    def get_current_user(self) -> Optional[Dict]:
        """Return the authenticated Hardcover user for a lightweight token check."""
        data = self._request(self.CURRENT_USER, {})
        users = data.get("me") or []
        if isinstance(users, dict):
            return users
        return users[0] if isinstance(users, list) and users else None

    @staticmethod
    def _search_results(value) -> List[Dict]:
        """Normalize the JSON scalar returned by Hardcover's search resolver."""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                return []
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if not isinstance(value, dict):
            return []
        for key in ("results", "hits", "documents"):
            nested = value.get(key)
            if isinstance(nested, list):
                normalized = []
                for item in nested:
                    if not isinstance(item, dict):
                        continue
                    document = item.get("document")
                    normalized.append(document if isinstance(document, dict) else item)
                return normalized
        return []

    @staticmethod
    def _title_key(value: str) -> str:
        return " ".join(str(value or "").casefold().split())

    def find_author(self, author_name: str, known_titles: Iterable[str] = ()) -> Optional[Dict]:
        data = self._request(self.AUTHOR_SEARCH, {"query": author_name})
        search = data.get("search") or {}
        results = self._search_results(search.get("results"))
        for index, result in enumerate(results):
            ids = search.get("ids") or []
            if result.get("id") is None and index < len(ids):
                result["id"] = ids[index]
        if not results:
            return None

        requested = self._title_key(author_name)
        known = {self._title_key(title) for title in known_titles if title}

        def score(result: Dict) -> tuple:
            name = self._title_key(result.get("name"))
            alternates = {
                self._title_key(value) for value in result.get("alternate_names") or [] if value
            }
            books = {self._title_key(value) for value in result.get("books") or [] if value}
            return (
                3 if name == requested else 2 if requested in alternates else 0,
                len(known & books),
                int(result.get("books_count") or 0),
            )

        best = max(results, key=score)
        if score(best)[:2] == (0, 0):
            return None
        try:
            return {**best, "id": int(best["id"])}
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_series(rows) -> List[Dict]:
        output = []
        for raw_series in rows or []:
            if not isinstance(raw_series, dict):
                continue
            books = []
            seen = set()
            for membership in raw_series.get("book_series") or []:
                if not isinstance(membership, dict) or membership.get("compilation"):
                    continue
                book = membership.get("book") or {}
                # Hardcover can leave deduped translations attached to a series.
                # The GraphQL relationship filter handles current responses; keep
                # these checks here as a defensive boundary for fixtures and API
                # behavior changes.
                if book.get("canonical_id") is not None or book.get("compilation"):
                    continue
                if book.get("book_status_id") not in (None, 1):
                    continue
                english_editions = book.get("english_editions") or []
                known_language_editions = book.get("known_language_editions") or []
                language_status = (
                    "english" if english_editions else
                    "non_english" if known_language_editions else
                    "unknown"
                )
                if language_status == "non_english":
                    continue
                title = str(book.get("title") or "").strip()
                if not title:
                    continue
                key = (book.get("id"), membership.get("position"), title.casefold())
                if key in seen:
                    continue
                seen.add(key)
                books.append({
                    "title": title,
                    "position": membership.get("position"),
                    "featured": bool(membership.get("featured")),
                    "details": membership.get("details"),
                    "hardcover_book_id": book.get("id"),
                    "alternative_titles": book.get("alternative_titles") or [],
                    "language_status": language_status,
                    "users_count": int(book.get("users_count") or 0),
                    "editions_count": int(book.get("editions_count") or 0),
                })
            # A valid series order should have one primary work per position.
            # When Hardcover still has multiple OK records, prefer a verified
            # English record and then the record used by the most readers.
            by_position = {}
            unpositioned = []
            for book in books:
                position = book.get("position")
                if position is None:
                    unpositioned.append(book)
                    continue
                current = by_position.get(position)
                rank = (
                    book.get("language_status") == "english",
                    book.get("users_count", 0),
                    book.get("editions_count", 0),
                )
                current_rank = (
                    current.get("language_status") == "english",
                    current.get("users_count", 0),
                    current.get("editions_count", 0),
                ) if current else None
                if current is None or rank > current_rank:
                    by_position[position] = book
            books = list(by_position.values()) + unpositioned
            books.sort(key=lambda item: (
                item.get("position") is None,
                item.get("position") if item.get("position") is not None else 0,
                item.get("title", "").casefold(),
            ))
            if books:
                output.append({
                    "name": raw_series.get("name") or "Unnamed series",
                    "slug": raw_series.get("slug"),
                    "hardcover_series_id": raw_series.get("id"),
                    "books": books,
                })
        return output

    def get_author_series(
        self,
        author_id: int,
        excluded_series_ids: Iterable[int] = (),
    ) -> List[Dict]:
        excluded = sorted({int(value) for value in excluded_series_ids})
        data = self._request(self.AUTHOR_SERIES, {
            "authorId": int(author_id),
            "excludedSeriesIds": excluded,
        })
        return self._normalize_series(data.get("series"))

    def get_series_by_ids(self, series_ids: Iterable[int]) -> List[Dict]:
        """Return ordered series records for stable Hardcover IDs."""
        ids = sorted({int(value) for value in series_ids if int(value) > 0})
        if not ids:
            return []
        data = self._request(self.SERIES_BY_IDS, {"seriesIds": ids})
        return self._normalize_series(data.get("series"))

    def lookup_author_series(
        self,
        author_name: str,
        known_titles: Iterable[str] = (),
        excluded_series_ids: Iterable[int] = (),
    ) -> Dict:
        author = self.find_author(author_name, known_titles)
        if not author:
            return {"author": None, "series": []}
        return {
            "author": {
                "id": author["id"],
                "name": author.get("name") or author_name,
                "slug": author.get("slug"),
            },
            "series": self.get_author_series(author["id"], excluded_series_ids),
        }
