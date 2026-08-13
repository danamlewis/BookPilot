import unittest

from src.api.hardcover import HardcoverClient


class _Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)

    def json(self):
        return self.payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class HardcoverClientTests(unittest.TestCase):
    def test_series_normalization_removes_deduped_non_english_and_position_collisions(self):
        rows = [{
            "id": 12,
            "name": "Example Series",
            "book_series": [
                {"position": 1, "book": {
                    "id": 1, "title": "English Primary", "book_status_id": 1,
                    "canonical_id": None, "compilation": False,
                    "english_editions": [{"id": 10}], "known_language_editions": [{"language_id": 1}],
                    "users_count": 25, "editions_count": 4,
                }},
                {"position": 1, "book": {
                    "id": 2, "title": "Unknown Low Use", "book_status_id": 1,
                    "canonical_id": None, "compilation": False,
                    "english_editions": [], "known_language_editions": [],
                    "users_count": 1, "editions_count": 1,
                }},
                {"position": 2, "book": {
                    "id": 3, "title": "Traduccion", "book_status_id": 1,
                    "canonical_id": None, "compilation": False,
                    "english_editions": [], "known_language_editions": [{"language_id": 148}],
                }},
                {"position": 3, "book": {
                    "id": 4, "title": "Deduped Title", "book_status_id": 4,
                    "canonical_id": 9, "compilation": False,
                }},
                {"position": 4, "book": {
                    "id": 5, "title": "Box Set", "book_status_id": 1,
                    "canonical_id": None, "compilation": True,
                }},
            ],
        }]

        result = HardcoverClient._normalize_series(rows)

        self.assertEqual(
            [(book["position"], book["title"]) for book in result[0]["books"]],
            [(1, "English Primary")],
        )

    def test_current_user_runs_the_documented_token_check(self):
        session = _Session([
            _Response({"data": {"me": [{"username": "reader"}]}}),
        ])
        client = HardcoverClient("test-token", rate_limit_delay=0, session=session)

        self.assertEqual(client.get_current_user(), {"username": "reader"})
        request = session.calls[0][1]
        self.assertIn("query Test", request["json"]["query"])
        self.assertEqual(request["json"]["variables"], {})
        self.assertEqual(request["headers"]["Authorization"], "Bearer test-token")

    def test_selects_exact_author_and_returns_ordered_non_compilation_books(self):
        session = _Session([
            _Response({"data": {"search": {"ids": [2, 7], "results": {
                "hits": [
                    {"document": {"name": "Similar Name", "books_count": 50}},
                    {"document": {"name": "Example Author", "books": ["Known Book"], "books_count": 12}},
                ]
            }}}}),
            _Response({"data": {"series": [{
                "id": 9,
                "name": "Example Series",
                "slug": "example-series",
                "book_series": [
                    {"position": 1, "featured": True, "compilation": False, "book": {"id": 11, "title": "First"}},
                    {"position": 2, "featured": False, "compilation": True, "book": {"id": 12, "title": "Boxed Set"}},
                ],
            }]}}),
        ])
        client = HardcoverClient("secret", rate_limit_delay=0, session=session)

        result = client.lookup_author_series(
            "Example Author", ["Known Book"], excluded_series_ids=[4, 3],
        )

        self.assertEqual(result["author"]["id"], 7)
        self.assertEqual(result["series"][0]["books"][0]["title"], "First")
        self.assertEqual(len(result["series"][0]["books"]), 1)
        self.assertEqual(session.calls[0][1]["headers"]["Authorization"], "Bearer secret")
        self.assertEqual(session.calls[1][1]["json"]["variables"]["excludedSeriesIds"], [3, 4])
        query = session.calls[1][1]["json"]["query"]
        self.assertIn("canonical_id: {_is_null: true}", query)
        self.assertIn("book_status_id: {_eq: 1}", query)
        self.assertIn("english_editions", query)

    def test_fetches_ignored_series_directly_by_stable_ids(self):
        session = _Session([_Response({"data": {"series": [{
            "id": 12,
            "name": "Ignored Series",
            "slug": "ignored-series",
            "book_series": [{
                "position": 1,
                "featured": True,
                "compilation": False,
                "book": {"id": 21, "title": "First Book"},
            }],
        }]}})])
        client = HardcoverClient("secret", rate_limit_delay=0, session=session)

        result = client.get_series_by_ids([12, 12])

        self.assertEqual(result[0]["hardcover_series_id"], 12)
        self.assertEqual(result[0]["books"][0]["title"], "First Book")
        self.assertEqual(session.calls[0][1]["json"]["variables"], {"seriesIds": [12]})


if __name__ == "__main__":
    unittest.main()
