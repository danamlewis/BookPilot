import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

from src.api.openlibrary import OpenLibraryClient


class OpenLibraryRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self.temporary_directory.name)

    def tearDown(self):
        # The production client intentionally keeps this process-wide. Tests
        # use short-lived directories, so release those keys before deletion
        # to avoid retaining one dead path per test run.
        with OpenLibraryClient._AUTOMATIC_CLEANUP_LOCK:
            OpenLibraryClient._AUTOMATICALLY_CLEANED_DIRS = {
                path for path in OpenLibraryClient._AUTOMATICALLY_CLEANED_DIRS
                if path != self.cache_dir.resolve()
            }
        self.temporary_directory.cleanup()

    def make_client(self, **overrides):
        options = {
            'cache_dir': self.cache_dir,
            'rate_limit_delay': 0,
            'cleanup_stale_cache': False,
        }
        options.update(overrides)
        return OpenLibraryClient(**options)

    def test_expired_cache_entry_is_not_returned(self):
        client = self.make_client(cache_ttl_seconds=60)
        client._set_cache('old-response', {'source': 'cache'})
        cache_path = client._get_cache_path('old-response')
        old_time = time.time() - 61
        os.utime(cache_path, (old_time, old_time))

        self.assertIsNone(client._get_cached('old-response'))
        self.assertFalse(cache_path.exists())

    def test_unexpired_empty_response_is_a_valid_cache_hit(self):
        client = self.make_client(cache_ttl_seconds=60)
        client._set_cache('/missing.json_', {})

        with patch('src.api.openlibrary.requests.get') as get:
            result = client._request('/missing.json')

        self.assertEqual(result, {})
        get.assert_not_called()

    def test_fresh_request_bypasses_and_replaces_cache(self):
        client = self.make_client(cache_ttl_seconds=60)
        endpoint = '/works/OL1W.json'
        cache_key = f"{endpoint}_"
        client._set_cache(cache_key, {'title': 'Old title'})

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {'title': 'Current title'}
        with patch('src.api.openlibrary.requests.get', return_value=response) as get:
            result = client.get_work_details('/works/OL1W', fresh=True)

        self.assertEqual(result['title'], 'Current title')
        get.assert_called_once()
        self.assertEqual(client._get_cached(cache_key)['title'], 'Current title')

    def test_author_works_limit_none_follows_all_pages(self):
        client = self.make_client(cache_enabled=False)
        requested = []

        def request(_endpoint, params=None, fresh=False):
            requested.append((params.copy(), fresh))
            offset = params['offset']
            count = min(params['limit'], 450 - offset)
            return {
                'size': 450,
                'entries': [
                    {'key': f'/works/OL{number}W'}
                    for number in range(offset, offset + count)
                ],
            }

        client._request = request
        works = client.get_author_works(
            '/authors/OL1A', limit=None, fresh=True, page_size=200
        )

        self.assertEqual(len(works), 450)
        self.assertEqual(
            requested,
            [
                ({'limit': 200, 'offset': 0}, True),
                ({'limit': 200, 'offset': 200}, True),
                ({'limit': 200, 'offset': 400}, True),
            ],
        )

    def test_author_works_finite_limit_can_cross_page_boundary(self):
        client = self.make_client(cache_enabled=False)
        requested = []

        def request(_endpoint, params=None, fresh=False):
            requested.append(params.copy())
            offset = params['offset']
            return {
                'size': 600,
                'entries': [
                    {'key': f'/works/OL{number}W'}
                    for number in range(offset, offset + params['limit'])
                ],
            }

        client._request = request
        works = client.get_author_works('OL1A', limit=250, page_size=200)

        self.assertEqual(len(works), 250)
        self.assertEqual(
            requested,
            [
                {'limit': 200, 'offset': 0},
                {'limit': 50, 'offset': 200},
            ],
        )

    def test_short_server_pages_continue_when_size_reports_more(self):
        client = self.make_client(cache_enabled=False)
        requested_offsets = []

        def request(_endpoint, params=None, fresh=False):
            offset = params['offset']
            requested_offsets.append(offset)
            count = min(2, 5 - offset)
            return {
                'size': 5,
                'entries': [{'key': f'/works/{number}'} for number in range(offset, offset + count)],
            }

        client._request = request
        works = client.get_author_works('OL1A', limit=None, page_size=200)

        self.assertEqual(len(works), 5)
        self.assertEqual(requested_offsets, [0, 2, 4])

    def test_stale_cleanup_has_scan_and_delete_bounds(self):
        client = self.make_client(cache_ttl_seconds=60)
        old_time = time.time() - 61
        for number in range(5):
            path = self.cache_dir / f'stale-{number}.json'
            path.write_text(json.dumps({'number': number}))
            os.utime(path, (old_time, old_time))
        (self.cache_dir / 'leave-me.tmp').write_text('temporary')

        result = client.cleanup_stale_cache(scan_limit=3, delete_limit=2)

        self.assertLessEqual(result['scanned'], 3)
        self.assertEqual(result['deleted'], 2)
        self.assertEqual(len(list(self.cache_dir.glob('stale-*.json'))), 3)
        self.assertTrue((self.cache_dir / 'leave-me.tmp').exists())

    def test_first_cache_use_cleans_the_overridden_cache_directory(self):
        old_time = time.time() - 61
        stale_path = self.cache_dir / 'stale.json'
        stale_path.write_text('{}')
        os.utime(stale_path, (old_time, old_time))

        client = self.make_client(
            cache_ttl_seconds=60,
            cleanup_stale_cache=True,
            cleanup_scan_limit=10,
            cleanup_delete_limit=10,
        )
        client._get_cached('not-present')

        self.assertFalse(stale_path.exists())

    def test_automatic_cleanup_runs_once_for_multiple_clients_in_same_directory(self):
        workers = 8
        start_together = threading.Barrier(workers)

        def construct_client():
            start_together.wait()
            client = self.make_client(cleanup_stale_cache=True)
            client._get_cached('not-present')
            return client

        with patch.object(
            OpenLibraryClient,
            'cleanup_stale_cache',
            autospec=True,
            return_value={'scanned': 0, 'deleted': 0},
        ) as cleanup:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                clients = list(executor.map(lambda _number: construct_client(), range(workers)))

        self.assertEqual(cleanup.call_count, 1)
        self.assertEqual(len(clients), workers)
        self.assertTrue(all(call.args[0].cache_dir == self.cache_dir for call in cleanup.call_args_list))

    def test_automatic_cleanup_runs_once_for_each_separate_directory(self):
        with tempfile.TemporaryDirectory() as second_directory:
            with patch.object(
                OpenLibraryClient,
                'cleanup_stale_cache',
                autospec=True,
                return_value={'scanned': 0, 'deleted': 0},
            ) as cleanup:
                first_directory_client = self.make_client(cleanup_stale_cache=True)
                second_directory_client = OpenLibraryClient(
                    cache_dir=second_directory,
                    rate_limit_delay=0,
                    cleanup_stale_cache=True,
                )
                another_second_directory_client = OpenLibraryClient(
                    cache_dir=second_directory,
                    rate_limit_delay=0,
                    cleanup_stale_cache=True,
                )
                first_directory_client._get_cached('not-present')
                second_directory_client._get_cached('not-present')
                another_second_directory_client._get_cached('not-present')

        self.assertEqual(cleanup.call_count, 2)
        cleaned_directories = {call.args[0].cache_dir.resolve() for call in cleanup.call_args_list}
        self.assertEqual(
            cleaned_directories,
            {self.cache_dir.resolve(), Path(second_directory).resolve()},
        )

    def test_explicit_cleanup_remains_callable_after_automatic_cleanup(self):
        client = self.make_client(cache_ttl_seconds=60, cleanup_stale_cache=True)
        client._get_cached('not-present')
        old_time = time.time() - 61
        stale_path = self.cache_dir / 'created-after-automatic-cleanup.json'
        stale_path.write_text('{}')
        os.utime(stale_path, (old_time, old_time))

        result = client.cleanup_stale_cache(scan_limit=10, delete_limit=10)

        self.assertEqual(result['deleted'], 1)
        self.assertFalse(stale_path.exists())

    def test_constructing_unused_clients_does_not_touch_the_cache(self):
        unused_directory = self.cache_dir / 'unused-cache'
        with patch.object(
            OpenLibraryClient,
            'cleanup_stale_cache',
            autospec=True,
            return_value={'scanned': 0, 'deleted': 0},
        ) as cleanup:
            OpenLibraryClient(cache_dir=unused_directory, cleanup_stale_cache=True)
            OpenLibraryClient(cache_dir=unused_directory, cleanup_stale_cache=True)

        cleanup.assert_not_called()
        self.assertFalse(unused_directory.exists())


if __name__ == '__main__':
    unittest.main()
