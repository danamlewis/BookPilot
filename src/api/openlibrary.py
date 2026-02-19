"""Open Library API client"""
import requests
import time
import re
from typing import Dict, List, Optional
from pathlib import Path
import json


def sanitize_filename(text: str) -> str:
    """
    Sanitize a string to be safe for use as a filename.
    
    Replaces all problematic characters that can cause issues with:
    - Cloud sync and filesystems (apostrophes, forward slashes)
    - Windows filenames (colons, angle brackets, pipes, etc.)
    - Unix filenames (forward slashes)
    - General filesystem issues
    
    Args:
        text: The string to sanitize
        
    Returns:
        A sanitized string safe for use in filenames
    """
    # Replace problematic characters with underscores
    # Characters that are problematic: / \ ' " ? * < > | : & and control characters
    # Also replace multiple consecutive underscores/spaces with a single underscore
    safe = re.sub(r'[/\\\'"<>|:*?&]', '_', text)
    # Replace control characters (0x00-0x1F and 0x7F)
    safe = re.sub(r'[\x00-\x1F\x7F]', '_', safe)
    # Replace multiple consecutive underscores with a single underscore
    safe = re.sub(r'_+', '_', safe)
    # Remove leading/trailing underscores and spaces
    safe = safe.strip('_ ')
    # If empty after sanitization, use a default value
    if not safe:
        safe = 'empty'
    return safe


class OpenLibraryClient:
    """Client for Open Library API"""
    
    BASE_URL = "https://openlibrary.org"
    CACHE_DIR = Path(__file__).parent.parent.parent / 'data' / 'cache' / 'openlibrary'
    
    def __init__(self, cache_enabled=True, rate_limit_delay=0.5):
        """
        Args:
            cache_enabled: Enable response caching
            rate_limit_delay: Seconds to wait between API calls
        """
        self.cache_enabled = cache_enabled
        self.rate_limit_delay = rate_limit_delay
        if cache_enabled:
            self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """
        Get cache file path for a key.
        
        Note: This sanitization changed from the original implementation to handle
        problematic characters (apostrophes, slashes, etc.) for cloud sync and cross-platform compatibility.
        Old cache files created with the previous sanitization will not be found,
        but this is acceptable as the cache is purely for performance optimization.
        If a cache file isn't found, a fresh API call will be made.
        """
        # Sanitize cache key for filename
        safe_key = sanitize_filename(cache_key)
        return self.CACHE_DIR / f"{safe_key}.json"
    
    def _get_cached(self, cache_key: str) -> Optional[Dict]:
        """Get cached response"""
        if not self.cache_enabled:
            return None
        
        cache_path = self._get_cache_path(cache_key)
        if cache_path.exists():
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def _set_cache(self, cache_key: str, data: Dict):
        """Cache response"""
        if not self.cache_enabled:
            return
        
        cache_path = self._get_cache_path(cache_key)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, 'w') as f:
                json.dump(data, f, indent=2)
        except:
            pass
    
    def _request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with caching and rate limiting"""
        cache_key = f"{endpoint}_{params or ''}"
        
        # Check cache
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Cache response
            self._set_cache(cache_key, data)
            return data
        except requests.RequestException as e:
            # 404 is normal for some endpoints (e.g. editions.json missing for a work); don't spam console
            response = getattr(e, 'response', None)
            is_404 = (response is not None and getattr(response, 'status_code', None) == 404) or '404' in str(e)
            if is_404:
                return {}
            print(f"API request failed: {endpoint} - {e}")
            return {}
    
    def search_author(self, author_name: str) -> List[Dict]:
        """Search for author by name"""
        endpoint = "/search/authors.json"
        params = {'q': author_name}
        result = self._request(endpoint, params)
        return result.get('docs', [])
    
    def get_author_works(self, author_key: str, limit: int = 100) -> List[Dict]:
        """
        Get all works by an author
        
        Args:
            author_key: Open Library author key (e.g., "/authors/OL123456A")
            limit: Maximum number of works to return
        """
        # Ensure author_key starts with /
        if not author_key.startswith('/'):
            author_key = f"/authors/{author_key}"
        endpoint = f"{author_key}/works.json"
        params = {'limit': limit}
        result = self._request(endpoint, params)
        return result.get('entries', [])
    
    def get_work_details(self, work_key: str) -> Dict:
        """Get detailed information about a work"""
        # Ensure work_key starts with /
        if not work_key.startswith('/'):
            work_key = f"/works/{work_key}"
        endpoint = f"{work_key}.json"
        return self._request(endpoint)
    
    def get_book_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Get book information by ISBN"""
        endpoint = "/isbn/{isbn}.json".format(isbn=isbn)
        return self._request(endpoint)
    
    def get_editions(self, work_key: str) -> List[Dict]:
        """Get all editions of a work"""
        # Ensure work_key starts with /
        if not work_key.startswith('/'):
            work_key = f"/works/{work_key}"
        endpoint = f"{work_key}/editions.json"
        result = self._request(endpoint, params={'limit': 100})
        return result.get('entries', [])


def extract_series_info(work_data: Dict) -> tuple:
    """
    Extract series information from work data and title
    
    Returns:
        (series_name, series_position) or (None, None)
    """
    import re
    
    # First, try Open Library's explicit series data
    series = work_data.get('series', [])
    if series:
        # Usually first series is primary
        series_name = series[0] if isinstance(series[0], str) else series[0].get('name', '')
        # Position might be in work data or edition
        position = work_data.get('series_position', None)
        if series_name:
            return (series_name, position)
    
    # If no explicit series data, try to extract from title
    # Pattern: "Title (Series Name Book #3)" or "Title (Series Name #3)"
    title = work_data.get('title', '')
    if title:
        # Pattern 1: "Title (Series Name Book #3)" or "Title (Series Name #3)"
        # Pattern 2: "Title (Series Name, Book 3)" or "Title (Series Name, #3)"
        paren_match = re.search(r'\(([^)]+)\)', title)
        if paren_match:
            paren_content = paren_match.group(1)
            
            # Look for "Book #N" or "#N" or "Book N" pattern
            # Examples: "Brookstone Brides Book #3", "Brookstone Brides #3", "Series Name, Book 3"
            book_pattern = re.search(r'(.+?)(?:\s+Book)?\s*#?\s*(\d+)', paren_content, re.IGNORECASE)
            if book_pattern:
                # Extract series name (everything before "Book #N" or "#N")
                potential_series = book_pattern.group(1).strip()
                position_str = book_pattern.group(2).strip()
                
                # Clean up series name - remove trailing "Book" if present
                potential_series = re.sub(r'\s+Book\s*$', '', potential_series, flags=re.IGNORECASE).strip()
                
                # Only use if it looks like a series name (not just a number or very short)
                if potential_series and len(potential_series) > 2:
                    try:
                        position = int(position_str)
                        return (potential_series, position)
                    except ValueError:
                        pass
            
            # Pattern 2: Just series name in parentheses without number
            # "Title (Series Name)" - might be a series but no position info
            # We'll skip this case since we can't determine position
    
    return (None, None)


def extract_isbn(work_data: Dict, edition_data: Dict = None) -> Optional[str]:
    """Extract ISBN from work or edition data"""
    # Try edition first (more reliable)
    if edition_data:
        isbns = edition_data.get('isbn_13', []) or edition_data.get('isbn_10', [])
        if isbns:
            return isbns[0]
    
    # Try work
    isbns = work_data.get('isbn_13', []) or work_data.get('isbn_10', [])
    if isbns:
        return isbns[0]
    
    return None


def is_english_language(work_data: Dict, edition_data: Dict = None) -> bool:
    """
    Check if a work/edition is in English
    
    Args:
        work_data: Work data from Open Library
        edition_data: Optional edition data (more reliable for language)
    
    Returns:
        True if English, False otherwise. Returns True if language info not available (assume English)
    """
    # Check edition first (more reliable)
    if edition_data:
        languages = edition_data.get('languages', [])
        if languages:
            # Open Library uses keys like "/languages/eng"
            for lang in languages:
                lang_key = lang if isinstance(lang, str) else lang.get('key', '')
                if '/languages/eng' in lang_key or lang_key == 'eng':
                    return True
            # If languages specified but none are English, return False
            return False
    
    # Check work
    languages = work_data.get('languages', [])
    if languages:
        for lang in languages:
            lang_key = lang if isinstance(lang, str) else lang.get('key', '')
            if '/languages/eng' in lang_key or lang_key == 'eng':
                return True
        # If languages specified but none are English, return False
        return False
    
    # If no language info, check title for non-English characters as fallback
    title = work_data.get('title', '') or (edition_data.get('title', '') if edition_data else '')
    if title:
        # Check for common non-English character ranges
        # CJK (Chinese, Japanese, Korean), Cyrillic, Arabic, Hebrew, etc.
        import re
        # Pattern matches characters outside basic Latin, extended Latin, and common punctuation
        non_english_pattern = re.compile(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\u0400-\u04ff\u0600-\u06ff\u0590-\u05ff]')
        if non_english_pattern.search(title):
            # Found non-English characters, likely not English
            return False
    
    # If no language info and no obvious non-English characters, assume English
    # (many works don't have language data but are English)
    return True
