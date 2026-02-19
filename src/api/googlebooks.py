"""Google Books API client"""
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


class GoogleBooksClient:
    """Client for Google Books API"""
    
    BASE_URL = "https://www.googleapis.com/books/v1"
    CACHE_DIR = Path(__file__).parent.parent.parent / 'data' / 'cache' / 'googlebooks'
    
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
        """Make API request with caching, rate limiting, and 429 retry with backoff"""
        cache_key = f"{endpoint}_{params or ''}"
        
        # Check cache
        cached = self._get_cached(cache_key)
        if cached:
            return cached
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        url = f"{self.BASE_URL}{endpoint}"
        max_retries = 2  # Initial attempt + up to 2 retries on 429
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 429:
                    # Too Many Requests - back off and retry
                    try:
                        retry_after = int(response.headers.get('Retry-After', 60))
                    except (ValueError, TypeError):
                        retry_after = 60
                    retry_after = min(max(retry_after, 30), 120)  # Clamp between 30s and 2 min
                    if attempt < max_retries:
                        print(f"  Google Books rate limited (429). Waiting {retry_after}s before retry...")
                        time.sleep(retry_after)
                        continue
                    else:
                        print(f"API request failed: {endpoint} - 429 Too Many Requests (rate limit)")
                        return {}
                response.raise_for_status()
                data = response.json()
                
                # Cache response
                self._set_cache(cache_key, data)
                return data
            except requests.RequestException as e:
                if attempt < max_retries and hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                    retry_after = 60
                    print(f"  Google Books rate limited (429). Waiting {retry_after}s before retry...")
                    time.sleep(retry_after)
                    continue
                print(f"API request failed: {endpoint} - {e}")
                return {}
        return {}
    
    def search_by_author(self, author_name: str, max_results: int = 40) -> List[Dict]:
        """Search for books by author"""
        endpoint = "/volumes"
        params = {
            'q': f'inauthor:"{author_name}"',
            'maxResults': max_results,
            'orderBy': 'relevance'
        }
        result = self._request(endpoint, params)
        return result.get('items', [])
    
    def get_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Get book by ISBN"""
        endpoint = "/volumes"
        params = {'q': f'isbn:{isbn}'}
        result = self._request(endpoint, params)
        items = result.get('items', [])
        return items[0] if items else None
    
    def search_by_title(self, title: str, max_results: int = 10) -> List[Dict]:
        """Search for books by title"""
        endpoint = "/volumes"
        params = {
            'q': f'intitle:"{title}"',
            'maxResults': max_results,
            'orderBy': 'relevance'
        }
        result = self._request(endpoint, params)
        return result.get('items', [])
    
    def extract_categories(self, volume_data: Dict) -> List[str]:
        """Extract categories/genres from volume data"""
        volume_info = volume_data.get('volumeInfo', {})
        return volume_info.get('categories', [])
    
    def extract_description(self, volume_data: Dict) -> Optional[str]:
        """Extract description from volume data"""
        volume_info = volume_data.get('volumeInfo', {})
        return volume_info.get('description') or volume_info.get('description', '')
    
    def extract_series_info(self, volume_data: Dict) -> tuple:
        """
        Extract series information from volume data
        
        Returns:
            (series_name, series_position) or (None, None)
        """
        volume_info = volume_data.get('volumeInfo', {})
        title = volume_info.get('title', '')
        subtitle = volume_info.get('subtitle', '')
        
        # Google Books doesn't always have explicit series info
        # Check subtitle for series indicators
        if subtitle:
            # Look for patterns like "Book 2" or "#2" in subtitle
            import re
            match = re.search(r'(?:book|#)\s*(\d+)', subtitle.lower())
            if match:
                # Try to extract series name from title or subtitle
                series_name = title  # Fallback to title
                position = int(match.group(1))
                return (series_name, position)
        
        return (None, None)
    
    def is_english_language(self, volume_data: Dict) -> bool:
        """
        Check if a Google Books volume is in English
        
        Args:
            volume_data: Volume data from Google Books API
        
        Returns:
            True if English, False otherwise. Returns True if language info not available (assume English)
        """
        volume_info = volume_data.get('volumeInfo', {})
        language = volume_info.get('language', '')
        
        if language:
            # Google Books uses ISO 639-1 codes: 'en' for English
            # Also check for 'en-US', 'en-GB', etc.
            return language.lower().startswith('en')
        
        # If no language info, check title for non-English characters as fallback
        title = volume_info.get('title', '')
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
        # (many books don't have language data but are English)
        return True
