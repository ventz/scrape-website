import argparse
import asyncio
import aiohttp
import aiofiles
import os
import re
import sqlite3
import logging
import json
from urllib.parse import urlparse, urljoin, urlsplit, urlunsplit, parse_qsl, urlencode
import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request
from urllib.error import URLError
from pathlib import Path
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from typing import Deque
import mimetypes
import hashlib
from datetime import datetime
from functools import lru_cache

import lxml.html
import trafilatura

# Configuration defaults
CONFIG = {
    'max_concurrent': 100,  # Number of concurrent downloads
    'timeout': 30,  # Request timeout in seconds
    'max_retries': 3,  # Max retries for failed requests
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'delay_between_requests': 0.1,  # Politeness delay in seconds
    'max_file_size': 100 * 1024 * 1024,  # 100MB max file size
    'checkpoint_interval': 30,  # Seconds between queue checkpoints
    'progress_interval': 5,  # Seconds between progress reports
}

# File extensions to download
DOWNLOADABLE_EXTENSIONS = {
    '.pdf', '.doc', '.docx', '.ppt', '.pptx',
    '.xls', '.xlsx', '.txt', '.csv', '.zip',
    '.rtf', '.odt', '.ods', '.odp'
}

# MIME types to download
DOWNLOADABLE_MIMES = {
    'application/pdf',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.ms-powerpoint',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'text/plain',
    'text/csv',
    'application/zip',
    'application/rtf',
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.presentation',
}


# Regex patterns for URLs commonly worth skipping on blog/CMS sites.
# These are matched against the full URL (re.search). Override via
# --exclude-pattern (repeatable) or programmatic API.
_DEFAULT_EXCLUDE_PATTERNS: list[str] = [
    r"/tag/",
    r"/author/",
    r"/feed/?$",
    r"/print/",
    r"\?print=",
    r"/comments/",
    r"/page/\d+",
    r"/cdn-cgi/",
]

# Query-string params that are tracking only — safe to drop to prevent
# `/page?utm_source=email` and `/page?utm_source=twitter` from being
# stored as two different pages. Add more as you encounter them.
_DEFAULT_TRACKING_PARAMS: frozenset[str] = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_eid", "mc_cid", "ref",
    "_ga", "_gl", "igshid", "msclkid", "dclid",
})


# ---------------------------------------------------------------------------
# Top-level helper functions
# ---------------------------------------------------------------------------

def _strip_tracking_params(url: str,
                           tracking_params: frozenset[str] = _DEFAULT_TRACKING_PARAMS) -> str:
    """Return *url* with tracking-only query-string keys removed.

    Preserves order of non-tracking params.  Returns the URL unchanged
    when it has no query string or all params are tracking-only (in which
    case the ``?`` is also dropped).
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    cleaned = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
               if k not in tracking_params]
    new_query = urlencode(cleaned)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, ''))


def _url_excluded(url: str, patterns: list[re.Pattern]) -> bool:
    """True iff any compiled regex pattern matches *url*.

    Empty *patterns* list means nothing is excluded (returns False).
    """
    for pat in patterns:
        if pat.search(url):
            return True
    return False


def _fetch_sitemap_urls(host: str, scheme: str = "https",
                        timeout: int = 10, max_urls: int = 5000) -> list[str]:
    """Best-effort sitemap discovery.

    Tries ``{scheme}://{host}/sitemap.xml`` then
    ``{scheme}://{host}/sitemap_index.xml``.  Recurses into
    ``<sitemap><loc>`` entries (sitemap-index format) up to one level.
    Returns a deduped list of ``<loc>`` URLs, capped at *max_urls*.
    Any fetch/parse failure returns ``[]``.

    Uses only stdlib (``urllib`` + ``xml.etree``) — no new deps.
    """
    # Common XML namespace used in sitemaps
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def _get(url: str) -> bytes | None:
        try:
            req = Request(url, headers={"User-Agent": CONFIG["user_agent"]})
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception:
            return None

    def _parse_locs(xml_bytes: bytes, tag: str = "url") -> list[str]:
        """Extract <loc> text from <url> or <sitemap> elements."""
        urls: list[str] = []
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            return urls
        # Try with namespace first, then without
        for elem in root.findall(f"sm:{tag}/sm:loc", ns):
            if elem.text:
                urls.append(elem.text.strip())
        if not urls:
            for elem in root.findall(f"{tag}/loc"):
                if elem.text:
                    urls.append(elem.text.strip())
            # Also try namespace-stripped approach
            if not urls:
                for elem in root.iter():
                    local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if local == "loc" and elem.text:
                        urls.append(elem.text.strip())
        return urls

    seen: set[str] = set()
    result: list[str] = []

    for path in ("/sitemap.xml", "/sitemap_index.xml"):
        sitemap_url = f"{scheme}://{host}{path}"
        data = _get(sitemap_url)
        if not data:
            continue

        # Check for sitemap index (contains <sitemap> elements)
        sub_sitemaps = _parse_locs(data, tag="sitemap")
        if sub_sitemaps:
            for sub_url in sub_sitemaps:
                sub_data = _get(sub_url)
                if sub_data:
                    for loc in _parse_locs(sub_data, tag="url"):
                        if loc not in seen:
                            seen.add(loc)
                            result.append(loc)
                            if len(result) >= max_urls:
                                return result

        # Also parse direct <url><loc> entries
        for loc in _parse_locs(data, tag="url"):
            if loc not in seen:
                seen.add(loc)
                result.append(loc)
                if len(result) >= max_urls:
                    return result

    return result


# ---------------------------------------------------------------------------
# Top-level functions for ProcessPoolExecutor (must be picklable)
# ---------------------------------------------------------------------------

def _normalize_url(url: str, strip_tracking: bool = False) -> str:
    """Normalize URL by removing fragments and trailing slashes.

    When *strip_tracking* is True, also removes well-known tracking
    query parameters (utm_*, fbclid, gclid, etc.).
    """
    parsed = urlparse(url)
    url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if parsed.query:
        url += f"?{parsed.query}"
    if url.endswith('/') and parsed.path != '/':
        url = url[:-1]
    if strip_tracking:
        url = _strip_tracking_params(url)
    return url


def _extract_links_lxml(html_content: str, base_url: str, base_domain: str,
                        strip_tracking: bool = False,
                        exclude_patterns: list[str] | None = None) -> set[str]:
    """Extract links using lxml (5-20x faster than BeautifulSoup).

    *exclude_patterns*: list of regex **strings** (not compiled) — we
    compile them here because compiled patterns are not picklable across
    the process-pool boundary.
    """
    compiled = [re.compile(p) for p in (exclude_patterns or [])]
    links = set()
    try:
        doc = lxml.html.fromstring(html_content)
        doc.make_links_absolute(base_url, resolve_base_href=True)

        for element, attribute, link, pos in doc.iterlinks():
            if not link or not link.startswith('http'):
                continue
            normalized = _normalize_url(link, strip_tracking=strip_tracking)
            parsed = urlparse(normalized)
            tag = element.tag

            if tag == 'a':
                # Follow all same-domain <a> links
                if parsed.netloc == base_domain:
                    if not _url_excluded(normalized, compiled):
                        links.add(normalized)
            elif tag in ('link', 'script', 'img'):
                # Only follow non-<a> tags if they point to downloadable files
                path_lower = parsed.path.lower()
                if any(path_lower.endswith(ext) for ext in DOWNLOADABLE_EXTENSIONS):
                    links.add(normalized)
    except Exception:
        pass
    return links


def _extract_text_trafilatura(html_content: str, url: str) -> str | None:
    """Extract clean text using trafilatura — best quality for LLM consumption."""
    try:
        text = trafilatura.extract(
            html_content,
            url=url,
            include_comments=False,
            include_tables=True,
            include_links=True,
            include_images=False,
            favor_recall=True,       # maximize content extraction
            deduplicate=True,
            output_format='txt',
        )
        return text
    except Exception:
        return None


def _parse_and_extract(html_content: str, url: str, base_domain: str,
                       strip_tracking: bool = False,
                       exclude_patterns: list[str] | None = None) -> tuple[set[str], str | None]:
    """Combined link extraction + text extraction in one process pool call."""
    links = _extract_links_lxml(html_content, url, base_domain,
                                strip_tracking=strip_tracking,
                                exclude_patterns=exclude_patterns)
    text = _extract_text_trafilatura(html_content, url)
    return links, text


# ---------------------------------------------------------------------------
# SQLite-backed URL store
# ---------------------------------------------------------------------------

class URLStore:
    """SQLite-backed visited URL tracking with in-memory LRU cache."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path), isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS visited (url TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS downloaded_files (hash TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS queue (url TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value TEXT)")
        # In-memory cache for fast lookups
        self._cache: set[str] = set()
        self._cache_limit = 100_000
        self._count = self.conn.execute("SELECT COUNT(*) FROM visited").fetchone()[0]

    def contains(self, url: str) -> bool:
        if url in self._cache:
            return True
        row = self.conn.execute("SELECT 1 FROM visited WHERE url=?", (url,)).fetchone()
        if row:
            self._add_to_cache(url)
            return True
        return False

    def add(self, url: str):
        try:
            self.conn.execute("INSERT INTO visited (url) VALUES (?)", (url,))
            self._add_to_cache(url)
            self._count += 1
        except sqlite3.IntegrityError:
            pass

    def _add_to_cache(self, url: str):
        if len(self._cache) >= self._cache_limit:
            # Evict ~20% of cache
            to_remove = list(self._cache)[:self._cache_limit // 5]
            for item in to_remove:
                self._cache.discard(item)
        self._cache.add(url)

    @property
    def count(self) -> int:
        return self._count

    def has_file_hash(self, file_hash: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM downloaded_files WHERE hash=?", (file_hash,)).fetchone()
        return row is not None

    def add_file_hash(self, file_hash: str):
        try:
            self.conn.execute("INSERT INTO downloaded_files (hash) VALUES (?)", (file_hash,))
        except sqlite3.IntegrityError:
            pass

    def save_queue(self, urls: Deque[str]):
        self.conn.execute("DELETE FROM queue")
        self.conn.executemany("INSERT OR IGNORE INTO queue (url) VALUES (?)", [(u,) for u in urls])

    def load_queue(self) -> Deque[str]:
        rows = self.conn.execute("SELECT url FROM queue").fetchall()
        return deque(row[0] for row in rows)

    def save_stats(self, stats: dict):
        self.conn.execute("INSERT OR REPLACE INTO stats (key, value) VALUES (?, ?)",
                          ('stats', json.dumps(stats)))

    def load_stats(self) -> dict | None:
        row = self.conn.execute("SELECT value FROM stats WHERE key='stats'").fetchone()
        if row:
            return json.loads(row[0])
        return None

    def clear(self):
        self.conn.execute("DELETE FROM visited")
        self.conn.execute("DELETE FROM downloaded_files")
        self.conn.execute("DELETE FROM queue")
        self.conn.execute("DELETE FROM stats")
        self._cache.clear()
        self._count = 0

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

class WebsiteScraper:
    def __init__(self, start_url: str, fresh: bool = False,
                 exclude_patterns: list[str] | None = None,
                 strip_tracking_params: bool = True,
                 use_sitemap: bool = True):
        self.start_url = start_url
        self.base_domain = self.extract_domain(start_url)

        # Crawl-quality knobs
        self.strip_tracking_params = strip_tracking_params
        self.use_sitemap = use_sitemap
        # Store patterns as strings (for pickling to process pool)
        self._exclude_pattern_strings: list[str] = (
            exclude_patterns if exclude_patterns is not None
            else list(_DEFAULT_EXCLUDE_PATTERNS)
        )
        # Pre-compile for in-process filtering (e.g. sitemap seed)
        self._compiled_exclude_patterns: list[re.Pattern] = [
            re.compile(p) for p in self._exclude_pattern_strings
        ]
        self.session = None
        self.semaphore = asyncio.Semaphore(CONFIG['max_concurrent'])
        self.denied_urls: list[str] = []
        self.failed_urls: list[str] = []

        # Stats
        self.stats = {
            'pages_downloaded': 0,
            'files_downloaded': 0,
            'text_extracted': 0,
            'errors': 0,
            'denied': 0,
            'total_bytes': 0,
        }

        # Setup directories
        self.base_dir = Path('data') / self.base_domain
        self.pages_dir = self.base_dir / 'pages'
        self.text_dir = self.base_dir / 'text'
        self.files_dir = self.base_dir / 'files'
        self.logs_dir = self.base_dir / 'logs'
        for d in (self.pages_dir, self.text_dir, self.files_dir, self.logs_dir):
            d.mkdir(parents=True, exist_ok=True)

        # Logging
        self.logger = logging.getLogger(f"scraper.{self.base_domain}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        # File handler
        fh = logging.FileHandler(self.logs_dir / 'scrape.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        self.logger.addHandler(fh)
        # Console handler (INFO only)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(ch)

        # SQLite-backed URL store
        self.url_store = URLStore(self.logs_dir / 'state.db')

        # Handle fresh start vs resume
        if fresh:
            self.url_store.clear()
            self.urls_to_visit: Deque[str] = deque([start_url])
            self.logger.info("Fresh start (--fresh): cleared previous state")
        else:
            # Try to resume from checkpoint
            saved_queue = self.url_store.load_queue()
            saved_stats = self.url_store.load_stats()
            if saved_queue and self.url_store.count > 0:
                self.urls_to_visit = saved_queue
                if saved_stats:
                    self.stats.update(saved_stats)
                self.logger.info(f"Resuming: {self.url_store.count} URLs visited, {len(saved_queue)} in queue")
            else:
                self.urls_to_visit = deque([start_url])

        # ProcessPoolExecutor for CPU-bound parsing
        self.executor = ProcessPoolExecutor(max_workers=os.cpu_count())

        self.logger.info(f"Output directory: {self.base_dir}")
        self.logger.info(f"Starting domain: {self.base_domain}")
        self.logger.info(f"Max concurrent requests: {CONFIG['max_concurrent']}")

    @staticmethod
    def extract_domain(url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc

    def normalize_url(self, url: str) -> str:
        return _normalize_url(url, strip_tracking=self.strip_tracking_params)

    def is_same_domain(self, url: str) -> bool:
        return self.extract_domain(url) == self.base_domain

    def should_download_file(self, url: str, content_type: str = None) -> bool:
        path = urlparse(url).path.lower()
        if any(path.endswith(ext) for ext in DOWNLOADABLE_EXTENSIONS):
            return True
        if content_type:
            content_type = content_type.lower().split(';')[0].strip()
            if content_type in DOWNLOADABLE_MIMES:
                return True
        return False

    def get_file_extension(self, url: str, content_type: str = None) -> str:
        path = urlparse(url).path
        if '.' in path:
            ext = path.split('.')[-1].lower()
            if f'.{ext}' in DOWNLOADABLE_EXTENSIONS:
                return f'.{ext}'
        if content_type:
            content_type = content_type.lower().split(';')[0].strip()
            ext = mimetypes.guess_extension(content_type)
            if ext:
                return ext
        return '.bin'

    def generate_filename(self, url: str, content_type: str = None) -> str:
        parsed = urlparse(url)
        path = parsed.path
        if path and path != '/':
            original_name = path.split('/')[-1]
            original_name = original_name.split('?')[0]
            if original_name:
                original_name = re.sub(r'[^\w\s\-\.]', '_', original_name)
                return original_name
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        ext = self.get_file_extension(url, content_type)
        return f"file_{url_hash}{ext}"

    def generate_html_filename(self, url: str) -> str:
        """Generate filename stem for HTML content (used for both .html and .txt)."""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        if not path:
            filename = 'index'
        else:
            filename = path.replace('/', '_')
            if filename.endswith('.html'):
                filename = filename[:-5]
        filename = re.sub(r'[^\w\s\-\.]', '_', filename)
        return filename

    async def init_session(self):
        connector = aiohttp.TCPConnector(
            limit=CONFIG['max_concurrent'],
            limit_per_host=CONFIG['max_concurrent'],
            resolver=aiohttp.AsyncResolver(),
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=CONFIG['timeout'])
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': CONFIG['user_agent']},
            max_field_size=32768,
        )

    async def close_session(self):
        if self.session:
            await self.session.close()

    async def fetch_with_retry(self, url: str, method: str = 'GET') -> tuple:
        last_error = None
        for attempt in range(CONFIG['max_retries']):
            try:
                async with self.session.request(method, url, allow_redirects=True) as response:
                    content_type = response.headers.get('Content-Type', '')
                    status = response.status
                    if self.should_download_file(url, content_type):
                        content = await response.read()
                        return content, content_type, 'file', status
                    else:
                        # Charset-safe decode: aiohttp's resp.text() falls back to
                        # chardet when Content-Type lacks a charset, and chardet
                        # frequently mis-guesses UTF-8 as Windows-1252 — producing
                        # mojibake like `—` → `â€"`. Prefer the declared charset
                        # unless it's one of the legacy HTTP defaults that servers
                        # send incorrectly; otherwise force UTF-8 with replacement.
                        raw = await response.read()
                        declared = (response.charset or '').lower()
                        encoding = declared if declared and declared not in ('iso-8859-1', 'windows-1252') else 'utf-8'
                        try:
                            content = raw.decode(encoding)
                        except (UnicodeDecodeError, LookupError):
                            content = raw.decode('utf-8', errors='replace')
                        return content, content_type, 'html', status
            except asyncio.TimeoutError:
                last_error = "Timeout"
                if attempt < CONFIG['max_retries'] - 1:
                    await asyncio.sleep(1 * (attempt + 1))
            except Exception as e:
                last_error = str(e)
                if attempt < CONFIG['max_retries'] - 1:
                    await asyncio.sleep(1 * (attempt + 1))
        raise Exception(f"Failed after {CONFIG['max_retries']} attempts: {last_error}")

    async def download_file(self, url: str, content: bytes, content_type: str):
        file_hash = hashlib.md5(content).hexdigest()
        if self.url_store.has_file_hash(file_hash):
            return

        filename = self.generate_filename(url, content_type)
        filepath = self.files_dir / filename

        counter = 1
        while filepath.exists():
            name, ext = os.path.splitext(filename)
            filepath = self.files_dir / f"{name}_{counter}{ext}"
            counter += 1

        async with aiofiles.open(filepath, 'wb') as f:
            await f.write(content)
        self.url_store.add_file_hash(file_hash)
        self.stats['files_downloaded'] += 1
        self.stats['total_bytes'] += len(content)

        size_mb = len(content) / (1024 * 1024)
        self.logger.debug(f"Downloaded file: {filepath.name} ({size_mb:.2f} MB)")

    async def save_html(self, url: str, content: str):
        stem = self.generate_html_filename(url)
        filepath = self.pages_dir / f"{stem}.html"

        counter = 1
        while filepath.exists():
            filepath = self.pages_dir / f"{stem}_{counter}.html"
            counter += 1

        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(content)
        self.stats['pages_downloaded'] += 1
        self.stats['total_bytes'] += len(content.encode('utf-8'))
        self.logger.debug(f"Saved page: {filepath.name}")

    async def save_text(self, url: str, text: str):
        """Save extracted clean text for LLM consumption."""
        stem = self.generate_html_filename(url)
        filepath = self.text_dir / f"{stem}.txt"

        counter = 1
        while filepath.exists():
            filepath = self.text_dir / f"{stem}_{counter}.txt"
            counter += 1

        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(text)
        self.stats['text_extracted'] += 1
        self.logger.debug(f"Saved text: {filepath.name}")

    def is_access_denied(self, content: str, status: int) -> bool:
        if status in (401, 403):
            return True
        if len(content) < 2000 and 'Access Denied' in content:
            return True
        return False

    async def process_url(self, url: str):
        async with self.semaphore:
            try:
                await asyncio.sleep(CONFIG['delay_between_requests'])

                content, content_type, content_kind, status = await self.fetch_with_retry(url)

                if content_kind == 'file':
                    if len(content) > CONFIG['max_file_size']:
                        self.logger.debug(f"Skipping large file: {url} ({len(content) / (1024*1024):.2f} MB)")
                        return
                    await self.download_file(url, content, content_type)
                else:
                    if self.is_access_denied(content, status):
                        self.stats['denied'] += 1
                        self.denied_urls.append(url)
                        self.logger.debug(f"Access denied ({status}): {url}")
                        return

                    # Offload parsing + text extraction to process pool
                    loop = asyncio.get_running_loop()
                    links, extracted_text = await loop.run_in_executor(
                        self.executor, _parse_and_extract, content, url,
                        self.base_domain, self.strip_tracking_params,
                        self._exclude_pattern_strings,
                    )

                    # Save HTML
                    await self.save_html(url, content)

                    # Save extracted text if we got any
                    if extracted_text and extracted_text.strip():
                        await self.save_text(url, extracted_text)

                    # Queue new links
                    for link in links:
                        if not self.url_store.contains(link):
                            self.urls_to_visit.append(link)

            except Exception as e:
                self.stats['errors'] += 1
                self.failed_urls.append(url)
                self.logger.debug(f"Error processing {url}: {e}")

    async def _progress_reporter(self):
        """Periodically log progress summary."""
        while True:
            await asyncio.sleep(CONFIG['progress_interval'])
            self.logger.info(
                f"Progress: {self.url_store.count} visited | "
                f"{self.stats['pages_downloaded']} pages | "
                f"{self.stats['text_extracted']} text | "
                f"{self.stats['files_downloaded']} files | "
                f"{self.stats['denied']} denied | "
                f"{self.stats['errors']} errors | "
                f"{self.stats['total_bytes'] / (1024*1024):.1f} MB | "
                f"{len(self.urls_to_visit)} queued"
            )

    async def _checkpoint_saver(self):
        """Periodically checkpoint queue + stats to SQLite for crash recovery."""
        while True:
            await asyncio.sleep(CONFIG['checkpoint_interval'])
            self.url_store.save_queue(self.urls_to_visit)
            self.url_store.save_stats(self.stats)
            self.logger.debug(f"Checkpoint saved: {len(self.urls_to_visit)} URLs in queue")

    async def crawl(self):
        await self.init_session()

        # Seed from sitemap if enabled (best-effort, non-blocking)
        if self.use_sitemap:
            parsed_start = urlparse(self.start_url)
            sitemap_urls = _fetch_sitemap_urls(
                self.base_domain, scheme=parsed_start.scheme or "https",
            )
            if sitemap_urls:
                added = 0
                for surl in sitemap_urls:
                    normalized = _normalize_url(surl, strip_tracking=self.strip_tracking_params)
                    nparsed = urlparse(normalized)
                    if nparsed.netloc != self.base_domain:
                        continue
                    if _url_excluded(normalized, self._compiled_exclude_patterns):
                        continue
                    if not self.url_store.contains(normalized):
                        self.urls_to_visit.append(normalized)
                        added += 1
                if added:
                    self.logger.info(f"Sitemap: seeded {added} URLs from sitemap.xml")

        # Start background tasks
        progress_task = asyncio.create_task(self._progress_reporter())
        checkpoint_task = asyncio.create_task(self._checkpoint_saver())

        try:
            tasks = []

            while self.urls_to_visit or tasks:
                while self.urls_to_visit and len(tasks) < CONFIG['max_concurrent']:
                    url = self.urls_to_visit.popleft()

                    if not self.url_store.contains(url):
                        self.url_store.add(url)
                        task = asyncio.create_task(self.process_url(url))
                        tasks.append(task)

                if tasks:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    tasks = list(tasks)

        finally:
            progress_task.cancel()
            checkpoint_task.cancel()
            # Final checkpoint
            self.url_store.save_queue(self.urls_to_visit)
            self.url_store.save_stats(self.stats)
            await self.close_session()

    async def run(self):
        start_time = datetime.now()
        self.logger.info(f"Starting scraper at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        await self.crawl()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Write denied URLs to file
        if self.denied_urls:
            denied_file = self.logs_dir / 'access_denied.txt'
            async with aiofiles.open(denied_file, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(self.denied_urls) + '\n')

        # Write failed URLs to file for retry
        if self.failed_urls:
            failed_file = self.logs_dir / 'failed_urls.txt'
            async with aiofiles.open(failed_file, 'w', encoding='utf-8') as f:
                await f.write('\n'.join(self.failed_urls) + '\n')

        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(f"URLs visited: {self.url_store.count}")
        self.logger.info(f"Pages downloaded: {self.stats['pages_downloaded']}")
        self.logger.info(f"Text extracted: {self.stats['text_extracted']}")
        self.logger.info(f"Files downloaded: {self.stats['files_downloaded']}")
        self.logger.info(f"Access denied: {self.stats['denied']}")
        self.logger.info(f"Total data: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info(f"Output location: {self.base_dir}")
        if self.denied_urls:
            self.logger.info(f"Denied URLs logged to: {self.logs_dir / 'access_denied.txt'}")
        if self.failed_urls:
            self.logger.info(f"Failed URLs logged to: {self.logs_dir / 'failed_urls.txt'}")
            self.logger.info(f"  Retry with: uv run python app.py --retry {self.logs_dir / 'failed_urls.txt'}")
        self.logger.info("=" * 80)

        # Cleanup
        self.executor.shutdown(wait=False)
        self.url_store.close()


def collect_urls(args) -> list[str]:
    """Collect URLs from CLI arg and/or file."""
    urls = []
    if args.url:
        urls.append(args.url)
    if args.file:
        path = Path(args.file)
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    if args.retry:
        path = Path(args.retry)
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


def parse_args():
    parser = argparse.ArgumentParser(description='Scrape an entire website (pages + documents + clean text)')
    parser.add_argument('url', nargs='?', help='Starting URL to scrape (e.g. https://example.com/)')
    parser.add_argument('--file', '-f', help='File with URLs to scrape (one per line)')
    parser.add_argument('--retry', '-r', help='File with failed URLs to retry (e.g. data/example.com/logs/failed_urls.txt)')
    parser.add_argument('--concurrency', type=int, default=CONFIG['max_concurrent'],
                        help=f"Max concurrent requests (default: {CONFIG['max_concurrent']})")
    parser.add_argument('--timeout', type=int, default=CONFIG['timeout'],
                        help=f"Request timeout in seconds (default: {CONFIG['timeout']})")
    parser.add_argument('--delay', type=float, default=CONFIG['delay_between_requests'],
                        help=f"Delay between requests in seconds (default: {CONFIG['delay_between_requests']})")
    parser.add_argument('--fresh', action='store_true',
                        help='Ignore any saved checkpoint and start fresh')
    parser.add_argument('--exclude-pattern', action='append', default=None,
                        metavar='PATTERN',
                        help='Regex pattern to exclude URLs (repeatable; appends to defaults)')
    parser.add_argument('--no-default-excludes', action='store_true',
                        help='Clear the default exclude patterns (use only --exclude-pattern values)')
    tracking_group = parser.add_mutually_exclusive_group()
    tracking_group.add_argument('--strip-tracking-params', action='store_true', default=True,
                                dest='strip_tracking_params',
                                help='Strip tracking query params like utm_* (default)')
    tracking_group.add_argument('--no-strip-tracking-params', action='store_false',
                                dest='strip_tracking_params',
                                help='Keep tracking query params in URLs')
    sitemap_group = parser.add_mutually_exclusive_group()
    sitemap_group.add_argument('--use-sitemap', action='store_true', default=True,
                               dest='use_sitemap',
                               help='Seed crawl queue from sitemap.xml (default)')
    sitemap_group.add_argument('--no-use-sitemap', action='store_false',
                               dest='use_sitemap',
                               help='Do not fetch sitemap.xml for seed URLs')
    return parser.parse_args()


async def main():
    args = parse_args()
    urls = collect_urls(args)

    if not urls:
        print("Error: provide a URL, --file, or --retry")
        raise SystemExit(1)

    CONFIG['max_concurrent'] = args.concurrency
    CONFIG['timeout'] = args.timeout
    CONFIG['delay_between_requests'] = args.delay

    # Build exclude patterns list
    if args.no_default_excludes:
        exclude_patterns = list(args.exclude_pattern or [])
    elif args.exclude_pattern:
        exclude_patterns = list(_DEFAULT_EXCLUDE_PATTERNS) + args.exclude_pattern
    else:
        exclude_patterns = None  # use defaults inside WebsiteScraper

    # Group URLs by domain so each domain gets one scraper
    by_domain: dict[str, list[str]] = {}
    for url in urls:
        domain = urlparse(url).netloc
        by_domain.setdefault(domain, []).append(url)

    # Run all domains concurrently
    async with asyncio.TaskGroup() as tg:
        for domain, domain_urls in by_domain.items():
            scraper = WebsiteScraper(
                domain_urls[0], fresh=args.fresh,
                exclude_patterns=exclude_patterns,
                strip_tracking_params=args.strip_tracking_params,
                use_sitemap=args.use_sitemap,
            )
            # Seed any additional URLs for this domain
            for extra in domain_urls[1:]:
                normalized = scraper.normalize_url(extra)
                if not scraper.url_store.contains(normalized):
                    scraper.urls_to_visit.append(normalized)
            tg.create_task(scraper.run())


if __name__ == '__main__':
    asyncio.run(main())
