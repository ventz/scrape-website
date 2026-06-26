import argparse
import asyncio
import aiohttp
import aiofiles
import os
import re
import ssl
import sys
import time
import random
import sqlite3
import logging
import json
import platform
import subprocess
import threading
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
from trafilatura.deduplication import LRU_TEST

# Bump on every user-visible improvement/change (see CHANGELOG.md). Surfaced via
# `--version` and logged at the start of each crawl so a run's output is traceable
# to the code that produced it.
__version__ = "0.4.0"

# Configuration defaults
CONFIG = {
    'max_concurrent': 100,  # Number of concurrent downloads
    'timeout': 30,  # Request timeout in seconds
    'max_retries': 3,  # Max retries for failed requests
    # IMPORTANT: a Cloudflare ``cf_clearance`` cookie is bound to domain + IP + the
    # EXACT User-Agent the real browser had when it solved the challenge. The cf-session
    # bridge (below) reuses the cookie your genuine Chrome earned, so this UA must match
    # your real Chrome's major version or the replayed cookie is rejected. Bump it in
    # lockstep with your installed Chrome. Override at runtime with SCRAPE_USER_AGENT.
    'user_agent': os.environ.get(
        'SCRAPE_USER_AGENT',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    ),
    'delay_between_requests': 0.1,  # Politeness delay in seconds
    'max_file_size': 100 * 1024 * 1024,  # 100MB max file size
    'checkpoint_interval': 30,  # Seconds between queue checkpoints
    'progress_interval': 5,  # Seconds between progress reports
    'render_timeout': 30,  # Max seconds for the initial headless navigation
    'render_settle_ms': 3000,  # Extra wait after DOM load for JS to hydrate
}

# HTTP status codes worth retrying (transient): rate-limit + server errors.
# 403 is handled separately via the curl_cffi impersonation fallback.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# Markers that indicate an HTML payload is a client-rendered SPA shell whose
# real content/links only appear after JavaScript runs. Matched case-insensitively
# against the raw HTML. Used purely as an escalation signal for headless rendering.
_SPA_SHELL_MARKERS: tuple[str, ...] = (
    '__next_f', '__next_data__', '__initial_state__', '__nuxt__',
    'data-reactroot', 'ng-version', 'id="__next"', 'id="root"', 'id="app"',
    'window.__apollo_state__',
)

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


# Markers that indicate an HTML payload is a Cloudflare/CAPTCHA interstitial rather
# than real content (a "Just a moment" 200 is a block, not a page). Matched
# case-insensitively. Shared by the interactive browser path and the cf-session bridge.
_CHALLENGE_MARKERS: tuple[str, ...] = (
    'just a moment', 'checking your browser', 'cf-browser-verification',
    'challenge-platform', 'cf_chl_', 'turnstile', 'hcaptcha', 'g-recaptcha',
    'attention required', 'verify you are human',
    'enable javascript and cookies to continue', 'ddos protection by',
)


# ===================================================================== #
# Browser-cookie reuse — borrow the cookies your REAL Chrome earned
# ===================================================================== #
# Modern anti-bot walls (Cloudflare Private Access Token, Imperva/Incapsula,
# Akamai Bot Manager, DataDome, PerimeterX, ...) issue a clearance cookie that an
# AUTOMATED browser (Playwright/Chromium) can never legitimately earn — Cloudflare's
# PAT is a hardware-attested token (Secure Enclave) only a genuine, OS-blessed
# browser can mint. So instead of trying to SOLVE the challenge in automation, we
# REUSE the cookies your real Chrome already holds (or that you solve once in a real
# tab we open). ``curl_cffi`` then replays them with a Chrome TLS fingerprint and the
# SAME User-Agent (CONFIG['user_agent']).
#
# We reuse **all** of a domain's cookies, not just Cloudflare's ``cf_clearance`` —
# that covers Imperva (visid_incap_*/incap_ses_*), Akamai (_abck/bm_sz), etc. with
# one uniform mechanism, and carries any login session the real browser holds too.
# Caveats vs. Cloudflare: (1) the replay UA must match the real Chrome that earned
# the cookies (cf_clearance is UA-bound), and (2) Imperva/Akamai tokens are more
# tightly bound to the browser fingerprint + IP than Cloudflare's, so replay from a
# different TLS stack is less reliable even when the cookies are valid.
#
# Solve ONCE per domain: cookies are cached for the run, so later URLs on the same
# domain reuse them silently. Sources, in order:
#   1. SCRAPE_CF_COOKIES / IB_CF_COOKIES — a JSON or Netscape cookies file you export
#      (most reliable; works regardless of Chrome's cookie encryption).
#   2. The live Chrome cookie store via ``browser_cookie3`` (optional dep).
#   3. Interactive (--human): open the URL as a tab in your real Chrome, you solve it,
#      we poll the cookie store until a clearance cookie appears.
class _CFSession:
    """Process-wide cache of a domain's browser cookies, keyed by cookie domain.

    Started life as a Cloudflare ``cf_clearance`` bridge; now reuses ALL of a
    domain's cookies so one replay path covers Imperva, Akamai Bot Manager,
    DataDome, PerimeterX, etc. — plus any login session — not just Cloudflare."""

    # Cookie names (exact or prefix, case-insensitive) that signal a SOLVED
    # anti-bot / WAF challenge. Used ONLY to decide "do we already have clearance?";
    # capture and replay use ALL cookies regardless of name.
    _CLEARANCE_MARKERS = (
        'cf_clearance',                          # Cloudflare
        'visid_incap_', 'incap_ses_', 'nlbi_',   # Imperva / Incapsula
        '_abck', 'bm_sz', 'ak_bmsc', 'bm_sv',    # Akamai Bot Manager
        'datadome',                              # DataDome
        '_px', '_pxhd', '_pxvid',                # PerimeterX
        'reese84',                               # F5 / Shape (Distil)
    )
    REAL_BROWSER = os.environ.get('SCRAPE_REAL_BROWSER',
                                  os.environ.get('IB_REAL_BROWSER', 'Google Chrome'))
    SOLVE_TIMEOUT = float(os.environ.get('SCRAPE_HUMAN_SOLVE_TIMEOUT', '300'))

    def __init__(self):
        self._cache: dict[str, dict[str, str]] = {}  # domain (no leading dot) -> {name: value}
        self._lock = threading.Lock()
        self._manual_loaded = False

    # -- cache ------------------------------------------------------------
    def cache(self, domain: str, cookies: dict[str, str]) -> None:
        cookies = {k: v for k, v in cookies.items() if v}
        if not cookies:
            return
        with self._lock:
            self._cache.setdefault(domain.lstrip('.').lower(), {}).update(cookies)

    def _load_manual_once(self) -> None:
        """Load ALL cookies from SCRAPE_CF_COOKIES / IB_CF_COOKIES (JSON or Netscape
        cookies.txt). We keep every cookie, not just Cloudflare's, so the replay also
        carries Imperva/Akamai/etc. clearance and any login session."""
        if self._manual_loaded:
            return
        self._manual_loaded = True
        path = os.environ.get('SCRAPE_CF_COOKIES') or os.environ.get('IB_CF_COOKIES')
        if not path:
            return
        try:
            raw = Path(path).expanduser().read_text()
        except OSError:
            return
        try:  # JSON: [{"domain","name","value"}, ...] OR {"domain": {"name": "value"}}
            data = json.loads(raw)
            if isinstance(data, list):
                for c in data:
                    name = c.get('name')
                    if name:
                        self.cache(str(c.get('domain', '')), {name: c.get('value', '')})
            elif isinstance(data, dict):
                for dom, cookies in data.items():
                    self.cache(dom, dict(cookies))
            return
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
        # Netscape cookies.txt: domain \t flag \t path \t secure \t expiry \t name \t value
        for line in raw.splitlines():
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) >= 7 and parts[5]:
                self.cache(parts[0], {parts[5]: parts[6]})

    # -- lookup -----------------------------------------------------------
    @staticmethod
    def _host(url: str) -> str:
        try:
            return (urlparse(url).hostname or '').lower()
        except Exception:
            return ''

    @classmethod
    def _is_clearance(cls, name: str) -> bool:
        n = (name or '').lower()
        return any(n == m or n.startswith(m) for m in cls._CLEARANCE_MARKERS)

    def cookie_header_for(self, url: str) -> str | None:
        """``Cookie:`` header value for ALL cached cookies matching ``url``'s host."""
        self._load_manual_once()
        host = self._host(url)
        if not host:
            return None
        parts: list[str] = []
        with self._lock:
            for dom, cookies in self._cache.items():
                if host == dom or host.endswith('.' + dom):
                    parts += [f'{k}={v}' for k, v in cookies.items()]
        return '; '.join(parts) if parts else None

    def has_clearance_for(self, url: str) -> bool:
        """True if we hold a cookie that signals a solved WAF/anti-bot challenge for
        this host (cf_clearance, or an Imperva/Akamai/DataDome/PerimeterX marker)."""
        self._load_manual_once()
        host = self._host(url)
        if not host:
            return False
        with self._lock:
            for dom, cookies in self._cache.items():
                if host == dom or host.endswith('.' + dom):
                    if any(self._is_clearance(k) for k in cookies):
                        return True
        return False

    # -- acquisition (sync; call via asyncio.to_thread) -------------------
    def _read_from_chrome(self, host: str) -> dict[str, str]:
        """Best-effort read of ALL cookies for ``host`` from the live Chrome cookie
        store into the cache. Returns the cookies read (empty if browser_cookie3 is
        absent or cannot decrypt)."""
        try:
            import browser_cookie3  # type: ignore
        except Exception:
            return {}
        try:
            jar = browser_cookie3.chrome(domain_name=host)
        except Exception:  # locked DB / decryption failure / keychain declined
            return {}
        got: dict[str, str] = {}
        for c in jar:
            if c.value:
                self.cache((c.domain or host), {c.name: c.value})
                got[c.name] = c.value
        return got

    def _open_in_real_chrome(self, url: str) -> bool:
        """Open ``url`` as a tab in the user's REAL Chrome (macOS ``open -a``). The
        genuine, OS-attested browser CAN pass PAT/Turnstile — the human solves there."""
        if platform.system() != 'Darwin':
            return False  # `open -a` is macOS-only; elsewhere rely on SCRAPE_CF_COOKIES
        try:
            subprocess.run(['open', '-a', self.REAL_BROWSER, url], check=False, timeout=15)
            return True
        except Exception:
            return False

    def obtain_clearance(self, url: str, interactive: bool, on_wait=None,
                         poll: float = 2.0) -> bool:
        """Get a usable cf_clearance for ``url``'s host (cached for the run). Order:
        already-cached -> manual file -> silent read from Chrome -> (interactive only)
        open a real Chrome tab and poll while the human solves. Returns True on success."""
        self._load_manual_once()
        if self.has_clearance_for(url):
            return True
        host = self._host(url)
        if not host:
            return False
        # The clearance cookie may already be in your real Chrome from normal browsing.
        self._read_from_chrome(host)
        if self.has_clearance_for(url):
            return True
        if not interactive:
            return False
        if not self._open_in_real_chrome(url):
            print(f"[cf] couldn't open {self.REAL_BROWSER}; set SCRAPE_CF_COOKIES to a "
                  f"cookies file instead.", file=sys.stderr, flush=True)
            return False
        print(f"[cf] Opened {url} in {self.REAL_BROWSER} — solve the challenge there; "
              f"reusing its cookies for all of {host}.", file=sys.stderr, flush=True)
        if on_wait:
            on_wait()
        deadline = time.monotonic() + max(10.0, self.SOLVE_TIMEOUT)
        while time.monotonic() < deadline:
            time.sleep(poll)
            self._read_from_chrome(host)
            if self.has_clearance_for(url):
                print(f"[cf] clearance obtained for {host} — continuing.",
                      file=sys.stderr, flush=True)
                return True
        print(f"[cf] no clearance for {host} within {int(self.SOLVE_TIMEOUT)}s "
              f"(if browser_cookie3 can't read your Chrome, export cookies to "
              f"SCRAPE_CF_COOKIES).", file=sys.stderr, flush=True)
        return False


# Single process-wide instance (clearance is bound to this machine's IP + UA).
CF_SESSION = _CFSession()


def _looks_challenged(html: str, status: int) -> bool:
    """Is this an HTTP payload a Cloudflare/CAPTCHA interstitial rather than content?"""
    low = (html or '').lower()
    if any(m in low for m in _CHALLENGE_MARKERS):
        return True
    if status in (403, 503) and 'cloudflare' in low:
        return True
    return False


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
    """Extract clean Markdown (with metadata front matter) for LLM consumption."""
    try:
        # Reset trafilatura's process-global dedup cache before every page so
        # deduplication is strictly intra-page. Without this, the LRU_TEST
        # cache accumulates across all pages handled by a long-lived
        # ProcessPoolExecutor worker, silently stripping content that legitimately
        # repeats across pages (e.g. an FAQ answer on both the FAQ page and its
        # own page) — and producing no file at all when a page is only such text.
        LRU_TEST.clear()
        text = trafilatura.extract(
            html_content,
            url=url,
            include_comments=False,
            include_tables=True,
            include_links=True,
            include_images=False,
            favor_recall=True,       # maximize content extraction
            deduplicate=True,        # intra-page only (cache cleared above)
            with_metadata=True,      # YAML front matter: title, url, hostname...
            output_format='markdown',
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


def _looks_like_spa_shell(html_content: str, extracted_text: str | None,
                          link_count: int, min_text: int = 200) -> bool:
    """Heuristic: does this static HTML look like an un-hydrated SPA shell?

    A client-rendered single-page app returns an near-empty document whose
    real content and navigation only materialize once JavaScript runs, so the
    static pass yields almost no text and almost no followable links. We
    escalate to a headless render only when BOTH the extractable text is tiny
    AND there's a positive signal that JS would produce more — never just
    because a page happens to include scripts.
    """
    text_len = len((extracted_text or '').strip())
    if text_len >= min_text:
        return False
    low = html_content.lower()
    has_marker = any(m in low for m in _SPA_SHELL_MARKERS)
    wants_js = 'enable javascript' in low or 'please enable js' in low
    # A near-empty body with no same-domain links to follow is a dead end for
    # a static crawler regardless of markers — worth one render attempt.
    dead_end = link_count == 0
    return has_marker or wants_js or dead_end


def _extract_document_to_markdown(filepath: str, url: str, hostname: str) -> str | None:
    """Convert a downloaded document (PDF / Office / text) to Markdown for RAG.

    Tiered, best-effort: PyMuPDF4LLM for native PDFs, MarkItDown for Office
    formats, and an optional Docling fallback for complex/scanned PDFs when the
    fast path yields almost nothing (only if docling is installed). Returns the
    Markdown body (with YAML front matter) or None on failure / empty output.
    Runs in a worker thread — keep it import-lazy so non-document crawls pay no
    import cost.
    """
    ext = os.path.splitext(filepath)[1].lower()
    body: str | None = None
    try:
        if ext == '.pdf':
            import pymupdf4llm
            body = pymupdf4llm.to_markdown(filepath)
            if not body or len(body.strip()) < 50:
                # Fast path produced almost nothing (scanned / complex layout).
                # Try Docling only if the user installed it (heavy, optional).
                try:
                    from docling.document_converter import DocumentConverter
                    body = DocumentConverter().convert(filepath).document.export_to_markdown()
                except ImportError:
                    pass
                except Exception:
                    pass
        elif ext in ('.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', '.odt', '.ods', '.odp', '.rtf'):
            from markitdown import MarkItDown
            body = MarkItDown().convert(filepath).text_content
        elif ext in ('.txt', '.csv'):
            with open(filepath, 'r', encoding='utf-8', errors='replace') as fh:
                body = fh.read()
    except Exception:
        return None

    if not body or not body.strip():
        return None

    title = os.path.basename(filepath)
    date = datetime.now().strftime('%Y-%m-%d')
    front_matter = (
        "---\n"
        f"title: {title}\n"
        f"url: {url}\n"
        f"hostname: {hostname}\n"
        f"filetype: {ext.lstrip('.')}\n"
        f"date: {date}\n"
        "---\n\n"
    )
    return front_matter + body.strip() + "\n"


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
                 use_sitemap: bool = True,
                 render_mode: str = 'auto',
                 allow_insecure_tls: bool = False,
                 ignore_robots: bool = False,
                 fullname: bool = False,
                 extract_docs: bool = True,
                 human: bool = False):
        self.start_url = start_url
        self.base_domain = self.extract_domain(start_url)

        # Crawl-quality knobs
        self.strip_tracking_params = strip_tracking_params
        self.use_sitemap = use_sitemap
        self.render_mode = render_mode            # 'auto' | 'never' | 'always'
        self.allow_insecure_tls = allow_insecure_tls
        self.ignore_robots = ignore_robots
        self.fullname = fullname                  # fully-qualified output filenames
        self.extract_docs = extract_docs          # convert downloaded docs -> Markdown
        self.human = human                        # interactive headful browser mode

        # Browser state (lazy: Chromium only launches if a page needs it).
        # _browser: shared headless instance for SPA-render escalation.
        # _context: persistent headful context for --human interactive mode.
        self._browser = None
        self._context = None
        self._playwright = None
        self._browser_lock = asyncio.Lock()
        # Politeness state
        self._robots = None                       # Protego parser, loaded in crawl()
        self._rate_limiter = None                 # aiolimiter.AsyncLimiter for Crawl-Delay
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
            'docs_extracted': 0,
            'rendered': 0,
            'robots_skipped': 0,
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
                if self.fullname and parsed.netloc:
                    original_name = f"{parsed.netloc}_{original_name}"
                original_name = re.sub(r'[^\w\s\-\.]', '_', original_name)
                return original_name
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        ext = self.get_file_extension(url, content_type)
        return f"file_{url_hash}{ext}"

    def generate_html_filename(self, url: str) -> str:
        """Generate filename stem for HTML content (used for both .html and .txt).

        With ``--fullname``/``-n`` the stem is fully-qualified with the host
        (e.g. ``example.com_about_team``) so text/ files stay unambiguous when
        you aggregate corpora from several domains; default keeps the shorter
        path-only stem.
        """
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        if not path:
            filename = 'index'
        else:
            filename = path.replace('/', '_')
            if filename.endswith('.html'):
                filename = filename[:-5]
        if self.fullname and parsed.netloc:
            filename = f"{parsed.netloc}_{filename}"
        filename = re.sub(r'[^\w\s\-\.]', '_', filename)
        return filename

    async def init_session(self):
        # TLS: strict by default. --allow-insecure-tls disables verification for
        # the whole run (misconfigured/expired-cert sites) — logged loudly so it
        # is never a silent downgrade.
        ssl_param: ssl.SSLContext | bool = True
        if self.allow_insecure_tls:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ssl_param = ctx
            self.logger.warning(
                "TLS verification DISABLED for this run (--allow-insecure-tls). "
                "Only use this for trusted hosts with broken certificates."
            )
        connector = aiohttp.TCPConnector(
            limit=CONFIG['max_concurrent'],
            limit_per_host=CONFIG['max_concurrent'],
            resolver=aiohttp.AsyncResolver(),
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            ssl=ssl_param,
        )
        timeout = aiohttp.ClientTimeout(total=CONFIG['timeout'])
        # aiohttp advertises Accept-Encoding for the codecs it can decode; with
        # brotli + zstandard installed that includes br and zstd automatically.
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': CONFIG['user_agent']},
            max_field_size=32768,
        )

    async def close_session(self):
        if self.session:
            await self.session.close()
        await self._close_browser()

    # ------------------------------------------------------------------
    # Headless rendering (lazy Chromium) — escalation tier for SPA shells
    # ------------------------------------------------------------------
    async def _ensure_browser(self):
        """Launch the browser on first use.

        Two modes:
        - default: a single shared **headless** Chromium for SPA-render escalation.
        - ``--human``: a **headful, persistent** context (visible window) whose
          profile is stored under ``logs/browser_profile`` so a manually-solved
          Cloudflare/CAPTCHA/login session (cookies incl. ``cf_clearance``)
          persists across pages and across runs.
        """
        if self._browser is not None or self._context is not None:
            return
        async with self._browser_lock:
            if self._browser is not None or self._context is not None:
                return
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                self.logger.warning(
                    "Playwright not installed; cannot render JS pages. "
                    "Install with: uv add playwright && uv run playwright install chromium"
                )
                return
            self._playwright = await async_playwright().start()
            if self.human:
                profile_dir = self.logs_dir / 'browser_profile'
                self._context = await self._playwright.chromium.launch_persistent_context(
                    user_data_dir=str(profile_dir),
                    headless=False,
                    user_agent=CONFIG['user_agent'],
                    viewport={'width': 1280, 'height': 900},
                )
                self.logger.info(
                    "Interactive browser (--human) launched; session profile: %s",
                    profile_dir,
                )
            else:
                self._browser = await self._playwright.chromium.launch(headless=True)
                self.logger.debug("Headless Chromium launched for JS rendering")

    async def _close_browser(self):
        try:
            if self._context is not None:
                await self._context.close()
                self._context = None
            if self._browser is not None:
                await self._browser.close()
                self._browser = None
            if self._playwright is not None:
                await self._playwright.stop()
                self._playwright = None
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Interactive (--human) browser fetching: fetch through the visible
    # persistent context, auto-pausing when a challenge/login is detected.
    # ------------------------------------------------------------------
    def _looks_challenged(self, html: str, status: int) -> bool:
        """Heuristic: is this page a bot/CAPTCHA/Cloudflare interstitial?"""
        return _looks_challenged(html, status)

    async def _await_human_solve(self, page, url: str):
        """Surface the window and block until the user solves the challenge.

        Runs with concurrency forced to 1 (see main()), so a single blocking
        prompt is safe and unambiguous.
        """
        try:
            await page.bring_to_front()
        except Exception:
            pass
        prompt = (
            f"\n{'=' * 72}\n"
            f"  CHALLENGE / LOGIN DETECTED\n"
            f"  {url}\n"
            f"  Solve it in the browser window (CAPTCHA / Cloudflare / sign-in),\n"
            f"  then press <Enter> here to continue the crawl...\n"
            f"{'=' * 72}\n"
        )
        self.logger.warning("Challenge detected; waiting for manual solve: %s", url)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, input, prompt)

    async def _browser_fetch(self, url: str) -> tuple:
        """Fetch *url* through the persistent (visible) browser context.

        Returns the same tuple shape as fetch_with_retry. Documents are pulled
        via the context's request API so they carry the solved session cookies;
        HTML pages are navigated and snapshotted from the hydrated DOM.
        """
        await self._ensure_browser()
        if self._context is None:
            raise Exception("Interactive browser context unavailable")

        # Binary documents: fetch bytes with the context's cookies.
        if self.should_download_file(url):
            resp = await self._context.request.get(
                url, timeout=CONFIG['timeout'] * 1000)
            ctype = resp.headers.get('content-type', '')
            return await resp.body(), ctype, 'file', resp.status

        page = await self._context.new_page()
        try:
            resp = await page.goto(url, wait_until='domcontentloaded',
                                   timeout=CONFIG['render_timeout'] * 1000)
            status = resp.status if resp else 0
            ctype = resp.headers.get('content-type', '') if resp else 'text/html'
            html = await page.content()

            if self._looks_challenged(html, status):
                await self._await_human_solve(page, url)
                # Re-evaluate after the solve; the page has navigated past the
                # interstitial and the context now holds the clearance cookie.
                status = 200
                await page.wait_for_timeout(CONFIG['render_settle_ms'])
                html = await page.content()
                # Playwright/Chromium can NEVER mint a Cloudflare Private Access Token
                # (PAT) — a hardware-attested token only your genuine OS browser can
                # produce. If the page is STILL a challenge after the solve, this is a
                # PAT wall: hand off to the cf-clearance bridge (open the URL in your
                # REAL Chrome, reuse the cookie it earns, replay via curl_cffi).
                if self._looks_challenged(html, status):
                    self.logger.warning(
                        "Still challenged after solve (likely a PAT wall); "
                        "escalating to the real-Chrome cf-clearance bridge: %s", url)
                    bridged = await self._fetch_via_curl_cffi(url)
                    if bridged is not None:
                        return bridged
            else:
                try:
                    await page.wait_for_load_state(
                        'networkidle', timeout=CONFIG['render_settle_ms'])
                except Exception:
                    pass
                await page.wait_for_timeout(CONFIG['render_settle_ms'])
                html = await page.content()

            return html, ctype or 'text/html', 'html', status
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _render_with_playwright(self, url: str) -> str | None:
        """Render *url* in headless Chromium and return the hydrated HTML.

        Only the heaviest resources (images/media) are blocked — blocking CSS
        or fonts can make some SPAs throw a client-side exception and render
        their error boundary instead of content. We wait for the DOM to load
        (NOT network idle: sites with chat widgets / analytics / websockets
        never go idle and would time out), then give the framework a short
        fixed window to hydrate before snapshotting the DOM.
        """
        await self._ensure_browser()
        if self._browser is None:
            return None
        page = None
        try:
            page = await self._browser.new_page(user_agent=CONFIG['user_agent'])

            async def _block(route):
                # Block only large media; keep CSS/fonts/scripts so client JS
                # doesn't crash on missing resources it expects to load.
                if route.request.resource_type in ('image', 'media'):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route('**/*', _block)
            await page.goto(url, wait_until='domcontentloaded',
                            timeout=CONFIG['render_timeout'] * 1000)
            # Best-effort: let in-flight XHR settle, but never block on idle
            # that may never arrive — the fixed settle below is the real wait.
            try:
                await page.wait_for_load_state(
                    'networkidle', timeout=CONFIG['render_settle_ms'])
            except Exception:
                pass
            await page.wait_for_timeout(CONFIG['render_settle_ms'])
            html = await page.content()
            return html
        except Exception as e:
            self.logger.debug(f"Render failed for {url}: {e}")
            return None
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # curl_cffi browser-impersonation fallback (for 403 / WAF challenges)
    # ------------------------------------------------------------------
    async def _curl_get(self, url: str) -> tuple | None:
        """One curl_cffi GET impersonating Chrome, replaying ALL cached cookies for
        this host (Cloudflare/Imperva/Akamai clearance + any login session) with the
        MATCHED User-Agent. Returns a fetch tuple, or None on transport error / missing
        dep. The caller decides if a 403/challenge response is worth escalating to the
        cookie bridge."""
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError:
            return None
        # cf_clearance is bound to UA — send the SAME UA the cookies were minted with.
        headers = {'User-Agent': CONFIG['user_agent']}
        cookie = CF_SESSION.cookie_header_for(url)
        if cookie:
            headers['Cookie'] = cookie
        try:
            async with AsyncSession() as s:
                resp = await s.get(
                    url, impersonate='chrome', headers=headers,
                    timeout=CONFIG['timeout'],
                    verify=not self.allow_insecure_tls, allow_redirects=True,
                )
                content_type = resp.headers.get('Content-Type', '')
                status = resp.status_code
                if self.should_download_file(url, content_type):
                    return resp.content, content_type, 'file', status
                return resp.text, content_type, 'html', status
        except Exception as e:
            self.logger.debug(f"curl_cffi fetch failed for {url}: {e}")
            return None

    @staticmethod
    def _curl_blocked(result: tuple | None) -> bool:
        """Did a curl_cffi result fail to yield real content (403/5xx/challenge page)?"""
        if result is None:
            return True
        content, _ctype, kind, status = result
        if status == 403 or status >= 500:
            return True
        if kind == 'html' and _looks_challenged(content, status):
            return True
        return False

    async def _fetch_via_curl_cffi(self, url: str) -> tuple | None:
        """Real-browser-fingerprint fallback for 403 / WAF / Cloudflare challenges.

        aiohttp's TLS/HTTP2 fingerprint is increasingly fingerprint-blocked by WAFs
        (Cloudflare/Akamai/Imperva). curl_cffi impersonates a real Chrome, which clears
        most fingerprint blocks. When a host still answers with a challenge, escalate to
        the **cookie bridge**: reuse ALL the cookies your real Chrome earned (exported
        file / live cookie store / a one-time solve in real Chrome under --human — the
        only thing that can mint a Cloudflare Private Access Token) and replay with the
        matched UA. Returns a fetch tuple, or None if nothing helped.
        """
        result = await self._curl_get(url)
        if not self._curl_blocked(result):
            return result
        # Still blocked. Try to obtain clearance cookies, then replay once.
        # Silent sources (cookies file / live Chrome) always run; opening a real
        # Chrome tab to solve interactively only happens under --human.
        if not CF_SESSION.has_clearance_for(url):
            got = await asyncio.to_thread(CF_SESSION.obtain_clearance, url, self.human)
            if not got:
                return None
            self.logger.info("cf-clearance obtained; replaying %s via curl_cffi", url)
        replay = await self._curl_get(url)
        return None if self._curl_blocked(replay) else replay

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter (caps growth, avoids thundering herd)."""
        return random.uniform(0, min(8.0, 1.0 * (2 ** attempt)))

    async def fetch_with_retry(self, url: str, method: str = 'GET') -> tuple:
        last_error = None
        for attempt in range(CONFIG['max_retries']):
            try:
                async with self.session.request(method, url, allow_redirects=True) as response:
                    content_type = response.headers.get('Content-Type', '')
                    status = response.status

                    # Transient server / rate-limit responses: honor Retry-After
                    # when present, else exponential backoff, then retry.
                    if status in RETRYABLE_STATUS and attempt < CONFIG['max_retries'] - 1:
                        retry_after = response.headers.get('Retry-After')
                        try:
                            wait = float(retry_after) if retry_after else self._backoff(attempt)
                        except ValueError:
                            wait = self._backoff(attempt)
                        last_error = f"HTTP {status}"
                        await asyncio.sleep(min(wait, 30.0))
                        continue

                    # Forbidden / WAF block: try a real-browser fingerprint (and the
                    # cf-clearance bridge) once before giving up.
                    if status in (401, 403):
                        fallback = await self._fetch_via_curl_cffi(url)
                        if fallback is not None:
                            self.logger.debug(f"curl_cffi fallback succeeded for {url}")
                            return fallback

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
                        # A 200 that is really a Cloudflare "Just a moment" interstitial
                        # must not be saved as content — escalate to curl_cffi + the
                        # cf-clearance bridge, and only fall back to the junk if it fails.
                        if 'html' in content_type.lower() and _looks_challenged(content, status):
                            fallback = await self._fetch_via_curl_cffi(url)
                            if fallback is not None:
                                self.logger.debug(f"cleared challenge interstitial for {url}")
                                return fallback
                        return content, content_type, 'html', status
            except asyncio.TimeoutError:
                last_error = "Timeout"
                if attempt < CONFIG['max_retries'] - 1:
                    await asyncio.sleep(self._backoff(attempt))
            except Exception as e:
                last_error = str(e)
                if attempt < CONFIG['max_retries'] - 1:
                    await asyncio.sleep(self._backoff(attempt))
        raise Exception(f"Failed after {CONFIG['max_retries']} attempts: {last_error}")

    # ------------------------------------------------------------------
    # Politeness: robots.txt + adaptive per-host rate limiting
    # ------------------------------------------------------------------
    async def _load_robots(self):
        """Fetch and parse robots.txt once for the crawl host (best-effort).

        Also picks up Crawl-Delay and turns it into a per-host rate limiter so
        we honor the site's requested pace without throttling extraction.
        """
        if self.ignore_robots:
            return
        parsed = urlparse(self.start_url)
        robots_url = f"{parsed.scheme or 'https'}://{self.base_domain}/robots.txt"
        try:
            from protego import Protego
            async with self.session.get(robots_url, allow_redirects=True) as resp:
                ctype = resp.headers.get('Content-Type', '').lower()
                # Some SPA/CMS hosts serve their HTML app shell (status 200) for
                # a missing /robots.txt — don't parse that as rules.
                if resp.status == 200 and 'html' not in ctype:
                    body = await resp.text()
                    self._robots = Protego.parse(body)
                    self.logger.info(f"robots.txt loaded from {robots_url}")
                else:
                    self.logger.debug(
                        f"No usable robots.txt at {robots_url} "
                        f"(status {resp.status}, type {ctype or 'unknown'})")
        except Exception as e:
            self.logger.debug(f"Could not load robots.txt ({robots_url}): {e}")
            self._robots = None

        if self._robots is not None:
            try:
                delay = self._robots.crawl_delay(CONFIG['user_agent']) \
                    or self._robots.crawl_delay('*')
            except Exception:
                delay = None
            if delay and delay > 0:
                from aiolimiter import AsyncLimiter
                self._rate_limiter = AsyncLimiter(1, float(delay))
                self.logger.info(f"robots.txt Crawl-Delay: pacing to 1 request / {delay}s")

    def _robots_allows(self, url: str) -> bool:
        if self.ignore_robots or self._robots is None:
            return True
        try:
            return self._robots.can_fetch(url, CONFIG['user_agent'])
        except Exception:
            return True

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

        # Convert the document to RAG-ready Markdown alongside the raw file.
        if self.extract_docs:
            await self._save_document_text(filepath, url)

    async def _save_document_text(self, filepath: Path, url: str):
        """Extract a downloaded document to Markdown in text/ (off-thread)."""
        loop = asyncio.get_running_loop()
        try:
            markdown = await loop.run_in_executor(
                None, _extract_document_to_markdown,
                str(filepath), url, self.base_domain,
            )
        except Exception as e:
            self.logger.debug(f"Document extraction failed for {filepath.name}: {e}")
            return
        if not markdown:
            return
        stem = os.path.splitext(filepath.name)[0]
        out = self.text_dir / f"{stem}.md"
        counter = 1
        while out.exists():
            out = self.text_dir / f"{stem}_{counter}.md"
            counter += 1
        async with aiofiles.open(out, 'w', encoding='utf-8') as f:
            await f.write(markdown)
        self.stats['docs_extracted'] += 1
        self.logger.debug(f"Extracted document text: {out.name}")

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
        filepath = self.text_dir / f"{stem}.md"

        counter = 1
        while filepath.exists():
            filepath = self.text_dir / f"{stem}_{counter}.md"
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
                # Politeness: respect robots.txt unless explicitly ignored.
                if not self._robots_allows(url):
                    self.stats['robots_skipped'] += 1
                    self.logger.debug(f"robots.txt disallows, skipping: {url}")
                    return

                # Honor Crawl-Delay (per-host) if robots.txt declared one,
                # else fall back to the flat politeness delay.
                if self._rate_limiter is not None:
                    async with self._rate_limiter:
                        pass
                else:
                    await asyncio.sleep(CONFIG['delay_between_requests'])

                # --human: fetch through the visible persistent browser so a
                # manually-solved Cloudflare/CAPTCHA/login session is reused;
                # otherwise use the fast static aiohttp path.
                if self.human:
                    content, content_type, content_kind, status = await self._browser_fetch(url)
                else:
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

                    # JS-render escalation tier: when the static pass yields an
                    # un-hydrated SPA shell (tiny text, no links), re-fetch the
                    # page in headless Chromium and re-extract from the rendered
                    # DOM. Static-first by design — only shells pay the cost.
                    # (Skipped in --human mode: the browser already rendered it.)
                    needs_render = not self.human and (
                        self.render_mode == 'always' or (
                            self.render_mode == 'auto'
                            and _looks_like_spa_shell(content, extracted_text, len(links))
                        )
                    )
                    if needs_render:
                        rendered = await self._render_with_playwright(url)
                        if rendered:
                            content = rendered
                            self.stats['rendered'] += 1
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
                f"{self.stats['rendered']} rendered | "
                f"{self.stats['files_downloaded']} files | "
                f"{self.stats['docs_extracted']} docs | "
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
        self.logger.info("scrape-website v%s — crawling %s", __version__, self.base_domain)
        await self.init_session()

        # Load robots.txt (and any Crawl-Delay) before fetching anything.
        await self._load_robots()

        # In interactive mode, open the visible browser up front so the window
        # is ready (and any initial challenge can be solved immediately).
        if self.human:
            await self._ensure_browser()

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
        self.logger.info(f"Pages rendered (JS): {self.stats['rendered']}")
        self.logger.info(f"Files downloaded: {self.stats['files_downloaded']}")
        self.logger.info(f"Documents extracted: {self.stats['docs_extracted']}")
        self.logger.info(f"Access denied: {self.stats['denied']}")
        self.logger.info(f"Skipped (robots.txt): {self.stats['robots_skipped']}")
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
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('url', nargs='?', help='Starting URL to scrape (e.g. https://example.com/)')
    parser.add_argument('--file', '-f', help='File with URLs to scrape (one per line)')
    parser.add_argument('--retry', '-r', help='File with failed URLs to retry (e.g. data/example.com/logs/failed_urls.txt)')
    parser.add_argument('--concurrency', '-c', type=int, default=CONFIG['max_concurrent'],
                        help=f"Max concurrent requests (default: {CONFIG['max_concurrent']})")
    parser.add_argument('--timeout', '-t', type=int, default=CONFIG['timeout'],
                        help=f"Request timeout in seconds (default: {CONFIG['timeout']})")
    parser.add_argument('--delay', '-d', type=float, default=CONFIG['delay_between_requests'],
                        help=f"Delay between requests in seconds (default: {CONFIG['delay_between_requests']})")
    parser.add_argument('--fresh', '-F', action='store_true',
                        help='Ignore any saved checkpoint and start fresh')
    parser.add_argument('--fullname', '-n', action='store_true',
                        help='Prefix output filenames with the host (fully-qualified, e.g. example.com_about.md)')
    parser.add_argument('--render', choices=('auto', 'never', 'always'), default='auto',
                        help="Headless-render JS pages: auto=only when a page looks like an "
                             "un-hydrated SPA shell, always=every page, never=disable (default: auto)")
    parser.add_argument('--human', action='store_true',
                        help="Interactive mode: open a VISIBLE browser and fetch through it; "
                             "auto-pause for you to solve Cloudflare/CAPTCHA/login challenges "
                             "(session persists across runs). Forces --concurrency 1.")
    parser.add_argument('--allow-insecure-tls', action='store_true',
                        help='Disable TLS certificate verification (for trusted hosts with broken certs)')
    parser.add_argument('--ignore-robots', action='store_true',
                        help='Do not fetch or honor robots.txt (default: honor it)')
    parser.add_argument('--no-extract-docs', dest='extract_docs', action='store_false', default=True,
                        help='Do not convert downloaded PDFs/Office docs to Markdown')
    parser.add_argument('--exclude-pattern', '-e', action='append', default=None,
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

    # Interactive mode drives one visible browser; force single-flight so the
    # solve prompt is unambiguous and only one window is in play.
    if args.human and args.concurrency != 1:
        print("--human: forcing --concurrency 1 (interactive single window)")
        args.concurrency = 1

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
                render_mode=args.render,
                allow_insecure_tls=args.allow_insecure_tls,
                ignore_robots=args.ignore_robots,
                fullname=args.fullname,
                extract_docs=args.extract_docs,
                human=args.human,
            )
            # Seed any additional URLs for this domain
            for extra in domain_urls[1:]:
                normalized = scraper.normalize_url(extra)
                if not scraper.url_store.contains(normalized):
                    scraper.urls_to_visit.append(normalized)
            tg.create_task(scraper.run())


if __name__ == '__main__':
    asyncio.run(main())
