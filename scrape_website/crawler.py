"""WebsiteScraper — the full crawl-to-disk pipeline behind the CLI.

Fetching (retry/backoff, curl_cffi WAF fallback, headless render escalation,
robots politeness) is delegated to a composed :class:`FetchEngine`; this class
owns everything CLI-specific: the ``data/<domain>/`` output tree, SQLite
checkpoints/resume, the progress reporter, the ProcessPoolExecutor for
CPU-bound parsing, and the interactive ``--human`` profile location.
"""

import asyncio
import hashlib
import logging
import mimetypes
import os
import re
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Deque
from urllib.parse import urlparse

import aiofiles

from .config import CONFIG, DOWNLOADABLE_EXTENSIONS, _DEFAULT_EXCLUDE_PATTERNS
from .extract import _extract_document_to_markdown, _parse_and_extract
from .fetch import FetchEngine, should_download_file
from .sitemap import _fetch_sitemap_urls
from .store import URLStore
from .urls import _canonicalize_host, _normalize_url, _same_host, _url_excluded


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
                 human: bool = False,
                 verbose: bool = False):
        self.start_url = start_url
        self.base_domain = self.extract_domain(start_url)
        self.base_scheme = urlparse(start_url).scheme or 'https'
        self.fresh = fresh

        # The netloc becomes a path segment of the output tree — refuse
        # anything that isn't a plain host[:port] / [v6][:port] so a malformed
        # seed URL (e.g. from an untrusted --file list) can't steer writes
        # outside data/ (urlparse('http://../x').netloc is '..').
        if ('..' in self.base_domain
                or not re.fullmatch(
                    r'(?:[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?|\[[0-9A-Fa-f:.]+\])(?::\d+)?',
                    self.base_domain)):
            raise ValueError(f"Refusing to crawl invalid host in URL: {start_url!r}")

        # Crawl-quality knobs
        self.strip_tracking_params = strip_tracking_params
        self.use_sitemap = use_sitemap
        self.render_mode = render_mode            # 'auto' | 'never' | 'always'
        self.allow_insecure_tls = allow_insecure_tls
        self.ignore_robots = ignore_robots
        self.fullname = fullname                  # fully-qualified output filenames
        self.extract_docs = extract_docs          # convert downloaded docs -> Markdown
        self.human = human                        # interactive headful browser mode

        # Store patterns as strings (for pickling to process pool)
        self._exclude_pattern_strings: list[str] = (
            exclude_patterns if exclude_patterns is not None
            else list(_DEFAULT_EXCLUDE_PATTERNS)
        )
        # Pre-compile for in-process filtering (e.g. sitemap seed)
        self._compiled_exclude_patterns: list[re.Pattern] = [
            re.compile(p) for p in self._exclude_pattern_strings
        ]
        self.semaphore = asyncio.Semaphore(CONFIG['max_concurrent'])
        self.denied_urls: list[str] = []
        self.failed_urls: list[str] = []
        self.not_found_urls: list[str] = []
        self.challenged_urls: list[str] = []

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
            'not_found': 0,
            'challenged': 0,
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
        # Console handler (INFO by default; --verbose shows every per-URL event)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG if verbose else logging.INFO)
        ch.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(ch)

        # The shared tiered fetcher (session + lazy Chromium + robots state).
        self.engine = FetchEngine(
            render_mode=render_mode,
            allow_insecure_tls=allow_insecure_tls,
            respect_robots=not ignore_robots,
            human=human,
            profile_dir=self.logs_dir / 'browser_profile',
            logger=self.logger,
        )

        # SQLite-backed URL store
        self.url_store = URLStore(self.logs_dir / 'state.db')

        # Output paths claimed this run (see _reserve_path): the event loop is
        # single-threaded and claims happen without an intervening await, so
        # this set makes filename collision handling race-free.
        self._claimed_paths: set[str] = set()

        # Handle fresh start vs resume. _queued mirrors urls_to_visit as a set
        # so a link seen on many pages is enqueued once, not once per page —
        # keeping the queue (and its 30s checkpoint) proportional to the site.
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
                # Restore the report lists too, so access_denied.txt /
                # failed_urls.txt etc. stay complete across crash + resume.
                self.denied_urls = self.url_store.load_url_list('denied')
                self.failed_urls = self.url_store.load_url_list('failed')
                self.not_found_urls = self.url_store.load_url_list('not_found')
                self.challenged_urls = self.url_store.load_url_list('challenged')
                self.logger.info(f"Resuming: {self.url_store.count} URLs visited, {len(saved_queue)} in queue")
            else:
                self.urls_to_visit = deque([start_url])
        self._queued: set[str] = set(self.urls_to_visit)

        # ProcessPoolExecutor for CPU-bound parsing
        self.executor = ProcessPoolExecutor(max_workers=os.cpu_count())

        self.logger.info(f"Output directory: {self.base_dir}")
        self.logger.info(f"Starting domain: {self.base_domain}")
        self.logger.info(f"Max concurrent requests: {CONFIG['max_concurrent']}")
        self.logger.info(
            f"Mode: render={render_mode}"
            f"{', human (interactive browser)' if human else ''}"
            f"{', robots ignored' if ignore_robots else ''}"
            f"{'' if extract_docs else ', no doc extraction'}"
            f"{' — verbose console' if verbose else ''}"
        )
        if not verbose:
            self.logger.info("Tip: per-URL detail is in "
                             f"{self.logs_dir / 'scrape.log'} (or run with --verbose)")

    @staticmethod
    def extract_domain(url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc

    def normalize_url(self, url: str) -> str:
        return _normalize_url(url, strip_tracking=self.strip_tracking_params)

    def is_same_domain(self, url: str) -> bool:
        return _same_host(self.extract_domain(url), self.base_domain)

    def enqueue(self, url: str) -> bool:
        """Add *url* to the crawl queue unless already visited or queued.
        Returns True iff it was actually enqueued."""
        if url in self._queued or self.url_store.contains(url):
            return False
        self._queued.add(url)
        self.urls_to_visit.append(url)
        return True

    def requeue(self, url: str) -> bool:
        """Force *url* back into the crawl queue even if a previous run already
        visited it — the --retry path. Without the forget(), retry URLs would
        be silently rejected by the visited-set guard and the retry would be a
        no-op against an existing state.db."""
        normalized = self.normalize_url(url)
        for u in {url, normalized}:
            self.url_store.forget(u)
        return self.enqueue(normalized)

    def _reserve_path(self, directory: Path, stem: str, ext: str) -> Path:
        """Claim a unique output path for this run (race-free: no await between
        the check and the claim, and the event loop is single-threaded).

        Files left on disk by a PREVIOUS run force a collision suffix only when
        resuming; under --fresh they are overwritten, so a fresh re-crawl
        produces deterministic filenames instead of accumulating _1/_2 dupes.
        Two different URLs mapping to the same stem within one run still get
        distinct suffixed files.
        """
        counter = 0
        while True:
            name = f"{stem}{ext}" if counter == 0 else f"{stem}_{counter}{ext}"
            path = directory / name
            if str(path) not in self._claimed_paths and (self.fresh or not path.exists()):
                self._claimed_paths.add(str(path))
                return path
            counter += 1

    def should_download_file(self, url: str, content_type: str = None) -> bool:
        return should_download_file(url, content_type)

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
                # A stem of only dots/underscores/dashes (e.g. a path ending
                # in '/..') is not a usable filename — fall through to the
                # hash-based name instead of handing '..' to _reserve_path.
                if original_name.strip('._- \t'):
                    # Extension-less paths (/resource/guidance served as
                    # application/pdf) take their extension from the
                    # Content-Type, so the saved file is self-describing and
                    # the document extractor can pick a converter for it.
                    if os.path.splitext(original_name)[1].lower() not in DOWNLOADABLE_EXTENSIONS:
                        ext = self.get_file_extension(url, content_type)
                        if ext in DOWNLOADABLE_EXTENSIONS:
                            original_name += ext
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
        await self.engine.start()

    async def close_session(self):
        await self.engine.close()

    async def download_file(self, url: str, content: bytes, content_type: str):
        # Atomic check-and-claim: two concurrent downloads of identical bytes
        # can't both pass the check and write duplicate files.
        file_hash = hashlib.md5(content).hexdigest()
        if not self.url_store.add_file_hash(file_hash):
            return

        name, ext = os.path.splitext(self.generate_filename(url, content_type))
        filepath = self._reserve_path(self.files_dir, name, ext)

        async with aiofiles.open(filepath, 'wb') as f:
            await f.write(content)
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
        out = self._reserve_path(self.text_dir, stem, '.md')
        async with aiofiles.open(out, 'w', encoding='utf-8') as f:
            await f.write(markdown)
        self.stats['docs_extracted'] += 1
        self.logger.debug(f"Extracted document text: {out.name}")

    async def save_html(self, url: str, content: str):
        stem = self.generate_html_filename(url)
        filepath = self._reserve_path(self.pages_dir, stem, '.html')

        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(content)
        self.stats['pages_downloaded'] += 1
        self.stats['total_bytes'] += len(content.encode('utf-8'))
        self.logger.debug(f"Saved page: {filepath.name}")

    async def save_text(self, url: str, text: str):
        """Save extracted clean text for LLM consumption."""
        stem = self.generate_html_filename(url)
        filepath = self._reserve_path(self.text_dir, stem, '.md')

        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(text)
        self.stats['text_extracted'] += 1
        self.logger.debug(f"Saved text: {filepath.name}")

    async def _run_extract(self, html: str, url: str):
        """Dispatch CPU-bound parsing + text extraction to the process pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor, _parse_and_extract, html, url,
            self.base_domain, self.strip_tracking_params,
            self._exclude_pattern_strings,
        )

    async def process_url(self, url: str):
        async with self.semaphore:
            try:
                # Politeness: respect robots.txt unless explicitly ignored.
                if not self.engine.robots_allows(url):
                    self.stats['robots_skipped'] += 1
                    self.logger.debug(f"robots.txt disallows, skipping: {url}")
                    return

                # Honor Crawl-Delay (per-host) if robots.txt declared one,
                # else fall back to the flat politeness delay.
                await self.engine.wait_politeness()

                outcome, links, extracted_text = await self.engine.fetch_page(
                    url, run_extract=self._run_extract)

                if outcome.kind == 'file':
                    # The aiohttp path already skips oversized files up front
                    # (outcome.detail); the length check covers the curl_cffi /
                    # browser paths, which still buffer whole responses.
                    if (outcome.detail == 'file too large'
                            or len(outcome.content) > CONFIG['max_file_size']):
                        self.logger.debug(f"Skipping large file: {url}")
                        return
                    await self.download_file(url, outcome.content, outcome.content_type)
                else:
                    if outcome.classification == 'not_found':
                        self.stats['not_found'] += 1
                        self.not_found_urls.append(url)
                        self.logger.debug(f"Not found ({outcome.detail}): {url}")
                        return
                    if outcome.classification == 'challenge':
                        # Every escalation tier failed to clear this challenge.
                        # Rare and actionable (--human / cookie bridge), so INFO.
                        self.stats['challenged'] += 1
                        self.challenged_urls.append(url)
                        self.logger.info(
                            f"Blocked by challenge ({outcome.detail}): {url}")
                        return
                    if outcome.denied:
                        self.stats['denied'] += 1
                        self.denied_urls.append(url)
                        self.logger.debug(
                            f"Access denied ({outcome.detail or outcome.status}): {url}")
                        return
                    if outcome.classification == 'search':
                        self.logger.debug(f"Search-results page: {url}")

                    # Escalated headless render (auto/always); --human fetches are
                    # browser-native already and were never counted here.
                    if outcome.rendered and not self.human:
                        self.stats['rendered'] += 1

                    # Save HTML
                    await self.save_html(url, outcome.content)

                    # Save extracted text if we got any
                    if extracted_text and extracted_text.strip():
                        await self.save_text(url, extracted_text)

                    # Queue new links (deduped against visited AND queued)
                    for link in links:
                        self.enqueue(link)

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
                f"{self.stats['not_found']} 404s | "
                f"{self.stats['errors']} errors | "
                f"{self.stats['total_bytes'] / (1024*1024):.1f} MB | "
                f"{len(self.urls_to_visit)} queued"
            )

    def _save_checkpoint(self):
        self.url_store.save_queue(self.urls_to_visit)
        self.url_store.save_stats(self.stats)
        self.url_store.save_url_list('denied', self.denied_urls)
        self.url_store.save_url_list('failed', self.failed_urls)
        self.url_store.save_url_list('not_found', self.not_found_urls)
        self.url_store.save_url_list('challenged', self.challenged_urls)

    async def _checkpoint_saver(self):
        """Periodically checkpoint queue + stats + report lists to SQLite for
        crash recovery."""
        while True:
            await asyncio.sleep(CONFIG['checkpoint_interval'])
            self._save_checkpoint()
            self.logger.debug(f"Checkpoint saved: {len(self.urls_to_visit)} URLs in queue")

    def _sitemap_fallback(self, url: str, loop) -> bytes | None:
        """Sitemap 401/403 fallback, called from the sitemap worker thread: run the
        engine's curl_cffi fingerprint fallback (no cookie bridge) on the crawl's
        event loop and return the body bytes, or None if it is still blocked."""
        self.logger.info(
            f"Sitemap blocked at {url} — trying Chrome-fingerprint fallback (curl_cffi)")
        try:
            result = asyncio.run_coroutine_threadsafe(
                self.engine._fetch_via_curl_cffi(url, cookie_bridge=False),
                loop).result()
        except Exception as e:
            self.logger.debug(f"curl_cffi sitemap fallback failed for {url}: {e}")
            result = None
        if result is None:
            self.logger.warning(
                f"Sitemap at {url} is blocked and the curl_cffi fallback did not "
                f"get it — skipping it")
            return None
        if result[3] != 200:
            self.logger.debug(f"No sitemap at {url} (HTTP {result[3]} via curl_cffi)")
            return None
        content = result[0]
        return content.encode('utf-8') if isinstance(content, str) else content

    async def crawl(self):
        from . import __version__
        self.logger.info("scrape-website v%s — crawling %s", __version__, self.base_domain)
        await self.engine.start()

        # Load robots.txt (and any Crawl-Delay) before fetching anything.
        await self.engine.load_robots(self.start_url)

        # In interactive mode, open the visible browser up front so the window
        # is ready (and any initial challenge can be solved immediately).
        if self.human:
            await self.engine._ensure_browser()

        # Seed from sitemap if enabled (best-effort). This runs before the
        # progress reporter starts and can take a while on large sitemap
        # indexes — narrate it so the crawl never looks hung here.
        if self.use_sitemap:
            self.logger.info("Checking sitemap.xml for seed URLs...")
            parsed_start = urlparse(self.start_url)
            loop = asyncio.get_running_loop()
            sitemap_urls = await loop.run_in_executor(
                None, lambda: _fetch_sitemap_urls(
                    self.base_domain, scheme=parsed_start.scheme or "https",
                    allow_insecure_tls=self.allow_insecure_tls,
                    fallback=lambda u: self._sitemap_fallback(u, loop),
                ))
            if sitemap_urls:
                added = 0
                for surl in sitemap_urls:
                    normalized = _normalize_url(surl, strip_tracking=self.strip_tracking_params)
                    nparsed = urlparse(normalized)
                    if not _same_host(nparsed.netloc, self.base_domain):
                        continue
                    if nparsed.netloc != self.base_domain or nparsed.scheme != self.base_scheme:
                        # Collapse www/scheme aliases onto the crawl's canonical
                        # host so sitemap seeds dedup against discovered links.
                        normalized = _canonicalize_host(
                            normalized, self.base_scheme, self.base_domain,
                            strip_tracking=self.strip_tracking_params)
                    if _url_excluded(normalized, self._compiled_exclude_patterns):
                        continue
                    if self.enqueue(normalized):
                        added += 1
                if added:
                    self.logger.info(f"Sitemap: seeded {added} URLs from sitemap.xml")
            else:
                self.logger.info("No usable sitemap.xml; discovering links by crawling")

        # Start background tasks
        progress_task = asyncio.create_task(self._progress_reporter())
        checkpoint_task = asyncio.create_task(self._checkpoint_saver())

        try:
            tasks = []

            while self.urls_to_visit or tasks:
                while self.urls_to_visit and len(tasks) < CONFIG['max_concurrent']:
                    url = self.urls_to_visit.popleft()
                    self._queued.discard(url)

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
            self._save_checkpoint()
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

        # Write not-found and still-challenged URLs for post-crawl review
        if self.not_found_urls:
            async with aiofiles.open(self.logs_dir / 'not_found.txt', 'w',
                                     encoding='utf-8') as f:
                await f.write('\n'.join(self.not_found_urls) + '\n')
        if self.challenged_urls:
            async with aiofiles.open(self.logs_dir / 'challenged_urls.txt', 'w',
                                     encoding='utf-8') as f:
                await f.write('\n'.join(self.challenged_urls) + '\n')

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
        self.logger.info(f"Not found (404): {self.stats['not_found']}")
        self.logger.info(f"Blocked by challenge: {self.stats['challenged']}")
        self.logger.info(f"Skipped (robots.txt): {self.stats['robots_skipped']}")
        self.logger.info(f"Total data: {self.stats['total_bytes'] / (1024*1024):.2f} MB")
        self.logger.info(f"Errors: {self.stats['errors']}")
        self.logger.info(f"Output location: {self.base_dir}")
        if self.denied_urls:
            self.logger.info(f"Denied URLs logged to: {self.logs_dir / 'access_denied.txt'}")
        if self.not_found_urls:
            self.logger.info(f"Not-found URLs logged to: {self.logs_dir / 'not_found.txt'}")
        if self.challenged_urls:
            self.logger.info(f"Challenge-blocked URLs logged to: {self.logs_dir / 'challenged_urls.txt'}")
            self.logger.info("  Retry them with --human (or export cookies to SCRAPE_CF_COOKIES)")
        if self.failed_urls:
            self.logger.info(f"Failed URLs logged to: {self.logs_dir / 'failed_urls.txt'}")
            self.logger.info(f"  Retry with: uv run python app.py --retry {self.logs_dir / 'failed_urls.txt'}")
        self.logger.info("=" * 80)

        # Cleanup
        self.executor.shutdown(wait=False)
        self.url_store.close()
