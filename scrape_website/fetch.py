"""FetchEngine — the shared tiered fetcher extracted from WebsiteScraper.

Tiering (static-first by design; only problem pages pay for escalation):
  1. aiohttp GET with retry/backoff (+ Retry-After) and charset-safe decode
  2. 401/403 or challenge interstitial -> curl_cffi Chrome-fingerprint fallback
     (+ the real-Chrome cookie bridge in waf.py for PAT/WAF walls)
  3. un-hydrated SPA shell -> headless Chromium render, then re-extract

Politeness (robots.txt via protego + Crawl-Delay rate limiting) lives here too
so every consumer of the engine honors it the same way.

This class is consumed by BOTH the CLI crawler (``scrape_website.crawler``)
and external projects (the scrape-website-mcp server). Keep its public surface
stable: ``start``/``close``, ``fetch``, ``render``, ``fetch_page``,
``load_robots``/``robots_allows``/``wait_politeness``.
"""

import asyncio
import logging
import random
import ssl
from dataclasses import dataclass
from urllib.parse import urlparse

import aiohttp

from .config import CONFIG, DOWNLOADABLE_EXTENSIONS, DOWNLOADABLE_MIMES, RETRYABLE_STATUS
from .extract import _looks_challenged, _looks_like_spa_shell, is_access_denied
from .waf import CF_SESSION

_module_logger = logging.getLogger("scrape_website.fetch")


def should_download_file(url: str, content_type: str = None) -> bool:
    """Is this URL / Content-Type a downloadable document rather than a page?"""
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in DOWNLOADABLE_EXTENSIONS):
        return True
    if content_type:
        content_type = content_type.lower().split(';')[0].strip()
        if content_type in DOWNLOADABLE_MIMES:
            return True
    return False


@dataclass
class FetchOutcome:
    """Result of one tiered fetch. ``content`` is ``str`` for HTML pages and
    ``bytes`` for downloadable documents (``kind == 'file'``)."""
    content: str | bytes
    content_type: str
    kind: str                 # 'html' | 'file'
    status: int
    rendered: bool = False    # True when the content is a headless-Chromium snapshot
    via: str = 'aiohttp'      # 'aiohttp' | 'curl_cffi' | 'playwright'
    denied: bool = False      # True when the HTML response is an access-denied page


class FetchEngine:
    """Reusable tiered fetcher. One instance per crawl host (robots/rate state)
    or per long-lived server process (MCP): the aiohttp session and the lazy
    Chromium instance are shared across all fetches until ``close()``."""

    def __init__(self, *,
                 user_agent: str | None = None,
                 timeout: int | None = None,
                 max_retries: int | None = None,
                 delay_between_requests: float | None = None,
                 max_concurrent: int | None = None,
                 render_timeout: int | None = None,
                 render_settle_ms: int | None = None,
                 render_mode: str = 'auto',
                 allow_insecure_tls: bool = False,
                 respect_robots: bool = True,
                 human: bool = False,
                 browser_launch_args: list[str] | None = None,
                 profile_dir=None,
                 logger: logging.Logger | None = None):
        # Defaults resolve from the module CONFIG (the CLI mutates CONFIG from
        # argparse BEFORE constructing scrapers, exactly as it always has).
        self.user_agent = user_agent or CONFIG['user_agent']
        self.timeout = timeout if timeout is not None else CONFIG['timeout']
        self.max_retries = max_retries if max_retries is not None else CONFIG['max_retries']
        self.delay_between_requests = (delay_between_requests
                                       if delay_between_requests is not None
                                       else CONFIG['delay_between_requests'])
        self.max_concurrent = max_concurrent if max_concurrent is not None else CONFIG['max_concurrent']
        self.render_timeout = render_timeout if render_timeout is not None else CONFIG['render_timeout']
        self.render_settle_ms = render_settle_ms if render_settle_ms is not None else CONFIG['render_settle_ms']
        self.render_mode = render_mode            # 'auto' | 'never' | 'always'
        self.allow_insecure_tls = allow_insecure_tls
        self.respect_robots = respect_robots
        self.human = human                        # interactive headful browser mode
        self.browser_launch_args = list(browser_launch_args or [])
        self.profile_dir = profile_dir            # persistent context dir (--human)
        self.logger = logger or _module_logger

        # Browser state (lazy: Chromium only launches if a page needs it).
        # _browser: shared headless instance for SPA-render escalation.
        # _context: persistent headful context for --human interactive mode.
        self._browser = None
        self._context = None
        self._playwright = None
        self._browser_lock = asyncio.Lock()
        # Politeness state
        self._robots = None                       # Protego parser, loaded in load_robots()
        self._rate_limiter = None                 # aiolimiter.AsyncLimiter for Crawl-Delay
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        """Create the shared aiohttp session (idempotent)."""
        if self.session is not None:
            return
        # TLS: strict by default. allow_insecure_tls disables verification for
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
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            resolver=aiohttp.AsyncResolver(),
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            ssl=ssl_param,
        )
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        # aiohttp advertises Accept-Encoding for the codecs it can decode; with
        # brotli + zstandard installed that includes br and zstd automatically.
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': self.user_agent},
            max_field_size=32768,
        )

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
        await self._close_browser()

    # ------------------------------------------------------------------
    # Headless rendering (lazy Chromium) — escalation tier for SPA shells
    # ------------------------------------------------------------------
    async def _ensure_browser(self):
        """Launch the browser on first use.

        Two modes:
        - default: a single shared **headless** Chromium for SPA-render escalation.
        - ``human``: a **headful, persistent** context (visible window) whose
          profile is stored under ``profile_dir`` so a manually-solved
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
                if self.profile_dir is None:
                    self.logger.warning(
                        "human mode requires a profile_dir; falling back to headless")
                else:
                    self._context = await self._playwright.chromium.launch_persistent_context(
                        user_data_dir=str(self.profile_dir),
                        headless=False,
                        user_agent=self.user_agent,
                        viewport={'width': 1280, 'height': 900},
                    )
                    self.logger.info(
                        "Interactive browser (--human) launched; session profile: %s",
                        self.profile_dir,
                    )
                    return
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=self.browser_launch_args or None,
            )
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
    async def _await_human_solve(self, page, url: str):
        """Surface the window and block until the user solves the challenge.

        Runs with concurrency forced to 1 (see cli.main()), so a single blocking
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

    async def _browser_fetch(self, url: str) -> FetchOutcome:
        """Fetch *url* through the persistent (visible) browser context.

        Documents are pulled via the context's request API so they carry the
        solved session cookies; HTML pages are navigated and snapshotted from
        the hydrated DOM.
        """
        await self._ensure_browser()
        if self._context is None:
            raise Exception("Interactive browser context unavailable")

        # Binary documents: fetch bytes with the context's cookies.
        if should_download_file(url):
            resp = await self._context.request.get(
                url, timeout=self.timeout * 1000)
            ctype = resp.headers.get('content-type', '')
            return FetchOutcome(await resp.body(), ctype, 'file', resp.status,
                                via='playwright')

        page = await self._context.new_page()
        try:
            resp = await page.goto(url, wait_until='domcontentloaded',
                                   timeout=self.render_timeout * 1000)
            status = resp.status if resp else 0
            ctype = resp.headers.get('content-type', '') if resp else 'text/html'
            html = await page.content()

            if _looks_challenged(html, status):
                await self._await_human_solve(page, url)
                # Re-evaluate after the solve; the page has navigated past the
                # interstitial and the context now holds the clearance cookie.
                status = 200
                await page.wait_for_timeout(self.render_settle_ms)
                html = await page.content()
                # Playwright/Chromium can NEVER mint a Cloudflare Private Access Token
                # (PAT) — a hardware-attested token only your genuine OS browser can
                # produce. If the page is STILL a challenge after the solve, this is a
                # PAT wall: hand off to the cf-clearance bridge (open the URL in your
                # REAL Chrome, reuse the cookie it earns, replay via curl_cffi).
                if _looks_challenged(html, status):
                    self.logger.warning(
                        "Still challenged after solve (likely a PAT wall); "
                        "escalating to the real-Chrome cf-clearance bridge: %s", url)
                    bridged = await self._fetch_via_curl_cffi(url)
                    if bridged is not None:
                        content, btype, kind, bstatus = bridged
                        return FetchOutcome(content, btype, kind, bstatus,
                                            via='curl_cffi')
            else:
                try:
                    await page.wait_for_load_state(
                        'networkidle', timeout=self.render_settle_ms)
                except Exception:
                    pass
                await page.wait_for_timeout(self.render_settle_ms)
                html = await page.content()

            return FetchOutcome(html, ctype or 'text/html', 'html', status,
                                rendered=True, via='playwright')
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def render(self, url: str) -> str | None:
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
            page = await self._browser.new_page(user_agent=self.user_agent)

            async def _block(route):
                # Block only large media; keep CSS/fonts/scripts so client JS
                # doesn't crash on missing resources it expects to load.
                if route.request.resource_type in ('image', 'media'):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route('**/*', _block)
            await page.goto(url, wait_until='domcontentloaded',
                            timeout=self.render_timeout * 1000)
            # Best-effort: let in-flight XHR settle, but never block on idle
            # that may never arrive — the fixed settle below is the real wait.
            try:
                await page.wait_for_load_state(
                    'networkidle', timeout=self.render_settle_ms)
            except Exception:
                pass
            await page.wait_for_timeout(self.render_settle_ms)
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
        headers = {'User-Agent': self.user_agent}
        cookie = CF_SESSION.cookie_header_for(url)
        if cookie:
            headers['Cookie'] = cookie
        try:
            async with AsyncSession() as s:
                resp = await s.get(
                    url, impersonate='chrome', headers=headers,
                    timeout=self.timeout,
                    verify=not self.allow_insecure_tls, allow_redirects=True,
                )
                content_type = resp.headers.get('Content-Type', '')
                status = resp.status_code
                if should_download_file(url, content_type):
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

    # ------------------------------------------------------------------
    # Static fetch tier (aiohttp) with retry/backoff + fallbacks
    # ------------------------------------------------------------------
    async def fetch(self, url: str, method: str = 'GET') -> FetchOutcome:
        """Tiered fetch WITHOUT render escalation (see ``fetch_page`` for that).

        Raises on repeated transport failure, mirroring the original
        ``fetch_with_retry`` contract.
        """
        if self.session is None:
            await self.start()
        last_error = None
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(method, url, allow_redirects=True) as response:
                    content_type = response.headers.get('Content-Type', '')
                    status = response.status

                    # Transient server / rate-limit responses: honor Retry-After
                    # when present, else exponential backoff, then retry.
                    if status in RETRYABLE_STATUS and attempt < self.max_retries - 1:
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
                            content, ctype, kind, fstatus = fallback
                            return FetchOutcome(content, ctype, kind, fstatus,
                                                via='curl_cffi')

                    if should_download_file(url, content_type):
                        content = await response.read()
                        return FetchOutcome(content, content_type, 'file', status)
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
                                fcontent, fctype, fkind, fstatus = fallback
                                return FetchOutcome(fcontent, fctype, fkind, fstatus,
                                                    via='curl_cffi')
                        return FetchOutcome(content, content_type, 'html', status)
            except asyncio.TimeoutError:
                last_error = "Timeout"
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self._backoff(attempt))
            except Exception as e:
                last_error = str(e)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self._backoff(attempt))
        raise Exception(f"Failed after {self.max_retries} attempts: {last_error}")

    # ------------------------------------------------------------------
    # Politeness: robots.txt + adaptive per-host rate limiting
    # ------------------------------------------------------------------
    async def load_robots(self, base_url: str):
        """Fetch and parse robots.txt once for the crawl host (best-effort).

        Also picks up Crawl-Delay and turns it into a per-host rate limiter so
        we honor the site's requested pace without throttling extraction.
        """
        if not self.respect_robots:
            return
        if self.session is None:
            await self.start()
        parsed = urlparse(base_url)
        robots_url = f"{parsed.scheme or 'https'}://{parsed.netloc}/robots.txt"
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
                delay = self._robots.crawl_delay(self.user_agent) \
                    or self._robots.crawl_delay('*')
            except Exception:
                delay = None
            if delay and delay > 0:
                from aiolimiter import AsyncLimiter
                self._rate_limiter = AsyncLimiter(1, float(delay))
                self.logger.info(f"robots.txt Crawl-Delay: pacing to 1 request / {delay}s")

    def robots_allows(self, url: str) -> bool:
        if not self.respect_robots or self._robots is None:
            return True
        try:
            return self._robots.can_fetch(url, self.user_agent)
        except Exception:
            return True

    async def wait_politeness(self):
        """Honor Crawl-Delay (per-host) if robots.txt declared one, else fall
        back to the flat politeness delay."""
        if self._rate_limiter is not None:
            async with self._rate_limiter:
                pass
        else:
            await asyncio.sleep(self.delay_between_requests)

    # ------------------------------------------------------------------
    # Full page pipeline: fetch -> extract -> SPA render escalation -> re-extract
    # ------------------------------------------------------------------
    async def fetch_page(self, url: str, *, run_extract) -> tuple[FetchOutcome, set[str], str | None]:
        """Fetch *url* and (for HTML) extract links + text, escalating to a
        headless render when the static pass yields an un-hydrated SPA shell.

        ``run_extract(html, url)`` is an awaitable callable returning
        ``(links, text)`` — the CLI passes a ProcessPoolExecutor dispatch, the
        MCP server passes an ``asyncio.to_thread`` wrapper, so the engine stays
        executor-agnostic.

        Returns ``(outcome, links, text)``. For ``kind == 'file'`` and for
        access-denied pages (``outcome.denied``), ``links`` is empty and
        ``text`` is None; the caller decides what to do with the bytes.
        """
        # --human: fetch through the visible persistent browser so a
        # manually-solved Cloudflare/CAPTCHA/login session is reused;
        # otherwise use the fast static aiohttp path.
        if self.human:
            outcome = await self._browser_fetch(url)
        else:
            outcome = await self.fetch(url)

        if outcome.kind == 'file':
            return outcome, set(), None

        if is_access_denied(outcome.content, outcome.status):
            outcome.denied = True
            return outcome, set(), None

        links, extracted_text = await run_extract(outcome.content, url)

        # JS-render escalation tier: when the static pass yields an
        # un-hydrated SPA shell (tiny text, no links), re-fetch the
        # page in headless Chromium and re-extract from the rendered
        # DOM. Static-first by design — only shells pay the cost.
        # (Skipped in --human mode: the browser already rendered it.)
        needs_render = not self.human and (
            self.render_mode == 'always' or (
                self.render_mode == 'auto'
                and _looks_like_spa_shell(outcome.content, extracted_text, len(links))
            )
        )
        if needs_render:
            rendered = await self.render(url)
            if rendered:
                outcome.content = rendered
                outcome.rendered = True
                links, extracted_text = await run_extract(outcome.content, url)

        return outcome, links, extracted_text
