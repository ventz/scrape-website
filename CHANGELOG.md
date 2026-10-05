# Changelog

All notable changes to scrape-website are recorded here. Versions follow
[Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH). The running version
is `__version__` in `scrape_website/__init__.py` (also `pyproject.toml`); `python
app.py --version` prints it and every crawl logs it at start so output is traceable
to the code that produced it.

## [0.7.3]

### Fixed
- **`robots.txt` and `sitemap.xml` now get the same WAF fallback as pages.**
  Hosts behind Akamai (e.g. hr.harvard.edu) answer every aiohttp/urllib request
  with `403`. Page fetches already recovered through the `curl_cffi`
  Chrome-fingerprint fallback, but robots.txt and sitemap discovery did not, so
  robots.txt was silently not enforced and sitemaps were never seeded. Both now
  retry a `401`/`403` (and, for robots.txt, a challenge interstitial) through
  the same `_fetch_via_curl_cffi` path, with the cookie bridge turned off
  (`cookie_bridge=False`): no cookie replay, no cookie file and no Chrome cookie
  store reads. Successful responses take the same path as before.
- **A robots.txt that is still blocked after the fallback logs a WARNING**
  ("proceeding WITHOUT robots.txt enforcement") instead of a debug line. The
  crawl still proceeds, as before.
- **Plain sitemaps no longer re-fetch every page URL as a child sitemap.** The
  namespace-agnostic fallback in the sitemap parser matched every `<loc>`
  whatever its parent. So a plain `<urlset>` sent each page URL to the
  child-sitemap fetcher (an extra, unpaced request per page), and a sitemap
  index seeded its own child `.xml` URLs as pages. It now only matches `<loc>`
  elements under the expected parent.

## [0.7.2]

### Fixed
- **Documents served from extension-less URLs are now converted to Markdown.**
  Sites (commonly Drupal) serve files from paths like
  `/resource/proposals-dashboard-guidance` with `Content-Type: application/pdf`.
  They were downloaded but saved without an extension, so the extractor matched
  no converter and dropped them silently: counted under `files_downloaded`,
  never under `docs_extracted`. On two affected sites only 21 of ~250 PDFs were
  converted. Reported and fixed by [@winston975](https://github.com/winston975)
  in [#3](https://github.com/ventz/scrape-website/pull/3):
  `_document_extension()` resolves the type from the file extension, then the
  URL path, then `%PDF` magic bytes.
- **Saved files take their extension from the Content-Type** when the URL path
  has none (`guidance` → `guidance.pdf` / `.docx` / `.xlsx`). This extends the
  fix above to Office formats, which share a ZIP signature and can't be
  identified by magic bytes, and makes `files/` self-describing.

## [0.7.1]

### Changed
- **Default User-Agent bumped to Chrome 154** to match the current stable
  Chrome. The cookie bridge only works when the replayed UA matches the real
  browser that solved the challenge; override with `SCRAPE_USER_AGENT`.
- **Dependencies upgraded** to current releases, with minimum versions raised
  to match: trafilatura 2.2, lxml 6.1, aiohttp 3.14, playwright 1.63 (re-run
  `uv run playwright install chromium`), curl-cffi 0.16, pymupdf4llm 1.28,
  markitdown 0.1.8, protego 0.7, pytest 9.

### Documentation
- README rewritten as a landing page (table of contents, Quick Install, doc
  index); detailed material moved to `docs/` (usage, protected sites, output,
  configuration, architecture, library usage).
- New "Does it use an LLM?" section: extraction is deterministic, with no model
  API calls; only the optional Docling fallback runs local, non-generative
  models.
- Added the MIT `LICENSE` file.

## [0.7.0]

### Fixed (correctness)
- **`--delay` is now a real rate limit.** `wait_politeness` previously did a
  per-task `asyncio.sleep`, which at concurrency 100 throttled nothing (100
  tasks sleeping in parallel still fire 100 requests at once). Requests are now
  paced GLOBALLY to one per `--delay` seconds (default 0.1 → 10 req/s across
  the whole crawl). **This makes default crawls politer and slower than before**
  — use `--delay 0` to disable pacing entirely.
- **A 429/5xx that survives all retries is no longer archived as content.** The
  final error response used to fall through and be saved into `pages/`+`text/`,
  silently poisoning the corpus; it now fails the fetch so the URL lands in
  `failed_urls.txt` for `--retry`.
- **`_normalize_url` no longer corrupts query strings ending in `/`**
  (`?next=/` used to become `?next=`), and now lowercases the scheme + host
  (case-insensitive per RFC) so casing aliases dedup.
- **`www.` and http/https aliases collapse to one canonical URL.** Same-site
  links, sitemap seeds, and `is_same_domain` treat `www.x.com` == `x.com` and
  rewrite links onto the crawl's canonical scheme + host — previously www links
  from a bare-domain start were dropped entirely, and mixed-scheme sites were
  crawled (and saved) twice.

### Fixed (scalability)
- **Headless renders are capped** (`max_render_concurrency`, default 4) so an
  SPA-heavy site at high crawl concurrency can't open 100 Chromium pages at
  once and thrash itself into spurious timeouts.
- **Oversized files are no longer buffered into memory before the size check**:
  the cap is enforced via Content-Length up front, else via a chunked read that
  bails the moment the cap is crossed (`FetchOutcome.detail == 'file too large'`).
- **The crawl queue is deduplicated** (a link appearing on every page is queued
  once, not once per page), fixing the inflated "queued" gauge and shrinking
  the 30-second queue checkpoint accordingly.
- **Sub-sitemaps fetch concurrently** (8 workers) instead of serially — a large
  sitemap index no longer stalls the crawl start for minutes.
- **The visited-URL set now lives fully in memory** (write-through to SQLite),
  removing per-link database lookups from the crawl hot path.

### Fixed (audit follow-ups: security + retry)
- **`--retry` actually retries now.** URLs in a retry file were silently
  rejected by the resumed visited-set guard (they were *visited* — that's how
  they failed), so retry runs processed nothing. Retry URLs are now force-
  requeued (`WebsiteScraper.requeue` / `URLStore.forget`).
- **SSRF hardening** (`_is_safe_fetch_target`): URLs sourced from crawled
  content — cross-host `<img>/<script>/<link>` document links and
  sitemap-index children — are now restricted to http(s) and non-internal
  hosts, so a crawled page or crafted sitemap can't point the scraper at
  `file:///…` or cloud-metadata/link-local/private addresses. Sitemap children
  must additionally be same-site (www-alias ok).
- **Response-size caps everywhere**: HTML pages are read in chunks and fail at
  `max_page_size` (default 50 MB decompressed — compression-bomb guard;
  aiohttp transparently inflates gzip/br/zstd), curl_cffi results are
  size-checked, and sitemaps are capped at 10 MB.
- **Sitemap XML with a DTD is refused** (`<!DOCTYPE`/`<!ENTITY` → ignored):
  stdlib `xml.etree` is not hardened against entity-expansion bombs and real
  sitemaps never declare DTDs. Sitemap fetching also honors
  `--allow-insecure-tls` and the configured timeout now.
- **Output-path hardening**: the start URL's netloc is validated before being
  used as the `data/<domain>` directory name (`http://../x` no longer steers
  writes outside `data/`), and URL paths that sanitize to a dots-only stem
  (`/..`) fall back to hash-based filenames.
- **Cookie-safety warning**: combining `--allow-insecure-tls` with the
  real-Chrome cookie bridge now warns loudly that a MITM could capture the
  replayed session cookies.

### Fixed (robustness)
- **Report lists survive crash + resume**: denied/failed/not-found/challenged
  URL lists are checkpointed to SQLite, so the post-crawl `.txt` reports stay
  complete after a resume.
- **Race-free file writes**: identical-content downloads use an atomic
  check-and-claim on the content hash, and output filename collision handling
  reserves paths without awaiting in between.
- **`--fresh` now overwrites previous output** instead of accumulating
  `_1`/`_2` collision-suffixed duplicates — a fresh re-crawl of a domain yields
  deterministic filenames without `rm -rf data/<domain>` first. (Resumed,
  non-fresh runs still suffix rather than clobber pre-existing files.)

## [0.6.0]

### Added
- **Page classification (`classify_page` in `scrape_website/extract.py`)** — every
  HTML response is now classified as `content` / `challenge` / `not_found` /
  `denied` / `search` (with a human-readable detail such as "Cloudflare Turnstile
  CAPTCHA" or "soft 404"), surfaced on `FetchOutcome.classification`/`.detail`.
  Consequences:
  - **`--human` only pauses for genuine challenges.** A 404, plain 403, or
    search-results page no longer triggers the "press Enter to continue" solve
    prompt, and the prompt now names what was detected (Turnstile vs hCaptcha vs
    reCAPTCHA vs Cloudflare interstitial).
  - **404/410 and soft-404 pages are no longer archived as content** — they are
    counted (`404s` in the progress line, `Not found` in the final summary) and
    written to `logs/not_found.txt`.
  - **Challenge pages that no escalation tier could clear** are counted separately,
    logged at INFO with the vendor detail, and written to
    `logs/challenged_urls.txt` with a retry hint (`--human` / `SCRAPE_CF_COOKIES`).
- **`-v`/`--verbose`** — console shows every per-URL event (fetches, saves,
  fallbacks, errors). Default console keeps milestones + the 5-second progress
  line; full detail continues to go to `logs/scrape.log` either way.

### Changed
- **The console now narrates the slow paths** so a crawl never looks hung:
  sitemap discovery (now also off the event loop in an executor), retry/backoff
  waits with the wait time, 401/403 → curl_cffi fallback escalation,
  challenge-interstitial escalation, the real-Chrome cookie-bridge attempt,
  SPA-shell headless-render escalation, and the browser_cookie3 cookie-store
  read (which can block on a macOS Keychain prompt). Startup logs the active
  mode (render/human/robots/docs) and where the detailed log lives.

## [0.5.0]

### Changed
- **Restructured into an importable package (`scrape_website/`).** The single-file
  `app.py` is now a thin compatibility shim re-exporting every legacy name (so
  `uv run python app.py <url>` and `from app import _normalize_url` keep working
  unchanged); the implementation lives in `scrape_website/{config,urls,sitemap,
  extract,waf,fetch,store,crawler,cli}.py`. CLI behavior is byte-identical
  (verified by a pre/post-refactor crawl diff of the same fixture site).
- **New `FetchEngine` (`scrape_website/fetch.py`)** — the tiered fetcher
  (aiohttp retry/backoff + `Retry-After` → curl_cffi WAF/403 fallback + cookie
  bridge → headless-Chromium SPA render escalation, plus protego robots +
  Crawl-Delay pacing) extracted from `WebsiteScraper` into a reusable class,
  consumed by both the CLI and external projects (scrape-website-mcp). Extras:
  per-call `render_mode` override on `fetch_page()`, response `headers` on
  `FetchOutcome`, injectable Chromium launch args.
- **Packaging**: hatchling build backend; heavy capability tiers moved to extras —
  `render` (playwright), `waf` (curl-cffi/brotli/zstandard), `docs`
  (pymupdf4llm/markitdown), `human` (browser-cookie3), `all`. The CLI's `uv sync`
  still installs everything (dev group depends on `all`); library consumers pick
  the tiers they ship. All tiers keep degrading gracefully when absent.

### Added
- Test suite (59 tests): urls/sitemap/extract/waf/fetch-engine units plus an
  integration-marked end-to-end CLI crawl against a local fixture server.

## [0.4.0]

### Changed
- **Cookie bridge now reuses ALL of a domain's cookies, not just Cloudflare's
  `cf_clearance`.** The same reuse-the-real-Chrome-cookies mechanism now defeats
  Imperva/Incapsula (`visid_incap_*`/`incap_ses_*`), Akamai Bot Manager
  (`_abck`/`bm_sz`), DataDome, PerimeterX, etc. uniformly — and carries any login
  session the real browser holds. The "do we already have clearance?" gate
  (`has_clearance_for`) now recognizes a set of WAF clearance-cookie markers
  (`_CLEARANCE_MARKERS`) across vendors; the interactive `--human` solve polls for any
  of them, not only `cf_clearance`. Cookie capture (exported file, live Chrome store,
  real-Chrome solve) and replay are no longer name-filtered.

  Two honest caveats: (1) this only benefits **future** runs — a host already crawled
  successfully won't re-fetch, so re-run that domain to pick up the broader coverage;
  and (2) Imperva/Akamai tokens are more tightly bound to the browser fingerprint + IP
  than Cloudflare's `cf_clearance`, so replaying them from `curl_cffi`'s TLS stack is
  less reliable even when the cookies themselves are valid.

## [0.3.0]

### Added
- **Cloudflare `cf_clearance` bridge** — defeats modern Cloudflare **Private Access
  Token (PAT)** walls that no automated browser (Playwright/Chromium, even headful)
  can pass. Instead of solving the challenge in automation, it **reuses the
  `cf_clearance` cookie your real, OS-attested Chrome earned** and replays it via
  `curl_cffi` with a matched Chrome TLS fingerprint + User-Agent (the cookie is bound
  to domain + IP + UA). Cookie sources, in order:
  1. `SCRAPE_CF_COOKIES` (or `IB_CF_COOKIES`) — an exported JSON / Netscape cookies
     file (most reliable; immune to Chrome cookie-encryption changes).
  2. The live Chrome cookie store via `browser_cookie3` (silent, optional dep).
  3. `--human` only: open the URL as a tab in your **real Chrome** (`open -a`), solve
     once, then poll the cookie store until `cf_clearance` appears. Cached per host.
- Cloudflare **200 "Just a moment" interstitials** are now detected and escalated
  (previously archived as if they were real content).
- `--human` now hands off to the cf-clearance bridge when a page is **still
  challenged after a manual Playwright solve** (the PAT case Playwright cannot win).
- `--version` flag and a per-crawl version log line.
- `CHANGELOG.md` + version tracking.

### Changed
- Default User-Agent bumped to Chrome 148 to match the real Chrome that mints
  `cf_clearance`. Override with `SCRAPE_USER_AGENT`.
- curl_cffi fallback now triggers on 401/403 (not just 403) and on 200 challenge
  pages, and replays any cached `cf_clearance` cookie for the host.

### Notes
- Env vars: `SCRAPE_CF_COOKIES`, `SCRAPE_REAL_BROWSER` (default "Google Chrome"),
  `SCRAPE_HUMAN_SOLVE_TIMEOUT` (default 300s), `SCRAPE_USER_AGENT`. `IB_CF_COOKIES`
  is also honored so a cookies file works across this tool and the industry-background
  tool that the bridge was ported from.

## [0.2.0]

- `--human` interactive headful persistent-browser mode for challenge/login walls.
- Reliable SPA rendering (drop CSS blocking, `domcontentloaded` + settle wait).
- Tiered fetch (aiohttp → headless render → curl_cffi), robots.txt + Crawl-Delay,
  document extraction (PDF/Office → Markdown).
- Markdown output with YAML front matter; fixed cross-page dedup loss.
- URL exclude patterns, tracking-param stripping, sitemap seeding.
- UTF-8 mojibake fix (charset mis-detection).

## [0.1.0]

- Initial async website scraper with text extraction.
