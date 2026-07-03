# Changelog

All notable changes to scrape-website are recorded here. Versions follow
[Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH). The running version
is `__version__` in `scrape_website/__init__.py` (also `pyproject.toml`); `python
app.py --version` prints it and every crawl logs it at start so output is traceable
to the code that produced it.

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
