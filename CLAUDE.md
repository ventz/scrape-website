# scrape-website — Project Notes

Async domain scraper: crawls one domain, saves raw HTML + extracted Markdown + linked documents. Since **0.5.0** the implementation is an importable package (`scrape_website/`); the repo-root `app.py` is a thin shim that re-exports every legacy name and remains the CLI entrypoint. Python via `uv` (deps pinned in `uv.lock`; heavy tiers are extras `render`/`waf`/`docs`/`human`, CLI installs `all` via the dev group). Tiered fetch: static aiohttp first → headless Chromium (Playwright) only when a page is detected as an un-hydrated SPA shell → `curl_cffi` real-browser fingerprint fallback on 401/403/WAF/challenge → **cookie bridge** (reuse **all** of the real Chrome's cookies for the domain) for modern Cloudflare PAT walls and other WAFs (Imperva/Akamai/DataDome/PerimeterX). `--human` swaps the fetch path to a **visible, persistent** browser for manual challenge/login solving.

## Versioning
`__version__` in `scrape_website/__init__.py` (kept in lockstep with `pyproject.toml`); bump on every user-visible change and add a `CHANGELOG.md` entry. `python app.py --version` prints it and every crawl logs `scrape-website vX.Y.Z` at start (output is traceable to the code that made it). Current: **0.5.0**.

## Quick Start

```bash
uv sync && uv run playwright install chromium  # browser needed for --render (default auto)
uv run python app.py https://example.com/        # crawl a domain
uv run python app.py --file urls.txt             # seed from a URL list
uv run python app.py --retry data/<d>/logs/failed_urls.txt
uv run python app.py https://example.com/ --render never   # disable JS rendering (no browser)
.venv/bin/python -m py_compile app.py            # no lint tooling in repo
```

Output per domain: `data/<domain>/{pages/,text/,files/,logs/}`. `text/` is Markdown (`.md`) with YAML front matter, LLM/RAG-ready — holds **both** extracted page text and extracted document text.

CLI short flags: `-c`/`--concurrency`, `-t`/`--timeout`, `-d`/`--delay`, `-F`/`--fresh`, `-e`/`--exclude-pattern`, `-n`/`--fullname`, plus existing `-f`/`--file`, `-r`/`--retry`. (`-f` was already `--file`, so `--fullname` is `-n`.) Other new flags: `--render {auto,never,always}`, `--human`, `--allow-insecure-tls`, `--ignore-robots`, `--no-extract-docs`.

## Critical Constraints / Gotchas

### trafilatura dedup is a PROCESS-GLOBAL cache
`trafilatura.deduplication.LRU_TEST` is a module-global LRU (`MAX_REPETITIONS=2`, `MIN_DUPLCHECK_SIZE=100`). Extraction runs in a long-lived `ProcessPoolExecutor` (`max_workers=cpu_count()`, **no `maxtasksperchild`**), so without intervention the cache accumulates across every page a worker handles → silent **cross-page** content loss (a block seen >2× anywhere gets stripped; a page that is only such a block yields **no file at all**).

**Fix in place** (`scrape_website/extract.py::_extract_text_trafilatura`): call `LRU_TEST.clear()` at the start of every extraction so dedup is strictly **intra-page**. Keep `deduplicate=True`. Do not remove the clear() without understanding this.

- This is correct for knowledge bases: every page must be a self-contained, independently retrievable document. Cross-document dedup is a training-corpus concern, not a RAG one.
- Concurrency-safe: each pool worker processes one `_parse_and_extract` task at a time, so per-call clear() never races.

### Output is Markdown + metadata
`trafilatura.extract(..., output_format='markdown', with_metadata=True)`. Files are `.md` with a `---` front-matter block (`title`, `url`, `hostname`, `sitename`, `date`). `save_text` (`scrape_website/crawler.py`) writes `.md` (collision counter `_1`, `_2`, …). Don't revert to `txt`.

### Two unrelated "dedup" concepts
- **URL dedup** — SQLite-backed exact-URL visited tracking (`URLStore`). Unrelated to text dedup.
- **Text dedup** — the trafilatura LRU above.

## Key Files (0.5.0 package layout — grep the symbol)

Module map: `config.py` (CONFIG + marker tuples + downloadable types + default excludes/tracking-params), `urls.py` (`_normalize_url`/`_strip_tracking_params`/`_url_excluded`), `sitemap.py`, `extract.py` (links/text/docs/SPA+challenge heuristics), `waf.py` (`_CFSession`/`CF_SESSION`), **`fetch.py` (`FetchEngine`/`FetchOutcome` — the reusable tiered fetcher; also consumed by scrape-website-mcp; keep its public surface stable: `start/close`, `fetch`, `render`, `fetch_page(render_mode=...)`, `load_robots`/`robots_allows`/`wait_politeness`)**, `store.py` (`URLStore`), `crawler.py` (`WebsiteScraper` — composes a FetchEngine; owns output tree/checkpoints/progress/process pool), `cli.py`. `app.py` = compatibility shim only — never add implementation there.
- `_extract_text_trafilatura` — extraction config + per-page cache clear (the LRU gotcha above).
- `_parse_and_extract` — lxml links + text, runs in process pool.
- `_looks_like_spa_shell` — heuristic that triggers JS-render escalation (tiny text + SPA marker / zero links). Markers in `_SPA_SHELL_MARKERS`.
- `_render_with_playwright` / `_ensure_browser` / `_close_browser` — lazy Chromium. `process_url` re-runs `_parse_and_extract` on the rendered HTML. **Render strategy is load-bearing** (see gotcha): block only `image`/`media`, `wait_until='domcontentloaded'` + `render_settle_ms`, NOT `networkidle`.
- `_browser_fetch` / `_looks_challenged` / `_await_human_solve` — **`--human` mode**: `_ensure_browser` opens a headful **persistent context** (`logs/browser_profile/`); `process_url` fetches via `_browser_fetch` instead of `fetch_with_retry`; auto-pauses (blocking `input()`) when `_looks_challenged` fires (`_CHALLENGE_MARKERS`, now a module-level constant). Files fetched via `context.request.get` so they carry solved cookies. **After a manual solve, if the page is STILL challenged it's a PAT wall** (Playwright can't mint a Private Access Token) → hands off to the cookie bridge via `_fetch_via_curl_cffi`.
- `_CFSession` / `CF_SESSION` / `_looks_challenged` (module-level) — **cookie bridge** (see gotcha below). Despite the `_CF*` names it now reuses **all** of a domain's cookies, not just Cloudflare's, so the same path clears Imperva/Akamai/DataDome/PerimeterX too. `_fetch_via_curl_cffi` calls `_curl_get` (replays **all** cached cookies for the host + matched UA), and on a still-blocked result calls `CF_SESSION.obtain_clearance(url, interactive=self.human)` then replays once. `has_clearance_for` detects a solved challenge via `_CLEARANCE_MARKERS` (per-vendor clearance-cookie names: `cf_clearance`, `visid_incap_`/`incap_ses_`, `_abck`/`bm_sz`, `datadome`, `_px*`, …); `_is_clearance` does the exact/prefix match. `_curl_blocked` decides if a curl result is a 403/5xx/challenge non-result.
- `_fetch_via_curl_cffi` / `_curl_get` — 401/403/WAF/challenge fallback with `impersonate='chrome'`. Called inside `fetch_with_retry` (on 401/403 AND on a 200 Cloudflare interstitial) and from `_browser_fetch`.
- `fetch_with_retry` — backoff w/ jitter (`_backoff`), `Retry-After` on `RETRYABLE_STATUS` (429/5xx).
- `_load_robots` / `_robots_allows` — Protego robots.txt + `aiolimiter` Crawl-Delay; loaded at start of `crawl()`.
- `_extract_document_to_markdown` (top-level) — PDF/Office → Markdown; `_save_document_text` writes it to `text/`. Called from `download_file`.
- `save_text` / `save_html` / `generate_html_filename` / `generate_filename` — output writers; `--fullname` host-prefixes stems.
- `parse_args` — all CLI flags; `main()` threads them into `WebsiteScraper(...)`.

## Cloudflare PAT + the cookie bridge (ported from ~/git/private/proj/industry-background)
- **Modern Cloudflare = Private Access Token (PAT) → automation can NEVER pass it.** PAT is a hardware-attested token (Secure Enclave); only a genuine, OS-blessed browser (your real Chrome/Safari) can mint it. Playwright/Chromium can't — headful or not, no matter how many times you click. So `--human`'s Playwright window solves Turnstile/login walls, but **not PAT walls** (psa.gov.ph-class).
- **The bridge reuses ALL of the cookies your REAL Chrome earned for the domain** (Cloudflare's `cf_clearance` *plus* Imperva `visid_incap_*`/`incap_ses_*`, Akamai `_abck`/`bm_sz`, DataDome, PerimeterX, and any login session), then replays them via `curl_cffi` with the SAME Chrome TLS fingerprint + UA. Clearance is bound to **domain + IP + UA**, so: (1) the replay UA MUST match the real Chrome that solved it — `CONFIG['user_agent']` is Chrome **148**, bump it in lockstep with your installed Chrome (override `SCRAPE_USER_AGENT`); (2) same machine (IP) only. Solve **once per host** → cached for the run. **Caveat:** Imperva/Akamai tokens are bound to the browser *fingerprint* + IP more tightly than `cf_clearance`, so replay from curl's TLS stack is less reliable for them even with valid cookies. Generalizing the bridge only helps **future** runs — re-crawl a domain to benefit (an already-passed host won't re-fetch).
- **Cookie sources, in order** (`_CFSession.obtain_clearance`): cached → `SCRAPE_CF_COOKIES`/`IB_CF_COOKIES` file (JSON list `[{"domain","name","value"}]`, JSON map `{"domain": {"name":"value"}}`, or Netscape cookies.txt — **all** cookie names captured, not just `cf_clearance`) → live Chrome cookie store via `browser_cookie3` (silent; optional dep, degrades to {}) → **`--human` only:** `open -a "Google Chrome" <url>` to solve in real Chrome, then poll the cookie store until any `_CLEARANCE_MARKERS` cookie appears (`SCRAPE_HUMAN_SOLVE_TIMEOUT`, default 300s). The file/cookie-store paths work **without** `--human`; only opening a real tab is interactive.
- **No-browser path (most reliable, no `--human`):** export the cookies once and point `SCRAPE_CF_COOKIES` at the file — curl reuses them with no browser. `SCRAPE_REAL_BROWSER` overrides the app name (default "Google Chrome"). `browser_cookie3` may fail to decrypt the newest Chrome / trigger a keychain prompt → the file path is the fallback.
- A 200 whose body is a Cloudflare "Just a moment" interstitial (`_looks_challenged`) is treated as a **block**, not content, and escalated — never archived as a real page.

## Gotchas added with the tiered-fetch work
- **Render strategy is fragile — don't "optimize" it back**: blocking CSS/fonts makes some SPAs (e.g. Next.js) throw a client-side exception and render their error boundary (`"Application error"`) instead of content → block only `image`/`media`. And `wait_until='networkidle'` **times out** on sites with chat widgets / analytics / websockets (network never idles) → use `domcontentloaded` + a fixed `render_settle_ms` hydration wait. Both were real bugs; the current settings are deliberate.
- **Render is opt-out-able, not free**: `--render auto` (default) only escalates SPA shells; `--render never` skips the browser entirely (and avoids needing `playwright install chromium`). Don't make Playwright the default fetch path.
- **`--human` forces `--concurrency 1`** (single visible window, one unambiguous solve prompt) and fetches everything through the browser. Session (incl. `cf_clearance`) persists on disk in `logs/browser_profile/` across runs — a manually-solved challenge or login is reused. Reason it's fetch-through-browser not cookie-handoff: `cf_clearance` is bound to the browser's TLS fingerprint, so aiohttp can't reuse it.
- **`--fresh` clears crawl *state* (SQLite) but NOT output files** → re-runs accumulate `_1`/`_2` collision-suffixed dupes. `rm -rf data/<domain>` for a truly clean re-crawl.
- **Queue overcounts**: `urls_to_visit` dedups against the *visited* set at enqueue/pop, not against itself, so a nav link enqueues many times and the "queued" gauge is inflated (visited count is the real coverage).
- **SPA hosts serve HTML for `/robots.txt` and `/sitemap.xml`**: `_load_robots` skips non-`text/*`-typed robots; sitemap parse just yields 0 (link discovery + homepage render carry coverage instead).
- **Heavy deps are lazy-imported** inside the functions that use them (`playwright`, `pymupdf4llm`, `markitdown`, `curl_cffi`, `protego`, `aiolimiter`, optional `docling`) so a `--render never` / no-docs crawl pays no import cost. Keep them lazy.
- **`docling` is optional and NOT in `pyproject.toml`** (it pulls torch). Code falls back gracefully if absent; only used when PyMuPDF4LLM output is near-empty.

## Conventions
- License: MIT, copyright "Ventz Petkov".
- Harvard repos: set `git config user.email "ventz@g.harvard.edu"` per-repo (not global).
- Test suite (0.5.0): `uv run pytest tests/` — 59 tests covering urls/sitemap/extract/waf/fetch-engine + an `integration`-marked end-to-end CLI crawl against a local fixture server. Never run a full live domain crawl unprompted (outward-facing load).
