# scrape-website — Project Notes

Async domain scraper: crawls one domain, saves raw HTML + extracted Markdown + linked documents. Single-file app (`app.py`). Python via `uv` (deps pinned in `uv.lock`). Tiered fetch: static aiohttp first → headless Chromium (Playwright) only when a page is detected as an un-hydrated SPA shell → `curl_cffi` real-browser fingerprint fallback on 403/WAF.

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

CLI short flags: `-c`/`--concurrency`, `-t`/`--timeout`, `-d`/`--delay`, `-F`/`--fresh`, `-e`/`--exclude-pattern`, `-n`/`--fullname`, plus existing `-f`/`--file`, `-r`/`--retry`. (`-f` was already `--file`, so `--fullname` is `-n`.) Other new flags: `--render {auto,never,always}`, `--allow-insecure-tls`, `--ignore-robots`, `--no-extract-docs`.

## Critical Constraints / Gotchas

### trafilatura dedup is a PROCESS-GLOBAL cache
`trafilatura.deduplication.LRU_TEST` is a module-global LRU (`MAX_REPETITIONS=2`, `MIN_DUPLCHECK_SIZE=100`). Extraction runs in a long-lived `ProcessPoolExecutor` (`max_workers=cpu_count()`, **no `maxtasksperchild`**), so without intervention the cache accumulates across every page a worker handles → silent **cross-page** content loss (a block seen >2× anywhere gets stripped; a page that is only such a block yields **no file at all**).

**Fix in place** (`_extract_text_trafilatura`, app.py ~257): call `LRU_TEST.clear()` at the start of every extraction so dedup is strictly **intra-page**. Keep `deduplicate=True`. Do not remove the clear() without understanding this.

- This is correct for knowledge bases: every page must be a self-contained, independently retrievable document. Cross-document dedup is a training-corpus concern, not a RAG one.
- Concurrency-safe: each pool worker processes one `_parse_and_extract` task at a time, so per-call clear() never races.

### Output is Markdown + metadata
`trafilatura.extract(..., output_format='markdown', with_metadata=True)`. Files are `.md` with a `---` front-matter block (`title`, `url`, `hostname`, `sitename`, `date`). `save_text` (app.py ~621) writes `.md` (collision counter `_1`, `_2`, …). Don't revert to `txt`.

### Two unrelated "dedup" concepts
- **URL dedup** — SQLite-backed exact-URL visited tracking (`URLStore`). Unrelated to text dedup.
- **Text dedup** — the trafilatura LRU above.

## Key Files (all in `app.py`; line numbers approximate — grep the symbol)
- `_extract_text_trafilatura` — extraction config + per-page cache clear (the LRU gotcha above).
- `_parse_and_extract` — lxml links + text, runs in process pool.
- `_looks_like_spa_shell` — heuristic that triggers JS-render escalation (tiny text + SPA marker / zero links). Markers in `_SPA_SHELL_MARKERS`.
- `_render_with_playwright` / `_ensure_browser` / `_close_browser` — lazy shared headless Chromium; blocks images/media/fonts/css, waits for network idle. `process_url` re-runs `_parse_and_extract` on the rendered HTML.
- `_fetch_via_curl_cffi` — 403/WAF fallback with `impersonate='chrome'`. Called inside `fetch_with_retry`.
- `fetch_with_retry` — backoff w/ jitter (`_backoff`), `Retry-After` on `RETRYABLE_STATUS` (429/5xx).
- `_load_robots` / `_robots_allows` — Protego robots.txt + `aiolimiter` Crawl-Delay; loaded at start of `crawl()`.
- `_extract_document_to_markdown` (top-level) — PDF/Office → Markdown; `_save_document_text` writes it to `text/`. Called from `download_file`.
- `save_text` / `save_html` / `generate_html_filename` / `generate_filename` — output writers; `--fullname` host-prefixes stems.
- `parse_args` — all CLI flags; `main()` threads them into `WebsiteScraper(...)`.

## Gotchas added with the tiered-fetch work
- **Render is opt-out-able, not free**: `--render auto` (default) only escalates SPA shells; `--render never` skips the browser entirely (and avoids needing `playwright install chromium`). Don't make Playwright the default fetch path.
- **Heavy deps are lazy-imported** inside the functions that use them (`playwright`, `pymupdf4llm`, `markitdown`, `curl_cffi`, `protego`, `aiolimiter`, optional `docling`) so a `--render never` / no-docs crawl pays no import cost. Keep them lazy.
- **`docling` is optional and NOT in `pyproject.toml`** (it pulls torch). Code falls back gracefully if absent; only used when PyMuPDF4LLM output is near-empty.

## Conventions
- License: MIT, copyright "Ventz Petkov".
- Harvard repos: set `git config user.email "ventz@g.harvard.edu"` per-repo (not global).
- No test suite; verify by exercising the top-level functions (`_extract_text_trafilatura`, `_looks_like_spa_shell`, `_render_with_playwright`, `_extract_document_to_markdown`) directly on a fetched/rendered page or a generated doc, rather than running a full live domain crawl unprompted (outward-facing load).
