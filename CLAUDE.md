# scrape-website — Project Notes

Async domain scraper: crawls one domain, saves raw HTML + extracted Markdown + linked documents. Single-file app (`app.py`, ~37KB). Python via `uv` (deps pinned in `uv.lock`).

## Quick Start

```bash
uv run python app.py https://example.com/        # crawl a domain
uv run python app.py --file urls.txt             # seed from a URL list
uv run python app.py --retry data/<d>/logs/failed_urls.txt
.venv/bin/python -m py_compile app.py            # no lint tooling in repo
```

Output per domain: `data/<domain>/{pages/,text/,files/,logs/}`. `text/` is Markdown (`.md`) with YAML front matter, LLM/RAG-ready.

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

## Key Files (all in `app.py`)
- `_extract_text_trafilatura` (~257) — extraction config + per-page cache clear.
- `_parse_and_extract` (~276) — lxml links + text, runs in process pool.
- `save_text` (~621) / `save_html` — output writers; `text_dir` set ~420.
- `ProcessPoolExecutor` created ~462; `LRU_TEST` imported near top (~25).

## Conventions
- License: MIT, copyright "Ventz Petkov".
- Harvard repos: set `git config user.email "ventz@g.harvard.edu"` per-repo (not global).
- No test suite; verify by exercising `_extract_text_trafilatura` directly on a fetched page rather than running a full live domain crawl unprompted (outward-facing load).
