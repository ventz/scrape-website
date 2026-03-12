# scrape-website

Async website scraper that crawls an entire domain and downloads all pages (HTML), extracts clean text (for LLMs), and saves documents (PDF, DOCX, XLSX, etc.). Stays within the target domain — it will never follow links to external sites.

## Features

- **Fast async crawling** — up to 100 concurrent requests (configurable)
- **Async DNS** — non-blocking DNS resolution with caching (via `aiodns`)
- **Async file I/O** — non-blocking writes with `aiofiles`
- **Clean text extraction** — extracts main content using `trafilatura` (strips nav, headers, footers, boilerplate) for LLM consumption
- **Parallel HTML parsing** — `lxml` link extraction + text extraction offloaded to process pool (uses all CPU cores)
- **SQLite-backed dedup** — exact URL deduplication with minimal RAM usage (scales to millions of URLs)
- **Crash recovery** — auto-resumes from checkpoint on restart; use `--fresh` to start over
- **Multi-domain concurrency** — all domains run in parallel via `asyncio.TaskGroup`
- **Domain-scoped** — only follows links within the starting domain
- **Document downloads** — PDF, DOC(X), PPT(X), XLS(X), CSV, ZIP, RTF, ODT, ODS, ODP
- **Multiple input modes** — single URL, file with URL list, or retry from failed URLs
- **Access-denied detection** — identifies HTTP 401/403 and CDN/WAF denial pages
- **Automatic retry** — failed URLs are saved for easy re-run
- **Structured logging** — per-URL events logged to file, progress summaries every 5 seconds to console

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

```bash
git clone https://github.com/ventz/scrape-website.git
cd scrape-website
uv sync
```

## Usage

### Scrape a single website

```bash
uv run python app.py https://example.com/
```

This crawls every page on `example.com`, saving HTML pages, extracted text, and any linked documents.

### Scrape multiple websites

Create a file with one URL per line:

```
# urls.txt
# Lines starting with # are ignored, blank lines are skipped
https://example.com/
https://docs.example.com/
https://blog.example.com/
```

Then run:

```bash
uv run python app.py --file urls.txt
```

All domains run concurrently. Each domain gets its own output directory under `data/`.

You can also combine a URL argument with a file:

```bash
uv run python app.py https://example.com/ --file more-urls.txt
```

### Retry failed URLs

Failed URLs are automatically saved to `data/<domain>/logs/failed_urls.txt` after each run. Retry them with:

```bash
uv run python app.py --retry data/example.com/logs/failed_urls.txt
```

### Resume after crash

The scraper automatically checkpoints its queue and stats to SQLite every 30 seconds. If interrupted, just re-run the same command — it will resume from where it left off.

To force a clean start (ignoring any saved checkpoint):

```bash
uv run python app.py https://example.com/ --fresh
```

### Tuning options

```bash
# Throttle to 20 concurrent requests with a 0.5s delay (be polite)
uv run python app.py https://example.com/ --concurrency 20 --delay 0.5

# Increase timeout for slow servers
uv run python app.py https://example.com/ --timeout 60

# All options together
uv run python app.py https://example.com/ --concurrency 50 --timeout 60 --delay 0.25
```

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrency` | `100` | Max concurrent requests |
| `--timeout` | `30` | Request timeout in seconds |
| `--delay` | `0.1` | Delay between requests in seconds |
| `--file`, `-f` | — | File with URLs to scrape (one per line) |
| `--retry`, `-r` | — | File with failed URLs to retry |
| `--fresh` | — | Ignore saved checkpoint and start fresh |

## Output structure

```
data/
  example.com/
    pages/              # Raw HTML files
    text/               # Clean extracted text (.txt) — LLM-ready
    files/              # Downloaded documents (PDF, DOCX, etc.)
    logs/
      scrape.log        # Full debug log
      state.db          # SQLite DB (visited URLs, queue, stats)
      access_denied.txt # URLs that returned 401/403 (if any)
      failed_urls.txt   # URLs that failed after retries (if any)
  docs.example.com/
    pages/
    text/
    files/
    logs/
```

Each domain is stored separately, so scraping multiple sites keeps everything organized.

The `text/` directory contains clean, extracted main content — ideal for feeding into LLMs, RAG pipelines, or text analysis. Navigation, headers, footers, and boilerplate are stripped by `trafilatura`.

## Example

```
% python app.py 'https://privsec.harvard.edu'
Output directory: data/privsec.harvard.edu
Starting domain: privsec.harvard.edu
Max concurrent requests: 100
Starting scraper at 2026-03-12 14:01:58

================================================================================
SCRAPING COMPLETED
================================================================================
Duration: 4.00 seconds
URLs visited: 104
Pages downloaded: 98
Text extracted: 91
Files downloaded: 3
Access denied: 3
Total data: 4.63 MB
Errors: 0
Output location: data/privsec.harvard.edu
Denied URLs logged to: data/privsec.harvard.edu/logs/access_denied.txt
================================================================================

% ls data/privsec.harvard.edu/
files/  logs/  pages/  text/
```

## License

MIT
