# scrape-website

Async website scraper that crawls an entire domain and downloads all pages (HTML), extracts clean Markdown (for LLMs/RAG knowledge bases), and saves documents (PDF, DOCX, XLSX, etc.). Stays within the target domain — it will never follow links to external sites.

## Features

- **Fast async crawling** — up to 100 concurrent requests (configurable)
- **JavaScript rendering (auto-escalation)** — static fetch first; when a page is detected as an un-hydrated client-rendered SPA shell (tiny text, no links), it is automatically re-fetched in headless Chromium (Playwright) and re-extracted from the hydrated DOM. Static-first by design, so only SPA pages pay the browser cost (`--render auto|never|always`)
- **Robust fetching** — exponential backoff with jitter, `Retry-After`-aware retries on 429/5xx, and a `curl_cffi` real-browser TLS/fingerprint fallback that retries `403`/WAF-challenge responses
- **robots.txt politeness** — honors `robots.txt` (via `protego`) and `Crawl-Delay` (adaptive per-host rate limiting via `aiolimiter`) by default; opt out with `--ignore-robots`
- **Document text extraction** — downloaded PDFs/Office docs are converted to RAG-ready Markdown (PyMuPDF4LLM for PDFs, MarkItDown for DOC(X)/PPT(X)/XLS(X); optional Docling fallback for complex/scanned PDFs)
- **Async DNS** — non-blocking DNS resolution with caching (via `aiodns`)
- **Async file I/O** — non-blocking writes with `aiofiles`
- **Clean Markdown extraction** — extracts main content as Markdown using `trafilatura` (strips nav, headers, footers, boilerplate), with YAML front-matter metadata (title, url, hostname, sitename) at the top of each file
- **Per-page deduplication** — repeated boilerplate is dropped only *within* a page; content that legitimately repeats across pages (e.g. an FAQ answer on both the FAQ page and its own page) is kept in full, so every page is a self-contained knowledge-base document
- **Parallel HTML parsing** — `lxml` link extraction + text extraction offloaded to process pool (uses all CPU cores)
- **SQLite-backed dedup** — exact URL deduplication with minimal RAM usage (scales to millions of URLs)
- **Crash recovery** — auto-resumes from checkpoint on restart; use `--fresh` to start over
- **Multi-domain concurrency** — all domains run in parallel via `asyncio.TaskGroup`
- **Domain-scoped** — only follows links within the starting domain
- **Document downloads** — PDF, DOC(X), PPT(X), XLS(X), CSV, ZIP, RTF, ODT, ODS, ODP
- **Multiple input modes** — single URL, file with URL list, or retry from failed URLs
- **Access-denied detection** — identifies HTTP 401/403 and CDN/WAF denial pages
- **TLS escape hatch** — strict certificate verification by default; `--allow-insecure-tls` for trusted hosts with broken/expired certs
- **Convenient short flags** — `-c`/`-t`/`-d`/`-F`/`-e`/`-n` aliases for common options
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
# One-time: download the headless browser used for JS rendering
uv run playwright install chromium
```

> The Chromium download is only needed if you use JavaScript rendering (`--render auto` is the default). To run without a browser, pass `--render never`.
>
> **Optional:** for complex/scanned PDFs you can install [Docling](https://github.com/docling-project/docling) (`uv add docling`); it's lazy-loaded as a fallback only when the fast PDF path yields almost nothing.

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
| `--concurrency`, `-c` | `100` | Max concurrent requests |
| `--timeout`, `-t` | `30` | Request timeout in seconds |
| `--delay`, `-d` | `0.1` | Delay between requests in seconds |
| `--file`, `-f` | — | File with URLs to scrape (one per line) |
| `--retry`, `-r` | — | File with failed URLs to retry |
| `--fresh`, `-F` | — | Ignore saved checkpoint and start fresh |
| `--fullname`, `-n` | — | Prefix output filenames with the host (`example.com_about.md`) |
| `--render` | `auto` | JS rendering: `auto` (only SPA shells), `always` (every page), `never` (disable) |
| `--allow-insecure-tls` | — | Disable TLS certificate verification (trusted hosts with broken certs) |
| `--ignore-robots` | — | Do not fetch or honor `robots.txt` |
| `--no-extract-docs` | — | Do not convert downloaded PDFs/Office docs to Markdown |
| `--exclude-pattern`, `-e` | see below | Regex to exclude URLs (repeatable; appends to defaults) |
| `--no-default-excludes` | — | Clear built-in exclude patterns (only use `--exclude-pattern` values) |
| `--no-strip-tracking-params` | — | Keep tracking query params (`utm_*`, `fbclid`, etc.) |
| `--no-use-sitemap` | — | Skip sitemap.xml discovery for seed URLs |

### JavaScript-rendered (SPA) sites

Many modern sites (React/Next.js, Vue/Nuxt, Angular, etc.) ship a near-empty HTML shell and render content client-side. A static fetch of such a page yields almost no text and no followable links. By default (`--render auto`) the scraper detects these shells and transparently re-fetches them in headless Chromium, then extracts from the hydrated DOM:

```bash
# Default: auto-escalate only the pages that need it
uv run python app.py https://example.com/

# Force a browser render for every page (slower; for fully dynamic sites)
uv run python app.py https://example.com/ --render always

# Disable rendering entirely (no browser needed)
uv run python app.py https://example.com/ --render never
```

### Politeness & robots.txt

`robots.txt` is honored by default, and any `Crawl-Delay` it declares becomes an adaptive per-host rate limit. Disable with `--ignore-robots` (use responsibly):

```bash
uv run python app.py https://example.com/ --ignore-robots
```

### Hard / misconfigured sites

```bash
# Trusted host with a broken or expired TLS certificate
uv run python app.py https://example.com/ --allow-insecure-tls
```

`403`/WAF-challenge responses are automatically retried once with a real-browser TLS fingerprint (`curl_cffi`) before being recorded as failures.

### Crawl-quality knobs

Three features are **on by default** and improve crawl quality on most sites:

**URL exclude patterns** — skip URLs matching common noise patterns (tag pages, author archives, pagination, print views, etc.):

```bash
# Add a custom exclude pattern (appended to defaults)
uv run python app.py https://blog.example.com/ --exclude-pattern '/category/'

# Use only your own patterns (no defaults)
uv run python app.py https://blog.example.com/ --no-default-excludes --exclude-pattern '/archive/'
```

Default patterns: `/tag/`, `/author/`, `/feed/`, `/print/`, `?print=`, `/comments/`, `/page/\d+`, `/cdn-cgi/`.

**Tracking-param stripping** — removes `utm_source`, `fbclid`, `gclid`, and similar query params so the same page isn't scraped twice with different tracking links:

```bash
# Opt out (keep all query params as-is)
uv run python app.py https://example.com/ --no-strip-tracking-params
```

**Sitemap seeding** — fetches `sitemap.xml` (and sitemap index files) to discover pages that might not be linked from the homepage:

```bash
# Opt out
uv run python app.py https://example.com/ --no-use-sitemap
```

## Output structure

```
data/
  example.com/
    pages/              # Raw HTML files
    text/               # Clean extracted Markdown (.md) w/ metadata — LLM-ready
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

The `text/` directory contains clean, extracted main content as Markdown (`.md`) — ideal for feeding into LLMs, RAG pipelines, or text analysis. Navigation, headers, footers, and boilerplate are stripped by `trafilatura`. Each file opens with a YAML front-matter block (`title`, `url`, `hostname`, `sitename`) for provenance and better retrieval, followed by the page content with headings and links preserved. Downloaded documents (PDF, DOC(X), etc.) are also extracted to `.md` here, with their own front matter (`title`, `url`, `hostname`, `filetype`, `date`), so the whole corpus — pages and documents alike — is uniform Markdown.

Deduplication is **per-page only**: `trafilatura`'s repetition cache is reset before every page, so a passage is removed only if it repeats within that same page. Text that legitimately appears on multiple pages (a shared FAQ answer, a reused policy blurb) is retained in full on every page — there is no cross-page/cross-domain content loss, which would otherwise leave some pages with a truncated file or no file at all.

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
