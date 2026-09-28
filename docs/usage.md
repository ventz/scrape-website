# Usage

Everything the CLI can do, with the full flag reference at the end.

## Contents

- [Input modes](#input-modes)
- [Retry failed URLs](#retry-failed-urls)
- [Resume after a crash](#resume-after-a-crash)
- [Rate limiting and robots.txt](#rate-limiting-and-robotstxt)
- [Crawl-quality knobs](#crawl-quality-knobs)
- [JavaScript-rendered sites](#javascript-rendered-sites)
- [CLI reference](#cli-reference)

## Input modes

```bash
# One domain
uv run python app.py https://example.com/

# Many domains, one URL per line (# comments and blank lines are skipped)
uv run python app.py --file urls.txt

# Both
uv run python app.py https://example.com/ --file more-urls.txt
```

All domains run concurrently, each in its own `data/<domain>/` directory.
Crawls stay on the starting domain: `www.example.com`, `example.com`, and their
http/https variants count as one site and collapse to a single canonical URL.

## Retry failed URLs

URLs that still fail after all retries (timeouts, persistent 429/5xx) go to
`data/<domain>/logs/failed_urls.txt` and are never archived as content.

```bash
uv run python app.py --retry data/example.com/logs/failed_urls.txt
```

`--retry` clears those URLs from the visited state so they're really
re-fetched, without re-crawling anything else.

## Resume after a crash

Queue, stats, and report lists checkpoint to `logs/state.db` every 30 seconds.
Re-run the same command to resume. To start over:

```bash
uv run python app.py https://example.com/ --fresh
```

A `--fresh` re-crawl overwrites the previous output with the same filenames. A
resumed run adds `_1`, `_2` suffixes instead of overwriting.

## Rate limiting and robots.txt

`--delay` paces the **whole crawl**: one request per `--delay` seconds across
all concurrent tasks, whatever `--concurrency` is set to. The default `0.1`
caps a crawl at about 10 requests/second.

```bash
uv run python app.py https://example.com/ --delay 0.5   # ~2 req/s
uv run python app.py https://example.com/ --delay 0     # no pacing; concurrency only
```

> **Changed in 0.7.0:** before 0.7.0 each task slept on its own, so at the
> default concurrency of 100 the delay throttled almost nothing. Default crawls
> are now politer and slower.

`robots.txt` is honored by default (via `protego`). A `Crawl-Delay` it declares
takes precedence over `--delay`. Opt out with `--ignore-robots`, and use it
responsibly.

## Crawl-quality knobs

Three features are on by default:

- **URL excludes** skip noise: `/tag/`, `/author/`, `/feed/`, `/print/`,
  `?print=`, `/comments/`, `/page/\d+`, `/cdn-cgi/`. Add more with
  `-e REGEX` (repeatable), or use only your own with `--no-default-excludes`.
- **Tracking-param stripping** removes `utm_*`, `fbclid`, `gclid`, and similar,
  so one page isn't crawled twice. Disable with `--no-strip-tracking-params`.
- **Sitemap seeding** reads `sitemap.xml` and sitemap indexes to find unlinked
  pages. Disable with `--no-use-sitemap`.

```bash
uv run python app.py https://blog.example.com/ -e '/category/'
uv run python app.py https://blog.example.com/ --no-default-excludes -e '/archive/'
```

## JavaScript-rendered sites

React, Vue, Angular, and similar sites often ship a near-empty HTML shell. With
the default `--render auto`, the scraper detects those shells and re-fetches
just those pages in headless Chromium, then extracts from the hydrated DOM.

```bash
uv run python app.py https://example.com/ --render always  # every page (slow)
uv run python app.py https://example.com/ --render never   # no browser needed
```

Rendering needs a one-time `uv run playwright install chromium`.

For Cloudflare, CAPTCHA, and login walls, see [Protected Sites](protected-sites.md).

## CLI reference

| Flag | Default | Description |
|------|---------|-------------|
| `URL` (positional) | — | Domain to crawl |
| `--file`, `-f` | — | File with URLs to scrape (one per line) |
| `--retry`, `-r` | — | File with failed URLs to force-requeue |
| `--concurrency`, `-c` | `100` | Max concurrent requests |
| `--timeout`, `-t` | `30` | Request timeout in seconds |
| `--delay`, `-d` | `0.1` | Global pacing: one request per N seconds crawl-wide (`0` disables) |
| `--fresh`, `-F` | — | Ignore the checkpoint and overwrite previous output |
| `--fullname`, `-n` | — | Prefix output filenames with the host (`example.com_about.md`) |
| `--verbose`, `-v` | — | Per-URL activity on the console (full detail is always in `logs/scrape.log`) |
| `--render` | `auto` | JS rendering: `auto` (SPA shells only), `always`, `never` |
| `--human` | — | Visible browser; pause to solve challenges/logins (forces `--concurrency 1`) |
| `--allow-insecure-tls` | — | Skip TLS verification (trusted hosts with broken certs) |
| `--ignore-robots` | — | Don't fetch or honor `robots.txt` |
| `--no-extract-docs` | — | Don't convert downloaded PDFs/Office docs to Markdown |
| `--exclude-pattern`, `-e` | see above | Regex to exclude URLs (repeatable; appends to defaults) |
| `--no-default-excludes` | — | Drop the built-in exclude patterns |
| `--no-strip-tracking-params` | — | Keep tracking query params |
| `--no-use-sitemap` | — | Skip `sitemap.xml` discovery |
| `--version` | — | Print the version and exit |

Environment variables are listed in [Configuration](configuration.md).
