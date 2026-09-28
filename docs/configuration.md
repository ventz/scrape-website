# Configuration

The CLI flags are documented in [Usage](usage.md#cli-reference). This page
covers environment variables, built-in limits, and install extras.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `SCRAPE_USER_AGENT` | Chrome 154 on macOS | User-Agent for every request. Keep its Chrome major version matched to your installed Chrome, or bridged cookies are rejected |
| `SCRAPE_CF_COOKIES` | — | Path to an exported cookies file for the [cookie bridge](protected-sites.md#the-cookie-bridge-private-access-token-walls). `IB_CF_COOKIES` is accepted as an alias |
| `SCRAPE_REAL_BROWSER` | `Google Chrome` | App the `--human` bridge opens to solve a PAT wall. `IB_REAL_BROWSER` is accepted as an alias |
| `SCRAPE_HUMAN_SOLVE_TIMEOUT` | `300` | Seconds to wait for a clearance cookie after opening the real browser |

## Built-in limits

Defined in `CONFIG` in `scrape_website/config.py`.

| Setting | Value | Purpose |
|---|---|---|
| `max_page_size` | 50 MB | Cap on decompressed HTML (compression-bomb guard) |
| `max_file_size` | 100 MB | Cap on downloaded documents, enforced before buffering |
| `checkpoint_interval` | 30 s | Queue/stats checkpoint to SQLite |
| `progress_interval` | 5 s | Console progress line |
| `render_timeout` | 30 s | Initial headless navigation timeout |
| `render_settle_ms` | 3000 ms | Hydration wait after DOM load |
| `max_render_concurrency` | 4 | Simultaneous headless renders |

Retries apply to 429, 500, 502, 503, and 504, with exponential backoff, jitter,
and `Retry-After` support.

## Install extras

The core install covers static fetching, extraction, and robots politeness.
Heavier capabilities are optional extras, and each one degrades gracefully when
it's missing. `uv sync` in this repo installs everything.

| Extra | Adds | Packages |
|---|---|---|
| `render` | Headless Chromium for SPA shells | `playwright` (then `playwright install chromium`) |
| `waf` | Real-browser TLS fingerprint fallback | `curl-cffi`, `brotli`, `zstandard` |
| `docs` | PDF / Office → Markdown | `pymupdf4llm`, `markitdown` |
| `human` | Read cookies from your live Chrome | `browser-cookie3` |
| `all` | Everything above | |

Optional and not in any extra: `docling`, a local-model fallback for scanned
PDFs (pulls in PyTorch). See [Architecture](architecture.md#does-it-use-an-llm).
