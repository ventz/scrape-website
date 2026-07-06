"""scrape-website — async website scraper, importable as a library.

Public surface:
    FetchEngine / FetchOutcome — the shared tiered fetcher (static aiohttp ->
        curl_cffi WAF fallback -> headless Chromium render escalation), with
        robots.txt politeness and retry/backoff. Import this from other
        projects (e.g. the scrape-website-mcp server).
    WebsiteScraper — the full crawl-to-disk pipeline behind the CLI.

The repo-root ``app.py`` remains the CLI entrypoint and re-exports every
legacy name, so ``uv run python app.py <url>`` and ``from app import ...``
both keep working.
"""

# Bump on every user-visible improvement/change (see CHANGELOG.md). Surfaced via
# `--version` and logged at the start of each crawl so a run's output is traceable
# to the code that produced it.
__version__ = "0.7.0"

from .config import (  # noqa: F401
    CONFIG,
    RETRYABLE_STATUS,
    DOWNLOADABLE_EXTENSIONS,
    DOWNLOADABLE_MIMES,
    _SPA_SHELL_MARKERS,
    _CHALLENGE_MARKERS,
    _DEFAULT_EXCLUDE_PATTERNS,
    _DEFAULT_TRACKING_PARAMS,
)
from .urls import (  # noqa: F401
    _canonicalize_host,
    _is_safe_fetch_target,
    _normalize_url,
    _same_host,
    _strip_tracking_params,
    _url_excluded,
)
from .sitemap import _fetch_sitemap_urls  # noqa: F401
from .extract import (  # noqa: F401
    _extract_links_lxml,
    _extract_text_trafilatura,
    _parse_and_extract,
    _looks_like_spa_shell,
    _extract_document_to_markdown,
    _looks_challenged,
    classify_page,
    is_access_denied,
)
from .waf import _CFSession, CF_SESSION  # noqa: F401
from .fetch import FetchEngine, FetchOutcome, should_download_file  # noqa: F401
from .store import URLStore  # noqa: F401
