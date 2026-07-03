"""Thin compatibility shim — the implementation lives in the ``scrape_website``
package (see scrape_website/{config,urls,sitemap,extract,waf,fetch,store,
crawler,cli}.py).

Kept so that (1) ``uv run python app.py <url>`` remains the CLI entrypoint,
byte-for-byte compatible with every prior release, and (2) legacy consumers
doing ``from app import _normalize_url`` (e.g. older scrape-website-mcp
checkouts) keep working.
"""

from scrape_website import __version__  # noqa: F401
from scrape_website.config import (  # noqa: F401
    CONFIG,
    RETRYABLE_STATUS,
    DOWNLOADABLE_EXTENSIONS,
    DOWNLOADABLE_MIMES,
    _SPA_SHELL_MARKERS,
    _CHALLENGE_MARKERS,
    _DEFAULT_EXCLUDE_PATTERNS,
    _DEFAULT_TRACKING_PARAMS,
)
from scrape_website.urls import (  # noqa: F401
    _normalize_url,
    _strip_tracking_params,
    _url_excluded,
)
from scrape_website.sitemap import _fetch_sitemap_urls  # noqa: F401
from scrape_website.extract import (  # noqa: F401
    _extract_links_lxml,
    _extract_text_trafilatura,
    _parse_and_extract,
    _looks_like_spa_shell,
    _extract_document_to_markdown,
    _looks_challenged,
)
from scrape_website.waf import _CFSession, CF_SESSION  # noqa: F401
from scrape_website.fetch import FetchEngine, FetchOutcome, should_download_file  # noqa: F401
from scrape_website.store import URLStore  # noqa: F401
from scrape_website.crawler import WebsiteScraper  # noqa: F401
from scrape_website.cli import collect_urls, main, parse_args  # noqa: F401

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
