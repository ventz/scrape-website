"""Shared configuration: defaults, marker tuples, and downloadable-type tables.

Everything here is import-cheap (stdlib only). ``CONFIG`` is a module-level
mutable dict on purpose — the CLI overrides values from argparse before
constructing scrapers, exactly as it always has.
"""

import os

# Configuration defaults
CONFIG = {
    'max_concurrent': 100,  # Number of concurrent downloads
    'timeout': 30,  # Request timeout in seconds
    'max_retries': 3,  # Max retries for failed requests
    # IMPORTANT: a Cloudflare ``cf_clearance`` cookie is bound to domain + IP + the
    # EXACT User-Agent the real browser had when it solved the challenge. The cf-session
    # bridge (waf.py) reuses the cookie your genuine Chrome earned, so this UA must match
    # your real Chrome's major version or the replayed cookie is rejected. Bump it in
    # lockstep with your installed Chrome. Override at runtime with SCRAPE_USER_AGENT.
    'user_agent': os.environ.get(
        'SCRAPE_USER_AGENT',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    ),
    'delay_between_requests': 0.1,  # Politeness delay in seconds
    'max_file_size': 100 * 1024 * 1024,  # 100MB max file size
    # Max DECOMPRESSED HTML page size. aiohttp transparently inflates
    # gzip/br/zstd, so without a cap a small compression-bomb response could
    # balloon to GBs in memory (x concurrency). Real pages are nowhere close.
    'max_page_size': 50 * 1024 * 1024,
    'checkpoint_interval': 30,  # Seconds between queue checkpoints
    'progress_interval': 5,  # Seconds between progress reports
    'render_timeout': 30,  # Max seconds for the initial headless navigation
    'render_settle_ms': 3000,  # Extra wait after DOM load for JS to hydrate
    'max_render_concurrency': 4,  # Max simultaneous headless-Chromium renders
}

# HTTP status codes worth retrying (transient): rate-limit + server errors.
# 403 is handled separately via the curl_cffi impersonation fallback.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# Markers that indicate an HTML payload is a client-rendered SPA shell whose
# real content/links only appear after JavaScript runs. Matched case-insensitively
# against the raw HTML. Used purely as an escalation signal for headless rendering.
_SPA_SHELL_MARKERS: tuple[str, ...] = (
    '__next_f', '__next_data__', '__initial_state__', '__nuxt__',
    'data-reactroot', 'ng-version', 'id="__next"', 'id="root"', 'id="app"',
    'window.__apollo_state__',
)

# File extensions to download
DOWNLOADABLE_EXTENSIONS = {
    '.pdf', '.doc', '.docx', '.ppt', '.pptx',
    '.xls', '.xlsx', '.txt', '.csv', '.zip',
    '.rtf', '.odt', '.ods', '.odp'
}

# MIME types to download
DOWNLOADABLE_MIMES = {
    'application/pdf',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.ms-powerpoint',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'text/plain',
    'text/csv',
    'application/zip',
    'application/rtf',
    'application/vnd.oasis.opendocument.text',
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.presentation',
}

# Markers that indicate an HTML payload is a Cloudflare/CAPTCHA interstitial rather
# than real content (a "Just a moment" 200 is a block, not a page). Matched
# case-insensitively. Shared by the interactive browser path and the cf-session bridge.
_CHALLENGE_MARKERS: tuple[str, ...] = (
    'just a moment', 'checking your browser', 'cf-browser-verification',
    'challenge-platform', 'cf_chl_', 'turnstile', 'hcaptcha', 'g-recaptcha',
    'attention required', 'verify you are human',
    'enable javascript and cookies to continue', 'ddos protection by',
)

# Regex patterns for URLs commonly worth skipping on blog/CMS sites.
# These are matched against the full URL (re.search). Override via
# --exclude-pattern (repeatable) or programmatic API.
_DEFAULT_EXCLUDE_PATTERNS: list[str] = [
    r"/tag/",
    r"/author/",
    r"/feed/?$",
    r"/print/",
    r"\?print=",
    r"/comments/",
    r"/page/\d+",
    r"/cdn-cgi/",
]

# Query-string params that are tracking only — safe to drop to prevent
# `/page?utm_source=email` and `/page?utm_source=twitter` from being
# stored as two different pages. Add more as you encounter them.
_DEFAULT_TRACKING_PARAMS: frozenset[str] = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_eid", "mc_cid", "ref",
    "_ga", "_gl", "igshid", "msclkid", "dclid",
})
