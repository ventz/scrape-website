"""URL normalization and filtering helpers (pure, picklable, import-cheap)."""

import ipaddress
import re
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

from .config import _DEFAULT_TRACKING_PARAMS


def _strip_tracking_params(url: str,
                           tracking_params: frozenset[str] = _DEFAULT_TRACKING_PARAMS) -> str:
    """Return *url* with tracking-only query-string keys removed.

    Preserves order of non-tracking params.  Returns the URL unchanged
    when it has no query string or all params are tracking-only (in which
    case the ``?`` is also dropped).
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    cleaned = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
               if k not in tracking_params]
    new_query = urlencode(cleaned)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, ''))


def _url_excluded(url: str, patterns: list[re.Pattern]) -> bool:
    """True iff any compiled regex pattern matches *url*.

    Empty *patterns* list means nothing is excluded (returns False).
    """
    for pat in patterns:
        if pat.search(url):
            return True
    return False


def _normalize_url(url: str, strip_tracking: bool = False) -> str:
    """Normalize URL: drop fragments, strip the trailing path slash (except
    root), and lowercase the scheme + host (they are case-insensitive; the
    path is not and is preserved as-is).

    When *strip_tracking* is True, also removes well-known tracking
    query parameters (utm_*, fbclid, gclid, etc.).
    """
    parsed = urlparse(url)
    url = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path}"
    # Strip the trailing slash from the PATH only, before the query is
    # appended — testing the full string would eat a query ending in '/'
    # (e.g. ?next=/).
    if url.endswith('/') and parsed.path != '/':
        url = url[:-1]
    if parsed.query:
        url += f"?{parsed.query}"
    if strip_tracking:
        url = _strip_tracking_params(url)
    return url


def _is_safe_fetch_target(url: str) -> bool:
    """SSRF guard for URLs sourced from CRAWLED CONTENT (cross-host document
    links, sitemap-index children): only http(s), and IP-literal hosts must not
    be loopback/private/link-local/reserved — a crawled page must not be able
    to point the scraper at ``file:///…`` or ``http://169.254.169.254/…``.

    Hostname targets are allowed (resolution-time rebinding is out of scope
    for this operator-run tool).
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ('http', 'https'):
        return False
    host = parsed.hostname or ''
    if not host:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return True  # named host
    return not (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified)


def _same_host(host_a: str, host_b: str) -> bool:
    """True when two netlocs refer to the same site, treating an optional
    leading ``www.`` as equivalent. Ports and other subdomains must match."""
    def strip_www(host: str) -> str:
        host = host.lower()
        return host[4:] if host.startswith('www.') else host
    return strip_www(host_a) == strip_www(host_b)


def _canonicalize_host(url: str, scheme: str, netloc: str,
                       strip_tracking: bool = False) -> str:
    """Rebuild *url* onto the crawl's canonical scheme + host, so www/non-www
    and http/https aliases of the same page collapse to ONE visited-store URL
    (instead of being crawled — and saved — twice)."""
    parts = urlsplit(url)
    rebuilt = urlunsplit((scheme, netloc, parts.path, parts.query, ''))
    return _normalize_url(rebuilt, strip_tracking=strip_tracking)
