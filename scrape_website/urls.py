"""URL normalization and filtering helpers (pure, picklable, import-cheap)."""

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
    """Normalize URL by removing fragments and trailing slashes.

    When *strip_tracking* is True, also removes well-known tracking
    query parameters (utm_*, fbclid, gclid, etc.).
    """
    parsed = urlparse(url)
    url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if parsed.query:
        url += f"?{parsed.query}"
    if url.endswith('/') and parsed.path != '/':
        url = url[:-1]
    if strip_tracking:
        url = _strip_tracking_params(url)
    return url
