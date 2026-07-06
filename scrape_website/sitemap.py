"""Best-effort sitemap discovery (stdlib only)."""

import ssl
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from urllib.request import urlopen, Request

from .config import CONFIG
from .urls import _is_safe_fetch_target, _same_host

# Sitemaps larger than this are ignored (a sitemap file may legally hold
# 50k URLs / 50MB uncompressed, but this crawler caps seeds at max_urls
# anyway — a multi-MB response here is bomb-shaped, not useful).
_MAX_SITEMAP_BYTES = 10 * 1024 * 1024


def _fetch_sitemap_urls(host: str, scheme: str = "https",
                        timeout: int | None = None, max_urls: int = 5000,
                        allow_insecure_tls: bool = False) -> list[str]:
    """Best-effort sitemap discovery.

    Tries ``{scheme}://{host}/sitemap.xml`` then
    ``{scheme}://{host}/sitemap_index.xml``.  Recurses into
    ``<sitemap><loc>`` entries (sitemap-index format) up to one level.
    Returns a deduped list of ``<loc>`` URLs, capped at *max_urls*.
    Any fetch/parse failure returns ``[]``.

    Uses only stdlib (``urllib`` + ``xml.etree``) — no new deps.
    """
    # Common XML namespace used in sitemaps
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    if timeout is None:
        timeout = CONFIG["timeout"]
    ssl_ctx = None
    if allow_insecure_tls:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    def _get(url: str) -> bytes | None:
        try:
            req = Request(url, headers={"User-Agent": CONFIG["user_agent"]})
            with urlopen(req, timeout=timeout, context=ssl_ctx) as resp:
                data = resp.read(_MAX_SITEMAP_BYTES + 1)
                return None if len(data) > _MAX_SITEMAP_BYTES else data
        except Exception:
            return None

    def _child_allowed(url: str) -> bool:
        """SSRF gate for sitemap-index children: the child <loc> comes from
        fetched content, so it must be http(s), on the crawl host (www-alias
        ok), and not an internal/link-local IP — a crafted sitemap must not
        be able to point us at file:// or cloud-metadata endpoints."""
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        return (_is_safe_fetch_target(url)
                and _same_host(parsed.netloc, host))

    def _parse_locs(xml_bytes: bytes, tag: str = "url") -> list[str]:
        """Extract <loc> text from <url> or <sitemap> elements."""
        urls: list[str] = []
        # Refuse DTDs outright: real sitemaps never declare one, and
        # xml.etree is not hardened against entity-expansion bombs.
        low = xml_bytes[:64 * 1024].lower()
        if b'<!doctype' in low or b'<!entity' in low:
            return urls
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            return urls
        # Try with namespace first, then without
        for elem in root.findall(f"sm:{tag}/sm:loc", ns):
            if elem.text:
                urls.append(elem.text.strip())
        if not urls:
            for elem in root.findall(f"{tag}/loc"):
                if elem.text:
                    urls.append(elem.text.strip())
            # Also try namespace-stripped approach
            if not urls:
                for elem in root.iter():
                    local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                    if local == "loc" and elem.text:
                        urls.append(elem.text.strip())
        return urls

    seen: set[str] = set()
    result: list[str] = []

    for path in ("/sitemap.xml", "/sitemap_index.xml"):
        sitemap_url = f"{scheme}://{host}{path}"
        data = _get(sitemap_url)
        if not data:
            continue

        # Check for sitemap index (contains <sitemap> elements). Children are
        # fetched CONCURRENTLY — a large index (50 children x 10s timeout)
        # fetched serially could stall the crawl start for minutes. ex.map
        # preserves child order, so results stay deterministic.
        sub_sitemaps = [u for u in _parse_locs(data, tag="sitemap")
                        if _child_allowed(u)]
        if sub_sitemaps:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(8, len(sub_sitemaps))) as ex:
                for sub_data in ex.map(_get, sub_sitemaps):
                    if sub_data:
                        for loc in _parse_locs(sub_data, tag="url"):
                            if loc not in seen:
                                seen.add(loc)
                                result.append(loc)
                                if len(result) >= max_urls:
                                    return result

        # Also parse direct <url><loc> entries
        for loc in _parse_locs(data, tag="url"):
            if loc not in seen:
                seen.add(loc)
                result.append(loc)
                if len(result) >= max_urls:
                    return result

    return result
