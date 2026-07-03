"""Best-effort sitemap discovery (stdlib only)."""

import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request

from .config import CONFIG


def _fetch_sitemap_urls(host: str, scheme: str = "https",
                        timeout: int = 10, max_urls: int = 5000) -> list[str]:
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

    def _get(url: str) -> bytes | None:
        try:
            req = Request(url, headers={"User-Agent": CONFIG["user_agent"]})
            with urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception:
            return None

    def _parse_locs(xml_bytes: bytes, tag: str = "url") -> list[str]:
        """Extract <loc> text from <url> or <sitemap> elements."""
        urls: list[str] = []
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

        # Check for sitemap index (contains <sitemap> elements)
        sub_sitemaps = _parse_locs(data, tag="sitemap")
        if sub_sitemaps:
            for sub_url in sub_sitemaps:
                sub_data = _get(sub_url)
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
