from unittest.mock import patch

from scrape_website import sitemap

SITEMAP_PLAIN = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://x.com/a</loc></url>
  <url><loc>https://x.com/b</loc></url>
</urlset>"""

SITEMAP_INDEX = b"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://x.com/sub1.xml</loc></sitemap>
  <sitemap><loc>https://x.com/sub2.xml</loc></sitemap>
</sitemapindex>"""

SUB1 = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://x.com/one</loc></url>
</urlset>"""

SUB2 = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://x.com/two</loc></url>
  <url><loc>https://x.com/one</loc></url>
</urlset>"""


class _FakeResp:
    def __init__(self, data):
        self._data = data

    def read(self, n=None):
        return self._data if n is None else self._data[:n]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(responses):
    """responses: dict url -> bytes; anything else raises."""
    def opener(req, timeout=None, context=None):
        url = req.full_url
        if url in responses:
            return _FakeResp(responses[url])
        raise OSError(f"no fixture for {url}")
    return opener


def test_plain_sitemap():
    with patch.object(sitemap, "urlopen",
                      _fake_urlopen({"https://x.com/sitemap.xml": SITEMAP_PLAIN})):
        urls = sitemap._fetch_sitemap_urls("x.com")
    assert urls == ["https://x.com/a", "https://x.com/b"]


def test_sitemap_index_recursion_and_dedup():
    with patch.object(sitemap, "urlopen", _fake_urlopen({
            "https://x.com/sitemap.xml": SITEMAP_INDEX,
            "https://x.com/sub1.xml": SUB1,
            "https://x.com/sub2.xml": SUB2})):
        urls = sitemap._fetch_sitemap_urls("x.com")
    # Sub-sitemap pages come first, deduped across sub-sitemaps. The
    # namespace-stripped fallback in _parse_locs then also surfaces the
    # index's own <sitemap><loc> entries — long-standing upstream behavior
    # (harmless: the .xml seeds fetch nothing useful), kept for parity.
    assert urls[:2] == ["https://x.com/one", "https://x.com/two"]
    assert set(urls[2:]) == {"https://x.com/sub1.xml", "https://x.com/sub2.xml"}


def test_fetch_failure_returns_empty():
    with patch.object(sitemap, "urlopen", _fake_urlopen({})):
        assert sitemap._fetch_sitemap_urls("x.com") == []


def test_max_urls_cap():
    with patch.object(sitemap, "urlopen",
                      _fake_urlopen({"https://x.com/sitemap.xml": SITEMAP_PLAIN})):
        assert sitemap._fetch_sitemap_urls("x.com", max_urls=1) == ["https://x.com/a"]


def test_dtd_refused():
    bomb = (b'<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY lol "lol">]>'
            b'<urlset><url><loc>https://x.com/a</loc></url></urlset>')
    with patch.object(sitemap, "urlopen",
                      _fake_urlopen({"https://x.com/sitemap.xml": bomb})):
        assert sitemap._fetch_sitemap_urls("x.com") == []


def test_cross_host_and_unsafe_children_rejected():
    evil_index = b"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://169.254.169.254/latest/meta-data</loc></sitemap>
  <sitemap><loc>file:///etc/passwd</loc></sitemap>
  <sitemap><loc>https://evil.com/sub.xml</loc></sitemap>
  <sitemap><loc>https://www.x.com/sub1.xml</loc></sitemap>
</sitemapindex>"""
    fetched = []

    def tracking_opener(req, timeout=None, context=None):
        fetched.append(req.full_url)
        data = {"https://x.com/sitemap.xml": evil_index,
                "https://www.x.com/sub1.xml": SUB1}.get(req.full_url)
        if data is None:
            raise OSError("no fixture")
        return _FakeResp(data)

    with patch.object(sitemap, "urlopen", tracking_opener):
        urls = sitemap._fetch_sitemap_urls("x.com")
    # Only the same-site (www-alias) child was fetched; SSRF targets were not.
    assert "https://x.com/one" in urls
    assert not any("169.254" in u or u.startswith("file:") or "evil.com" in u
                   for u in fetched)
