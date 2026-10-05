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
    # Sub-sitemap pages, deduped across sub-sitemaps. The index's own
    # <sitemap><loc> entries are NOT seeded as pages.
    assert urls == ["https://x.com/one", "https://x.com/two"]


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


def _urlopen_403(req, timeout=None, context=None):
    from urllib.error import HTTPError
    raise HTTPError(req.full_url, 403, "Forbidden", {}, None)


def test_403_uses_fallback():
    calls = []

    def fallback(url):
        calls.append(url)
        return SITEMAP_PLAIN if url == "https://x.com/sitemap.xml" else None
    with patch.object(sitemap, "urlopen", _urlopen_403):
        urls = sitemap._fetch_sitemap_urls("x.com", fallback=fallback)
    assert urls == ["https://x.com/a", "https://x.com/b"]
    assert calls == ["https://x.com/sitemap.xml", "https://x.com/sitemap_index.xml"]


def test_403_without_fallback_returns_empty():
    with patch.object(sitemap, "urlopen", _urlopen_403):
        assert sitemap._fetch_sitemap_urls("x.com") == []


def test_200_does_not_use_fallback():
    def fallback(url):
        raise AssertionError("fallback used on a 200")
    with patch.object(sitemap, "urlopen",
                      _fake_urlopen({"https://x.com/sitemap.xml": SITEMAP_PLAIN})):
        urls = sitemap._fetch_sitemap_urls("x.com", fallback=fallback)
    assert urls == ["https://x.com/a", "https://x.com/b"]


def test_fallback_respects_size_cap():
    with patch.object(sitemap, "urlopen", _urlopen_403), \
         patch.object(sitemap, "_MAX_SITEMAP_BYTES", 10):
        assert sitemap._fetch_sitemap_urls(
            "x.com", fallback=lambda u: SITEMAP_PLAIN) == []


def test_plain_sitemap_urls_not_fetched_as_child_sitemaps():
    calls = []
    opener = _fake_urlopen({"https://x.com/sitemap.xml": SITEMAP_PLAIN})

    def spy(req, timeout=None, context=None):
        calls.append(req.full_url)
        return opener(req, timeout, context)
    with patch.object(sitemap, "urlopen", spy):
        sitemap._fetch_sitemap_urls("x.com")
    assert calls == ["https://x.com/sitemap.xml", "https://x.com/sitemap_index.xml"]


def test_odd_namespace_index_still_recursed():
    index = (b'<sitemapindex xmlns="urn:odd"><sitemap><loc>https://x.com/sub1.xml'
             b'</loc></sitemap></sitemapindex>')
    with patch.object(sitemap, "urlopen", _fake_urlopen({
            "https://x.com/sitemap.xml": index,
            "https://x.com/sub1.xml": SUB1})):
        assert sitemap._fetch_sitemap_urls("x.com") == ["https://x.com/one"]
