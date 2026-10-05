import asyncio

import pytest

from scrape_website.fetch import FetchEngine, FetchOutcome, should_download_file


class FakeResponse:
    def __init__(self, status=200, body=b"<html><p>hello</p></html>",
                 headers=None, charset="utf-8"):
        self.status = status
        self._body = body
        self.headers = headers or {"Content-Type": "text/html"}
        self.charset = charset

    async def read(self):
        return self._body

    async def text(self):
        return self._body.decode(self.charset or "utf-8")

    @property
    def content(self):
        body = self._body

        class _Streamer:
            async def iter_chunked(self, size):
                for i in range(0, len(body), size):
                    yield body[i:i + size]
        return _Streamer()


class FakeSession:
    """Scripted aiohttp session: pops one FakeResponse per request."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.closed = False

    def request(self, method, url, allow_redirects=True):
        self.requests.append((method, url))
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise_exc = resp

            class _Raiser:
                async def __aenter__(self):
                    raise raise_exc

                async def __aexit__(self, *a):
                    return False
            return _Raiser()

        class _CM:
            async def __aenter__(self_inner):
                return resp

            async def __aexit__(self_inner, *a):
                return False
        return _CM()

    def get(self, url, allow_redirects=True):
        return self.request("GET", url, allow_redirects=allow_redirects)

    async def close(self):
        self.closed = True


def make_engine(responses, **kwargs):
    engine = FetchEngine(render_mode=kwargs.pop("render_mode", "never"), **kwargs)
    engine.session = FakeSession(responses)
    engine._backoff = lambda attempt: 0.0  # no sleeping in tests
    return engine


class TestFetch:
    async def test_plain_html(self):
        engine = make_engine([FakeResponse(body=b"<html><p>hi</p></html>")])
        outcome = await engine.fetch("https://x.com/")
        assert isinstance(outcome, FetchOutcome)
        assert outcome.kind == "html"
        assert outcome.status == 200
        assert outcome.via == "aiohttp"
        assert "hi" in outcome.content

    async def test_retry_after_honored_then_success(self):
        engine = make_engine([
            FakeResponse(status=503, headers={"Content-Type": "text/html",
                                              "Retry-After": "0"}),
            FakeResponse(body=b"<html>ok</html>"),
        ])
        outcome = await engine.fetch("https://x.com/")
        assert outcome.status == 200
        assert len(engine.session.requests) == 2

    async def test_backoff_retry_on_500_without_header(self):
        engine = make_engine([
            FakeResponse(status=500),
            FakeResponse(body=b"<html>recovered</html>"),
        ])
        outcome = await engine.fetch("https://x.com/")
        assert "recovered" in outcome.content

    async def test_exhausted_retries_raise(self):
        engine = make_engine([
            FakeResponse(status=503),
            FakeResponse(status=503),
            FakeResponse(status=503),
        ])
        # Retries exhausted -> the fetch FAILS (the URL goes to failed_urls.txt
        # for --retry); the 503 error page must never be archived as content.
        with pytest.raises(Exception, match="HTTP 503"):
            await engine.fetch("https://x.com/")

    async def test_transport_error_retries_then_raises(self):
        engine = make_engine([
            asyncio.TimeoutError(),
            asyncio.TimeoutError(),
            asyncio.TimeoutError(),
        ])
        with pytest.raises(Exception, match="Failed after"):
            await engine.fetch("https://x.com/")

    async def test_403_escalates_to_curl_cffi(self):
        engine = make_engine([FakeResponse(status=403)])

        async def fake_curl(url):
            return ("<html>via curl</html>", "text/html", "html", 200)
        engine._fetch_via_curl_cffi = fake_curl
        outcome = await engine.fetch("https://x.com/")
        assert outcome.via == "curl_cffi"
        assert outcome.status == 200

    async def test_403_with_failed_fallback_returns_403(self):
        engine = make_engine([FakeResponse(status=403, body=b"<html>no</html>")])

        async def fake_curl(url):
            return None
        engine._fetch_via_curl_cffi = fake_curl
        outcome = await engine.fetch("https://x.com/")
        assert outcome.status == 403

    async def test_challenge_interstitial_escalates(self):
        engine = make_engine([
            FakeResponse(body=b"<title>Just a moment...</title>"),
        ])

        async def fake_curl(url):
            return ("<html>real content</html>", "text/html", "html", 200)
        engine._fetch_via_curl_cffi = fake_curl
        outcome = await engine.fetch("https://x.com/")
        assert outcome.via == "curl_cffi"
        assert "real content" in outcome.content

    async def test_file_download(self):
        engine = make_engine([FakeResponse(
            body=b"%PDF-1.4 fake", headers={"Content-Type": "application/pdf"})])
        outcome = await engine.fetch("https://x.com/report.pdf")
        assert outcome.kind == "file"
        assert outcome.content == b"%PDF-1.4 fake"

    async def test_file_skipped_via_content_length(self):
        engine = make_engine([FakeResponse(
            body=b"x" * 10,
            headers={"Content-Type": "application/pdf", "Content-Length": "999"})],
            max_file_size=100)
        outcome = await engine.fetch("https://x.com/big.pdf")
        assert outcome.detail == "file too large"
        assert outcome.content == b""

    async def test_html_page_size_cap_fails_fetch(self):
        # A decompression-bomb-sized HTML body must fail the fetch, not be
        # buffered and archived.
        engine = make_engine([FakeResponse(body=b"<p>" + b"x" * 500)],
                             max_page_size=100)
        with pytest.raises(Exception, match="Failed after"):
            await engine.fetch("https://x.com/")

    async def test_file_skipped_via_streaming_cap(self):
        # No Content-Length: the capped chunked read must bail mid-stream.
        engine = make_engine([FakeResponse(
            body=b"x" * 300, headers={"Content-Type": "application/pdf"})],
            max_file_size=100)
        outcome = await engine.fetch("https://x.com/big.pdf")
        assert outcome.detail == "file too large"
        assert outcome.content == b""

    async def test_charset_windows1252_forced_utf8(self):
        engine = make_engine([FakeResponse(
            body="em—dash".encode("utf-8"),
            headers={"Content-Type": "text/html; charset=windows-1252"},
            charset="windows-1252")])
        outcome = await engine.fetch("https://x.com/")
        assert "em—dash" in outcome.content


class TestFetchPage:
    @staticmethod
    async def _extract_stub(results):
        async def run_extract(html, url):
            return results.pop(0)
        return run_extract

    async def test_html_page_extracts(self):
        engine = make_engine([FakeResponse(body=b"<html><p>content</p></html>")])

        async def run_extract(html, url):
            return {"https://x.com/next"}, "extracted text " * 20
        outcome, links, text = await engine.fetch_page("https://x.com/",
                                                       run_extract=run_extract)
        assert links == {"https://x.com/next"}
        assert "extracted" in text
        assert outcome.rendered is False

    async def test_denied_short_circuits(self):
        engine = make_engine([FakeResponse(status=403, body=b"<html>no</html>")])

        async def fake_curl(url):
            return None
        engine._fetch_via_curl_cffi = fake_curl
        outcome, links, text = await engine.fetch_page(
            "https://x.com/", run_extract=None)
        assert outcome.denied is True
        assert links == set() and text is None

    async def test_file_short_circuits(self):
        engine = make_engine([FakeResponse(
            body=b"bytes", headers={"Content-Type": "application/pdf"})])
        outcome, links, text = await engine.fetch_page(
            "https://x.com/a.pdf", run_extract=None)
        assert outcome.kind == "file"
        assert links == set() and text is None

    async def test_spa_shell_renders_once_and_reextracts(self):
        shell = b'<html><body><div id="root"></div></body></html>'
        engine = make_engine([FakeResponse(body=shell)], render_mode="auto")
        render_calls = []

        async def fake_render(url):
            render_calls.append(url)
            return "<html><p>hydrated content</p><a href='/in'>in</a></html>"
        engine.render = fake_render

        extractions = []

        async def run_extract(html, url):
            extractions.append(html)
            if "hydrated" in html:
                return {"https://x.com/in"}, "hydrated content " * 30
            return set(), ""

        outcome, links, text = await engine.fetch_page("https://x.com/",
                                                       run_extract=run_extract)
        assert render_calls == ["https://x.com/"]
        assert len(extractions) == 2
        assert outcome.rendered is True
        assert "hydrated" in text

    async def test_render_never_skips_escalation(self):
        shell = b'<html><body><div id="root"></div></body></html>'
        engine = make_engine([FakeResponse(body=shell)], render_mode="never")

        async def fake_render(url):
            raise AssertionError("render must not be called")
        engine.render = fake_render

        async def run_extract(html, url):
            return set(), ""
        outcome, links, text = await engine.fetch_page("https://x.com/",
                                                       run_extract=run_extract)
        assert outcome.rendered is False

    async def test_render_failure_keeps_static_content(self):
        shell = b'<html><body><div id="root"></div>static</body></html>'
        engine = make_engine([FakeResponse(body=shell)], render_mode="auto")

        async def fake_render(url):
            return None
        engine.render = fake_render

        async def run_extract(html, url):
            return set(), ""
        outcome, links, text = await engine.fetch_page("https://x.com/",
                                                       run_extract=run_extract)
        assert outcome.rendered is False
        assert "static" in outcome.content


class TestShouldDownloadFile:
    def test_by_extension(self):
        assert should_download_file("https://x.com/a.pdf") is True
        assert should_download_file("https://x.com/a.html") is False

    def test_by_mime(self):
        assert should_download_file("https://x.com/dl", "application/pdf; charset=x") is True
        assert should_download_file("https://x.com/dl", "text/html") is False


class TestRobots:
    def test_allows_when_not_loaded(self):
        engine = FetchEngine()
        assert engine.robots_allows("https://x.com/anything") is True

    def test_allows_when_ignoring(self):
        engine = FetchEngine(respect_robots=False)
        engine._robots = object()  # would blow up if consulted
        assert engine.robots_allows("https://x.com/a") is True

    async def test_wait_politeness_flat_delay(self):
        engine = FetchEngine(delay_between_requests=0)
        await engine.wait_politeness()  # should just not hang

    async def test_wait_politeness_paces_globally(self):
        # N concurrent waiters must be spaced ~delay apart in total, not all
        # sleep in parallel (the old no-op behavior).
        engine = FetchEngine(delay_between_requests=0.05)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(engine.wait_politeness() for _ in range(4)))
        elapsed = loop.time() - start
        assert elapsed >= 0.05 * 3 * 0.9  # 4 requests -> >= ~3 gaps


# ----------------------------------------------------------------------
# robots.txt / sitemap.xml: aiohttp/urllib 403 -> curl_cffi fallback
# ----------------------------------------------------------------------
class _FakeCurlResponse:
    def __init__(self, status, body, ctype):
        self.status_code = status
        self.content = body
        # Like curl_cffi, .text decodes with a guessed charset; garbles
        # non-UTF-8 bytes (which is why sitemaps must use .content).
        self.text = body.decode("utf-8", errors="replace")
        self.headers = {"Content-Type": ctype}


def patch_curl_cffi(monkeypatch, routes):
    """Replace curl_cffi's AsyncSession with one serving ``routes``
    (url -> (status, body, content-type)). Returns the list of (url, headers)
    requests it saw."""
    import curl_cffi.requests
    seen = []

    class _FakeAsyncSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, impersonate=None, headers=None, **kw):
            assert impersonate == "chrome"
            seen.append((url, dict(headers or {})))
            status, body, ctype = routes.get(url, (404, b"not found", "text/plain"))
            return _FakeCurlResponse(status, body, ctype)

    monkeypatch.setattr(curl_cffi.requests, "AsyncSession", _FakeAsyncSession)
    return seen


def forbid_cookie_reads(monkeypatch):
    """robots/sitemap fallbacks must never touch the cookie bridge."""
    from scrape_website import waf

    def boom(*a, **k):
        raise AssertionError("cookie bridge consulted for robots/sitemap")
    for name in ("cookie_header_for", "has_clearance_for", "obtain_clearance",
                 "_load_manual_once"):
        monkeypatch.setattr(waf.CF_SESSION, name, boom)


ROBOTS_BODY = b"User-agent: *\nDisallow: /private/\nCrawl-delay: 2\n"


class TestRobotsCurlFallback:
    async def test_403_falls_back_to_curl_cffi(self, monkeypatch):
        forbid_cookie_reads(monkeypatch)
        seen = patch_curl_cffi(monkeypatch, {
            "https://x.com/robots.txt": (200, ROBOTS_BODY, "text/plain"),
        })
        engine = make_engine([FakeResponse(status=403, body=b"Access Denied")])
        await engine.load_robots("https://x.com/")
        assert [u for u, _ in seen] == ["https://x.com/robots.txt"]
        assert "Cookie" not in seen[0][1]
        assert engine.robots_allows("https://x.com/private/a") is False
        assert engine.robots_allows("https://x.com/public") is True
        assert engine._rate_limiter is not None  # Crawl-delay picked up

    async def test_200_does_not_use_curl_cffi(self, monkeypatch):
        seen = patch_curl_cffi(monkeypatch, {})
        engine = make_engine([FakeResponse(
            body=ROBOTS_BODY, headers={"Content-Type": "text/plain"})])
        await engine.load_robots("https://x.com/")
        assert seen == []
        assert engine.robots_allows("https://x.com/private/a") is False

    async def test_still_blocked_warns_and_proceeds(self, monkeypatch, caplog):
        forbid_cookie_reads(monkeypatch)
        patch_curl_cffi(monkeypatch, {
            "https://x.com/robots.txt": (403, b"Access Denied", "text/html"),
        })
        engine = make_engine([FakeResponse(status=403, body=b"Access Denied")])
        with caplog.at_level("WARNING", logger=engine.logger.name):
            await engine.load_robots("https://x.com/")
        assert engine._robots is None
        assert engine.robots_allows("https://x.com/private/a") is True
        assert "WITHOUT robots.txt enforcement" in caplog.text
        assert "curl_cffi got HTTP 403 with an HTML body" in caplog.text

    async def test_curl_404_is_not_a_warning(self, monkeypatch, caplog):
        """Past the WAF there is simply no robots.txt: INFO, no warning."""
        forbid_cookie_reads(monkeypatch)
        patch_curl_cffi(monkeypatch, {})  # unrouted -> 404
        engine = make_engine([FakeResponse(status=403, body=b"Access Denied")])
        with caplog.at_level("INFO", logger=engine.logger.name):
            await engine.load_robots("https://x.com/")
        assert engine._robots is None
        assert not [r for r in caplog.records if r.levelname == "WARNING"]
        assert "No robots.txt at https://x.com/robots.txt (HTTP 404" in caplog.text

    async def test_html_body_reason(self, monkeypatch, caplog):
        forbid_cookie_reads(monkeypatch)
        patch_curl_cffi(monkeypatch, {
            "https://x.com/robots.txt": (200, b"<html>app shell</html>", "text/html"),
        })
        engine = make_engine([FakeResponse(status=403, body=b"Access Denied")])
        with caplog.at_level("WARNING", logger=engine.logger.name):
            await engine.load_robots("https://x.com/")
        assert engine._robots is None
        assert "curl_cffi got HTTP 200 with an HTML body" in caplog.text

    async def test_curl_not_installed_reason(self, monkeypatch, caplog):
        import importlib.util
        import sys
        monkeypatch.setitem(sys.modules, "curl_cffi.requests", None)
        real_find_spec = importlib.util.find_spec
        monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: (
            None if name == "curl_cffi" else real_find_spec(name, *a)))
        engine = make_engine([FakeResponse(status=403, body=b"Access Denied")])
        with caplog.at_level("WARNING", logger=engine.logger.name):
            await engine.load_robots("https://x.com/")
        assert engine._robots is None
        assert "curl_cffi is not installed" in caplog.text
