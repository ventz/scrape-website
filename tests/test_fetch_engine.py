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
        # The final 503 is returned as-is (no more retries left).
        outcome = await engine.fetch("https://x.com/")
        assert outcome.status == 503

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
