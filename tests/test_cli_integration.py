"""Integration smoke test: crawl a local fixture site end-to-end through the
real WebsiteScraper (guards the 'CLI must not regress' constraint)."""

import http.server
import threading

import pytest

from scrape_website.config import CONFIG
from scrape_website.crawler import WebsiteScraper

FIXTURES = {
    "/": '<html><head><title>Home</title></head><body>'
         '<p>Fixture home page with enough prose for extraction to produce a '
         'markdown file rather than skipping the page entirely.</p>'
         '<a href="/about.html">About</a> <a href="/notes.txt">Notes</a>'
         '</body></html>',
    "/about.html": '<html><head><title>About</title></head><body>'
                   '<p>The about page also carries a couple of sentences of real '
                   'content so trafilatura has something meaningful to keep.</p>'
                   '</body></html>',
    "/notes.txt": "Plain text notes served as a downloadable document.",
}


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = FIXTURES.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        ctype = "text/plain" if self.path.endswith(".txt") else "text/html"
        payload = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *a):
        pass


@pytest.fixture
def fixture_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}/"
    server.shutdown()


@pytest.mark.integration
async def test_full_crawl_writes_output_tree(fixture_server, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # data/ tree is cwd-relative
    old_concurrent = CONFIG['max_concurrent']
    CONFIG['max_concurrent'] = 5
    try:
        scraper = WebsiteScraper(fixture_server, fresh=True,
                                 render_mode='never', use_sitemap=False)
        await scraper.run()
    finally:
        CONFIG['max_concurrent'] = old_concurrent

    host = fixture_server.split("//")[1].rstrip("/")
    base = tmp_path / "data" / host
    assert (base / "pages" / "index.html").exists()
    assert (base / "pages" / "about.html").exists()
    md = (base / "text" / "about.md").read_text()
    assert "couple of sentences of real" in md
    assert (base / "files" / "notes.txt").exists()
    assert (base / "text" / "notes.md").exists()   # doc-extraction pipeline
    assert scraper.stats["pages_downloaded"] == 2
    assert scraper.stats["files_downloaded"] == 1
    assert scraper.stats["errors"] == 0
