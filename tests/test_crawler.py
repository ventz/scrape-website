import pytest

from scrape_website.crawler import WebsiteScraper


@pytest.fixture
def scraper(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s = WebsiteScraper("https://x.com/", fresh=True)
    yield s
    s.executor.shutdown(wait=False)
    s.url_store.close()


class TestHostValidation:
    def test_malformed_netloc_rejected(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        # urlparse('http://../x').netloc is '..' — must not become data/..
        with pytest.raises(ValueError, match="invalid host"):
            WebsiteScraper("http://../x", fresh=True)

    def test_host_with_port_ok(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        s = WebsiteScraper("http://127.0.0.1:8931/", fresh=True)
        assert s.base_domain == "127.0.0.1:8931"
        s.executor.shutdown(wait=False)
        s.url_store.close()


class TestEnqueueRequeue:
    def test_enqueue_dedups(self, scraper):
        assert scraper.enqueue("https://x.com/a") is True
        assert scraper.enqueue("https://x.com/a") is False  # already queued
        scraper.url_store.add("https://x.com/b")
        assert scraper.enqueue("https://x.com/b") is False  # already visited

    def test_requeue_forces_visited_url_back(self, scraper):
        scraper.url_store.add("https://x.com/failed")
        assert scraper.enqueue("https://x.com/failed") is False
        assert scraper.requeue("https://x.com/failed") is True
        assert scraper.url_store.contains("https://x.com/failed") is False
        assert "https://x.com/failed" in scraper.urls_to_visit


class TestFilenames:
    def test_dot_only_stem_falls_back_to_hash(self, scraper):
        name = scraper.generate_filename("https://x.com/downloads/..", "application/pdf")
        assert name not in ("..", ".")
        assert name.startswith("file_")

    @pytest.mark.parametrize("ctype, expected", [
        ("application/pdf", "guidance.pdf"),
        ("application/pdf; charset=binary", "guidance.pdf"),
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",
         "guidance.docx"),
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
         "guidance.xlsx"),
    ])
    def test_extensionless_path_takes_extension_from_content_type(
            self, scraper, ctype, expected):
        assert scraper.generate_filename(
            "https://x.com/resource/guidance", ctype) == expected

    def test_existing_extension_is_kept(self, scraper):
        assert scraper.generate_filename(
            "https://x.com/files/report.pdf", "application/pdf") == "report.pdf"

    def test_unknown_content_type_leaves_name_alone(self, scraper):
        assert scraper.generate_filename(
            "https://x.com/resource/guidance", "application/octet-stream") == "guidance"
        assert scraper.generate_filename("https://x.com/resource/guidance") == "guidance"

    def test_reserve_path_suffixes_within_run(self, scraper):
        p1 = scraper._reserve_path(scraper.text_dir, "page", ".md")
        p2 = scraper._reserve_path(scraper.text_dir, "page", ".md")
        assert p1.name == "page.md"
        assert p2.name == "page_1.md"

    def test_reserve_path_fresh_overwrites_stale_file(self, scraper):
        stale = scraper.text_dir / "page.md"
        stale.write_text("old run")
        p = scraper._reserve_path(scraper.text_dir, "page", ".md")
        assert p == stale  # fresh run reclaims the name instead of page_1.md


class TestSitemapCurlFallback:
    async def test_403_falls_back_to_curl_cffi(self, scraper, monkeypatch):
        """urllib 403 on sitemap.xml -> the crawler's bridge runs the engine's
        curl_cffi fallback (no cookie bridge) and the sitemap still seeds."""
        import asyncio
        from unittest.mock import patch

        from scrape_website import sitemap
        from test_fetch_engine import forbid_cookie_reads, patch_curl_cffi
        from test_sitemap import SITEMAP_PLAIN, _urlopen_403

        forbid_cookie_reads(monkeypatch)
        seen = patch_curl_cffi(monkeypatch, {
            "https://x.com/sitemap.xml": (200, SITEMAP_PLAIN, "application/xml"),
        })
        loop = asyncio.get_running_loop()
        with patch.object(sitemap, "urlopen", _urlopen_403):
            urls = await loop.run_in_executor(None, lambda: sitemap._fetch_sitemap_urls(
                "x.com", fallback=lambda u: scraper._sitemap_fallback(u, loop)))
        assert urls == ["https://x.com/a", "https://x.com/b"]
        assert seen[0][0] == "https://x.com/sitemap.xml"
        assert all("Cookie" not in h for _, h in seen)

    async def test_still_blocked_returns_none_and_warns(self, scraper, monkeypatch, caplog):
        import asyncio

        from test_fetch_engine import forbid_cookie_reads, patch_curl_cffi

        forbid_cookie_reads(monkeypatch)
        patch_curl_cffi(monkeypatch, {
            "https://x.com/sitemap.xml": (403, b"Access Denied", "text/html"),
        })
        loop = asyncio.get_running_loop()
        with caplog.at_level("WARNING"):
            data = await loop.run_in_executor(
                None, scraper._sitemap_fallback, "https://x.com/sitemap.xml", loop)
        assert data is None
        assert "is blocked and the curl_cffi fallback did not get it" in caplog.text
