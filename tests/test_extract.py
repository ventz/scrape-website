import pytest

from scrape_website.extract import (
    _extract_document_to_markdown,
    _extract_links_lxml,
    _extract_text_trafilatura,
    _looks_challenged,
    _looks_like_spa_shell,
    is_access_denied,
)

PAGE = """<html><body>
<h1>Title</h1>
<p>Some meaningful paragraph content that trafilatura should extract without
trouble because it is long enough to look like real prose on a real page.</p>
<a href="/about">About</a>
<a href="https://x.com/tag/skipme">Tag</a>
<a href="https://other.com/off-domain">Elsewhere</a>
<a href="/files/report.pdf">Report</a>
<img src="/assets/photo.jpg">
<script src="/data/feed.csv"></script>
</body></html>"""


class TestExtractLinks:
    def test_same_domain_and_documents(self):
        links = _extract_links_lxml(PAGE, "https://x.com/", "x.com")
        assert "https://x.com/about" in links
        assert "https://x.com/tag/skipme" in links          # no excludes passed
        assert "https://other.com/off-domain" not in links  # off-domain <a>
        assert "https://x.com/files/report.pdf" in links
        assert "https://x.com/data/feed.csv" in links       # non-<a> downloadable
        assert not any(link.endswith(".jpg") for link in links)

    def test_exclude_patterns(self):
        links = _extract_links_lxml(PAGE, "https://x.com/", "x.com",
                                    exclude_patterns=[r"/tag/"])
        assert "https://x.com/tag/skipme" not in links
        assert "https://x.com/about" in links


class TestTrafilatura:
    def test_extracts_markdown_with_front_matter(self):
        text = _extract_text_trafilatura(PAGE, "https://x.com/")
        assert text is not None
        assert "meaningful paragraph content" in text
        assert text.startswith("---")  # YAML front matter

    def test_garbage_returns_none_or_empty(self):
        assert not _extract_text_trafilatura("", "https://x.com/")


class TestSpaShell:
    def test_rich_page_not_shell(self):
        long_text = "word " * 100
        assert _looks_like_spa_shell(PAGE, long_text, link_count=3) is False

    def test_react_root_shell(self):
        html = '<html><body><div id="root"></div><script src="/b.js"></script></body></html>'
        assert _looks_like_spa_shell(html, "", link_count=0) is True

    def test_dead_end_without_marker(self):
        html = "<html><body><p>hi</p></body></html>"
        assert _looks_like_spa_shell(html, "hi", link_count=0) is True
        assert _looks_like_spa_shell(html, "hi", link_count=5) is False


class TestChallenged:
    def test_cloudflare_interstitial(self):
        assert _looks_challenged("<title>Just a moment...</title>", 200) is True

    def test_cloudflare_403(self):
        assert _looks_challenged("<html>cloudflare</html>", 403) is True

    def test_normal_page(self):
        assert _looks_challenged(PAGE, 200) is False


class TestAccessDenied:
    def test_status(self):
        assert is_access_denied("x", 403) is True
        assert is_access_denied("x", 401) is True

    def test_short_denied_body(self):
        assert is_access_denied("<h1>Access Denied</h1>", 200) is True

    def test_normal(self):
        assert is_access_denied(PAGE, 200) is False


class TestDocumentExtraction:
    def test_txt(self, tmp_path):
        p = tmp_path / "notes.txt"
        p.write_text("Plain text body for the document extractor.")
        md = _extract_document_to_markdown(str(p), "https://x.com/notes.txt", "x.com")
        assert md is not None
        assert md.startswith("---")
        assert "filetype: txt" in md
        assert "Plain text body" in md

    def test_pdf(self, tmp_path):
        pymupdf = pytest.importorskip("pymupdf")
        p = tmp_path / "doc.pdf"
        doc = pymupdf.open()
        page = doc.new_page()
        # A single insert_text line gets classified as OCR-needed by
        # pymupdf4llm's layout pass and extracts empty; a real multi-line
        # textbox extracts fine.
        page.insert_textbox(pymupdf.Rect(72, 72, 540, 700),
                            "Hello from a generated PDF document used in tests.\n" * 12,
                            fontsize=11)
        doc.save(str(p))
        doc.close()
        md = _extract_document_to_markdown(str(p), "https://x.com/doc.pdf", "x.com")
        assert md is not None
        assert "filetype: pdf" in md
        assert "Hello from a generated PDF" in md

    def test_missing_file_returns_none(self, tmp_path):
        assert _extract_document_to_markdown(
            str(tmp_path / "nope.pdf"), "https://x.com/nope.pdf", "x.com") is None
