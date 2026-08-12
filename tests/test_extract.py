import pytest

from scrape_website.extract import (
    _PDF_MAGIC,
    _document_extension,
    _extract_document_to_markdown,
    _extract_links_lxml,
    _extract_text_trafilatura,
    _looks_challenged,
    _looks_like_spa_shell,
    classify_page,
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

    def test_www_and_scheme_aliases_unify(self):
        html = """<html><body>
        <a href="https://www.x.com/team">team</a>
        <a href="http://x.com/contact">contact</a>
        </body></html>"""
        links = _extract_links_lxml(html, "https://x.com/", "x.com")
        assert "https://x.com/team" in links
        assert "https://x.com/contact" in links
        assert not any("www." in link or link.startswith("http://") for link in links)

    def test_non_anchor_downloadables_are_ssrf_gated(self):
        html = """<html><body>
        <img src="http://169.254.169.254/latest/meta-data/x.csv">
        <script src="file:///etc/passwd.csv"></script>
        <img src="https://cdn.other.com/report.pdf">
        </body></html>"""
        links = _extract_links_lxml(html, "https://x.com/", "x.com")
        assert "https://cdn.other.com/report.pdf" in links  # public CDN ok
        assert not any("169.254" in link or link.startswith("file:") for link in links)

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


class TestClassifyPage:
    def test_normal_content(self):
        assert classify_page(PAGE, 200, "https://x.com/about") == ('content', '')

    def test_challenge_wins_over_status(self):
        kind, detail = classify_page("<div class='cf-turnstile'></div>", 403)
        assert kind == 'challenge'
        assert 'Turnstile' in detail

    def test_challenge_vendor_labels(self):
        assert classify_page("<title>Just a moment...</title>", 200)[1] == 'Cloudflare interstitial'
        assert 'hCaptcha' in classify_page("<div class='hcaptcha'></div>", 200)[1]
        assert 'reCAPTCHA' in classify_page("<div class='g-recaptcha'></div>", 200)[1]

    def test_hard_404(self):
        assert classify_page(PAGE, 404) == ('not_found', 'HTTP 404')
        assert classify_page(PAGE, 410) == ('not_found', 'HTTP 410')

    def test_soft_404_small_body_only(self):
        assert classify_page("<h1>Page not found</h1>", 200)[0] == 'not_found'
        # A large real article that merely mentions the phrase is content.
        big = PAGE + ("<p>filler</p>" * 500) + "page not found"
        assert classify_page(big, 200)[0] == 'content'

    def test_denied(self):
        assert classify_page("x", 403)[0] == 'denied'
        assert classify_page("x", 401)[0] == 'denied'
        assert classify_page("<h1>Access denied</h1>", 200)[0] == 'denied'

    def test_search_url(self):
        assert classify_page(PAGE, 200, "https://x.com/search?q=foo")[0] == 'search'
        assert classify_page(PAGE, 200, "https://x.com/?s=term")[0] == 'search'
        assert classify_page(PAGE, 200, "https://x.com/research/")[0] == 'content'


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

    def test_extensionless_pdf_is_detected_by_magic_bytes(self, tmp_path):
        """A PDF served from a suffix-less path still converts.

        Drupal sites serve documents from URLs like
        /resource/proposals-dashboard-guidance with Content-Type application/pdf.
        The download is accepted on MIME, so the saved file has no extension.
        """
        pymupdf = pytest.importorskip("pymupdf")
        p = tmp_path / "proposals-dashboard-guidance"   # deliberately no suffix
        doc = pymupdf.open()
        page = doc.new_page()
        page.insert_textbox(pymupdf.Rect(72, 72, 540, 700),
                            "Guidance text inside an extension-less PDF file.\n" * 12,
                            fontsize=11)
        doc.save(str(p))
        doc.close()
        md = _extract_document_to_markdown(
            str(p), "https://x.com/resource/proposals-dashboard-guidance", "x.com")
        assert md is not None
        assert "filetype: pdf" in md
        assert "Guidance text inside an extension-less PDF" in md

    def test_extension_recovered_from_url_path(self, tmp_path):
        """When the saved name lost its suffix, the URL path supplies it."""
        p = tmp_path / "notes"                          # no suffix on disk
        p.write_text("Plain text body recovered via the URL extension.")
        md = _extract_document_to_markdown(
            str(p), "https://x.com/files/notes.txt?download=1", "x.com")
        assert md is not None
        assert "filetype: txt" in md
        assert "recovered via the URL extension" in md

    def test_unknown_binary_still_returns_none(self, tmp_path):
        """Sniffing must not turn unrecognised bytes into a bogus conversion."""
        p = tmp_path / "mystery"
        p.write_bytes(b"\x00\x01\x02not a document at all")
        assert _extract_document_to_markdown(
            str(p), "https://x.com/mystery", "x.com") is None


class TestDocumentExtensionResolution:
    def test_file_extension_wins(self, tmp_path):
        p = tmp_path / "a.pdf"
        p.write_bytes(_PDF_MAGIC + b"-1.7 rest")
        assert _document_extension(str(p), "https://x.com/a.docx") == ".pdf"

    def test_url_extension_used_when_file_has_none(self, tmp_path):
        p = tmp_path / "a"
        p.write_text("hello")
        assert _document_extension(str(p), "https://x.com/files/a.csv") == ".csv"

    def test_magic_bytes_are_last_resort(self, tmp_path):
        p = tmp_path / "a"
        p.write_bytes(_PDF_MAGIC + b"-1.4 rest")
        assert _document_extension(str(p), "https://x.com/resource/a") == ".pdf"

    def test_unknown_stays_unknown(self, tmp_path):
        p = tmp_path / "a"
        p.write_bytes(b"plain bytes")
        assert _document_extension(str(p), "https://x.com/resource/a") == ""

    def test_query_string_is_not_mistaken_for_an_extension(self, tmp_path):
        p = tmp_path / "a"
        p.write_text("hello")
        # urlparse().path drops the query, so ".com" from a param must not leak in
        assert _document_extension(str(p), "https://x.com/a?ref=foo.com") == ""

    def test_missing_file_does_not_raise(self, tmp_path):
        assert _document_extension(str(tmp_path / "gone"), "https://x.com/gone") == ""
