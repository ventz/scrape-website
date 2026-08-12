"""Content extraction: links (lxml), clean text (trafilatura), documents
(PyMuPDF4LLM / MarkItDown), plus the SPA-shell and challenge heuristics.

Everything here is a module-level function so it stays picklable across the
ProcessPoolExecutor boundary used by the CLI crawler.
"""

import os
import re
from datetime import datetime
from urllib.parse import urlparse

import lxml.html
import trafilatura
from trafilatura.deduplication import LRU_TEST

from .config import (
    DOWNLOADABLE_EXTENSIONS,
    _CHALLENGE_MARKERS,
    _SPA_SHELL_MARKERS,
)
from .urls import _normalize_url, _url_excluded


def _looks_challenged(html: str, status: int) -> bool:
    """Is this an HTTP payload a Cloudflare/CAPTCHA interstitial rather than content?"""
    low = (html or '').lower()
    if any(m in low for m in _CHALLENGE_MARKERS):
        return True
    if status in (403, 503) and 'cloudflare' in low:
        return True
    return False


def is_access_denied(content: str, status: int) -> bool:
    """Is this HTML response an access-denied page rather than content?"""
    if status in (401, 403):
        return True
    if len(content) < 2000 and 'Access Denied' in content:
        return True
    return False


def _extract_links_lxml(html_content: str, base_url: str, base_domain: str,
                        strip_tracking: bool = False,
                        exclude_patterns: list[str] | None = None) -> set[str]:
    """Extract links using lxml (5-20x faster than BeautifulSoup).

    *exclude_patterns*: list of regex **strings** (not compiled) — we
    compile them here because compiled patterns are not picklable across
    the process-pool boundary.
    """
    compiled = [re.compile(p) for p in (exclude_patterns or [])]
    links = set()
    try:
        doc = lxml.html.fromstring(html_content)
        doc.make_links_absolute(base_url, resolve_base_href=True)

        for element, attribute, link, pos in doc.iterlinks():
            if not link or not link.startswith('http'):
                continue
            normalized = _normalize_url(link, strip_tracking=strip_tracking)
            parsed = urlparse(normalized)
            tag = element.tag

            if tag == 'a':
                # Follow all same-domain <a> links
                if parsed.netloc == base_domain:
                    if not _url_excluded(normalized, compiled):
                        links.add(normalized)
            elif tag in ('link', 'script', 'img'):
                # Only follow non-<a> tags if they point to downloadable files
                path_lower = parsed.path.lower()
                if any(path_lower.endswith(ext) for ext in DOWNLOADABLE_EXTENSIONS):
                    links.add(normalized)
    except Exception:
        pass
    return links


def _extract_text_trafilatura(html_content: str, url: str) -> str | None:
    """Extract clean Markdown (with metadata front matter) for LLM consumption."""
    try:
        # Reset trafilatura's process-global dedup cache before every page so
        # deduplication is strictly intra-page. Without this, the LRU_TEST
        # cache accumulates across all pages handled by a long-lived
        # ProcessPoolExecutor worker, silently stripping content that legitimately
        # repeats across pages (e.g. an FAQ answer on both the FAQ page and its
        # own page) — and producing no file at all when a page is only such text.
        LRU_TEST.clear()
        text = trafilatura.extract(
            html_content,
            url=url,
            include_comments=False,
            include_tables=True,
            include_links=True,
            include_images=False,
            favor_recall=True,       # maximize content extraction
            deduplicate=True,        # intra-page only (cache cleared above)
            with_metadata=True,      # YAML front matter: title, url, hostname...
            output_format='markdown',
        )
        return text
    except Exception:
        return None


def _parse_and_extract(html_content: str, url: str, base_domain: str,
                       strip_tracking: bool = False,
                       exclude_patterns: list[str] | None = None) -> tuple[set[str], str | None]:
    """Combined link extraction + text extraction in one process pool call."""
    links = _extract_links_lxml(html_content, url, base_domain,
                                strip_tracking=strip_tracking,
                                exclude_patterns=exclude_patterns)
    text = _extract_text_trafilatura(html_content, url)
    return links, text


def _looks_like_spa_shell(html_content: str, extracted_text: str | None,
                          link_count: int, min_text: int = 200) -> bool:
    """Heuristic: does this static HTML look like an un-hydrated SPA shell?

    A client-rendered single-page app returns an near-empty document whose
    real content and navigation only materialize once JavaScript runs, so the
    static pass yields almost no text and almost no followable links. We
    escalate to a headless render only when BOTH the extractable text is tiny
    AND there's a positive signal that JS would produce more — never just
    because a page happens to include scripts.
    """
    text_len = len((extracted_text or '').strip())
    if text_len >= min_text:
        return False
    low = html_content.lower()
    has_marker = any(m in low for m in _SPA_SHELL_MARKERS)
    wants_js = 'enable javascript' in low or 'please enable js' in low
    # A near-empty body with no same-domain links to follow is a dead end for
    # a static crawler regardless of markers — worth one render attempt.
    dead_end = link_count == 0
    return has_marker or wants_js or dead_end


_PDF_MAGIC = b'%PDF'


def _document_extension(filepath: str, url: str) -> str:
    """Best-effort extension for *filepath*, used to pick a converter.

    Resolved in order:

    1. the saved file's own extension;
    2. the extension in the URL path — covers names that lost their suffix,
       e.g. ``/files/guide.pdf?download=1``;
    3. PDF magic bytes.

    Step 3 is what makes extension-less documents work. Drupal-backed sites
    commonly serve files from paths with no suffix at all
    (``/resource/proposals-dashboard-guidance`` returning
    ``Content-Type: application/pdf``). Downloads are accepted on MIME, so such
    a file arrives here with ``ext == ''``, matches no converter branch, and is
    dropped without an error — it shows up under files_downloaded but never
    under docs_extracted.

    Only ``%PDF`` is sniffed, because it is unambiguous. The ZIP-based Office
    formats (docx/xlsx/pptx/odt) all share ``PK\\x03\\x04``, so distinguishing
    them needs archive introspection; those still rely on steps 1-2.
    """
    ext = os.path.splitext(filepath)[1].lower()
    if ext in DOWNLOADABLE_EXTENSIONS:
        return ext

    url_ext = os.path.splitext(urlparse(url).path)[1].lower()
    if url_ext in DOWNLOADABLE_EXTENSIONS:
        return url_ext

    try:
        with open(filepath, 'rb') as fh:
            if fh.read(len(_PDF_MAGIC)) == _PDF_MAGIC:
                return '.pdf'
    except OSError:
        pass

    return ext


def _extract_document_to_markdown(filepath: str, url: str, hostname: str) -> str | None:
    """Convert a downloaded document (PDF / Office / text) to Markdown for RAG.

    Tiered, best-effort: PyMuPDF4LLM for native PDFs, MarkItDown for Office
    formats, and an optional Docling fallback for complex/scanned PDFs when the
    fast path yields almost nothing (only if docling is installed). Returns the
    Markdown body (with YAML front matter) or None on failure / empty output.
    Runs in a worker thread — keep it import-lazy so non-document crawls pay no
    import cost.
    """
    ext = _document_extension(filepath, url)
    body: str | None = None
    try:
        if ext == '.pdf':
            import pymupdf4llm
            body = pymupdf4llm.to_markdown(filepath)
            if not body or len(body.strip()) < 50:
                # Fast path produced almost nothing (scanned / complex layout).
                # Try Docling only if the user installed it (heavy, optional).
                try:
                    from docling.document_converter import DocumentConverter
                    body = DocumentConverter().convert(filepath).document.export_to_markdown()
                except ImportError:
                    pass
                except Exception:
                    pass
        elif ext in ('.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', '.odt', '.ods', '.odp', '.rtf'):
            from markitdown import MarkItDown
            body = MarkItDown().convert(filepath).text_content
        elif ext in ('.txt', '.csv'):
            with open(filepath, 'r', encoding='utf-8', errors='replace') as fh:
                body = fh.read()
    except Exception:
        return None

    if not body or not body.strip():
        return None

    title = os.path.basename(filepath)
    date = datetime.now().strftime('%Y-%m-%d')
    front_matter = (
        "---\n"
        f"title: {title}\n"
        f"url: {url}\n"
        f"hostname: {hostname}\n"
        f"filetype: {ext.lstrip('.')}\n"
        f"date: {date}\n"
        "---\n\n"
    )
    return front_matter + body.strip() + "\n"
