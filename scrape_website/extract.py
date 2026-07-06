"""Content extraction: links (lxml), clean text (trafilatura), documents
(PyMuPDF4LLM / MarkItDown), plus the SPA-shell and challenge heuristics.

Everything here is a module-level function so it stays picklable across the
ProcessPoolExecutor boundary used by the CLI crawler.
"""

import os
import re
from datetime import datetime
from urllib.parse import parse_qsl, urlparse

import lxml.html
import trafilatura
from trafilatura.deduplication import LRU_TEST

from .config import (
    DOWNLOADABLE_EXTENSIONS,
    _CHALLENGE_MARKERS,
    _SPA_SHELL_MARKERS,
)
from .urls import (
    _canonicalize_host,
    _is_safe_fetch_target,
    _normalize_url,
    _same_host,
    _url_excluded,
)

# Human-readable labels for the challenge markers in config._CHALLENGE_MARKERS,
# so logs and the --human solve prompt say WHAT was detected (a Turnstile CAPTCHA
# is solvable by a human; a plain Cloudflare block page usually is not).
_CHALLENGE_LABELS: dict[str, str] = {
    'just a moment': 'Cloudflare interstitial',
    'checking your browser': 'Cloudflare interstitial',
    'cf-browser-verification': 'Cloudflare browser verification',
    'challenge-platform': 'Cloudflare challenge',
    'cf_chl_': 'Cloudflare challenge',
    'turnstile': 'Cloudflare Turnstile CAPTCHA',
    'hcaptcha': 'hCaptcha CAPTCHA',
    'g-recaptcha': 'Google reCAPTCHA',
    'attention required': 'Cloudflare block page',
    'verify you are human': 'human-verification challenge',
    'enable javascript and cookies to continue': 'JS/cookie challenge interstitial',
    'ddos protection by': 'DDoS-protection interstitial',
}

# Query keys that mark a URL as a search-results page (?q=, WordPress ?s=, ...).
_SEARCH_QUERY_KEYS = frozenset({'q', 's', 'query', 'search', 'keyword', 'keywords'})

# Phrases that mark a small HTML payload as a soft 404 / denial served with a 200.
_SOFT_404_PHRASES = ('page not found', "page can't be found",
                     'page could not be found', 'nothing was found')


def _looks_like_search_url(url: str) -> bool:
    """Does this URL look like a search-results page rather than an article?"""
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if re.search(r'/search(?:/|$)', parsed.path.lower()):
        return True
    keys = {k.lower() for k, v in parse_qsl(parsed.query) if v}
    return bool(keys & _SEARCH_QUERY_KEYS)


def classify_page(html: str, status: int, url: str = '') -> tuple[str, str]:
    """Classify a fetched HTML payload so callers can react (and report)
    appropriately instead of lumping every non-page into "challenge".

    Returns ``(kind, detail)`` where *kind* is one of:
      - ``'challenge'`` — anti-bot/CAPTCHA interstitial (solvable, escalate);
      - ``'not_found'`` — hard 404/410 or a soft-404 body (skip, count);
      - ``'denied'``    — 401/403 or an access-denied body (skip, count);
      - ``'search'``    — a search-results page (real content, but flagged);
      - ``'content'``   — a normal page.
    *detail* is a short human-readable reason (e.g. which CAPTCHA vendor).
    """
    low = (html or '').lower()
    # Challenge markers win over status codes: Cloudflare serves its
    # interstitials as 403/503 AND as 200s, and a challenge is actionable
    # (curl_cffi / cookie bridge / --human) where a plain denial is not.
    for marker in _CHALLENGE_MARKERS:
        if marker in low:
            return 'challenge', _CHALLENGE_LABELS.get(marker, marker)
    if status in (403, 503) and 'cloudflare' in low:
        return 'challenge', 'Cloudflare interstitial'
    if status in (404, 410):
        return 'not_found', f'HTTP {status}'
    if status in (401, 403):
        return 'denied', f'HTTP {status}'
    # Soft 404s / denials served with a 200: only trust small pages, so a real
    # article that merely mentions the phrase is never misclassified.
    if len(low) < 5000:
        if any(p in low for p in _SOFT_404_PHRASES):
            return 'not_found', 'soft 404 (page-not-found body)'
        if 'access denied' in low:
            return 'denied', 'access-denied body'
    if url and _looks_like_search_url(url):
        return 'search', 'search-results URL'
    return 'content', ''


def _looks_challenged(html: str, status: int) -> bool:
    """Is this an HTTP payload a Cloudflare/CAPTCHA interstitial rather than content?"""
    return classify_page(html, status)[0] == 'challenge'


def is_access_denied(content: str, status: int) -> bool:
    """Is this HTML response an access-denied page rather than content?

    Legacy helper — prefer :func:`classify_page`, which also separates
    challenges, 404s, and search pages.
    """
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
        base_scheme = urlparse(base_url).scheme or 'https'

        for element, attribute, link, pos in doc.iterlinks():
            if not link or not link.startswith('http'):
                continue
            normalized = _normalize_url(link, strip_tracking=strip_tracking)
            parsed = urlparse(normalized)
            tag = element.tag

            if tag == 'a':
                # Follow all same-site <a> links (www.x.com == x.com), rewriting
                # them onto the crawl's canonical scheme + host so aliases of a
                # page dedup to one visited-store URL.
                if _same_host(parsed.netloc, base_domain):
                    if parsed.scheme != base_scheme or parsed.netloc != base_domain:
                        normalized = _canonicalize_host(
                            normalized, base_scheme, base_domain,
                            strip_tracking=strip_tracking)
                    if not _url_excluded(normalized, compiled):
                        links.add(normalized)
            elif tag in ('link', 'script', 'img'):
                # Only follow non-<a> tags if they point to downloadable files.
                # Cross-host is allowed (CDN-hosted documents are common) but
                # SSRF-gated: a crawled page must not be able to make us fetch
                # internal/link-local targets (e.g. cloud metadata endpoints).
                path_lower = parsed.path.lower()
                if (any(path_lower.endswith(ext) for ext in DOWNLOADABLE_EXTENSIONS)
                        and _is_safe_fetch_target(normalized)):
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


def _extract_document_to_markdown(filepath: str, url: str, hostname: str) -> str | None:
    """Convert a downloaded document (PDF / Office / text) to Markdown for RAG.

    Tiered, best-effort: PyMuPDF4LLM for native PDFs, MarkItDown for Office
    formats, and an optional Docling fallback for complex/scanned PDFs when the
    fast path yields almost nothing (only if docling is installed). Returns the
    Markdown body (with YAML front matter) or None on failure / empty output.
    Runs in a worker thread — keep it import-lazy so non-document crawls pay no
    import cost.
    """
    ext = os.path.splitext(filepath)[1].lower()
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
