import re

from scrape_website.urls import (
    _canonicalize_host,
    _is_safe_fetch_target,
    _normalize_url,
    _same_host,
    _strip_tracking_params,
    _url_excluded,
)


class TestNormalizeUrl:
    def test_strips_fragment(self):
        assert _normalize_url("https://x.com/a#frag") == "https://x.com/a"

    def test_strips_trailing_slash_on_path(self):
        assert _normalize_url("https://x.com/a/") == "https://x.com/a"

    def test_keeps_root_slash(self):
        assert _normalize_url("https://x.com/") == "https://x.com/"

    def test_keeps_query(self):
        assert _normalize_url("https://x.com/a?b=1") == "https://x.com/a?b=1"

    def test_strip_tracking(self):
        assert _normalize_url("https://x.com/a?utm_source=t&b=1",
                              strip_tracking=True) == "https://x.com/a?b=1"

    def test_query_ending_in_slash_preserved(self):
        # Regression: the trailing-slash strip must not eat the query's last char.
        assert _normalize_url("https://x.com/p?next=/") == "https://x.com/p?next=/"

    def test_lowercases_scheme_and_host_not_path(self):
        assert _normalize_url("HTTPS://X.com/About") == "https://x.com/About"


class TestSameHost:
    def test_www_equivalence(self):
        assert _same_host("www.x.com", "x.com") is True
        assert _same_host("x.com", "WWW.X.com") is True

    def test_other_subdomains_differ(self):
        assert _same_host("blog.x.com", "x.com") is False

    def test_ports_must_match(self):
        assert _same_host("x.com:8080", "x.com") is False


class TestSafeFetchTarget:
    def test_public_hosts_ok(self):
        assert _is_safe_fetch_target("https://cdn.example.com/a.pdf") is True
        assert _is_safe_fetch_target("http://8.8.8.8/a.pdf") is True

    def test_non_http_schemes_rejected(self):
        assert _is_safe_fetch_target("file:///etc/passwd") is False
        assert _is_safe_fetch_target("ftp://x.com/a.pdf") is False

    def test_internal_ip_literals_rejected(self):
        assert _is_safe_fetch_target("http://169.254.169.254/meta.csv") is False
        assert _is_safe_fetch_target("http://127.0.0.1/a.pdf") is False
        assert _is_safe_fetch_target("http://192.168.0.1/x.xlsx") is False
        assert _is_safe_fetch_target("http://10.0.0.5/x.pdf") is False
        assert _is_safe_fetch_target("http://[::1]/x.pdf") is False


class TestCanonicalizeHost:
    def test_rewrites_scheme_and_host(self):
        assert _canonicalize_host("http://www.x.com/a/?b=1", "https", "x.com") \
            == "https://x.com/a?b=1"


class TestStripTrackingParams:
    def test_no_query_unchanged(self):
        assert _strip_tracking_params("https://x.com/a") == "https://x.com/a"

    def test_all_tracking_drops_question_mark(self):
        assert _strip_tracking_params("https://x.com/a?utm_source=x&fbclid=y") == "https://x.com/a"

    def test_preserves_non_tracking_order(self):
        assert _strip_tracking_params("https://x.com/a?z=1&utm_medium=m&a=2") == "https://x.com/a?z=1&a=2"


class TestUrlExcluded:
    def test_empty_patterns(self):
        assert _url_excluded("https://x.com/tag/foo", []) is False

    def test_match(self):
        pats = [re.compile(r"/tag/")]
        assert _url_excluded("https://x.com/tag/foo", pats) is True
        assert _url_excluded("https://x.com/post/foo", pats) is False
