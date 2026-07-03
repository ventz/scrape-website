import re

from scrape_website.urls import _normalize_url, _strip_tracking_params, _url_excluded


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
