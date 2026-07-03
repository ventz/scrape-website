import json

from scrape_website.waf import _CFSession


def _session_with_file(monkeypatch, tmp_path, content: str) -> _CFSession:
    p = tmp_path / "cookies"
    p.write_text(content)
    monkeypatch.setenv("SCRAPE_CF_COOKIES", str(p))
    monkeypatch.delenv("IB_CF_COOKIES", raising=False)
    return _CFSession()


class TestManualCookieFile:
    def test_json_list(self, monkeypatch, tmp_path):
        s = _session_with_file(monkeypatch, tmp_path, json.dumps([
            {"domain": ".x.com", "name": "cf_clearance", "value": "abc"},
            {"domain": ".x.com", "name": "session", "value": "s1"},
        ]))
        assert s.has_clearance_for("https://www.x.com/page") is True
        header = s.cookie_header_for("https://www.x.com/page")
        assert "cf_clearance=abc" in header
        assert "session=s1" in header

    def test_json_dict(self, monkeypatch, tmp_path):
        s = _session_with_file(monkeypatch, tmp_path,
                               json.dumps({"x.com": {"datadome": "v"}}))
        assert s.has_clearance_for("https://x.com/") is True

    def test_netscape(self, monkeypatch, tmp_path):
        line = ".x.com\tTRUE\t/\tTRUE\t0\tcf_clearance\tzzz"
        s = _session_with_file(monkeypatch, tmp_path, f"# comment\n{line}\n")
        assert s.cookie_header_for("https://x.com/") == "cf_clearance=zzz"

    def test_no_file(self, monkeypatch):
        monkeypatch.delenv("SCRAPE_CF_COOKIES", raising=False)
        monkeypatch.delenv("IB_CF_COOKIES", raising=False)
        s = _CFSession()
        assert s.cookie_header_for("https://x.com/") is None
        assert s.has_clearance_for("https://x.com/") is False


class TestClearanceDetection:
    def test_non_clearance_cookie_is_not_clearance(self, monkeypatch, tmp_path):
        s = _session_with_file(monkeypatch, tmp_path,
                               json.dumps({"x.com": {"plain_session": "v"}}))
        assert s.has_clearance_for("https://x.com/") is False
        # ...but it still gets replayed in the Cookie header.
        assert s.cookie_header_for("https://x.com/") == "plain_session=v"

    def test_prefix_markers(self):
        assert _CFSession._is_clearance("visid_incap_123") is True
        assert _CFSession._is_clearance("_abck") is True
        assert _CFSession._is_clearance("hello") is False

    def test_subdomain_match(self, monkeypatch, tmp_path):
        s = _session_with_file(monkeypatch, tmp_path,
                               json.dumps({"x.com": {"cf_clearance": "a"}}))
        assert s.has_clearance_for("https://deep.sub.x.com/") is True
        assert s.has_clearance_for("https://notx.com/") is False
