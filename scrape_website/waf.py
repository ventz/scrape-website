"""Browser-cookie reuse — borrow the cookies your REAL Chrome earned.

Modern anti-bot walls (Cloudflare Private Access Token, Imperva/Incapsula,
Akamai Bot Manager, DataDome, PerimeterX, ...) issue a clearance cookie that an
AUTOMATED browser (Playwright/Chromium) can never legitimately earn — Cloudflare's
PAT is a hardware-attested token (Secure Enclave) only a genuine, OS-blessed
browser can mint. So instead of trying to SOLVE the challenge in automation, we
REUSE the cookies your real Chrome already holds (or that you solve once in a real
tab we open). ``curl_cffi`` then replays them with a Chrome TLS fingerprint and the
SAME User-Agent (CONFIG['user_agent']).

We reuse **all** of a domain's cookies, not just Cloudflare's ``cf_clearance`` —
that covers Imperva (visid_incap_*/incap_ses_*), Akamai (_abck/bm_sz), etc. with
one uniform mechanism, and carries any login session the real browser holds too.
Caveats vs. Cloudflare: (1) the replay UA must match the real Chrome that earned
the cookies (cf_clearance is UA-bound), and (2) Imperva/Akamai tokens are more
tightly bound to the browser fingerprint + IP than Cloudflare's, so replay from a
different TLS stack is less reliable even when the cookies are valid.

Solve ONCE per domain: cookies are cached for the run, so later URLs on the same
domain reuse them silently. Sources, in order:
  1. SCRAPE_CF_COOKIES / IB_CF_COOKIES — a JSON or Netscape cookies file you export
     (most reliable; works regardless of Chrome's cookie encryption; the ONLY
     headless-server-safe source).
  2. The live Chrome cookie store via ``browser_cookie3`` (optional dep).
  3. Interactive (--human): open the URL as a tab in your real Chrome, you solve it,
     we poll the cookie store until a clearance cookie appears.
"""

import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse


class _CFSession:
    """Process-wide cache of a domain's browser cookies, keyed by cookie domain.

    Started life as a Cloudflare ``cf_clearance`` bridge; now reuses ALL of a
    domain's cookies so one replay path covers Imperva, Akamai Bot Manager,
    DataDome, PerimeterX, etc. — plus any login session — not just Cloudflare."""

    # Cookie names (exact or prefix, case-insensitive) that signal a SOLVED
    # anti-bot / WAF challenge. Used ONLY to decide "do we already have clearance?";
    # capture and replay use ALL cookies regardless of name.
    _CLEARANCE_MARKERS = (
        'cf_clearance',                          # Cloudflare
        'visid_incap_', 'incap_ses_', 'nlbi_',   # Imperva / Incapsula
        '_abck', 'bm_sz', 'ak_bmsc', 'bm_sv',    # Akamai Bot Manager
        'datadome',                              # DataDome
        '_px', '_pxhd', '_pxvid',                # PerimeterX
        'reese84',                               # F5 / Shape (Distil)
    )
    REAL_BROWSER = os.environ.get('SCRAPE_REAL_BROWSER',
                                  os.environ.get('IB_REAL_BROWSER', 'Google Chrome'))
    SOLVE_TIMEOUT = float(os.environ.get('SCRAPE_HUMAN_SOLVE_TIMEOUT', '300'))

    def __init__(self):
        self._cache: dict[str, dict[str, str]] = {}  # domain (no leading dot) -> {name: value}
        self._lock = threading.Lock()
        self._manual_loaded = False

    # -- cache ------------------------------------------------------------
    def cache(self, domain: str, cookies: dict[str, str]) -> None:
        cookies = {k: v for k, v in cookies.items() if v}
        if not cookies:
            return
        with self._lock:
            self._cache.setdefault(domain.lstrip('.').lower(), {}).update(cookies)

    def _load_manual_once(self) -> None:
        """Load ALL cookies from SCRAPE_CF_COOKIES / IB_CF_COOKIES (JSON or Netscape
        cookies.txt). We keep every cookie, not just Cloudflare's, so the replay also
        carries Imperva/Akamai/etc. clearance and any login session."""
        if self._manual_loaded:
            return
        self._manual_loaded = True
        path = os.environ.get('SCRAPE_CF_COOKIES') or os.environ.get('IB_CF_COOKIES')
        if not path:
            return
        try:
            raw = Path(path).expanduser().read_text()
        except OSError:
            return
        try:  # JSON: [{"domain","name","value"}, ...] OR {"domain": {"name": "value"}}
            data = json.loads(raw)
            if isinstance(data, list):
                for c in data:
                    name = c.get('name')
                    if name:
                        self.cache(str(c.get('domain', '')), {name: c.get('value', '')})
            elif isinstance(data, dict):
                for dom, cookies in data.items():
                    self.cache(dom, dict(cookies))
            return
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
        # Netscape cookies.txt: domain \t flag \t path \t secure \t expiry \t name \t value
        for line in raw.splitlines():
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) >= 7 and parts[5]:
                self.cache(parts[0], {parts[5]: parts[6]})

    # -- lookup -----------------------------------------------------------
    @staticmethod
    def _host(url: str) -> str:
        try:
            return (urlparse(url).hostname or '').lower()
        except Exception:
            return ''

    @classmethod
    def _is_clearance(cls, name: str) -> bool:
        n = (name or '').lower()
        return any(n == m or n.startswith(m) for m in cls._CLEARANCE_MARKERS)

    def cookie_header_for(self, url: str) -> str | None:
        """``Cookie:`` header value for ALL cached cookies matching ``url``'s host."""
        self._load_manual_once()
        host = self._host(url)
        if not host:
            return None
        parts: list[str] = []
        with self._lock:
            for dom, cookies in self._cache.items():
                if host == dom or host.endswith('.' + dom):
                    parts += [f'{k}={v}' for k, v in cookies.items()]
        return '; '.join(parts) if parts else None

    def has_clearance_for(self, url: str) -> bool:
        """True if we hold a cookie that signals a solved WAF/anti-bot challenge for
        this host (cf_clearance, or an Imperva/Akamai/DataDome/PerimeterX marker)."""
        self._load_manual_once()
        host = self._host(url)
        if not host:
            return False
        with self._lock:
            for dom, cookies in self._cache.items():
                if host == dom or host.endswith('.' + dom):
                    if any(self._is_clearance(k) for k in cookies):
                        return True
        return False

    # -- acquisition (sync; call via asyncio.to_thread) -------------------
    def _read_from_chrome(self, host: str) -> dict[str, str]:
        """Best-effort read of ALL cookies for ``host`` from the live Chrome cookie
        store into the cache. Returns the cookies read (empty if browser_cookie3 is
        absent or cannot decrypt)."""
        try:
            import browser_cookie3  # type: ignore
        except Exception:
            return {}
        try:
            jar = browser_cookie3.chrome(domain_name=host)
        except Exception:  # locked DB / decryption failure / keychain declined
            return {}
        got: dict[str, str] = {}
        for c in jar:
            if c.value:
                self.cache((c.domain or host), {c.name: c.value})
                got[c.name] = c.value
        return got

    def _open_in_real_chrome(self, url: str) -> bool:
        """Open ``url`` as a tab in the user's REAL Chrome (macOS ``open -a``). The
        genuine, OS-attested browser CAN pass PAT/Turnstile — the human solves there."""
        if platform.system() != 'Darwin':
            return False  # `open -a` is macOS-only; elsewhere rely on SCRAPE_CF_COOKIES
        try:
            subprocess.run(['open', '-a', self.REAL_BROWSER, url], check=False, timeout=15)
            return True
        except Exception:
            return False

    def obtain_clearance(self, url: str, interactive: bool, on_wait=None,
                         poll: float = 2.0) -> bool:
        """Get a usable cf_clearance for ``url``'s host (cached for the run). Order:
        already-cached -> manual file -> silent read from Chrome -> (interactive only)
        open a real Chrome tab and poll while the human solves. Returns True on success."""
        self._load_manual_once()
        if self.has_clearance_for(url):
            return True
        host = self._host(url)
        if not host:
            return False
        # The clearance cookie may already be in your real Chrome from normal browsing.
        # browser_cookie3 decrypts Chrome's cookie store, which on macOS can block on
        # a Keychain permission prompt — say so, or the crawl looks hung here.
        print(f"[cf] reading {host} cookies from {self.REAL_BROWSER}'s cookie store "
              f"(may prompt for Keychain access)...", file=sys.stderr, flush=True)
        self._read_from_chrome(host)
        if self.has_clearance_for(url):
            return True
        if not interactive:
            return False
        if not self._open_in_real_chrome(url):
            print(f"[cf] couldn't open {self.REAL_BROWSER}; set SCRAPE_CF_COOKIES to a "
                  f"cookies file instead.", file=sys.stderr, flush=True)
            return False
        print(f"[cf] Opened {url} in {self.REAL_BROWSER} — solve the challenge there; "
              f"reusing its cookies for all of {host}.", file=sys.stderr, flush=True)
        if on_wait:
            on_wait()
        deadline = time.monotonic() + max(10.0, self.SOLVE_TIMEOUT)
        while time.monotonic() < deadline:
            time.sleep(poll)
            self._read_from_chrome(host)
            if self.has_clearance_for(url):
                print(f"[cf] clearance obtained for {host} — continuing.",
                      file=sys.stderr, flush=True)
                return True
        print(f"[cf] no clearance for {host} within {int(self.SOLVE_TIMEOUT)}s "
              f"(if browser_cookie3 can't read your Chrome, export cookies to "
              f"SCRAPE_CF_COOKIES).", file=sys.stderr, flush=True)
        return False


# Single process-wide instance (clearance is bound to this machine's IP + UA).
CF_SESSION = _CFSession()
