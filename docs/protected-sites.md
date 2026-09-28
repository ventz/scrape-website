# Protected Sites

How the scraper gets past WAFs, bot challenges, and login walls, from fully
automatic to you-in-the-loop.

## Contents

- [Automatic WAF fallback](#automatic-waf-fallback)
- [Interactive mode (`--human`)](#interactive-mode---human)
- [The cookie bridge (Private Access Token walls)](#the-cookie-bridge-private-access-token-walls)
- [Broken TLS certificates](#broken-tls-certificates)

## Automatic WAF fallback

No flags needed. Responses that are `401`/`403`, a WAF block page, or a `200`
Cloudflare "Just a moment…" interstitial are retried with `curl_cffi` using a
real Chrome TLS fingerprint. If that's still blocked, the scraper tries the
[cookie bridge](#the-cookie-bridge-private-access-token-walls) before recording
the URL as failed. An interstitial is never archived as content.

## Interactive mode (`--human`)

For Cloudflare challenges, Turnstile / hCaptcha / reCAPTCHA gates, or login walls:

```bash
uv run python app.py --human https://example.com/
```

- Opens a **visible Chromium window** and fetches every page through it.
- Crawls normally until `classify_page` detects a real challenge, then brings
  the window forward and pauses. The terminal prompt names what was detected.
  Solve it, press **Enter**, and the crawl continues with the cleared session.
- 404s, soft-404s, plain 403s, and search pages are classified and logged, and
  never pause the crawl.
- The profile is saved in `data/<domain>/logs/browser_profile/`, so a solved
  challenge or completed login is reused across pages and future runs.
- Forces `--concurrency 1`, so there's one window and one clear prompt.

Requires `uv run playwright install chromium`. Slower than the static path;
only use it when a site actually blocks you.

## The cookie bridge (Private Access Token walls)

Some Cloudflare sites use a **Private Access Token (PAT)** challenge.
Automated browsers can't pass it, including the `--human` Playwright window. A
PAT is hardware-attested (Secure Enclave), so only a genuine OS-blessed browser
(your real Chrome or Safari) can mint one.

The bridge reuses **all cookies your real Chrome earned for the domain** and
replays them through `curl_cffi` with a matching Chrome TLS fingerprint and
User-Agent. That covers Cloudflare `cf_clearance`, Imperva
`visid_incap_*`/`incap_ses_*`, Akamai `_abck`/`bm_sz`, DataDome, PerimeterX,
and any login session.

Cookie sources, tried in order:

1. **An exported cookies file** in `SCRAPE_CF_COOKIES` (or `IB_CF_COOKIES`).
   This is the most reliable option and needs no browser and no `--human`:
   ```bash
   SCRAPE_CF_COOKIES=~/cookies.json uv run python app.py https://protected.example/
   ```
   Formats: JSON list `[{"domain","name","value"}]`, JSON map
   `{"domain": {"name": "value"}}`, or Netscape `cookies.txt`.
2. **Your live Chrome cookie store**, read silently via `browser_cookie3`
   (the `human` extra). It may fail to decrypt the newest Chrome or trigger a
   keychain prompt; use the file option if so.
3. **`--human` only:** opens the URL in your real Chrome (macOS `open -a`).
   Solve it once, and the scraper polls until a clearance cookie appears. This
   also kicks in automatically when a Playwright solve leaves the page still
   challenged (the PAT case).

Constraints:

- Clearance is bound to **domain + IP + User-Agent**. Run on the same machine
  that solved it, and keep the User-Agent's Chrome version matched to your
  installed Chrome (default: Chrome 154; override with `SCRAPE_USER_AGENT`).
- Imperva and Akamai bind tokens to the browser fingerprint more tightly, so
  replay is less reliable for them even with valid cookies.
- Solve once per host; the cookies are cached for the rest of the run.

## Broken TLS certificates

For a trusted host with an expired or misconfigured certificate:

```bash
uv run python app.py https://example.com/ --allow-insecure-tls
```

Verification is strict by default.
