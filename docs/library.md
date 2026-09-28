# Library Usage

Since 0.5.0 the scraper is an importable package, `scrape_website`. Its tiered
fetcher (static → browser-fingerprint WAF fallback → headless render, with
robots politeness and retry/backoff) is reusable on its own through
`FetchEngine`.

```python
from scrape_website import FetchEngine

engine = FetchEngine(render_mode="auto")
await engine.start()
outcome = await engine.fetch("https://example.com/")   # FetchOutcome
await engine.close()
```

The stable public surface: `start()` / `close()`, `fetch()`, `render()`,
`fetch_page(render_mode=...)`, and `load_robots()` / `robots_allows()` /
`wait_politeness()`. Each `FetchOutcome` carries the response, its headers, and
a page `classification` (content, challenge, not_found, denied, search) with a
vendor `detail` for challenges.

Pick capability tiers with extras (see
[Configuration](configuration.md#install-extras)):

```bash
uv add "scrape-website[render,waf,docs]"   # or [all]
```

The companion [scrape-website-mcp](https://github.com/ventz/scrape-website-mcp)
server builds on this API.
