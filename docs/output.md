# Output

What a crawl writes to disk and how the Markdown is shaped.

## Directory layout

```
data/
  example.com/
    pages/                # Raw HTML
    text/                 # Markdown with front matter: pages AND documents
    files/                # Downloaded PDF, DOC(X), PPT(X), XLS(X), CSV, ZIP, RTF, ODT/ODS/ODP
    logs/
      scrape.log          # Full per-URL debug log
      state.db            # SQLite: visited URLs, queue, stats, report lists
      failed_urls.txt     # Failed after retries; feed to --retry
      access_denied.txt   # 401/403
      not_found.txt       # 404/410 and soft-404s
      challenged_urls.txt # Still blocked by an anti-bot challenge
      browser_profile/    # --human session (cookies, logins)
```

Report files appear only when they have entries.

## Markdown files

Every file in `text/` opens with YAML front matter, followed by the main
content with headings and links preserved. Navigation, headers, footers, and
other boilerplate are stripped.

```markdown
---
title: About Us
url: https://example.com/about
hostname: example.com
sitename: Example
date: 2026-03-12
---

# About Us
...
```

Extracted documents carry `title`, `url`, `hostname`, `filetype`, and `date`,
so pages and documents form one uniform Markdown corpus that's ready for RAG
ingestion.

Filenames come from the URL path. `--fullname` prefixes the host
(`example.com_about.md`), which helps when you merge several domains into one
corpus.

## Deduplication

Deduplication happens **per page only**. `trafilatura`'s repetition cache is
reset before every page, so a passage is removed only if it repeats within the
same page. Text that legitimately appears on several pages, like a shared FAQ
answer or policy blurb, is kept in full on every page, so each file stands on
its own as a retrievable document.

URL deduplication is separate: the visited set and the queue are both
deduplicated, so no page is fetched twice.

## Example run

```
% uv run python app.py https://privsec.harvard.edu
scrape-website v0.7.1 — crawling privsec.harvard.edu
Checking sitemap.xml for seed URLs...
Sitemap: seeded 87 URLs from sitemap.xml
Progress: 42 visited | 39 pages | 36 text | 0 rendered | 1 files | 1 docs | 2 denied | 0 404s | 0 errors | 1.9 MB | 55 queued

================================================================================
SCRAPING COMPLETED
================================================================================
Duration: 14.20 seconds
URLs visited: 104
Pages downloaded: 98
Text extracted: 91
Files downloaded: 3
Documents extracted: 3
Access denied: 3
Not found (404): 2
Total data: 4.63 MB
Errors: 0
Output location: data/privsec.harvard.edu
================================================================================
```
