"""CLI entrypoint (argparse) — behavior identical to the historical app.py."""

import argparse
import asyncio
from pathlib import Path
from urllib.parse import urlparse

from . import __version__
from .config import CONFIG, _DEFAULT_EXCLUDE_PATTERNS
from .crawler import WebsiteScraper


def collect_urls(args) -> list[str]:
    """Collect URLs from CLI arg and/or file."""
    urls = []
    if args.url:
        urls.append(args.url)
    if args.file:
        path = Path(args.file)
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    if args.retry:
        path = Path(args.retry)
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


def parse_args():
    parser = argparse.ArgumentParser(description='Scrape an entire website (pages + documents + clean text)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    parser.add_argument('url', nargs='?', help='Starting URL to scrape (e.g. https://example.com/)')
    parser.add_argument('--file', '-f', help='File with URLs to scrape (one per line)')
    parser.add_argument('--retry', '-r', help='File with failed URLs to retry (e.g. data/example.com/logs/failed_urls.txt)')
    parser.add_argument('--concurrency', '-c', type=int, default=CONFIG['max_concurrent'],
                        help=f"Max concurrent requests (default: {CONFIG['max_concurrent']})")
    parser.add_argument('--timeout', '-t', type=int, default=CONFIG['timeout'],
                        help=f"Request timeout in seconds (default: {CONFIG['timeout']})")
    parser.add_argument('--delay', '-d', type=float, default=CONFIG['delay_between_requests'],
                        help=f"Delay between requests in seconds (default: {CONFIG['delay_between_requests']})")
    parser.add_argument('--fresh', '-F', action='store_true',
                        help='Ignore any saved checkpoint and start fresh')
    parser.add_argument('--fullname', '-n', action='store_true',
                        help='Prefix output filenames with the host (fully-qualified, e.g. example.com_about.md)')
    parser.add_argument('--render', choices=('auto', 'never', 'always'), default='auto',
                        help="Headless-render JS pages: auto=only when a page looks like an "
                             "un-hydrated SPA shell, always=every page, never=disable (default: auto)")
    parser.add_argument('--human', action='store_true',
                        help="Interactive mode: open a VISIBLE browser and fetch through it; "
                             "auto-pause for you to solve Cloudflare/CAPTCHA/login challenges "
                             "(session persists across runs). Forces --concurrency 1.")
    parser.add_argument('--allow-insecure-tls', action='store_true',
                        help='Disable TLS certificate verification (for trusted hosts with broken certs)')
    parser.add_argument('--ignore-robots', action='store_true',
                        help='Do not fetch or honor robots.txt (default: honor it)')
    parser.add_argument('--no-extract-docs', dest='extract_docs', action='store_false', default=True,
                        help='Do not convert downloaded PDFs/Office docs to Markdown')
    parser.add_argument('--exclude-pattern', '-e', action='append', default=None,
                        metavar='PATTERN',
                        help='Regex pattern to exclude URLs (repeatable; appends to defaults)')
    parser.add_argument('--no-default-excludes', action='store_true',
                        help='Clear the default exclude patterns (use only --exclude-pattern values)')
    tracking_group = parser.add_mutually_exclusive_group()
    tracking_group.add_argument('--strip-tracking-params', action='store_true', default=True,
                                dest='strip_tracking_params',
                                help='Strip tracking query params like utm_* (default)')
    tracking_group.add_argument('--no-strip-tracking-params', action='store_false',
                                dest='strip_tracking_params',
                                help='Keep tracking query params in URLs')
    sitemap_group = parser.add_mutually_exclusive_group()
    sitemap_group.add_argument('--use-sitemap', action='store_true', default=True,
                               dest='use_sitemap',
                               help='Seed crawl queue from sitemap.xml (default)')
    sitemap_group.add_argument('--no-use-sitemap', action='store_false',
                               dest='use_sitemap',
                               help='Do not fetch sitemap.xml for seed URLs')
    return parser.parse_args()


async def main():
    args = parse_args()
    urls = collect_urls(args)

    if not urls:
        print("Error: provide a URL, --file, or --retry")
        raise SystemExit(1)

    # Interactive mode drives one visible browser; force single-flight so the
    # solve prompt is unambiguous and only one window is in play.
    if args.human and args.concurrency != 1:
        print("--human: forcing --concurrency 1 (interactive single window)")
        args.concurrency = 1

    CONFIG['max_concurrent'] = args.concurrency
    CONFIG['timeout'] = args.timeout
    CONFIG['delay_between_requests'] = args.delay

    # Build exclude patterns list
    if args.no_default_excludes:
        exclude_patterns = list(args.exclude_pattern or [])
    elif args.exclude_pattern:
        exclude_patterns = list(_DEFAULT_EXCLUDE_PATTERNS) + args.exclude_pattern
    else:
        exclude_patterns = None  # use defaults inside WebsiteScraper

    # Group URLs by domain so each domain gets one scraper
    by_domain: dict[str, list[str]] = {}
    for url in urls:
        domain = urlparse(url).netloc
        by_domain.setdefault(domain, []).append(url)

    # Run all domains concurrently
    async with asyncio.TaskGroup() as tg:
        for domain, domain_urls in by_domain.items():
            scraper = WebsiteScraper(
                domain_urls[0], fresh=args.fresh,
                exclude_patterns=exclude_patterns,
                strip_tracking_params=args.strip_tracking_params,
                use_sitemap=args.use_sitemap,
                render_mode=args.render,
                allow_insecure_tls=args.allow_insecure_tls,
                ignore_robots=args.ignore_robots,
                fullname=args.fullname,
                extract_docs=args.extract_docs,
                human=args.human,
            )
            # Seed any additional URLs for this domain
            for extra in domain_urls[1:]:
                normalized = scraper.normalize_url(extra)
                if not scraper.url_store.contains(normalized):
                    scraper.urls_to_visit.append(normalized)
            tg.create_task(scraper.run())


def run():
    """Sync console entrypoint."""
    asyncio.run(main())
