"""SQLite-backed URL store (visited set, file hashes, queue + stats checkpoints)."""

import json
import sqlite3
from collections import deque
from pathlib import Path
from typing import Deque, Iterable


class URLStore:
    """SQLite-backed visited URL tracking.

    The visited set is held fully in memory (loaded once at open) and every
    ``add`` is written through to SQLite, so ``contains`` — the crawl's
    hottest call, hit at least twice per discovered link — never touches the
    database or the event loop. Memory cost is roughly the URLs themselves
    (~100-200 MB per million URLs), which is fine at any realistic crawl size.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path), isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS visited (url TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS downloaded_files (hash TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS queue (url TEXT PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value TEXT)")
        # Named URL lists (denied/failed/not_found/challenged) checkpointed so
        # the post-crawl report files stay complete across crash + resume.
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS url_lists "
            "(kind TEXT, url TEXT, PRIMARY KEY (kind, url))")
        self._visited: set[str] = {
            row[0] for row in self.conn.execute("SELECT url FROM visited")
        }

    def contains(self, url: str) -> bool:
        return url in self._visited

    def add(self, url: str):
        if url in self._visited:
            return
        self._visited.add(url)
        self.conn.execute("INSERT OR IGNORE INTO visited (url) VALUES (?)", (url,))

    def forget(self, url: str):
        """Remove *url* from the visited set so it can be crawled again
        (used by --retry to force re-fetching previously failed URLs)."""
        self._visited.discard(url)
        self.conn.execute("DELETE FROM visited WHERE url=?", (url,))

    @property
    def count(self) -> int:
        return len(self._visited)

    def has_file_hash(self, file_hash: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM downloaded_files WHERE hash=?", (file_hash,)).fetchone()
        return row is not None

    def add_file_hash(self, file_hash: str) -> bool:
        """Atomically claim *file_hash*. Returns True iff it was NOT already
        claimed — callers use this as check-and-claim in one step, so two
        concurrent downloads of identical content can't both pass a separate
        ``has_file_hash`` check and write duplicate files."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO downloaded_files (hash) VALUES (?)", (file_hash,))
        return cur.rowcount == 1

    def save_queue(self, urls: Deque[str]):
        self.conn.execute("DELETE FROM queue")
        self.conn.executemany("INSERT OR IGNORE INTO queue (url) VALUES (?)", [(u,) for u in urls])

    def load_queue(self) -> Deque[str]:
        rows = self.conn.execute("SELECT url FROM queue").fetchall()
        return deque(row[0] for row in rows)

    def save_stats(self, stats: dict):
        self.conn.execute("INSERT OR REPLACE INTO stats (key, value) VALUES (?, ?)",
                          ('stats', json.dumps(stats)))

    def load_stats(self) -> dict | None:
        row = self.conn.execute("SELECT value FROM stats WHERE key='stats'").fetchone()
        if row:
            return json.loads(row[0])
        return None

    def save_url_list(self, kind: str, urls: Iterable[str]):
        self.conn.execute("DELETE FROM url_lists WHERE kind=?", (kind,))
        self.conn.executemany("INSERT OR IGNORE INTO url_lists (kind, url) VALUES (?, ?)",
                              [(kind, u) for u in urls])

    def load_url_list(self, kind: str) -> list[str]:
        rows = self.conn.execute("SELECT url FROM url_lists WHERE kind=?", (kind,)).fetchall()
        return [row[0] for row in rows]

    def clear(self):
        self.conn.execute("DELETE FROM visited")
        self.conn.execute("DELETE FROM downloaded_files")
        self.conn.execute("DELETE FROM queue")
        self.conn.execute("DELETE FROM stats")
        self.conn.execute("DELETE FROM url_lists")
        self._visited.clear()

    def close(self):
        self.conn.close()
