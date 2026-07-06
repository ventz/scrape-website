from collections import deque

from scrape_website.store import URLStore


class TestVisited:
    def test_contains_add_count(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        assert store.contains("https://x.com/a") is False
        store.add("https://x.com/a")
        assert store.contains("https://x.com/a") is True
        store.add("https://x.com/a")  # idempotent
        assert store.count == 1
        store.close()

    def test_survives_reopen(self, tmp_path):
        db = tmp_path / "state.db"
        store = URLStore(db)
        store.add("https://x.com/a")
        store.close()
        store = URLStore(db)
        assert store.contains("https://x.com/a") is True
        assert store.count == 1
        store.close()


class TestForget:
    def test_forget_allows_revisit(self, tmp_path):
        db = tmp_path / "state.db"
        store = URLStore(db)
        store.add("https://x.com/a")
        store.forget("https://x.com/a")
        assert store.contains("https://x.com/a") is False
        store.close()
        # And it stays forgotten across reopen (deleted from SQLite too).
        store = URLStore(db)
        assert store.contains("https://x.com/a") is False
        store.close()

    def test_forget_unknown_is_noop(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        store.forget("https://x.com/never-seen")
        assert store.count == 0
        store.close()


class TestFileHashClaim:
    def test_first_claim_wins(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        assert store.add_file_hash("abc") is True
        assert store.add_file_hash("abc") is False
        assert store.has_file_hash("abc") is True
        store.close()


class TestQueueAndLists:
    def test_queue_roundtrip(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        store.save_queue(deque(["https://x.com/a", "https://x.com/b"]))
        assert set(store.load_queue()) == {"https://x.com/a", "https://x.com/b"}
        store.close()

    def test_url_lists_roundtrip_and_replace(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        store.save_url_list("denied", ["https://x.com/a", "https://x.com/b"])
        store.save_url_list("failed", ["https://x.com/c"])
        assert set(store.load_url_list("denied")) == {"https://x.com/a", "https://x.com/b"}
        assert store.load_url_list("failed") == ["https://x.com/c"]
        # Each save replaces the kind's list wholesale (checkpoint semantics).
        store.save_url_list("denied", ["https://x.com/z"])
        assert store.load_url_list("denied") == ["https://x.com/z"]
        assert store.load_url_list("missing") == []
        store.close()

    def test_clear_wipes_everything(self, tmp_path):
        store = URLStore(tmp_path / "state.db")
        store.add("https://x.com/a")
        store.add_file_hash("abc")
        store.save_url_list("denied", ["https://x.com/a"])
        store.clear()
        assert store.count == 0
        assert store.has_file_hash("abc") is False
        assert store.load_url_list("denied") == []
        store.close()
