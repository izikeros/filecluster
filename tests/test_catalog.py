"""Tests for the per-library SQLite catalog.

Covers creation, cluster CRUD, file-hash CRUD, mtime-based cache hits/misses,
pruning of stale rows, and context-manager lifecycle.
"""

from pathlib import Path

import pytest

from filecluster.catalog import LibraryCatalog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def catalog(tmp_path):
    """Yield a fresh catalog in a temporary directory, closed after the test."""
    with LibraryCatalog.open(tmp_path) as cat:
        yield cat


# ---------------------------------------------------------------------------
# Creation and lifecycle
# ---------------------------------------------------------------------------
class TestCatalogLifecycle:
    """Opening, closing, and reopening the catalog."""

    def test_open_creates_db_file(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.db_path.exists()
            assert cat.db_path.name == ".filecluster.db"

    def test_reopen_preserves_data(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_cluster("2020/event", start_date="2020-01-01 00:00:00")

        with LibraryCatalog.open(tmp_path) as cat:
            row = cat.get_cluster("2020/event")
            assert row is not None
            assert row["start_date"] == "2020-01-01 00:00:00"

    def test_context_manager_closes(self, tmp_path):
        cat = LibraryCatalog.open(tmp_path)
        cat.close()
        # Closed connection raises on use
        with pytest.raises(Exception):
            cat.get_cluster("anything")


# ---------------------------------------------------------------------------
# Cluster operations
# ---------------------------------------------------------------------------
class TestClusterCRUD:
    """Insert, retrieve, update, and prune cluster rows."""

    def test_get_missing_returns_none(self, catalog):
        assert catalog.get_cluster("no/such/path") is None

    def test_put_then_get(self, catalog):
        catalog.put_cluster(
            "2020/[2020_06_15]_event",
            start_date="2020-06-15 10:00:00",
            end_date="2020-06-15 12:00:00",
            median="2020-06-15 11:00:00",
            is_continuous=True,
            file_count=42,
            folder_mtime=1234567890.0,
        )
        row = catalog.get_cluster("2020/[2020_06_15]_event")
        assert row is not None
        assert row["start_date"] == "2020-06-15 10:00:00"
        assert row["end_date"] == "2020-06-15 12:00:00"
        assert row["median"] == "2020-06-15 11:00:00"
        assert row["is_continuous"] == 1
        assert row["file_count"] == 42
        assert row["folder_mtime"] == 1234567890.0
        assert row["scanned_at"] is not None

    def test_put_updates_existing(self, catalog):
        catalog.put_cluster("2020/ev", file_count=1)
        catalog.put_cluster("2020/ev", file_count=99)
        row = catalog.get_cluster("2020/ev")
        assert row["file_count"] == 99

    def test_get_all_clusters(self, catalog):
        catalog.put_cluster("2020/a", file_count=1)
        catalog.put_cluster("2020/b", file_count=2)
        catalog.put_cluster("2021/c", file_count=3)
        rows = catalog.get_all_clusters()
        assert len(rows) == 3
        paths = {r["path"] for r in rows}
        assert paths == {"2020/a", "2020/b", "2021/c"}

    def test_prune_removes_stale_rows(self, catalog):
        catalog.put_cluster("2020/a", file_count=1)
        catalog.put_cluster("2020/b", file_count=2)
        catalog.put_cluster("2020/c", file_count=3)

        removed = catalog.prune_clusters({"2020/a", "2020/c"})
        assert removed == 1
        assert catalog.get_cluster("2020/b") is None
        assert catalog.get_cluster("2020/a") is not None
        assert catalog.get_cluster("2020/c") is not None

    def test_prune_with_all_present_removes_nothing(self, catalog):
        catalog.put_cluster("2020/a", file_count=1)
        removed = catalog.prune_clusters({"2020/a"})
        assert removed == 0

    def test_prune_empty_catalog(self, catalog):
        removed = catalog.prune_clusters({"2020/a"})
        assert removed == 0


# ---------------------------------------------------------------------------
# Mtime-based cache logic
# ---------------------------------------------------------------------------
class TestMtimeCache:
    """Verifying that mtime comparisons enable cache hits and misses."""

    def test_cache_hit_when_mtime_matches(self, catalog):
        catalog.put_cluster("2020/ev", folder_mtime=100.0, file_count=5)
        row = catalog.get_cluster("2020/ev")
        assert row["folder_mtime"] == 100.0
        # Caller compares this to os.stat().st_mtime — if equal, skip rescan

    def test_cache_miss_when_mtime_differs(self, catalog):
        catalog.put_cluster("2020/ev", folder_mtime=100.0, file_count=5)
        row = catalog.get_cluster("2020/ev")
        # Simulate folder modification: filesystem mtime is now 200.0
        disk_mtime = 200.0
        assert row["folder_mtime"] != disk_mtime
        # Caller should rescan and update the row


# ---------------------------------------------------------------------------
# File-hash operations
# ---------------------------------------------------------------------------
class TestFileHashes:
    """Insert, retrieve, and prune per-file hash rows."""

    def test_empty_catalog_returns_empty_dict(self, catalog):
        assert catalog.get_file_hashes() == {}

    def test_put_then_get(self, catalog):
        catalog.put_file_hashes([
            ("2020/ev/IMG_001.jpg", 1024, 100.0, "abc123", "def456"),
            ("2020/ev/IMG_002.jpg", 2048, 200.0, "ghi789", "jkl012"),
        ])
        hashes = catalog.get_file_hashes()
        assert len(hashes) == 2
        assert hashes["2020/ev/IMG_001.jpg"] == (1024, "abc123", "def456")
        assert hashes["2020/ev/IMG_002.jpg"] == (2048, "ghi789", "jkl012")

    def test_get_by_size(self, catalog):
        catalog.put_file_hashes([
            ("a.jpg", 1024, 100.0, "h1", "f1"),
            ("b.jpg", 1024, 200.0, "h2", "f2"),
            ("c.jpg", 2048, 300.0, "h3", "f3"),
        ])
        by_size = catalog.get_file_hashes_by_size()
        assert len(by_size[1024]) == 2
        assert len(by_size[2048]) == 1

    def test_upsert_updates_existing(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.0, "h1", "f1")])
        catalog.put_file_hashes([("a.jpg", 1024, 100.0, "h1_new", "f1_new")])
        hashes = catalog.get_file_hashes()
        assert hashes["a.jpg"] == (1024, "h1_new", "f1_new")

    def test_prune_files(self, catalog):
        catalog.put_file_hashes([
            ("a.jpg", 100, 1.0, None, None),
            ("b.jpg", 200, 2.0, None, None),
            ("c.jpg", 300, 3.0, None, None),
        ])
        removed = catalog.prune_files({"a.jpg", "c.jpg"})
        assert removed == 1
        hashes = catalog.get_file_hashes()
        assert "b.jpg" not in hashes
        assert "a.jpg" in hashes

    def test_prune_files_empty(self, catalog):
        assert catalog.prune_files(set()) == 0

    def test_partial_hash_only(self, catalog):
        """A file can have a partial hash but no full hash yet."""
        catalog.put_file_hashes([("a.jpg", 1024, 100.0, "partial", None)])
        hashes = catalog.get_file_hashes()
        assert hashes["a.jpg"] == (1024, "partial", None)
