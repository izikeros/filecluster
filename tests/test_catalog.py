"""Tests for the per-library SQLite catalog.

Covers creation, cluster CRUD, file-hash CRUD, mtime-based cache hits/misses,
pruning of stale rows, context-manager lifecycle, read-only opening, and
backup/restore integrity.
"""

import sqlite3
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
        catalog.put_file_hashes(
            [
                ("2020/ev/IMG_001.jpg", 1024, 100.0, "abc123", "def456"),
                ("2020/ev/IMG_002.jpg", 2048, 200.0, "ghi789", "jkl012"),
            ]
        )
        hashes = catalog.get_file_hashes()
        assert len(hashes) == 2
        assert hashes["2020/ev/IMG_001.jpg"] == (1024, "abc123", "def456")
        assert hashes["2020/ev/IMG_002.jpg"] == (2048, "ghi789", "jkl012")

    def test_get_by_size(self, catalog):
        catalog.put_file_hashes(
            [
                ("a.jpg", 1024, 100.0, "h1", "f1"),
                ("b.jpg", 1024, 200.0, "h2", "f2"),
                ("c.jpg", 2048, 300.0, "h3", "f3"),
            ]
        )
        by_size = catalog.get_file_hashes_by_size()
        assert len(by_size[1024]) == 2
        assert len(by_size[2048]) == 1

    def test_upsert_updates_existing(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.0, "h1", "f1")])
        catalog.put_file_hashes([("a.jpg", 1024, 100.0, "h1_new", "f1_new")])
        hashes = catalog.get_file_hashes()
        assert hashes["a.jpg"] == (1024, "h1_new", "f1_new")

    def test_prune_files(self, catalog):
        catalog.put_file_hashes(
            [
                ("a.jpg", 100, 1.0, None, None),
                ("b.jpg", 200, 2.0, None, None),
                ("c.jpg", 300, 3.0, None, None),
            ]
        )
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


# ---------------------------------------------------------------------------
# File records (mtime-aware cache validation)
# ---------------------------------------------------------------------------
class TestFileRecords:
    def test_get_file_records_carries_mtime(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.5, "p", "f")])
        records = catalog.get_file_records()
        assert records["a.jpg"].size == 1024
        assert records["a.jpg"].mtime == 100.5
        assert records["a.jpg"].partial_hash == "p"
        assert records["a.jpg"].full_hash == "f"

    def test_record_matches_unchanged_file(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.5, "p", "f")])
        record = catalog.get_file_records()["a.jpg"]
        assert record.matches(1024, 100.5)

    def test_record_rejects_new_mtime(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.5, "p", "f")])
        record = catalog.get_file_records()["a.jpg"]
        assert not record.matches(1024, 200.0)

    def test_record_rejects_new_size(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1024, 100.5, "p", "f")])
        record = catalog.get_file_records()["a.jpg"]
        assert not record.matches(2048, 100.5)

    def test_delete_file_rows(self, catalog):
        catalog.put_file_hashes(
            [
                ("a.jpg", 1, 1.0, None, None),
                ("b.jpg", 2, 2.0, None, None),
            ]
        )
        assert catalog.delete_file_rows(["a.jpg"]) == 1
        assert set(catalog.get_file_hashes()) == {"b.jpg"}

    def test_delete_file_rows_empty(self, catalog):
        assert catalog.delete_file_rows([]) == 0


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------
class TestMaintenance:
    def test_stats_reports_row_counts(self, catalog):
        catalog.put_cluster("2024/[2024_01_01]_ev", file_count=3)
        catalog.put_file_hashes(
            [
                ("a.jpg", 100, 1.0, "p", "f"),
                ("b.jpg", 200, 2.0, "p2", None),
            ]
        )
        stats = catalog.stats()
        assert stats["clusters"] == 1
        assert stats["files"] == 2
        assert stats["partial_hashes"] == 2
        assert stats["full_hashes"] == 1
        assert stats["total_bytes"] == 300
        assert stats["schema_version"] == 1
        assert stats["db_bytes"] > 0

    def test_verify_classifies_rows(self, tmp_path):
        good = tmp_path / "good.jpg"
        good.write_bytes(b"unchanged")
        changed = tmp_path / "changed.jpg"
        changed.write_bytes(b"before")

        with LibraryCatalog.open(tmp_path) as cat:
            good_st = good.stat()
            changed_st = changed.stat()
            cat.put_file_hashes(
                [
                    ("good.jpg", good_st.st_size, good_st.st_mtime, "p", "f"),
                    (
                        "changed.jpg",
                        changed_st.st_size + 5,
                        changed_st.st_mtime,
                        "p",
                        "f",
                    ),
                    ("gone.jpg", 10, 1.0, "p", "f"),
                ]
            )
            result = cat.verify(tmp_path)

        assert result["ok"] == ["good.jpg"]
        assert result["stale"] == ["changed.jpg"]
        assert result["missing"] == ["gone.jpg"]

    def test_vacuum_keeps_data(self, catalog):
        catalog.put_file_hashes([("a.jpg", 1, 1.0, "p", "f")])
        catalog.vacuum()
        assert "a.jpg" in catalog.get_file_hashes()


# ---------------------------------------------------------------------------
# Backup and restore
# ---------------------------------------------------------------------------
class TestBackupRestore:
    def test_list_backups_is_ordered(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 1, 1.0, "p", "f")])
        first = LibraryCatalog.backup(tmp_path)
        second = LibraryCatalog.backup(tmp_path)

        backups = LibraryCatalog.list_backups(tmp_path)
        assert backups == sorted([first, second])
        assert backups[-1] == second

    def test_restore_brings_back_deleted_rows(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 1, 1.0, "p", "f")])
        LibraryCatalog.backup(tmp_path)

        with LibraryCatalog.open(tmp_path) as cat:
            cat.clear_file_hashes()
            assert cat.get_file_hashes() == {}

        LibraryCatalog.restore(tmp_path)

        with LibraryCatalog.open(tmp_path) as cat:
            assert "a.jpg" in cat.get_file_hashes()

    def test_restore_backs_up_the_current_database_first(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 1, 1.0, "p", "f")])
        LibraryCatalog.backup(tmp_path)
        LibraryCatalog.restore(tmp_path)

        assert len(LibraryCatalog.list_backups(tmp_path)) == 2

    def test_restore_from_explicit_backup(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("first.jpg", 1, 1.0, "p", "f")])
        chosen = LibraryCatalog.backup(tmp_path)

        with LibraryCatalog.open(tmp_path) as cat:
            cat.clear_file_hashes()
            cat.put_file_hashes([("second.jpg", 2, 2.0, "p", "f")])

        LibraryCatalog.restore(tmp_path, chosen)

        with LibraryCatalog.open(tmp_path) as cat:
            assert set(cat.get_file_hashes()) == {"first.jpg"}

    def test_restore_without_backup_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            LibraryCatalog.restore(tmp_path)

    def test_restore_from_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            LibraryCatalog.restore(tmp_path, tmp_path / "nope.bak")

    def test_backup_includes_rows_committed_while_a_reader_holds_a_snapshot(
        self, tmp_path
    ):
        """A copy of the main file alone would omit still-unmerged WAL rows."""
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 10, 1.0, "p", "f")])
            reader = sqlite3.connect(str(cat.db_path), timeout=5)
            try:
                reader.execute("BEGIN")
                reader.execute("SELECT COUNT(*) FROM files").fetchone()
                backup_path = LibraryCatalog.backup(tmp_path)
            finally:
                reader.close()

        assert backup_path is not None
        conn = sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True)
        try:
            rows = conn.execute("SELECT path FROM files").fetchall()
        finally:
            conn.close()
        assert [r[0] for r in rows] == ["a.jpg"]

    def test_restore_rejects_a_corrupt_backup(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("keep.jpg", 10, 1.0, "p", "f")])
        junk = tmp_path / ".filecluster.corrupt.bak"
        junk.write_bytes(b"definitely not a database" * 50)

        with pytest.raises(sqlite3.DatabaseError):
            LibraryCatalog.restore(tmp_path, junk)

        with LibraryCatalog.open(tmp_path) as cat:
            assert "keep.jpg" in cat.get_file_hashes()


# ---------------------------------------------------------------------------
# Read-only mode
# ---------------------------------------------------------------------------
class TestReadOnlyMode:
    def test_missing_catalog_raises_instead_of_being_created(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            LibraryCatalog.open(tmp_path, read_only=True)

        assert list(tmp_path.iterdir()) == []

    def test_existing_rows_are_still_readable(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 10, 1.0, "p", "f")])

        with LibraryCatalog.open(tmp_path, read_only=True) as cat:
            assert "a.jpg" in cat.get_file_hashes()
            assert cat.read_only is True

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("put_file_hashes", ([("b.jpg", 1, 1.0, None, None)],)),
            ("clear_file_hashes", ()),
            ("delete_file_rows", (["a.jpg"],)),
            ("prune_files", (set(),)),
            ("prune_clusters", (set(),)),
            ("put_cluster", ("2020/ev",)),
            ("vacuum", ()),
        ],
    )
    def test_writes_are_refused(self, tmp_path, method, args):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 10, 1.0, "p", "f")])

        with (
            LibraryCatalog.open(tmp_path, read_only=True) as cat,
            pytest.raises(sqlite3.OperationalError),
        ):
            getattr(cat, method)(*args)

        with LibraryCatalog.open(tmp_path) as cat:
            assert set(cat.get_file_hashes()) == {"a.jpg"}

    def test_reading_does_not_alter_the_database_file(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 10, 1.0, "p", "f")])
            db_path = cat.db_path
        before = db_path.read_bytes()

        with LibraryCatalog.open(tmp_path, read_only=True) as cat:
            cat.get_file_hashes()
            cat.stats()
            cat.verify()

        assert db_path.read_bytes() == before
