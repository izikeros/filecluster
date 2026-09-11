"""Tests for the per-library SQLite catalog.

Covers creation, cluster CRUD, file-hash CRUD, mtime-based cache hits/misses,
pruning of stale rows, context-manager lifecycle, read-only opening, and
backup/restore integrity.
"""

import os
import sqlite3

import pytest

from filecluster.catalog import HashAlgo, HashMode, LibraryCatalog


def _make_media(root, rel, data=b"some jpeg bytes"):
    """Create a media file at *root/rel* and return its path."""
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _stored_exif(library_path, rel):
    """Read the exif_date column for one file row, bypassing the API."""
    db = library_path / LibraryCatalog.DB_FILENAME
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT exif_date FROM files WHERE path = ?", (rel,)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


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
        with pytest.raises(sqlite3.ProgrammingError):
            cat.get_cluster("anything")

    def test_migrates_v1_catalog_to_current(self, tmp_path):
        # Build a schema-v1 files table (no hash_algo / crc32 columns and no
        # library_settings table).
        db = tmp_path / LibraryCatalog.DB_FILENAME
        conn = sqlite3.connect(str(db))
        conn.executescript(
            """
            CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
            INSERT INTO schema_version (version) VALUES (1);
            CREATE TABLE files (
                path TEXT PRIMARY KEY, size INTEGER, mtime REAL,
                partial_hash TEXT, full_hash TEXT, exif_date TEXT, scanned_at TEXT
            );
            INSERT INTO files (path, size, mtime, partial_hash, full_hash)
            VALUES ('old.jpg', 10, 1.0, 'p', 'f');
            """
        )
        conn.commit()
        conn.close()

        with LibraryCatalog.open(tmp_path) as cat:
            cols = {r["name"] for r in cat._conn.execute("PRAGMA table_info(files)")}
            assert {"hash_algo", "crc32"} <= cols
            assert cat.stats()["schema_version"] == 3
            # No policy is pinned until the first build.
            assert cat.get_hash_policy() is None
            rec = cat.get_file_records()["old.jpg"]
            # Existing rows read back as legacy hashing.
            assert rec.hash_algo is None
            assert rec.uses_legacy_hashes
            assert rec.crc32 is None


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
        assert stats["crc32_checksums"] == 0
        assert stats["schema_version"] == 3
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

    def test_verify_deep_classifies_content(self, tmp_path):
        from PIL import Image

        good = tmp_path / "good.jpg"
        Image.new("RGB", (16, 16), (10, 20, 30)).save(good, "JPEG")
        broken = tmp_path / "broken.jpg"
        data = good.read_bytes()
        broken.write_bytes(data[: len(data) // 2])

        # Build with real hashes so the deep re-hash has a genuine baseline.
        LibraryCatalog.build(tmp_path)
        with LibraryCatalog.open(tmp_path) as cat:
            baseline = cat.get_file_hashes()["good.jpg"]
            result = cat.verify(tmp_path, deep=True)
            # Baseline hashes are untouched by a deep verify.
            assert cat.get_file_hashes()["good.jpg"] == baseline

        assert result["decoded_ok"] == ["good.jpg"]
        assert result["corrupt"] == ["broken.jpg"]

    def test_verify_deep_verifies_undecodable_via_full_hash(self, tmp_path):
        # A RAW file cannot be decoded here, but the default build still gives
        # it a full SHA-1 hash, so the deep re-hash can still verify it.
        raw = tmp_path / "photo.cr2"
        raw.write_bytes(b"raw payload bytes")
        LibraryCatalog.build(tmp_path)
        with LibraryCatalog.open(tmp_path) as cat:
            result = cat.verify(tmp_path, deep=True)
        assert result["decoded_ok"] == ["photo.cr2"]

    def test_verify_deep_detects_bit_rot_via_hash(self, tmp_path):
        from PIL import Image

        photo = tmp_path / "p.jpg"
        Image.new("RGB", (16, 16), (5, 5, 5)).save(photo, "JPEG")
        LibraryCatalog.build(tmp_path)

        # Flip a byte in place, then restore size and mtime so the row still
        # looks up to date. The decode may pass, but the stored hash will not.
        with LibraryCatalog.open(tmp_path) as cat:
            mtime = cat.get_file_records()["p.jpg"].mtime
        data = bytearray(photo.read_bytes())
        data[len(data) // 2] ^= 0xFF
        photo.write_bytes(bytes(data))
        os.utime(photo, (mtime, mtime))

        with LibraryCatalog.open(tmp_path) as cat:
            result = cat.verify(tmp_path, deep=True)
        assert result["ok"] == ["p.jpg"]  # size/mtime unchanged
        assert result["corrupt"] == ["p.jpg"]  # but content hash mismatches

    def test_verify_deep_skips_hash_check_for_stale_files(self, tmp_path):
        from PIL import Image

        photo = tmp_path / "p.jpg"
        Image.new("RGB", (16, 16), (5, 5, 5)).save(photo, "JPEG")
        LibraryCatalog.build(tmp_path)

        # Re-save: the content and size/mtime change, so the row is "stale".
        # An edit must never be reported as corruption.
        Image.new("RGB", (24, 24), (9, 9, 9)).save(photo, "JPEG")

        with LibraryCatalog.open(tmp_path) as cat:
            result = cat.verify(tmp_path, deep=True)
        assert result["stale"] == ["p.jpg"]
        assert result["corrupt"] == []
        assert result["decoded_ok"] == ["p.jpg"]

    def test_verify_deep_crc32_verifies_undecodable(self, tmp_path):
        # With only a short hash there is no full hash, so CRC32 is the sole
        # baseline for an undecodable RAW file.
        raw = tmp_path / "photo.cr2"
        raw.write_bytes(b"raw payload bytes")
        LibraryCatalog.build(tmp_path, image_hash=HashMode.SHORT, crc32=True)

        with LibraryCatalog.open(tmp_path) as cat:
            rec = cat.get_file_records()["photo.cr2"]
            assert rec.full_hash is None
            assert rec.crc32 is not None
            ok = cat.verify(tmp_path, deep=True)
        assert ok["decoded_ok"] == ["photo.cr2"]

        # Same length, one byte changed; restore mtime so the row still matches.
        raw.write_bytes(b"raw payload bytez")
        os.utime(raw, (rec.mtime, rec.mtime))
        with LibraryCatalog.open(tmp_path) as cat:
            bad = cat.verify(tmp_path, deep=True)
        assert bad["corrupt"] == ["photo.cr2"]

    def test_verify_shallow_has_no_content_buckets(self, tmp_path):
        good = tmp_path / "good.jpg"
        good.write_bytes(b"bytes")
        with LibraryCatalog.open(tmp_path) as cat:
            st = good.stat()
            cat.put_file_hashes([("good.jpg", st.st_size, st.st_mtime, "p", "f")])
            result = cat.verify(tmp_path)
        assert "corrupt" not in result
        assert set(result) == {"ok", "stale", "missing"}

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
# Building the catalog from disk
# ---------------------------------------------------------------------------
class TestBuild:
    """`LibraryCatalog.build` scanning an organised library."""

    def test_build_populates_file_rows(self, tmp_path):
        _make_media(tmp_path, "2020/[2020_06_15]_trip/a.jpg", b"aaaa")
        _make_media(tmp_path, "2020/[2020_06_15]_trip/b.jpg", b"bbbbbb")

        result = LibraryCatalog.build(tmp_path)

        assert result["mode"] == "update"
        assert result["scanned"] == 2
        assert result["added"] == 2
        assert result["updated"] == 0
        assert result["backup"] is None

        with LibraryCatalog.open(tmp_path) as cat:
            hashes = cat.get_file_hashes()
        assert set(hashes) == {
            "2020/[2020_06_15]_trip/a.jpg",
            "2020/[2020_06_15]_trip/b.jpg",
        }
        # Images receive both short and full hashes by default.
        size, partial, full = hashes["2020/[2020_06_15]_trip/a.jpg"]
        assert size == 4
        assert partial is not None
        assert full is not None

    def test_build_populates_cluster_rows(self, tmp_path):
        _make_media(tmp_path, "2020/[2020_06_15]_trip/a.jpg", b"aaaa")
        _make_media(tmp_path, "2020/[2020_06_15]_trip/b.jpg", b"bbbbbb")
        _make_media(tmp_path, "2021/[2021_01_02]_home/c.jpg", b"cccc")

        result = LibraryCatalog.build(tmp_path)

        assert result["clusters_scanned"] == 2
        assert result["clusters_added"] == 2
        assert result["clusters_updated"] == 0

        with LibraryCatalog.open(tmp_path) as cat:
            clusters = {row["path"]: row for row in cat.get_all_clusters()}
        assert set(clusters) == {
            "2020/[2020_06_15]_trip",
            "2021/[2021_01_02]_home",
        }
        assert clusters["2020/[2020_06_15]_trip"]["file_count"] == 2
        assert clusters["2021/[2021_01_02]_home"]["file_count"] == 1
        assert clusters["2020/[2020_06_15]_trip"]["start_date"] is not None
        assert clusters["2020/[2020_06_15]_trip"]["end_date"] is not None

    def test_build_skips_unchanged_clusters_on_update(self, tmp_path):
        _make_media(tmp_path, "2020/[2020_06_15]_trip/a.jpg", b"aaaa")
        LibraryCatalog.build(tmp_path)

        result = LibraryCatalog.build(tmp_path)
        assert result["clusters_skipped"] == 1
        assert result["clusters_added"] == 0
        assert result["clusters_updated"] == 0

    def test_build_prunes_vanished_clusters(self, tmp_path):
        trip = tmp_path / "2020" / "[2020_06_15]_trip"
        home = tmp_path / "2021" / "[2021_01_02]_home"
        _make_media(tmp_path, "2020/[2020_06_15]_trip/a.jpg", b"aaaa")
        _make_media(tmp_path, "2021/[2021_01_02]_home/b.jpg", b"bbbb")
        LibraryCatalog.build(tmp_path)

        for child in home.iterdir():
            child.unlink()
        home.rmdir()
        (tmp_path / "2021").rmdir()

        result = LibraryCatalog.build(tmp_path)
        assert result["clusters_pruned"] == 1
        with LibraryCatalog.open(tmp_path) as cat:
            assert {row["path"] for row in cat.get_all_clusters()} == {
                "2020/[2020_06_15]_trip"
            }
        assert trip.exists()

    def test_build_skips_the_catalog_file_itself(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        result = LibraryCatalog.build(tmp_path)
        with LibraryCatalog.open(tmp_path) as cat:
            assert set(cat.get_file_hashes()) == {"a.jpg"}
        assert result["scanned"] == 1

    def test_update_skips_unchanged_and_adds_new(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path)

        _make_media(tmp_path, "b.jpg")
        result = LibraryCatalog.build(tmp_path)

        assert result["scanned"] == 2
        assert result["added"] == 1
        assert result["skipped"] == 1
        assert result["updated"] == 0

    def test_update_prunes_vanished_files(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        gone = _make_media(tmp_path, "b.jpg")
        LibraryCatalog.build(tmp_path)

        gone.unlink()
        result = LibraryCatalog.build(tmp_path)

        assert result["pruned"] == 1
        with LibraryCatalog.open(tmp_path) as cat:
            assert set(cat.get_file_hashes()) == {"a.jpg"}

    def test_rebuild_backs_up_then_reindexes(self, tmp_path):
        _make_media(tmp_path, "2020/[2020_06_15]_trip/a.jpg")
        LibraryCatalog.build(tmp_path)

        result = LibraryCatalog.build(tmp_path, rebuild=True)

        assert result["mode"] == "rebuild"
        assert result["backup"] is not None
        # rebuild clears first, so every file/cluster counts as newly added
        assert result["added"] == 1
        assert result["skipped"] == 0
        assert result["clusters_added"] == 1
        assert result["clusters_skipped"] == 0
        assert len(LibraryCatalog.list_backups(tmp_path)) == 1

    def test_default_hashes_images_fully_and_videos_short(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        _make_media(tmp_path, "clip.mp4")
        result = LibraryCatalog.build(tmp_path)

        with LibraryCatalog.open(tmp_path) as cat:
            hashes = cat.get_file_hashes()
        assert hashes["a.jpg"][1] is not None
        assert hashes["a.jpg"][2] is not None
        assert hashes["clip.mp4"][1] is not None
        assert hashes["clip.mp4"][2] is None
        assert result["image_hash"] == "full"
        assert result["video_hash"] == "short"

    def test_default_build_uses_legacy_algo(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        result = LibraryCatalog.build(tmp_path)
        assert result["hash_algo"] == "legacy"
        assert result["crc32"] is False
        with LibraryCatalog.open(tmp_path) as cat:
            rec = cat.get_file_records()["a.jpg"]
        # Legacy split: MD5 partial (32 hex) + SHA-1 full (40 hex), NULL algo.
        assert rec.hash_algo is None
        assert rec.uses_legacy_hashes
        assert len(rec.partial_hash) == 32
        assert len(rec.full_hash) == 40
        assert rec.crc32 is None

    def test_blake3_build_records_algo_and_hashes(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        result = LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3)
        assert result["hash_algo"] == "blake3"
        with LibraryCatalog.open(tmp_path) as cat:
            rec = cat.get_file_records()["a.jpg"]
        assert rec.hash_algo == "blake3"
        assert not rec.uses_legacy_hashes
        # Both partial and full use blake3 (64 hex chars each).
        assert len(rec.partial_hash) == 64
        assert len(rec.full_hash) == 64

    def test_crc32_flag_populates_checksums(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        result = LibraryCatalog.build(tmp_path, crc32=True)
        assert result["crc32"] is True
        with LibraryCatalog.open(tmp_path) as cat:
            rec = cat.get_file_records()["a.jpg"]
            assert cat.stats()["crc32_checksums"] == 1
        assert rec.crc32 is not None and len(rec.crc32) == 8

    def test_crc32_is_independent_of_hash_algo(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3, crc32=True)
        with LibraryCatalog.open(tmp_path) as cat:
            rec = cat.get_file_records()["a.jpg"]
        assert rec.hash_algo == "blake3"
        assert rec.crc32 is not None

    @pytest.mark.parametrize(
        ("image_mode", "video_mode", "image_full", "video_full"),
        [
            (HashMode.SHORT, HashMode.SHORT, False, False),
            (HashMode.SHORT, HashMode.FULL, False, True),
            (HashMode.FULL, HashMode.FULL, True, True),
        ],
    )
    def test_image_and_video_hash_modes_are_independent(
        self, tmp_path, image_mode, video_mode, image_full, video_full
    ):
        _make_media(tmp_path, "a.jpg")
        _make_media(tmp_path, "clip.mp4")

        LibraryCatalog.build(
            tmp_path,
            image_hash=image_mode,
            video_hash=video_mode,
        )

        with LibraryCatalog.open(tmp_path) as cat:
            hashes = cat.get_file_hashes()
        assert (hashes["a.jpg"][2] is not None) is image_full
        assert (hashes["clip.mp4"][2] is not None) is video_full

    def test_short_policy_preserves_valid_existing_full_hash(self, tmp_path):
        media = _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path)
        with LibraryCatalog.open(tmp_path) as cat:
            original = cat.get_file_hashes()["a.jpg"][2]

        LibraryCatalog.build(tmp_path, image_hash=HashMode.SHORT)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_file_hashes()["a.jpg"][2] == original

        media.write_bytes(b"changed image")
        LibraryCatalog.build(tmp_path, image_hash=HashMode.SHORT)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_file_hashes()["a.jpg"][2] is None

    def test_no_exif_leaves_exif_column_null(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, read_exif=False)
        assert _stored_exif(tmp_path, "a.jpg") is None

    def test_empty_library_builds_empty_catalog(self, tmp_path):
        result = LibraryCatalog.build(tmp_path)
        assert result["scanned"] == 0
        assert result["added"] == 0
        assert result["clusters_scanned"] == 0
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_file_hashes() == {}
            assert cat.get_all_clusters() == []


# ---------------------------------------------------------------------------
# Pinned hashing policy
# ---------------------------------------------------------------------------
class TestHashPolicy:
    def test_first_build_pins_policy(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3, crc32=True)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": "blake3", "crc32": True}

    def test_default_build_pins_legacy_policy(self, tmp_path):
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": None, "crc32": False}

    def test_conflicting_explicit_policy_is_refused(self, tmp_path):
        from filecluster.exceptions import HashPolicyConflictError

        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3)
        with pytest.raises(HashPolicyConflictError):
            LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.SHA1)

    def test_conflicting_crc32_is_refused(self, tmp_path):
        from filecluster.exceptions import HashPolicyConflictError

        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, crc32=True)
        with pytest.raises(HashPolicyConflictError):
            LibraryCatalog.build(tmp_path, crc32=False)

    def test_default_flags_reuse_stored_policy(self, tmp_path):
        # A plain re-run (build() defaults marked non-explicit) must keep the
        # pinned blake3+crc32 policy rather than reverting to sha1/no-crc32.
        _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3, crc32=True)
        _make_media(tmp_path, "b.jpg")
        LibraryCatalog.build(
            tmp_path,
            hash_algo=None,
            crc32=False,
            hash_algo_explicit=False,
            crc32_explicit=False,
        )
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": "blake3", "crc32": True}
            rec = cat.get_file_records()["b.jpg"]
            assert rec.hash_algo == "blake3"  # new file followed the policy
            assert rec.crc32 is not None

    def test_rebuild_adopts_new_policy(self, tmp_path):
        media = _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_file_records()["a.jpg"].hash_algo == "blake3"

        # Rebuild switches the whole library to an explicit SHA-1 policy.
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.SHA1, rebuild=True)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": "sha1", "crc32": False}
            rec = cat.get_file_records()["a.jpg"]
            assert rec.hash_algo == "sha1"
        assert media.exists()

    def test_rebuild_can_adopt_legacy_policy(self, tmp_path):
        media = _make_media(tmp_path, "a.jpg")
        LibraryCatalog.build(tmp_path, hash_algo=HashAlgo.BLAKE3)

        # Rebuild with hash_algo=None reverts to the legacy (NULL) policy.
        LibraryCatalog.build(tmp_path, hash_algo=None, rebuild=True)
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": None, "crc32": False}
            rec = cat.get_file_records()["a.jpg"]
            assert rec.hash_algo is None
            assert rec.uses_legacy_hashes
        assert media.exists()


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
            ("clear_clusters", ()),
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
