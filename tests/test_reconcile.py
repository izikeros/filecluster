"""Tests for the reconcile module.

Covers LibraryIndex (indexing, hash caching and mtime-based invalidation,
force-reindex with backup), file matching (size, partial hash, full hash,
filename), source-mode detection across event-folder, flat and mixed layouts,
recursion and sidecar handling, duplicates found inside the source itself,
matching against several libraries, move/copy/scan actions, overwrite
protection, rejection of overlapping roots, dry runs that write nothing at
all, and dry-run vs execute behaviour.
"""

import os
import time
from pathlib import Path

import pytest

from filecluster.catalog import LibraryCatalog
from filecluster.exceptions import OverlappingPathsError
from filecluster.file_operations import CopyOp, MoveOp, SkipOp
from filecluster.reconcile import (
    FileMatch,
    FileStatus,
    FolderResult,
    FolderStatus,
    LibraryIndex,
    ReconcileAction,
    ReconcilePlan,
    SourceMode,
    _library_dest_for_event_folder,
    _match_file,
    _unique_roots,
    detect_source_mode,
    reconcile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write(path: Path, content: bytes = b"hello world") -> Path:
    """Write *content* to *path*, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _populate_library(lib: Path) -> dict[str, Path]:
    """Create a small fake library and return name→path mapping."""
    files = {}
    files["img1"] = _write(
        lib / "2024" / "[2024_01_15]_birthday" / "IMG_001.jpg",
        b"library-image-one",
    )
    files["img2"] = _write(
        lib / "2024" / "[2024_01_15]_birthday" / "IMG_002.jpg",
        b"library-image-two",
    )
    files["img3"] = _write(
        lib / "2023" / "[2023_06_20]_vacation" / "DSC_100.jpg",
        b"library-vacation-shot",
    )
    return files


# ---------------------------------------------------------------------------
# Source mode detection
# ---------------------------------------------------------------------------
class TestDetectSourceMode:
    def test_event_folder_mode(self, tmp_path):
        (tmp_path / "[2024_01_15]_birthday").mkdir()
        _write(tmp_path / "[2024_01_15]_birthday" / "IMG_001.jpg")
        assert detect_source_mode(tmp_path) == SourceMode.EVENT_FOLDERS

    def test_flat_mode_no_event_folders(self, tmp_path):
        _write(tmp_path / "IMG_001.jpg")
        _write(tmp_path / "IMG_002.jpg")
        assert detect_source_mode(tmp_path) == SourceMode.FLAT

    def test_flat_mode_non_matching_dirs(self, tmp_path):
        (tmp_path / "random_folder").mkdir()
        _write(tmp_path / "IMG_001.jpg")
        assert detect_source_mode(tmp_path) == SourceMode.FLAT

    def test_empty_dir_is_flat(self, tmp_path):
        assert detect_source_mode(tmp_path) == SourceMode.FLAT


# ---------------------------------------------------------------------------
# Library destination
# ---------------------------------------------------------------------------
class TestLibraryDest:
    def test_event_folder_placed_under_year(self):
        lib = Path("/photos")
        result = _library_dest_for_event_folder("[2024_01_15]_birthday", lib)
        assert result == Path("/photos/2024/[2024_01_15]_birthday")

    def test_unknown_year_for_bad_name(self):
        lib = Path("/photos")
        result = _library_dest_for_event_folder("random", lib)
        assert result == Path("/photos/unknown/random")


# ---------------------------------------------------------------------------
# LibraryIndex
# ---------------------------------------------------------------------------
class TestLibraryIndex:
    def test_indexes_all_supported_files(self, tmp_path):
        lib = tmp_path / "library"
        _populate_library(lib)
        idx = LibraryIndex(lib)
        assert idx.total_files == 3
        idx.close()

    def test_ignores_unsupported_extensions(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "readme.txt", b"not a photo")
        _write(lib / "photo.jpg", b"a photo")
        idx = LibraryIndex(lib)
        assert idx.total_files == 1
        idx.close()

    def test_candidates_by_size(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        content = b"exact-size-content"
        _write(lib / "a.jpg", content)
        _write(lib / "b.jpg", content)
        idx = LibraryIndex(lib)
        candidates = idx.candidates_by_size(len(content))
        assert len(candidates) == 2
        idx.close()

    def test_no_candidates_for_missing_size(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content")
        idx = LibraryIndex(lib)
        assert idx.candidates_by_size(999999) == []
        idx.close()

    def test_files_by_name(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "IMG_001.jpg", b"content")
        idx = LibraryIndex(lib)
        matches = idx.files_by_name("IMG_001.jpg")
        assert len(matches) == 1
        assert matches[0].name == "IMG_001.jpg"
        idx.close()

    def test_files_by_name_case_insensitive(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "IMG_001.JPG", b"content")
        idx = LibraryIndex(lib)
        matches = idx.files_by_name("img_001.jpg")
        assert len(matches) == 1
        idx.close()

    def test_lazy_partial_hash_computation(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content-for-hashing")
        idx = LibraryIndex(lib)
        fpath = lib / "a.jpg"
        # Not computed yet in the in-memory cache
        h = idx.partial_hash(fpath)
        assert h is not None
        assert isinstance(h, str)
        # Second call returns the cached value
        assert idx.partial_hash(fpath) == h
        idx.close()

    def test_lazy_full_hash_computation(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content-for-hashing")
        idx = LibraryIndex(lib)
        fpath = lib / "a.jpg"
        h = idx.full_hash(fpath)
        assert h is not None
        assert isinstance(h, str)
        assert idx.full_hash(fpath) == h
        idx.close()

    def test_flush_writes_to_catalog(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content")
        idx = LibraryIndex(lib)
        # Force hash computation
        fpath = lib / "a.jpg"
        idx.partial_hash(fpath)
        idx.flush()
        idx.close()

        # Verify catalog has the hash
        with LibraryCatalog.open(lib) as cat:
            hashes = cat.get_file_hashes()
            assert len(hashes) >= 1

    def test_incremental_uses_cached_hashes(self, tmp_path):
        """Second index build loads hashes from catalog, not recomputing."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content")

        # First run: compute and persist
        idx1 = LibraryIndex(lib)
        idx1.partial_hash(lib / "a.jpg")
        idx1.close()

        # Second run: should load from catalog
        idx2 = LibraryIndex(lib)
        assert str(lib / "a.jpg") in idx2._partial_cache
        idx2.close()


# ---------------------------------------------------------------------------
# Force reindex with backup
# ---------------------------------------------------------------------------
class TestForceReindex:
    def test_force_reindex_backs_up_catalog(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content")

        # Create initial catalog with some data
        idx1 = LibraryIndex(lib)
        idx1.partial_hash(lib / "a.jpg")
        idx1.close()

        # Verify catalog exists
        db_path = lib / ".filecluster.db"
        assert db_path.exists()

        # Force reindex — should create a .bak file
        idx2 = LibraryIndex(lib, force_reindex=True)
        idx2.close()

        bak_files = list(lib.glob(".filecluster.*.bak"))
        assert len(bak_files) == 1

    def test_force_reindex_clears_and_recomputes(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content-aaa")
        _write(lib / "b.jpg", b"content-bbb")

        # First index: compute partial hashes
        idx1 = LibraryIndex(lib)
        idx1.partial_hash(lib / "a.jpg")
        idx1.close()

        # Force reindex: should clear old hashes and eagerly recompute
        idx2 = LibraryIndex(lib, force_reindex=True)
        # Partial hashes are eagerly computed during force reindex
        assert str(lib / "a.jpg") in idx2._partial_cache
        assert str(lib / "b.jpg") in idx2._partial_cache
        idx2.close()

    def test_force_reindex_no_existing_catalog(self, tmp_path):
        """Force reindex on a fresh library (no catalog) works fine."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"content")

        idx = LibraryIndex(lib, force_reindex=True)
        assert idx.total_files == 1
        idx.close()

        # No backup created (nothing to back up)
        bak_files = list(lib.glob(".filecluster.*.bak"))
        assert len(bak_files) == 0


# ---------------------------------------------------------------------------
# File matching
# ---------------------------------------------------------------------------
class TestFileMatching:
    def test_duplicate_detected_by_content(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        content = b"identical-content-for-duplicate-test"
        _write(lib / "original.jpg", content)

        source = tmp_path / "source"
        source.mkdir()
        _write(source / "copy.jpg", content)

        idx = LibraryIndex(lib)
        match = _match_file(source / "copy.jpg", idx)
        assert match.status == FileStatus.DUPLICATE
        assert match.library_match is not None
        idx.close()

    def test_new_file_when_no_match(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "existing.jpg", b"library-content")

        source = tmp_path / "source"
        source.mkdir()
        _write(source / "new.jpg", b"completely-different-content")

        idx = LibraryIndex(lib)
        match = _match_file(source / "new.jpg", idx)
        assert match.status == FileStatus.NEW
        assert match.library_match is None
        idx.close()

    def test_same_size_different_content_is_new(self, tmp_path):
        """Files with the same size but different content are NEW."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "a.jpg", b"aaaa-content")

        source = tmp_path / "source"
        source.mkdir()
        _write(source / "b.jpg", b"bbbb-content")  # same length!

        idx = LibraryIndex(lib)
        match = _match_file(source / "b.jpg", idx)
        assert match.status == FileStatus.NEW
        idx.close()

    def test_name_collision_flagged(self, tmp_path):
        """Same filename in library but different content → NAME_COLLISION info."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "photo.jpg", b"original-library-content")

        source = tmp_path / "source"
        source.mkdir()
        _write(source / "photo.jpg", b"different-source-content-longer")

        idx = LibraryIndex(lib)
        match = _match_file(source / "photo.jpg", idx)
        assert match.status == FileStatus.NEW
        assert match.name_collision_path is not None
        idx.close()

    def test_empty_library_all_new(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        source.mkdir()
        _write(source / "a.jpg", b"aaa")

        idx = LibraryIndex(lib)
        match = _match_file(source / "a.jpg", idx)
        assert match.status == FileStatus.NEW
        idx.close()


# ---------------------------------------------------------------------------
# ReconcilePlan
# ---------------------------------------------------------------------------
class TestReconcilePlan:
    def test_empty_plan(self):
        plan = ReconcilePlan()
        assert plan.n_duplicates == 0
        assert plan.n_new == 0
        assert plan.n_moves == 0

    def test_summary_dict(self):
        plan = ReconcilePlan(
            file_matches=[
                FileMatch(source_path=Path("a.jpg"), status=FileStatus.DUPLICATE),
                FileMatch(source_path=Path("b.jpg"), status=FileStatus.NEW),
            ],
            source_mode=SourceMode.FLAT,
        )
        d = plan.summary_dict()
        assert d["duplicates"] == 1
        assert d["new"] == 1
        assert d["total_files"] == 2

    def test_csv_export(self, tmp_path):
        plan = ReconcilePlan(
            file_matches=[
                FileMatch(
                    source_path=Path("a.jpg"),
                    status=FileStatus.DUPLICATE,
                    library_match=Path("/lib/a.jpg"),
                ),
                FileMatch(source_path=Path("b.jpg"), status=FileStatus.NEW),
            ],
        )
        csv_path = tmp_path / "report.csv"
        n = plan.write_csv(csv_path)
        assert n == 2
        lines = csv_path.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 rows


# ---------------------------------------------------------------------------
# FolderResult
# ---------------------------------------------------------------------------
class TestFolderResult:
    def test_counts(self):
        fr = FolderResult(
            folder_name="[2024_01_15]_event",
            folder_path=Path("/src/[2024_01_15]_event"),
            status=FolderStatus.PARTIAL,
            files=[
                FileMatch(source_path=Path("a.jpg"), status=FileStatus.DUPLICATE),
                FileMatch(source_path=Path("b.jpg"), status=FileStatus.NEW),
                FileMatch(
                    source_path=Path("c.jpg"),
                    status=FileStatus.NEW,
                    name_collision_path=Path("/lib/c.jpg"),
                ),
            ],
        )
        assert fr.n_duplicates == 1
        assert fr.n_new == 2
        assert fr.n_name_collisions == 1


# ---------------------------------------------------------------------------
# Integration: reconcile() with event-folder source
# ---------------------------------------------------------------------------
class TestReconcileEventFolders:
    def test_all_duplicate_folder(self, tmp_path):
        lib = tmp_path / "library"
        content_a = b"library-content-aaa"
        content_b = b"library-content-bbb"
        _write(lib / "2024" / "[2024_01_15]_party" / "a.jpg", content_a)
        _write(lib / "2024" / "[2024_01_15]_party" / "b.jpg", content_b)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_party" / "a.jpg", content_a)
        _write(source / "[2024_01_15]_party" / "b.jpg", content_b)

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert plan.source_mode == SourceMode.EVENT_FOLDERS
        assert len(plan.folder_results) == 1
        assert plan.folder_results[0].status == FolderStatus.ALL_DUPLICATE
        assert plan.n_duplicates == 2
        assert plan.n_new == 0

    def test_all_new_folder(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        _write(lib / "2023" / "[2023_01_01]_existing" / "x.jpg", b"other-stuff")

        source = tmp_path / "source"
        _write(source / "[2024_06_20]_holiday" / "DSC_001.jpg", b"new-content-1")
        _write(source / "[2024_06_20]_holiday" / "DSC_002.jpg", b"new-content-2")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert len(plan.folder_results) == 1
        assert plan.folder_results[0].status == FolderStatus.ALL_NEW
        assert plan.n_new == 2
        assert plan.n_duplicates == 0

    def test_partial_folder(self, tmp_path):
        lib = tmp_path / "library"
        dup_content = b"this-is-a-duplicate-file"
        _write(lib / "2024" / "[2024_01_15]_party" / "dup.jpg", dup_content)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_party" / "dup.jpg", dup_content)
        _write(source / "[2024_01_15]_party" / "new.jpg", b"brand-new-file")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert len(plan.folder_results) == 1
        assert plan.folder_results[0].status == FolderStatus.PARTIAL
        assert plan.n_duplicates == 1
        assert plan.n_new == 1

    def test_execute_moves_files(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_03_10]_trip" / "photo.jpg", b"new-photo-data")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir, execute=True)

        assert plan.n_new == 1
        # File should have been moved to library
        dest = lib / "2024" / "[2024_03_10]_trip" / "photo.jpg"
        assert dest.exists()
        assert dest.read_bytes() == b"new-photo-data"
        # Source should be gone
        assert not (source / "[2024_03_10]_trip" / "photo.jpg").exists()

    def test_execute_moves_duplicates_aside(self, tmp_path):
        lib = tmp_path / "library"
        content = b"duplicate-content-here"
        _write(lib / "2024" / "[2024_01_01]_event" / "a.jpg", content)

        source = tmp_path / "source"
        _write(source / "[2024_01_01]_event" / "a.jpg", content)

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir, execute=True)

        assert plan.n_duplicates == 1
        # Duplicate moved to duplicates dir
        assert (dup_dir / "[2024_01_01]_event" / "a.jpg").exists()
        # Source gone
        assert not (source / "[2024_01_01]_event" / "a.jpg").exists()

    def test_multiple_event_folders(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "2024" / "[2024_01_01]_ev" / "old.jpg", b"old-lib-content")

        source = tmp_path / "source"
        _write(source / "[2024_01_01]_ev" / "old.jpg", b"old-lib-content")
        _write(source / "[2024_02_14]_val" / "new.jpg", b"valentines-photo")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert len(plan.folder_results) == 2
        statuses = {fr.folder_name: fr.status for fr in plan.folder_results}
        assert statuses["[2024_01_01]_ev"] == FolderStatus.ALL_DUPLICATE
        assert statuses["[2024_02_14]_val"] == FolderStatus.ALL_NEW


# ---------------------------------------------------------------------------
# Integration: reconcile() with flat source
# ---------------------------------------------------------------------------
class TestReconcileFlat:
    def test_flat_duplicates(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        content = b"flat-duplicate-content"
        _write(lib / "2024" / "[2024_01_01]_ev" / "img.jpg", content)

        source = tmp_path / "source"
        _write(source / "img.jpg", content)

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert plan.source_mode == SourceMode.FLAT
        assert plan.n_duplicates == 1
        assert plan.n_new == 0
        assert len(plan.folder_results) == 0  # no folder results in flat mode

    def test_flat_new_files(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "brand_new.jpg", b"entirely-new-content")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir)

        assert plan.source_mode == SourceMode.FLAT
        assert plan.n_new == 1
        assert plan.n_duplicates == 0

    def test_flat_execute_moves_to_library(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "photo.jpg", b"new-flat-photo")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir, execute=True)

        assert plan.n_new == 1
        # File should be somewhere under library (exact path depends on mtime)
        lib_files = list(lib.rglob("photo.jpg"))
        assert len(lib_files) == 1
        assert lib_files[0].read_bytes() == b"new-flat-photo"

    def test_flat_execute_moves_duplicates(self, tmp_path):
        lib = tmp_path / "library"
        content = b"flat-dup-content"
        _write(lib / "2024" / "[2024_01_01]_ev" / "x.jpg", content)

        source = tmp_path / "source"
        _write(source / "x.jpg", content)

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir, execute=True)

        assert plan.n_duplicates == 1
        assert (dup_dir / "x.jpg").exists()

    def test_dry_run_does_not_move(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "photo.jpg", b"dry-run-content")

        dup_dir = tmp_path / "duplicates"
        plan = reconcile(source, lib, dup_dir, execute=False)

        assert plan.n_new == 1
        # File should still be in source
        assert (source / "photo.jpg").exists()
        # Nothing in library yet
        assert list(lib.rglob("photo.jpg")) == []


# ---------------------------------------------------------------------------
# Catalog backup (unit-level)
# ---------------------------------------------------------------------------
class TestCatalogBackup:
    def test_backup_creates_timestamped_file(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 100, 1.0, "h1", "f1")])

        backup_path = LibraryCatalog.backup(tmp_path)
        assert backup_path is not None
        assert backup_path.exists()
        assert ".bak" in backup_path.name

    def test_backup_returns_none_when_no_db(self, tmp_path):
        assert LibraryCatalog.backup(tmp_path) is None

    def test_clear_file_hashes(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes(
                [
                    ("a.jpg", 100, 1.0, "h1", "f1"),
                    ("b.jpg", 200, 2.0, "h2", "f2"),
                ]
            )
            cleared = cat.clear_file_hashes()
            assert cleared == 2
            assert cat.get_file_hashes() == {}

    def test_clear_empty_table(self, tmp_path):
        with LibraryCatalog.open(tmp_path) as cat:
            cleared = cat.clear_file_hashes()
            assert cleared == 0


# ---------------------------------------------------------------------------
# Overwrite protection
# ---------------------------------------------------------------------------
class TestNoOverwrite:
    def test_new_file_never_replaces_a_library_file(self, tmp_path):
        """A same-name, different-content file must not clobber the library."""
        lib = tmp_path / "library"
        target = _write(
            lib / "2024" / "[2024_01_15]_party" / "IMG_1.jpg", b"ORIGINAL-LIBRARY"
        )

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_party" / "IMG_1.jpg", b"DIFFERENT-CONTENT")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_new == 1
        assert target.read_bytes() == b"ORIGINAL-LIBRARY"
        assert plan.n_renamed == 1
        renamed = lib / "2024" / "[2024_01_15]_party" / "IMG_1 (1).jpg"
        assert renamed.read_bytes() == b"DIFFERENT-CONTENT"

    def test_duplicate_never_replaces_a_quarantined_file(self, tmp_path):
        lib = tmp_path / "library"
        content = b"identical-duplicate-content"
        _write(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", content)

        dup_dir = tmp_path / "duplicates"
        existing = _write(dup_dir / "[2024_01_01]_ev" / "a.jpg", b"ALREADY-HERE")

        source = tmp_path / "source"
        _write(source / "[2024_01_01]_ev" / "a.jpg", content)

        reconcile(source, lib, dup_dir, execute=True)

        assert existing.read_bytes() == b"ALREADY-HERE"
        assert (dup_dir / "[2024_01_01]_ev" / "a (1).jpg").read_bytes() == content

    def test_two_source_files_with_one_name_both_survive(self, tmp_path):
        """Distinct content from two folders must not collapse into one file."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "cam_a" / "IMG_1.jpg", b"from-camera-a")
        _write(source / "cam_b" / "IMG_1.jpg", b"from-camera-b")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_new == 2
        found = sorted(p.read_bytes() for p in lib.rglob("*.jpg"))
        assert found == [b"from-camera-a", b"from-camera-b"]


# ---------------------------------------------------------------------------
# Stale cache invalidation
# ---------------------------------------------------------------------------
class TestStaleHashCache:
    def test_changed_file_invalidates_cached_hash(self, tmp_path):
        """Same size but new content must not be reported as a duplicate."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        lib_file = _write(lib / "photo.jpg", b"AAAAAAAAAAAA")

        idx = LibraryIndex(lib)
        idx.partial_hash(lib_file)
        idx.full_hash(lib_file)
        idx.close()

        # Rewrite with different content of identical length.
        time.sleep(0.01)
        lib_file.write_bytes(b"BBBBBBBBBBBB")

        source = tmp_path / "source"
        _write(source / "copy.jpg", b"AAAAAAAAAAAA")

        idx2 = LibraryIndex(lib)
        assert idx2.n_stale_cache_entries == 1
        match = _match_file(source / "copy.jpg", idx2)
        idx2.close()

        assert match.status == FileStatus.NEW

    def test_unchanged_file_keeps_cached_hash(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)
        lib_file = _write(lib / "photo.jpg", b"stable-content")

        idx = LibraryIndex(lib)
        idx.full_hash(lib_file)
        idx.close()

        idx2 = LibraryIndex(lib)
        assert idx2.n_stale_cache_entries == 0
        assert str(lib_file) in idx2._full_cache
        idx2.close()


# ---------------------------------------------------------------------------
# Mixed sources and recursion
# ---------------------------------------------------------------------------
class TestMixedAndRecursive:
    def test_mixed_source_mode_detected(self, tmp_path):
        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"in-event-folder")
        _write(source / "loose.jpg", b"outside-any-event-folder")
        assert detect_source_mode(source) == SourceMode.MIXED

    def test_loose_files_are_not_skipped_in_mixed_source(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"in-event-folder")
        _write(source / "loose.jpg", b"outside-any-event-folder")

        plan = reconcile(source, lib, tmp_path / "dup")

        assert plan.source_mode == SourceMode.MIXED
        assert plan.n_new == 2
        planned = {m.source_path.name for m in plan.file_matches}
        assert planned == {"a.jpg", "loose.jpg"}

    def test_nested_media_inside_event_folder_is_moved(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "top.jpg", b"top-level-photo")
        _write(source / "[2024_01_15]_ev" / "raw" / "deep.jpg", b"nested-photo")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_new == 2
        dest = lib / "2024" / "[2024_01_15]_ev"
        assert (dest / "top.jpg").exists()
        assert (dest / "raw" / "deep.jpg").exists()

    def test_no_recursive_stops_at_top_level(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "top.jpg", b"top-level-photo")
        _write(source / "sub" / "deep.jpg", b"nested-photo")

        plan = reconcile(source, lib, tmp_path / "dup", recursive=False)

        assert plan.n_new == 1
        assert plan.file_matches[0].source_path.name == "top.jpg"

    def test_cluster_ini_follows_the_folder(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"event-photo")
        _write(source / "[2024_01_15]_ev" / ".cluster.ini", b"[Cluster]\n")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_extra_files == 1
        assert (lib / "2024" / "[2024_01_15]_ev" / ".cluster.ini").exists()

    def test_all_duplicate_folder_takes_its_metadata_along(self, tmp_path):
        lib = tmp_path / "library"
        content = b"already-in-the-library"
        _write(lib / "2024" / "[2024_01_15]_ev" / "a.jpg", content)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", content)
        _write(source / "[2024_01_15]_ev" / ".cluster.ini", b"[Cluster]\n")

        dup_dir = tmp_path / "duplicates"
        reconcile(source, lib, dup_dir, execute=True)

        assert (dup_dir / "[2024_01_15]_ev" / "a.jpg").exists()
        assert (dup_dir / "[2024_01_15]_ev" / ".cluster.ini").exists()


# ---------------------------------------------------------------------------
# Sidecars
# ---------------------------------------------------------------------------
class TestSidecars:
    def test_sidecar_follows_its_media_file(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"raw-photo")
        _write(source / "[2024_01_15]_ev" / "a.xmp", b"<x:xmpmeta/>")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_sidecars == 1
        dest = lib / "2024" / "[2024_01_15]_ev"
        assert (dest / "a.jpg").exists()
        assert (dest / "a.xmp").exists()

    def test_appended_extension_sidecar_is_recognised(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "a.jpg", b"raw-photo")
        _write(source / "a.jpg.xmp", b"<x:xmpmeta/>")

        plan = reconcile(source, lib, tmp_path / "dup", execute=True)

        assert plan.n_sidecars == 1
        assert len(list(lib.rglob("a.jpg.xmp"))) == 1

    def test_sidecar_of_a_duplicate_goes_to_the_duplicates_dir(self, tmp_path):
        lib = tmp_path / "library"
        content = b"already-in-library"
        _write(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", content)

        source = tmp_path / "source"
        _write(source / "a.jpg", content)
        _write(source / "a.aae", b"adjustments")

        dup_dir = tmp_path / "duplicates"
        reconcile(source, lib, dup_dir, execute=True)

        assert (dup_dir / "a.jpg").exists()
        assert (dup_dir / "a.aae").exists()

    def test_no_sidecars_flag_leaves_them_behind(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "a.jpg", b"raw-photo")
        sidecar = _write(source / "a.xmp", b"<x:xmpmeta/>")

        plan = reconcile(
            source, lib, tmp_path / "dup", execute=True, include_sidecars=False
        )

        assert plan.n_sidecars == 0
        assert sidecar.exists()

    def test_no_sidecars_flag_also_applies_inside_event_folders(self, tmp_path):
        """An event folder sweeps up its extra files; sidecars must be exempt."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"raw-photo")
        sidecar = _write(source / "[2024_01_15]_ev" / "a.xmp", b"<x:xmpmeta/>")

        plan = reconcile(
            source, lib, tmp_path / "dup", execute=True, include_sidecars=False
        )

        assert plan.n_sidecars == 0
        assert sidecar.exists()
        assert list(lib.rglob("*.xmp")) == []

    def test_event_folder_metadata_still_follows_the_folder(self, tmp_path):
        """Turning sidecars off must not strand the cluster ini file."""
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_15]_ev" / "a.jpg", b"raw-photo")
        _write(source / "[2024_01_15]_ev" / ".cluster.ini", b"[Cluster]\n")

        reconcile(source, lib, tmp_path / "dup", execute=True, include_sidecars=False)

        assert (lib / "2024" / "[2024_01_15]_ev" / ".cluster.ini").exists()


# ---------------------------------------------------------------------------
# Duplicates inside the source itself
# ---------------------------------------------------------------------------
class TestSourceDuplicates:
    def test_same_content_twice_in_source(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "a" / "photo.jpg", b"the-very-same-bytes")
        _write(source / "b" / "photo_copy.jpg", b"the-very-same-bytes")

        plan = reconcile(source, lib, tmp_path / "dup")

        assert plan.n_new == 1
        assert plan.n_source_duplicates == 1
        dupe = next(
            m for m in plan.file_matches if m.status == FileStatus.SOURCE_DUPLICATE
        )
        assert dupe.source_duplicate_of is not None

    def test_source_duplicate_goes_to_duplicates_dir(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "one.jpg", b"repeated-content")
        _write(source / "two.jpg", b"repeated-content")

        dup_dir = tmp_path / "duplicates"
        reconcile(source, lib, dup_dir, execute=True)

        assert len(list(dup_dir.rglob("*.jpg"))) == 1
        assert len(list(lib.rglob("*.jpg"))) == 1

    def test_detection_can_be_disabled(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "one.jpg", b"repeated-content")
        _write(source / "two.jpg", b"repeated-content")

        plan = reconcile(source, lib, tmp_path / "dup", detect_source_duplicates=False)

        assert plan.n_source_duplicates == 0
        assert plan.n_new == 2


# ---------------------------------------------------------------------------
# Multiple libraries
# ---------------------------------------------------------------------------
class TestMultipleLibraries:
    def test_match_found_in_second_library(self, tmp_path):
        lib_a = tmp_path / "lib_a"
        lib_b = tmp_path / "lib_b"
        lib_a.mkdir()
        content = b"lives-in-the-second-library"
        _write(lib_b / "2024" / "[2024_01_01]_ev" / "x.jpg", content)

        source = tmp_path / "source"
        _write(source / "x.jpg", content)

        plan = reconcile(source, [lib_a, lib_b], tmp_path / "dup")

        assert plan.n_duplicates == 1
        assert plan.file_matches[0].library_match.is_relative_to(lib_b)

    def test_new_files_land_in_the_first_library(self, tmp_path):
        lib_a = tmp_path / "lib_a"
        lib_b = tmp_path / "lib_b"
        lib_a.mkdir()
        lib_b.mkdir()

        source = tmp_path / "source"
        _write(source / "[2024_01_01]_ev" / "new.jpg", b"brand-new")

        reconcile(source, [lib_a, lib_b], tmp_path / "dup", execute=True)

        assert list(lib_a.rglob("new.jpg"))
        assert not list(lib_b.rglob("new.jpg"))

    def test_all_library_matches_are_reported(self, tmp_path):
        lib = tmp_path / "library"
        content = b"stored-twice-in-the-library"
        _write(lib / "2024" / "[2024_01_01]_a" / "x.jpg", content)
        _write(lib / "2024" / "[2024_01_02]_b" / "y.jpg", content)

        source = tmp_path / "source"
        _write(source / "z.jpg", content)

        plan = reconcile(source, lib, tmp_path / "dup")

        assert plan.file_matches[0].n_library_matches == 2


# ---------------------------------------------------------------------------
# Copy and scan-only modes
# ---------------------------------------------------------------------------
class TestActions:
    def test_copy_mode_leaves_the_source_in_place(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        src_file = _write(source / "a.jpg", b"copy-me")

        plan = reconcile(
            source,
            lib,
            tmp_path / "dup",
            execute=True,
            action=ReconcileAction.COPY,
        )

        assert plan.n_copies == 1
        assert plan.n_moves == 0
        assert any(isinstance(op, CopyOp) for op in plan.ops)
        assert src_file.exists()
        assert len(list(lib.rglob("a.jpg"))) == 1

    def test_scan_only_writes_nothing(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        src_file = _write(source / "a.jpg", b"scan-me")

        plan = reconcile(
            source,
            lib,
            tmp_path / "dup",
            execute=True,
            action=ReconcileAction.SCAN,
        )

        assert plan.n_moves == 0
        assert plan.n_copies == 0
        assert plan.n_skips == 1
        assert all(isinstance(op, SkipOp) for op in plan.ops)
        assert src_file.exists()
        assert not list(lib.rglob("a.jpg"))

    def test_scan_only_still_previews_destinations(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "[2024_01_01]_ev" / "a.jpg", b"scan-me")

        plan = reconcile(source, lib, tmp_path / "dup", action=ReconcileAction.SCAN)

        folders = {folder for folder, _, _ in plan.move_destinations}
        assert str(lib / "2024" / "[2024_01_01]_ev") in folders

    def test_move_is_the_default(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir(parents=True)

        source = tmp_path / "source"
        _write(source / "a.jpg", b"move-me")

        plan = reconcile(source, lib, tmp_path / "dup")
        assert plan.action == ReconcileAction.MOVE
        assert any(isinstance(op, MoveOp) for op in plan.ops)


# ---------------------------------------------------------------------------
# Library self-duplicates via the index
# ---------------------------------------------------------------------------
class TestLibraryDuplicateGroups:
    def test_finds_duplicates_across_library_folders(self, tmp_path):
        lib = tmp_path / "library"
        content = b"the-same-photo-filed-twice"
        _write(lib / "2024" / "[2024_01_01]_a" / "x.jpg", content)
        _write(lib / "2024" / "[2024_06_01]_b" / "y.jpg", content)
        _write(lib / "2024" / "[2024_06_01]_b" / "unique.jpg", b"only-once-here")

        idx = LibraryIndex(lib)
        groups = idx.duplicate_groups()
        idx.close()

        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_no_groups_when_all_unique(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"first-unique")
        _write(lib / "b.jpg", b"second-unique-longer")

        idx = LibraryIndex(lib)
        assert idx.duplicate_groups() == []
        idx.close()


# ---------------------------------------------------------------------------
# Overlapping roots
# ---------------------------------------------------------------------------
class TestOverlappingRoots:
    """Roots that overlap are rejected before anything is read or written."""

    def test_source_equal_to_library_is_rejected(self, tmp_path):
        lib = tmp_path / "library"
        photo = _write(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", b"only-copy")

        with pytest.raises(OverlappingPathsError):
            reconcile(lib, lib, tmp_path / "dup", execute=True)

        assert photo.read_bytes() == b"only-copy"

    def test_source_inside_library_is_rejected(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        source = lib / "inbox"
        _write(source / "b.jpg", b"incoming-file")

        with pytest.raises(OverlappingPathsError):
            reconcile(source, lib, tmp_path / "dup")

    def test_library_inside_source_is_rejected(self, tmp_path):
        source = tmp_path / "source"
        lib = source / "library"
        _write(lib / "a.jpg", b"library-file")
        _write(source / "b.jpg", b"incoming-file")

        with pytest.raises(OverlappingPathsError):
            reconcile(source, lib, tmp_path / "dup")

    def test_symlinked_alias_of_the_library_is_rejected(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        alias = tmp_path / "alias"
        os.symlink(lib, alias)

        with pytest.raises(OverlappingPathsError):
            reconcile(alias, lib, tmp_path / "dup")

    def test_duplicates_dir_inside_the_library_is_rejected(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        with pytest.raises(OverlappingPathsError):
            reconcile(source, lib, lib / "duplicates")

    def test_duplicates_dir_inside_the_source_is_rejected(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir()
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        with pytest.raises(OverlappingPathsError):
            reconcile(source, lib, source / "duplicates")

    def test_duplicates_dir_beside_the_source_is_allowed(self, tmp_path):
        lib = tmp_path / "library"
        lib.mkdir()
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        plan = reconcile(source, lib, tmp_path / "duplicates")

        assert plan.n_new == 1

    def test_one_library_overlapping_out_of_several_is_rejected(self, tmp_path):
        lib_a = tmp_path / "lib_a"
        lib_a.mkdir()
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        with pytest.raises(OverlappingPathsError):
            reconcile(source, [lib_a, source / "nested"], tmp_path / "dup")


class TestUniqueRoots:
    """The same library passed twice must be indexed once."""

    def test_repeated_root_is_dropped(self, tmp_path):
        assert _unique_roots([tmp_path, tmp_path]) == [tmp_path]

    def test_symlinked_alias_is_dropped(self, tmp_path):
        real = tmp_path / "library"
        real.mkdir()
        alias = tmp_path / "alias"
        os.symlink(real, alias)

        assert _unique_roots([real, alias]) == [real]

    def test_distinct_roots_are_kept_in_order(self, tmp_path):
        first = tmp_path / "a"
        second = tmp_path / "b"

        assert _unique_roots([first, second]) == [first, second]

    def test_library_given_twice_does_not_self_duplicate(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        plan = reconcile(source, [lib, lib], tmp_path / "dup")

        assert plan.n_new == 1
        assert plan.n_duplicates == 0


# ---------------------------------------------------------------------------
# Dry runs write nothing
# ---------------------------------------------------------------------------
class TestDryRunIsReadOnly:
    def test_preview_creates_no_catalog(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")

        reconcile(source, lib, tmp_path / "dup", execute=False)

        assert list(lib.glob(".filecluster*")) == []

    def test_preview_does_not_write_new_hashes(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"library-file")
        with LibraryCatalog.open(lib) as cat:
            cat.put_file_hashes([("seed.jpg", 1, 1.0, "p", "f")])

        reconcile(source, lib, tmp_path / "dup", execute=False)

        with LibraryCatalog.open(lib) as cat:
            assert set(cat.get_file_hashes()) == {"seed.jpg"}

    def test_force_reindex_preview_keeps_the_cached_hashes(self, tmp_path):
        """A dry run must not clear the cache it was only supposed to read."""
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"incoming-file")
        with LibraryCatalog.open(lib) as cat:
            cat.put_file_hashes([("a.jpg", 12, 1.0, "p", "f")])

        reconcile(source, lib, tmp_path / "dup", execute=False, force_reindex=True)

        with LibraryCatalog.open(lib) as cat:
            assert set(cat.get_file_hashes()) == {"a.jpg"}
        assert LibraryCatalog.list_backups(lib) == []

    def test_execute_still_writes_the_catalog(self, tmp_path):
        lib = tmp_path / "library"
        _write(lib / "a.jpg", b"library-file")
        source = tmp_path / "source"
        _write(source / "b.jpg", b"library-file")

        reconcile(source, lib, tmp_path / "dup", execute=True)

        assert (lib / LibraryCatalog.DB_FILENAME).exists()
