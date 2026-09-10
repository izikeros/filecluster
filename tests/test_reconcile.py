"""Tests for the reconcile module.

Covers LibraryIndex (indexing, caching, incremental update, force-reindex
with backup), file matching (size, partial hash, full hash, filename),
integration with temp directories, dry-run vs execute mode, event-folder
mode vs flat mode detection, and partial-match folder handling.
"""

from pathlib import Path

from filecluster.catalog import LibraryCatalog
from filecluster.reconcile import (
    FileMatch,
    FileStatus,
    FolderResult,
    FolderStatus,
    LibraryIndex,
    ReconcilePlan,
    SourceMode,
    _extract_year_from_folder,
    _library_dest_for_event_folder,
    _match_file,
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
# Year extraction
# ---------------------------------------------------------------------------
class TestExtractYear:
    def test_standard_event_folder(self):
        assert _extract_year_from_folder("[2024_01_15]_birthday") == "2024"

    def test_no_match_returns_none(self):
        assert _extract_year_from_folder("random_folder") is None

    def test_partial_match(self):
        assert _extract_year_from_folder("[2023_06_20]") == "2023"


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
