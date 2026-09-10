"""Tests for the dedup module.

Covers duplicate detection within a single folder and across folders,
canonical-copy selection, quarantine planning and execution, the CSV report,
the size/partial/full cascade short-circuits, and catalog-backed hash reuse
including invalidation when a file changes.
"""

import time
from pathlib import Path

from filecluster.catalog import LibraryCatalog
from filecluster.dedup import (
    DedupAction,
    DuplicateGroup,
    build_dedup_plan,
    dedup,
    find_duplicate_groups,
)
from filecluster.file_operations import MkdirOp, MoveOp, SkipOp


def _write(path: Path, content: bytes = b"hello world") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
class TestFindDuplicateGroups:
    def test_no_duplicates_in_a_clean_tree(self, tmp_path):
        _write(tmp_path / "a.jpg", b"first-unique-file")
        _write(tmp_path / "b.jpg", b"second-unique-file-longer")

        groups, n_scanned, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups == []
        assert n_scanned == 2

    def test_duplicate_inside_one_folder(self, tmp_path):
        _write(tmp_path / "IMG_1.jpg", b"same-photo-bytes")
        _write(tmp_path / "IMG_1-Kopiuj.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert len(groups) == 1
        assert groups[0].n_copies == 2
        assert groups[0].is_intra_folder
        assert not groups[0].is_cross_folder

    def test_duplicate_across_folders(self, tmp_path):
        _write(tmp_path / "2024" / "[2024_01_01]_a" / "x.jpg", b"same-photo-bytes")
        _write(tmp_path / "2023" / "[2023_05_05]_b" / "y.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert len(groups) == 1
        assert groups[0].is_cross_folder
        assert len(groups[0].folders) == 2

    def test_same_size_different_content_is_not_a_group(self, tmp_path):
        _write(tmp_path / "a.jpg", b"aaaa-content")
        _write(tmp_path / "b.jpg", b"bbbb-content")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups == []

    def test_three_copies_form_one_group(self, tmp_path):
        for name in ("a.jpg", "b.jpg", "c.jpg"):
            _write(tmp_path / name, b"triplicated-content")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert len(groups) == 1
        assert groups[0].n_copies == 3
        assert len(groups[0].duplicates) == 2

    def test_two_independent_groups(self, tmp_path):
        _write(tmp_path / "a1.jpg", b"group-one-content")
        _write(tmp_path / "a2.jpg", b"group-one-content")
        _write(tmp_path / "b1.jpg", b"group-two-content-differs")
        _write(tmp_path / "b2.jpg", b"group-two-content-differs")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert len(groups) == 2

    def test_unsupported_extensions_are_ignored(self, tmp_path):
        _write(tmp_path / "notes.txt", b"identical-text")
        _write(tmp_path / "notes2.txt", b"identical-text")

        groups, n_scanned, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups == []
        assert n_scanned == 0

    def test_min_size_filter(self, tmp_path):
        _write(tmp_path / "a.jpg", b"tiny")
        _write(tmp_path / "b.jpg", b"tiny")

        groups, _, _ = find_duplicate_groups(tmp_path, min_size=100, use_catalog=False)

        assert groups == []

    def test_empty_files_are_skipped_by_default(self, tmp_path):
        _write(tmp_path / "a.jpg", b"")
        _write(tmp_path / "b.jpg", b"")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups == []

    def test_no_recursive_ignores_subfolders(self, tmp_path):
        _write(tmp_path / "a.jpg", b"same-photo-bytes")
        _write(tmp_path / "sub" / "b.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(
            tmp_path, recursive=False, use_catalog=False
        )

        assert groups == []

    def test_unique_sizes_are_never_hashed(self, tmp_path):
        _write(tmp_path / "a.jpg", b"one")
        _write(tmp_path / "b.jpg", b"two-longer")

        _groups, _scanned, n_hashed = find_duplicate_groups(tmp_path, use_catalog=False)

        assert n_hashed == 0

    def test_excluded_dir_is_not_scanned(self, tmp_path):
        _write(tmp_path / "a.jpg", b"same-photo-bytes")
        _write(tmp_path / "quarantine" / "a.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(
            tmp_path,
            use_catalog=False,
            exclude_dirs=[tmp_path / "quarantine"],
        )

        assert groups == []


# ---------------------------------------------------------------------------
# Canonical-copy selection
# ---------------------------------------------------------------------------
class TestCanonicalSelection:
    def test_event_folder_copy_wins(self, tmp_path):
        keeper = _write(
            tmp_path / "2024" / "[2024_01_01]_ev" / "x.jpg", b"same-photo-bytes"
        )
        _write(tmp_path / "loose" / "x.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups[0].canonical == keeper

    def test_unsuffixed_name_wins_over_a_copy_suffix(self, tmp_path):
        keeper = _write(tmp_path / "IMG_1.jpg", b"same-photo-bytes")
        _write(tmp_path / "IMG_1-Kopiuj.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups[0].canonical == keeper
        assert groups[0].duplicates == [tmp_path / "IMG_1-Kopiuj.jpg"]

    def test_shallower_path_wins(self, tmp_path):
        keeper = _write(tmp_path / "x.jpg", b"same-photo-bytes")
        _write(tmp_path / "a" / "b" / "x.jpg", b"same-photo-bytes")

        groups, _, _ = find_duplicate_groups(tmp_path, use_catalog=False)

        assert groups[0].canonical == keeper


# ---------------------------------------------------------------------------
# DuplicateGroup arithmetic
# ---------------------------------------------------------------------------
class TestDuplicateGroup:
    def test_wasted_bytes(self):
        group = DuplicateGroup(
            full_hash="h",
            size=1000,
            files=[Path("/a/x.jpg"), Path("/b/x.jpg"), Path("/c/x.jpg")],
        )
        assert group.wasted_bytes == 2000
        assert group.n_copies == 3
        assert group.is_cross_folder

    def test_intra_folder_group(self):
        group = DuplicateGroup(
            full_hash="h", size=10, files=[Path("/a/x.jpg"), Path("/a/y.jpg")]
        )
        assert group.is_intra_folder
        assert group.folders == [Path("/a")]


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------
class TestBuildDedupPlan:
    def test_report_mode_plans_no_moves(self, tmp_path):
        groups = [
            DuplicateGroup(
                full_hash="h", size=10, files=[tmp_path / "a.jpg", tmp_path / "b.jpg"]
            )
        ]
        plan = build_dedup_plan(tmp_path, groups, None, DedupAction.REPORT)

        assert plan.n_moves == 0
        assert all(isinstance(op, SkipOp) for op in plan.ops)
        assert plan.n_duplicate_files == 1

    def test_quarantine_mode_preserves_relative_layout(self, tmp_path):
        dup = tmp_path / "2024" / "[2024_01_01]_ev" / "b.jpg"
        groups = [
            DuplicateGroup(full_hash="h", size=10, files=[tmp_path / "a.jpg", dup])
        ]
        quarantine = tmp_path.parent / "quarantine"
        plan = build_dedup_plan(tmp_path, groups, quarantine, DedupAction.QUARANTINE)

        moves = [op for op in plan.ops if isinstance(op, MoveOp)]
        assert len(moves) == 1
        assert moves[0].dst == quarantine / "2024" / "[2024_01_01]_ev" / "b.jpg"
        assert any(isinstance(op, MkdirOp) for op in plan.ops)

    def test_quarantine_names_never_collide(self, tmp_path):
        groups = [
            DuplicateGroup(
                full_hash="h1", size=10, files=[tmp_path / "a.jpg", tmp_path / "b.jpg"]
            ),
            DuplicateGroup(
                full_hash="h2", size=10, files=[tmp_path / "c.jpg", tmp_path / "b2.jpg"]
            ),
        ]
        quarantine = tmp_path.parent / "quarantine"
        (quarantine).mkdir()
        (quarantine / "b.jpg").write_bytes(b"already-here")

        plan = build_dedup_plan(tmp_path, groups, quarantine, DedupAction.QUARANTINE)
        dests = [op.dst.name for op in plan.ops if isinstance(op, MoveOp)]

        assert "b.jpg" not in dests
        assert "b (1).jpg" in dests


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------
class TestDedup:
    def test_dry_run_moves_nothing(self, tmp_path):
        root = tmp_path / "lib"
        a = _write(root / "a.jpg", b"same-photo-bytes")
        b = _write(root / "b.jpg", b"same-photo-bytes")

        plan = dedup(root, tmp_path / "quarantine", action=DedupAction.QUARANTINE)

        assert plan.n_groups == 1
        assert a.exists()
        assert b.exists()
        assert not (tmp_path / "quarantine").exists()

    def test_execute_quarantines_the_redundant_copy(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "IMG_1.jpg", b"same-photo-bytes")
        _write(root / "IMG_1-Kopiuj.jpg", b"same-photo-bytes")
        quarantine = tmp_path / "quarantine"

        plan = dedup(root, quarantine, action=DedupAction.QUARANTINE, execute=True)

        assert plan.n_moves == 1
        assert (root / "IMG_1.jpg").exists()
        assert not (root / "IMG_1-Kopiuj.jpg").exists()
        assert (quarantine / "IMG_1-Kopiuj.jpg").read_bytes() == b"same-photo-bytes"

    def test_report_action_never_writes_even_with_execute(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        b = _write(root / "b.jpg", b"same-photo-bytes")

        plan = dedup(root, None, action=DedupAction.REPORT, execute=True)

        assert plan.n_moves == 0
        assert b.exists()

    def test_quarantine_inside_root_is_not_rescanned(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")
        quarantine = root / "_duplicates"

        first = dedup(root, quarantine, action=DedupAction.QUARANTINE, execute=True)
        assert first.n_moves == 1

        second = dedup(root, quarantine, action=DedupAction.QUARANTINE, execute=True)
        assert second.n_groups == 0

    def test_summary_dict(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")

        summary = dedup(root).summary_dict()

        assert summary["duplicate_groups"] == 1
        assert summary["duplicate_files"] == 1
        assert summary["intra_folder_groups"] == 1
        assert summary["wasted_bytes"] == len(b"same-photo-bytes")
        assert summary["action"] == "report"

    def test_csv_report_has_one_row_per_copy(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")

        plan = dedup(root)
        csv_path = tmp_path / "dupes.csv"
        n = plan.write_csv(csv_path)

        assert n == 2
        lines = csv_path.read_text().strip().split("\n")
        assert len(lines) == 3
        assert "keep" in lines[1]
        assert "duplicate" in lines[2]


# ---------------------------------------------------------------------------
# Catalog-backed hash reuse
# ---------------------------------------------------------------------------
class TestHashCaching:
    def test_hashes_are_persisted(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")

        find_duplicate_groups(root)

        with LibraryCatalog.open(root) as cat:
            assert len(cat.get_file_hashes()) == 2

    def test_second_run_reuses_cached_hashes(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")

        _g1, _s1, hashed_first = find_duplicate_groups(root)
        _g2, _s2, hashed_second = find_duplicate_groups(root)

        assert hashed_first == 2
        assert hashed_second == 0

    def test_changed_file_is_rehashed(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"AAAAAAAAAAAA")
        b = _write(root / "b.jpg", b"AAAAAAAAAAAA")

        groups_first, _, _ = find_duplicate_groups(root)
        assert len(groups_first) == 1

        time.sleep(0.01)
        b.write_bytes(b"BBBBBBBBBBBB")

        groups_second, _, hashed = find_duplicate_groups(root)

        assert groups_second == []
        assert hashed >= 1


# ---------------------------------------------------------------------------
# Dry runs write nothing
# ---------------------------------------------------------------------------
class TestDryRunIsReadOnly:
    def test_report_run_creates_no_catalog(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b" / "a.jpg", b"same-photo-bytes")

        plan = dedup(root, action=DedupAction.REPORT)

        assert plan.n_groups == 1
        assert list(root.glob(".filecluster*")) == []

    def test_quarantine_preview_creates_no_catalog(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b" / "a.jpg", b"same-photo-bytes")

        dedup(root, tmp_path / "quarantine", action=DedupAction.QUARANTINE)

        assert list(root.glob(".filecluster*")) == []
        assert not (tmp_path / "quarantine").exists()

    def test_preview_leaves_an_existing_catalog_untouched(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b" / "a.jpg", b"same-photo-bytes")
        with LibraryCatalog.open(root) as cat:
            cat.put_file_hashes([("seed.jpg", 1, 1.0, "p", "f")])

        dedup(root, action=DedupAction.REPORT)

        with LibraryCatalog.open(root) as cat:
            assert set(cat.get_file_hashes()) == {"seed.jpg"}

    def test_executed_quarantine_still_writes_the_catalog(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b" / "a.jpg", b"same-photo-bytes")

        dedup(
            root,
            tmp_path / "quarantine",
            action=DedupAction.QUARANTINE,
            execute=True,
        )

        with LibraryCatalog.open(root) as cat:
            assert len(cat.get_file_hashes()) >= 1

    def test_read_only_scan_still_reuses_cached_hashes(self, tmp_path):
        root = tmp_path / "lib"
        _write(root / "a.jpg", b"same-photo-bytes")
        _write(root / "b.jpg", b"same-photo-bytes")

        find_duplicate_groups(root)
        _groups, _scanned, hashed = find_duplicate_groups(root, read_only=True)

        assert hashed == 0
