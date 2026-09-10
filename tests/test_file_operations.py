"""Tests for the file_operations module.

Covers FileOperationPlan construction, operation counting,
build_file_operation_plan for NOP/COPY/MOVE modes, execute_plan, and the
write-time guarantees that keep an existing file from being replaced.
"""

import os
from pathlib import Path

import pandas as pd
import pytest

from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneError
from filecluster.file_operations import (
    CopyOp,
    FileOperationPlan,
    MkdirOp,
    MoveOp,
    SkipOp,
    build_file_operation_plan,
    execute_plan,
    numbered_name,
    reserve_exclusive,
)


class TestFileOperationPlan:
    """Tests for plan dataclass and its properties."""

    def test_empty_plan_has_zero_counts(self):
        """A freshly created plan has no operations."""
        plan = FileOperationPlan()
        assert plan.n_moves == 0
        assert plan.n_copies == 0
        assert plan.n_skips == 0
        assert plan.n_mkdirs == 0

    def test_counts_reflect_ops(self):
        """Each op type is counted independently."""
        plan = FileOperationPlan(
            ops=[
                MkdirOp(path=Path("/a")),
                MoveOp(src=Path("/s"), dst=Path("/d")),
                CopyOp(src=Path("/s"), dst=Path("/d")),
                SkipOp(src=Path("/s"), reason="nop"),
            ]
        )
        assert plan.n_mkdirs == 1
        assert plan.n_moves == 1
        assert plan.n_copies == 1
        assert plan.n_skips == 1

    def test_summary_string(self):
        """Summary includes all counts."""
        plan = FileOperationPlan(ops=[MoveOp(src=Path("/s"), dst=Path("/d"))])
        s = plan.summary()
        assert "1 moves" in s
        assert "0 copies" in s


class TestBuildFileOperationPlan:
    """Tests for build_file_operation_plan factory function."""

    @pytest.fixture()
    def sample_df(self):
        return pd.DataFrame(
            {
                "file_name": ["a.jpg", "b.jpg"],
                "target_path": ["new/cluster1", "new/cluster1"],
            }
        )

    def test_nop_mode_produces_only_skips(self, sample_df):
        """NOP mode should produce SkipOps for every file, no MkdirOps."""
        plan = build_file_operation_plan(
            inbox_media_df=sample_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.NOP,
        )
        assert plan.n_skips == 2
        assert plan.n_moves == 0
        assert plan.n_copies == 0
        assert plan.n_mkdirs == 0

    def test_copy_mode_produces_copy_ops(self, sample_df):
        """COPY mode should produce CopyOps and MkdirOps."""
        plan = build_file_operation_plan(
            inbox_media_df=sample_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.COPY,
        )
        assert plan.n_copies == 2
        assert plan.n_mkdirs == 1
        assert plan.n_moves == 0

    def test_move_mode_produces_move_ops(self, sample_df):
        """MOVE mode should produce MoveOps and MkdirOps."""
        plan = build_file_operation_plan(
            inbox_media_df=sample_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.MOVE,
        )
        assert plan.n_moves == 2
        assert plan.n_mkdirs == 1
        assert plan.n_copies == 0

    def test_none_target_path_raises(self):
        """None in target_path should raise DateStringNoneError."""
        df = pd.DataFrame({"file_name": ["a.jpg"], "target_path": [None]})
        with pytest.raises(DateStringNoneError):
            build_file_operation_plan(
                inbox_media_df=df,
                in_dir=Path("/inbox"),
                out_dir=Path("/out"),
                mode=CopyMode.MOVE,
            )

    def test_paths_are_correct(self, sample_df):
        """Verify source and destination paths are constructed correctly."""
        plan = build_file_operation_plan(
            inbox_media_df=sample_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.COPY,
        )
        copy_ops = [op for op in plan.ops if isinstance(op, CopyOp)]
        assert copy_ops[0].src == Path("/inbox/a.jpg")
        assert copy_ops[0].dst == Path("/out/new/cluster1/a.jpg")


class TestExecutePlan:
    """Tests for execute_plan with real filesystem."""

    def test_execute_creates_dirs_and_copies(self, tmp_path):
        """Execute a COPY plan and verify files end up in the right place."""
        src_dir = tmp_path / "inbox"
        src_dir.mkdir()
        (src_dir / "photo.jpg").write_text("data")

        out_dir = tmp_path / "out"

        plan = FileOperationPlan(
            ops=[
                MkdirOp(path=out_dir / "new" / "cluster1"),
                CopyOp(
                    src=src_dir / "photo.jpg",
                    dst=out_dir / "new" / "cluster1" / "photo.jpg",
                ),
            ]
        )

        execute_plan(plan)

        assert (out_dir / "new" / "cluster1" / "photo.jpg").exists()
        assert (src_dir / "photo.jpg").exists()  # Source preserved (copy)

    def test_execute_move_removes_source(self, tmp_path):
        """Execute a MOVE plan and verify source is removed."""
        src_dir = tmp_path / "inbox"
        src_dir.mkdir()
        (src_dir / "photo.jpg").write_text("data")

        out_dir = tmp_path / "out"

        plan = FileOperationPlan(
            ops=[
                MkdirOp(path=out_dir / "new" / "cluster1"),
                MoveOp(
                    src=src_dir / "photo.jpg",
                    dst=out_dir / "new" / "cluster1" / "photo.jpg",
                ),
            ]
        )

        execute_plan(plan)

        assert (out_dir / "new" / "cluster1" / "photo.jpg").exists()
        assert not (src_dir / "photo.jpg").exists()  # Source removed (move)

    def test_execute_skip_does_nothing(self, tmp_path):
        """SkipOps should not create any files."""
        plan = FileOperationPlan(ops=[SkipOp(src=Path("/nonexistent"), reason="test")])
        execute_plan(plan)
        # If we get here without error, skip was handled correctly

    @pytest.mark.parametrize("mode", [CopyMode.COPY, CopyMode.MOVE])
    def test_existing_destination_is_renamed_instead_of_overwritten(
        self, tmp_path, mode
    ):
        """An existing same-named file is preserved in both operation modes."""
        src_dir = tmp_path / "inbox"
        target_dir = tmp_path / "out" / "new" / "cluster1"
        src_dir.mkdir()
        target_dir.mkdir(parents=True)
        (src_dir / "photo.jpg").write_text("incoming")
        (target_dir / "photo.jpg").write_text("existing")

        df = pd.DataFrame({"file_name": ["photo.jpg"], "target_path": ["new/cluster1"]})
        plan = build_file_operation_plan(
            inbox_media_df=df,
            in_dir=src_dir,
            out_dir=tmp_path / "out",
            mode=mode,
        )
        execute_plan(plan)

        assert (target_dir / "photo.jpg").read_text() == "existing"
        assert (target_dir / "photo (1).jpg").read_text() == "incoming"


# ---------------------------------------------------------------------------
# DestinationAllocator
# ---------------------------------------------------------------------------
class TestUniqueName:
    """The numeric-suffix fallback used to avoid overwriting a file."""

    def test_free_name_is_returned_unchanged(self):
        from filecluster.file_operations import unique_name

        assert unique_name("photo.jpg", set()) == "photo.jpg"

    def test_taken_name_gets_a_counter(self):
        from filecluster.file_operations import unique_name

        assert unique_name("photo.jpg", {"photo.jpg"}) == "photo (1).jpg"

    def test_counter_keeps_climbing(self):
        from filecluster.file_operations import unique_name

        claimed = {"photo.jpg", "photo (1).jpg", "photo (2).jpg"}
        assert unique_name("photo.jpg", claimed) == "photo (3).jpg"

    def test_matching_is_case_insensitive(self):
        from filecluster.file_operations import unique_name

        assert unique_name("Photo.JPG", {"photo.jpg"}) == "Photo (1).JPG"

    def test_name_without_extension(self):
        from filecluster.file_operations import unique_name

        assert unique_name("README", {"readme"}) == "README (1)"


class TestDestinationAllocator:
    def test_existing_files_are_claimed_from_disk(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        (tmp_path / "photo.jpg").write_text("existing")
        allocator = DestinationAllocator()

        assert allocator.allocate(tmp_path, "photo.jpg").name == "photo (1).jpg"

    def test_two_allocations_never_collide(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        allocator = DestinationAllocator()
        first = allocator.allocate(tmp_path, "photo.jpg")
        second = allocator.allocate(tmp_path, "photo.jpg")

        assert first.name == "photo.jpg"
        assert second.name == "photo (1).jpg"

    def test_separate_directories_are_independent(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        allocator = DestinationAllocator()
        a = allocator.allocate(tmp_path / "a", "photo.jpg")
        b = allocator.allocate(tmp_path / "b", "photo.jpg")

        assert a.name == b.name == "photo.jpg"

    def test_peek_does_not_claim(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        allocator = DestinationAllocator()
        assert allocator.peek(tmp_path, "photo.jpg") == "photo.jpg"
        assert allocator.peek(tmp_path, "photo.jpg") == "photo.jpg"

    def test_reserve_blocks_a_name(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        allocator = DestinationAllocator()
        allocator.reserve(tmp_path / "photo.jpg")

        assert allocator.allocate(tmp_path, "photo.jpg").name == "photo (1).jpg"

    def test_existing_directory_is_claimed(self, tmp_path):
        """A directory would make a move nest the file inside it."""
        from filecluster.file_operations import DestinationAllocator

        (tmp_path / "photo.jpg").mkdir()
        allocator = DestinationAllocator()

        assert allocator.allocate(tmp_path, "photo.jpg").name == "photo (1).jpg"

    def test_symlink_to_a_directory_is_claimed(self, tmp_path):
        """A symlink would redirect the write outside the destination tree."""
        from filecluster.file_operations import DestinationAllocator

        outside = tmp_path / "outside"
        outside.mkdir()
        target = tmp_path / "dest"
        target.mkdir()
        os.symlink(outside, target / "photo.jpg")
        allocator = DestinationAllocator()

        assert allocator.allocate(target, "photo.jpg").name == "photo (1).jpg"

    def test_dangling_symlink_is_claimed(self, tmp_path):
        from filecluster.file_operations import DestinationAllocator

        os.symlink(tmp_path / "nowhere", tmp_path / "photo.jpg")
        allocator = DestinationAllocator()

        assert allocator.allocate(tmp_path, "photo.jpg").name == "photo (1).jpg"


# ---------------------------------------------------------------------------
# Write-time reservation
# ---------------------------------------------------------------------------
class TestNumberedName:
    def test_counter_goes_before_the_extension(self):
        assert numbered_name("photo.jpg", 3) == "photo (3).jpg"

    def test_name_without_extension(self):
        assert numbered_name("README", 1) == "README (1)"


class TestReserveExclusive:
    def test_free_name_is_created_as_given(self, tmp_path):
        reserved = reserve_exclusive(tmp_path / "photo.jpg")

        assert reserved == tmp_path / "photo.jpg"
        assert reserved.is_file()

    def test_taken_name_falls_back_to_a_suffix(self, tmp_path):
        (tmp_path / "photo.jpg").write_text("existing")

        reserved = reserve_exclusive(tmp_path / "photo.jpg")

        assert reserved == tmp_path / "photo (1).jpg"
        assert (tmp_path / "photo.jpg").read_text() == "existing"

    def test_directory_at_the_destination_is_not_entered(self, tmp_path):
        (tmp_path / "photo.jpg").mkdir()

        reserved = reserve_exclusive(tmp_path / "photo.jpg")

        assert reserved == tmp_path / "photo (1).jpg"
        assert (tmp_path / "photo.jpg").is_dir()


class TestWriteTimeCollisions:
    """Names taken between planning and writing must not be overwritten."""

    @pytest.mark.parametrize("op_type", [CopyOp, MoveOp])
    def test_file_created_after_planning_survives(self, tmp_path, op_type):
        src = tmp_path / "inbox" / "photo.jpg"
        src.parent.mkdir()
        src.write_text("incoming")
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        plan = FileOperationPlan(ops=[op_type(src=src, dst=dest_dir / "photo.jpg")])
        # Planning saw an empty directory; something else fills it in first.
        (dest_dir / "photo.jpg").write_text("precious")

        execute_plan(plan)

        assert (dest_dir / "photo.jpg").read_text() == "precious"
        assert (dest_dir / "photo (1).jpg").read_text() == "incoming"

    def test_move_does_not_follow_a_directory_symlink(self, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()
        os.symlink(outside, dest_dir / "photo.jpg")
        src = tmp_path / "inbox" / "photo.jpg"
        src.parent.mkdir()
        src.write_text("incoming")

        execute_plan(
            FileOperationPlan(ops=[MoveOp(src=src, dst=dest_dir / "photo.jpg")])
        )

        assert list(outside.iterdir()) == []
        assert (dest_dir / "photo (1).jpg").read_text() == "incoming"

    def test_failed_write_leaves_no_placeholder(self, tmp_path):
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()
        missing = tmp_path / "inbox" / "gone.jpg"

        with pytest.raises(OSError):
            execute_plan(
                FileOperationPlan(ops=[CopyOp(src=missing, dst=dest_dir / "gone.jpg")])
            )

        assert list(dest_dir.iterdir()) == []

    def test_moving_a_symlink_keeps_it_a_symlink(self, tmp_path):
        real = tmp_path / "real.jpg"
        real.write_text("data")
        link = tmp_path / "inbox" / "link.jpg"
        link.parent.mkdir()
        os.symlink(real, link)
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        execute_plan(
            FileOperationPlan(ops=[MoveOp(src=link, dst=dest_dir / "link.jpg")])
        )

        assert (dest_dir / "link.jpg").is_symlink()
        assert (dest_dir / "link.jpg").read_text() == "data"
        assert real.read_text() == "data"
