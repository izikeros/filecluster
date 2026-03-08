"""Tests for the file_operations module.

Covers FileOperationPlan construction, operation counting,
build_file_operation_plan for NOP/COPY/MOVE modes, and execute_plan.
"""

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
