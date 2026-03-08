"""File operation planning and execution.

Separates the *decision* of what to do with files (the plan) from the
*execution* of those decisions (the executor).  This makes dry-run mode
trivial, testing pure, and the I/O boundary explicit.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from shutil import copy2, move

from tqdm import tqdm

from filecluster import logger
from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneError


@dataclass(frozen=True)
class MoveOp:
    """Move a file from *src* to *dst*."""

    src: Path
    dst: Path


@dataclass(frozen=True)
class CopyOp:
    """Copy a file from *src* to *dst*."""

    src: Path
    dst: Path


@dataclass(frozen=True)
class SkipOp:
    """Record that a file was intentionally skipped."""

    src: Path
    reason: str


@dataclass(frozen=True)
class MkdirOp:
    """Create a directory (and parents)."""

    path: Path


FileOp = MoveOp | CopyOp | SkipOp | MkdirOp


@dataclass
class FileOperationPlan:
    """An ordered list of file-system operations to perform."""

    ops: list[FileOp] = field(default_factory=list)

    # Convenience counts
    @property
    def n_moves(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MoveOp))

    @property
    def n_copies(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, CopyOp))

    @property
    def n_skips(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, SkipOp))

    @property
    def n_mkdirs(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MkdirOp))

    def summary(self) -> str:
        """Human-readable one-line summary."""
        return (
            f"Plan: {self.n_mkdirs} dirs, "
            f"{self.n_moves} moves, {self.n_copies} copies, "
            f"{self.n_skips} skips"
        )


def build_file_operation_plan(
    inbox_media_df,
    in_dir: Path,
    out_dir: Path,
    mode: CopyMode,
) -> FileOperationPlan:
    """Build a plan of file operations from the fully-annotated media DataFrame.

    Args:
        inbox_media_df: DataFrame with 'file_name' and 'target_path' columns set.
        in_dir: Inbox directory (source).
        out_dir: Output directory (destination root).
        mode: COPY, MOVE, or NOP.

    Returns:
        A FileOperationPlan ready to be executed (or inspected).
    """
    plan = FileOperationPlan()

    if mode == CopyMode.NOP:
        for _, row in inbox_media_df.iterrows():
            plan.ops.append(
                SkipOp(
                    src=Path(in_dir) / row["file_name"],
                    reason="NOP mode",
                )
            )
        return plan

    # Collect unique target directories and validate them
    dirs = inbox_media_df["target_path"].unique()
    for dir_name in dirs:
        if dir_name is None:
            raise DateStringNoneError()
        isnan = isinstance(dir_name, float) and math.isnan(dir_name)
        if isnan:
            raise DateStringNoneError()
        plan.ops.append(MkdirOp(path=Path(out_dir) / str(dir_name)))

    # Plan file operations
    for _, row in inbox_media_df.iterrows():
        src = Path(in_dir) / row["file_name"]
        dst = Path(out_dir) / str(row["target_path"]) / row["file_name"]
        if mode == CopyMode.COPY:
            plan.ops.append(CopyOp(src=src, dst=dst))
        elif mode == CopyMode.MOVE:
            plan.ops.append(MoveOp(src=src, dst=dst))

    return plan


def execute_plan(plan: FileOperationPlan) -> None:
    """Execute every operation in the plan against the real filesystem."""
    file_ops = [op for op in plan.ops if not isinstance(op, SkipOp)]
    for op in tqdm(file_ops, total=len(file_ops), disable=len(file_ops) < 50):
        if isinstance(op, MkdirOp):
            os.makedirs(op.path, exist_ok=True)
        elif isinstance(op, CopyOp):
            copy2(str(op.src), str(op.dst))
        elif isinstance(op, MoveOp):
            move(str(op.src), str(op.dst))

    if plan.n_skips:
        logger.info(f"Skipped {plan.n_skips} files (NOP mode)")
