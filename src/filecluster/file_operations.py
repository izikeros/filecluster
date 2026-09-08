"""File operation planning and execution.

Separates the *decision* of what to do with files (the plan) from the
*execution* of those decisions (the executor).  This makes dry-run mode
trivial, testing pure, and the I/O boundary explicit.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from shutil import copy2, move

from filecluster import logger
from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneError
from filecluster.ui import NullProgress, ProgressSink

# Copy-suffix patterns appended (by file managers) just before the extension.
# Stripped, in order, from the end of the file *stem*:
#   - Polish Windows: "-Kopiuj", "-Kopiuj(1)"
#   - English Windows: " - Copy", " - Copy (2)"
#   - Bare numeric duplicate marker: " (1)", "(1)"
_COPY_SUFFIX_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"-Kopiuj(?:\s*\(\d+\))?$"),
    re.compile(r"\s*-\s*Copy(?:\s*\(\d+\))?$", re.IGNORECASE),
    re.compile(r"\s*\(\d+\)$"),
)


def strip_copy_suffix(name: str) -> str:
    """Return *name* with a trailing copy-suffix removed from its stem.

    The file extension is preserved.  If no known copy-suffix is present, the
    original name is returned unchanged.

    Examples:
        ``IMG_0017-Kopiuj(2).HEIC`` -> ``IMG_0017.HEIC``
        ``photo - Copy (3).jpg``    -> ``photo.jpg``
        ``photo (1).jpg``           -> ``photo.jpg``
        ``IMG_0017.HEIC``           -> ``IMG_0017.HEIC`` (unchanged)
    """
    p = Path(name)
    suffix = p.suffix  # includes leading dot, '' if none
    stem = p.name[: len(p.name) - len(suffix)] if suffix else p.name

    for pattern in _COPY_SUFFIX_PATTERNS:
        new_stem, n_subs = pattern.subn("", stem)
        if n_subs:
            return f"{new_stem}{suffix}"
    return name


def resolve_destination_names(
    inbox_media_df,
    out_dir: Path,
    restore: bool,
) -> dict[str, str]:
    """Map each inbox ``file_name`` to its destination basename.

    Existing files and files from the same run are never overwritten. When
    *restore* is True, copy-suffixes are stripped where it is safe to do so.
    Files that already have no suffix claim their name first, so true originals
    win. Any remaining collision receives a numeric suffix.

    Args:
        inbox_media_df: DataFrame with ``file_name`` and ``target_path`` columns.
        out_dir: Output directory root (used to inspect existing target dirs).
        restore: Whether to attempt reverting to original (de-suffixed) names.

    Returns:
        Mapping of original ``file_name`` -> destination basename.
    """
    # Per-target-dir set of already-claimed (lower-cased) names, seeded with
    # whatever already exists on disk in that directory.
    claimed: dict[str, set[str]] = {}

    def _claimed_for(target_path: str) -> set[str]:
        if target_path not in claimed:
            existing: set[str] = set()
            dir_path = Path(out_dir) / str(target_path)
            if dir_path.is_dir():
                existing = {p.name.lower() for p in dir_path.iterdir() if p.is_file()}
            claimed[target_path] = existing
        return claimed[target_path]

    mapping: dict[str, str] = {}

    def _unique_name(name: str, claimed_names: set[str]) -> str:
        if name.lower() not in claimed_names:
            return name

        path = Path(name)
        suffix = path.suffix
        stem = path.name[: -len(suffix)] if suffix else path.name
        counter = 1
        while True:
            candidate = f"{stem} ({counter}){suffix}"
            if candidate.lower() not in claimed_names:
                return candidate
            counter += 1

    # When restoring, process un-suffixed originals first so they always keep
    # their names. Otherwise preserve the inbox order.
    rows = list(inbox_media_df.iterrows())
    if restore:
        originals = [
            r for _, r in rows if strip_copy_suffix(r["file_name"]) == r["file_name"]
        ]
        suffixed = [
            r for _, r in rows if strip_copy_suffix(r["file_name"]) != r["file_name"]
        ]
        suffixed.sort(key=lambda r: str(r["file_name"]))
        ordered_rows = [*originals, *suffixed]
    else:
        ordered_rows = [r for _, r in rows]

    for row in ordered_rows:
        name = row["file_name"]
        target_path = row["target_path"]
        claimed_names = _claimed_for(str(target_path))

        desired = strip_copy_suffix(name) if restore else name
        fallback = name if desired.lower() in claimed_names else desired
        chosen = _unique_name(fallback, claimed_names)

        claimed_names.add(chosen.lower())
        mapping[name] = chosen

    return mapping


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
    """Record that a file was intentionally skipped.

    ``dst`` carries the destination the file *would* have received. It lets a
    dry run preview the resolved names (including collision renames) without
    the plan performing any I/O.
    """

    src: Path
    reason: str
    dst: Path | None = None


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
    def n_renamed(self) -> int:
        """Files whose destination name differs from the source name."""
        return sum(
            1
            for op in self.ops
            if isinstance(op, MoveOp | CopyOp | SkipOp)
            and op.dst is not None
            and op.src.name != op.dst.name
        )

    @property
    def destinations(self) -> list[tuple[str, str, str]]:
        """``(target_folder, source_name, destination_name)`` for every file.

        Populated in every mode, so a dry run can be previewed identically to
        a real run. The folder is the full destination directory; callers that
        display it are expected to shorten it.
        """
        out: list[tuple[str, str, str]] = []
        for op in self.ops:
            if isinstance(op, MoveOp | CopyOp | SkipOp) and op.dst is not None:
                out.append((str(op.dst.parent), op.src.name, op.dst.name))
        return out

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
    restore_original_names: bool = False,
) -> FileOperationPlan:
    """Build a plan of file operations from the fully-annotated media DataFrame.

    Args:
        inbox_media_df: DataFrame with 'file_name' and 'target_path' columns set.
        in_dir: Inbox directory (source).
        out_dir: Output directory (destination root).
        mode: COPY, MOVE, or NOP.
        restore_original_names: When True, strip copy-suffixes from destination
            file names (e.g. ``IMG_0017-Kopiuj(2).HEIC`` -> ``IMG_0017.HEIC``),
            unless doing so would collide with an existing or already-claimed
            name in the target folder.

    Returns:
        A FileOperationPlan ready to be executed (or inspected).
    """
    plan = FileOperationPlan()

    if mode == CopyMode.NOP:
        preview_names = resolve_destination_names(
            inbox_media_df, Path(out_dir), restore=restore_original_names
        )
        for _, row in inbox_media_df.iterrows():
            name = row["file_name"]
            plan.ops.append(
                SkipOp(
                    src=Path(in_dir) / name,
                    reason="NOP mode",
                    dst=Path(out_dir) / str(row["target_path"]) / preview_names[name],
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

    # Resolve destination basenames (optionally reverting copy-suffixes)
    dst_names = resolve_destination_names(
        inbox_media_df, Path(out_dir), restore=restore_original_names
    )

    # Plan file operations
    for _, row in inbox_media_df.iterrows():
        src = Path(in_dir) / row["file_name"]
        dst_name = dst_names[row["file_name"]]
        dst = Path(out_dir) / str(row["target_path"]) / dst_name
        if mode == CopyMode.COPY:
            plan.ops.append(CopyOp(src=src, dst=dst))
        elif mode == CopyMode.MOVE:
            plan.ops.append(MoveOp(src=src, dst=dst))

    return plan


def execute_plan(plan: FileOperationPlan, progress: ProgressSink | None = None) -> None:
    """Execute every operation in the plan against the real filesystem.

    Args:
        plan: Operations to perform.
        progress: Optional sink notified of each completed operation.
    """
    progress = progress or NullProgress()
    file_ops = [op for op in plan.ops if not isinstance(op, SkipOp)]
    progress.start(len(file_ops), "Writing files")
    for op in file_ops:
        if isinstance(op, MkdirOp):
            os.makedirs(op.path, exist_ok=True)
        elif isinstance(op, CopyOp):
            copy2(str(op.src), str(op.dst))
        elif isinstance(op, MoveOp):
            move(str(op.src), str(op.dst))
        progress.advance()

    if plan.n_skips:
        logger.debug(f"Skipped {plan.n_skips} files (NOP mode)")
