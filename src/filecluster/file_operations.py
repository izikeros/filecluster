"""File operation planning and execution.

Separates the *decision* of what to do with files (the plan) from the
*execution* of those decisions (the executor).  This makes dry-run mode
trivial, testing pure, and the I/O boundary explicit.
"""

from __future__ import annotations

import errno
import math
import os
import re
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from shutil import copy2

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


class OperationMode(StrEnum):
    """The filesystem action selected by a workflow plan."""

    COPY = "copy"
    MOVE = "move"
    SKIP = "skip"


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


def numbered_name(name: str, counter: int) -> str:
    """Return *name* with ``(counter)`` inserted before the extension."""
    path = Path(name)
    suffix = path.suffix
    stem = path.name[: -len(suffix)] if suffix else path.name
    return f"{stem} ({counter}){suffix}"


def unique_name(name: str, claimed_names: set[str]) -> str:
    """Return *name*, or the first ``stem (n).ext`` variant not yet claimed.

    *claimed_names* holds lower-cased names, matching the case-insensitive
    behaviour of macOS and Windows filesystems.
    """
    if name.lower() not in claimed_names:
        return name

    counter = 1
    while True:
        candidate = numbered_name(name, counter)
        if candidate.lower() not in claimed_names:
            return candidate
        counter += 1


class DestinationAllocator:
    """Hands out destination paths that cannot overwrite an existing file.

    Names already on disk in a target directory are claimed lazily on first
    use, and every name handed out is claimed too, so neither an existing file
    nor an earlier file from the same run can be clobbered.

    ``shutil.move`` silently replaces the destination on POSIX, so any planner
    that moves files into a shared directory has to route through this.
    """

    def __init__(self) -> None:
        self._claimed: dict[Path, set[str]] = {}

    def claimed_for(self, directory: str | Path) -> set[str]:
        """Return the mutable set of claimed lower-cased names in *directory*."""
        key = Path(directory)
        if key not in self._claimed:
            existing: set[str] = set()
            # Every entry counts, not just regular files: a directory would
            # make `move` nest the source inside it, and a symlink (even a
            # dangling one) would redirect the write to its target.
            with suppress(OSError):
                existing = {entry.name.lower() for entry in os.scandir(key)}
            self._claimed[key] = existing
        return self._claimed[key]

    def peek(self, directory: str | Path, name: str) -> str:
        """Resolve *name* against *directory* without claiming it."""
        return unique_name(name, self.claimed_for(directory))

    def allocate(self, directory: str | Path, name: str) -> Path:
        """Claim and return a free destination path for *name* in *directory*."""
        claimed = self.claimed_for(directory)
        chosen = unique_name(name, claimed)
        claimed.add(chosen.lower())
        return Path(directory) / chosen

    def reserve(self, path: str | Path) -> None:
        """Mark *path* as taken without allocating a new name for it."""
        p = Path(path)
        self.claimed_for(p.parent).add(p.name.lower())


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
    allocator = DestinationAllocator()
    mapping: dict[str, str] = {}

    # When restoring, process un-suffixed originals first so they always keep
    # their names. Otherwise preserve the inbox order.
    rows = list(inbox_media_df.iterrows())
    if restore:
        originals = [
            r
            for _, r in rows
            if strip_copy_suffix(Path(r["file_name"]).name) == Path(r["file_name"]).name
        ]
        suffixed = [
            r
            for _, r in rows
            if strip_copy_suffix(Path(r["file_name"]).name) != Path(r["file_name"]).name
        ]
        suffixed.sort(key=lambda r: str(r["file_name"]))
        ordered_rows = [*originals, *suffixed]
    else:
        ordered_rows = [r for _, r in rows]

    for row in ordered_rows:
        name = row["file_name"]
        base_name = Path(name).name
        target_dir = Path(out_dir) / str(row["target_path"])
        claimed_names = allocator.claimed_for(target_dir)

        desired = strip_copy_suffix(base_name) if restore else base_name
        fallback = base_name if desired.lower() in claimed_names else desired
        mapping[name] = allocator.allocate(target_dir, fallback).name

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


def reserve_exclusive(dst: Path) -> Path:
    """Atomically create an empty placeholder at *dst*, or the next free name.

    Planning claims names against a directory listing, which is a
    time-of-check/time-of-use gap: anything created in between would be
    silently replaced at write time.  ``O_CREAT | O_EXCL`` closes it, because
    the kernel refuses the call when the name exists -- including when it is a
    directory or a symlink, dangling or not.

    Returns the path actually reserved, which differs from *dst* only when the
    planned name was taken after the plan was built.
    """
    counter = 0
    while True:
        candidate = (
            dst if counter == 0 else dst.with_name(numbered_name(dst.name, counter))
        )
        try:
            fd = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
        except FileExistsError:
            counter += 1
            continue
        os.close(fd)
        if counter:
            logger.warning(
                f"{dst} appeared after planning; writing to {candidate} instead"
            )
        return candidate


def execute_file_operation(op: CopyOp | MoveOp) -> Path:
    """Perform one copy or move and return its exclusively reserved destination.

    The destination may gain a numeric suffix when another process creates the
    planned name after the plan was built.  Callers that maintain their own
    plans can use the returned path to report that outcome accurately.
    """
    dst = reserve_exclusive(op.dst)
    try:
        if isinstance(op, MoveOp):
            _move_onto(op.src, dst)
        else:
            copy2(str(op.src), str(dst))
    except BaseException:
        # The placeholder is ours and nothing pre-existing can be at *dst*, so
        # removing a failed (possibly truncated) write cannot lose data.
        with suppress(OSError):
            os.unlink(dst)
        raise
    return dst


def _move_onto(src: Path, dst: Path) -> None:
    """Move *src* onto the reserved placeholder at *dst*."""
    try:
        # Atomic on the same filesystem, and the only thing it can replace is
        # the placeholder reserved by the caller.
        os.replace(str(src), str(dst))
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise

    # Different filesystem: copy, then drop the original. A symlink is
    # recreated rather than dereferenced, so the move does not silently turn a
    # link into a full copy of its target.
    if src.is_symlink():
        os.unlink(dst)
        os.symlink(os.readlink(src), dst)
    else:
        copy2(str(src), str(dst))
    os.unlink(str(src))


def execute_plan(plan: FileOperationPlan, progress: ProgressSink | None = None) -> None:
    """Execute every operation in the plan against the real filesystem.

    No operation can replace an existing file: each destination name is claimed
    exclusively at write time, and a name taken since planning gets a numeric
    suffix instead.

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
        elif isinstance(op, CopyOp | MoveOp):
            execute_file_operation(op)
        progress.advance()

    if plan.n_skips:
        logger.debug(f"Skipped {plan.n_skips} files (NOP mode)")
