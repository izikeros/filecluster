"""Planning and execution of the file moves that follow a curation run.

Classification stays pure with respect to the filesystem: the whole plan is built
first, shown once, and only then executed. A dry run and a real run compute the
same destination names, including the renames that avoid collisions, so the
preview is the truth.

Nothing is ever deleted and nothing is ever overwritten. ``reject`` means "do not
import automatically", and it is a folder like any other.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from shutil import copy2, move

from filecluster import logger
from filecluster.curation.exceptions import UnsafeRelativePathError
from filecluster.curation.types import CurationDecision, CurationResult
from filecluster.file_operations import DestinationAllocator
from filecluster.ui import NullProgress, ProgressSink


class OperationMode(StrEnum):
    """What to do with the analysed files."""

    COPY = "copy"
    MOVE = "move"
    #: Analyse only. The plan still resolves destinations so a dry run can show
    #: exactly what a real run would write.
    SKIP = "skip"


class OperationStatus(StrEnum):
    """Lifecycle of a single planned operation."""

    PLANNED = "planned"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class CurationFileOp:
    """One planned operation on one file."""

    src: Path
    dst: Path
    decision: CurationDecision
    mode: OperationMode


@dataclass
class CurationOperationPlan:
    """An ordered, previewable set of operations plus their outcome."""

    output_dir: Path
    mode: OperationMode
    ops: list[CurationFileOp] = field(default_factory=list)
    directories: list[Path] = field(default_factory=list)
    statuses: dict[str, OperationStatus] = field(default_factory=dict)

    @property
    def n_copies(self) -> int:
        """Planned copies."""
        return sum(1 for op in self.ops if op.mode is OperationMode.COPY)

    @property
    def n_moves(self) -> int:
        """Planned moves."""
        return sum(1 for op in self.ops if op.mode is OperationMode.MOVE)

    @property
    def n_writes(self) -> int:
        """Operations that will touch the destination filesystem."""
        return self.n_copies + self.n_moves

    @property
    def n_renamed(self) -> int:
        """Files whose destination name had to change to avoid a collision."""
        return sum(1 for op in self.ops if op.src.name != op.dst.name)

    @property
    def n_failed(self) -> int:
        """Operations that were attempted and did not succeed."""
        return sum(1 for s in self.statuses.values() if s is OperationStatus.FAILED)

    @property
    def n_completed(self) -> int:
        """Operations that finished successfully."""
        return sum(1 for s in self.statuses.values() if s is OperationStatus.COMPLETED)

    def status_for(self, src: Path) -> OperationStatus:
        """Return the status recorded for the operation on *src*."""
        return self.statuses.get(str(src), OperationStatus.PLANNED)

    def destination_for(self, src: Path) -> Path | None:
        """Return the resolved destination of *src*, if it was planned."""
        for op in self.ops:
            if op.src == src:
                return op.dst
        return None

    def preview(self) -> list[tuple[str, str, str]]:
        """``(destination_folder, source_name, destination_name)`` per file."""
        return [(str(op.dst.parent), op.src.name, op.dst.name) for op in self.ops]

    def counts_by_decision(self) -> dict[str, int]:
        """How many operations were planned per decision folder."""
        counts = dict.fromkeys((d.value for d in CurationDecision), 0)
        for op in self.ops:
            counts[op.decision.value] += 1
        return counts


def safe_relative_path(relative_path: str) -> Path:
    """Validate that *relative_path* stays inside its root.

    Destination paths are built from a controlled decision name plus this value,
    so an absolute path or a ``..`` component has to be refused rather than
    normalised away.

    Raises:
        UnsafeRelativePathError: when the path escapes its root.
    """
    candidate = Path(relative_path)
    if candidate.is_absolute() or any(part == ".." for part in candidate.parts):
        raise UnsafeRelativePathError(f"Unsafe relative path: {relative_path!r}")
    return candidate


def build_operation_plan(
    results: Sequence[CurationResult],
    output_dir: str | Path,
    mode: OperationMode = OperationMode.COPY,
) -> CurationOperationPlan:
    """Resolve a destination for every result, without touching the filesystem.

    Destinations follow ``<output>/<decision>/<original relative path>``. Names
    are allocated through :class:`DestinationAllocator`, which claims both the
    names already on disk and the ones handed out earlier in the same run, so
    neither an existing file nor a sibling from this run can be overwritten.
    """
    root = Path(output_dir)
    allocator = DestinationAllocator()
    plan = CurationOperationPlan(output_dir=root, mode=mode)
    seen_dirs: set[Path] = set()

    for result in sorted(results, key=lambda r: r.item.relative_path):
        relative = safe_relative_path(result.item.relative_path)
        target_dir = root / result.decision.value / relative.parent
        if target_dir not in seen_dirs:
            seen_dirs.add(target_dir)
            plan.directories.append(target_dir)
        destination = allocator.allocate(target_dir, relative.name)
        op = CurationFileOp(
            src=result.item.path,
            dst=destination,
            decision=result.decision,
            mode=mode,
        )
        plan.ops.append(op)
        plan.statuses[str(op.src)] = OperationStatus.PLANNED

    return plan


def execute_plan(
    plan: CurationOperationPlan,
    progress: ProgressSink | None = None,
) -> CurationOperationPlan:
    """Perform the planned operations, recording the outcome of each one.

    A failing file is recorded and the run continues; an interrupt stops further
    operations without attempting to undo the ones already finished, which is
    why the report distinguishes planned from completed.
    """
    progress = progress or NullProgress()
    if plan.mode is OperationMode.SKIP:
        logger.debug("Curation plan is analysis-only; nothing to write")
        return plan

    for directory in plan.directories:
        os.makedirs(directory, exist_ok=True)

    progress.start(len(plan.ops), "Writing files")
    for op in plan.ops:
        try:
            if op.mode is OperationMode.COPY:
                copy2(str(op.src), str(op.dst))
            else:
                move(str(op.src), str(op.dst))
            plan.statuses[str(op.src)] = OperationStatus.COMPLETED
        except OSError as exc:
            logger.warning(f"Could not {op.mode.value} {op.src}: {exc}")
            plan.statuses[str(op.src)] = OperationStatus.FAILED
        progress.advance()
    return plan
