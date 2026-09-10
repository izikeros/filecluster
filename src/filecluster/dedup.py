"""Find and quarantine duplicate media files inside a single directory tree.

Where :mod:`filecluster.reconcile` compares a *source* against a *library*,
this module looks for duplicates **within** one tree: the same photo stored
twice in the same event folder, or spread across several folders after years of
ad-hoc copying.

The scan uses the same 3-level cascade as the rest of the project (size →
first-1 MB MD5 → full SHA1), so a library where most files are unique is barely
read at all.  One file per group is chosen as the canonical copy and left in
place; the rest are reported and, when asked, moved to a quarantine folder.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.catalog import LibraryCatalog
from filecluster.configuration import FileClusterSettings
from filecluster.file_operations import (
    DestinationAllocator,
    FileOperationPlan,
    MkdirOp,
    MoveOp,
    SkipOp,
    execute_plan,
    strip_copy_suffix,
)
from filecluster.ui import NullProgress, ProgressSink
from filecluster.utlis import (
    get_partial_hash,
    hash_file,
    is_event_folder_name,
    walk_media_files,
)

_settings = FileClusterSettings()
_ALL_EXTENSIONS = _settings.image_extensions + _settings.video_extensions


class DedupAction(StrEnum):
    """What to do with the non-canonical copies."""

    REPORT = "report"
    QUARANTINE = "quarantine"


# ---------------------------------------------------------------------------
# DuplicateGroup
# ---------------------------------------------------------------------------
@dataclass
class DuplicateGroup:
    """A set of two or more files with byte-identical content."""

    full_hash: str
    size: int
    files: list[Path]

    @property
    def canonical(self) -> Path:
        """The copy to keep."""
        return self.files[0]

    @property
    def duplicates(self) -> list[Path]:
        """The copies that can go."""
        return self.files[1:]

    @property
    def n_copies(self) -> int:
        return len(self.files)

    @property
    def wasted_bytes(self) -> int:
        """Space that would be freed by keeping only the canonical copy."""
        return self.size * len(self.duplicates)

    @property
    def folders(self) -> list[Path]:
        """Distinct parent directories holding a copy, in first-seen order."""
        seen: list[Path] = []
        for f in self.files:
            if f.parent not in seen:
                seen.append(f.parent)
        return seen

    @property
    def is_intra_folder(self) -> bool:
        """True when every copy sits in the same directory."""
        return len(self.folders) == 1

    @property
    def is_cross_folder(self) -> bool:
        """True when copies are spread over more than one directory."""
        return len(self.folders) > 1


# ---------------------------------------------------------------------------
# DedupPlan
# ---------------------------------------------------------------------------
@dataclass
class DedupPlan:
    """Duplicate groups found in a tree, plus the operations to resolve them."""

    root: Path
    groups: list[DuplicateGroup] = field(default_factory=list)
    ops: list[MoveOp | SkipOp | MkdirOp] = field(default_factory=list)
    action: DedupAction = DedupAction.REPORT
    n_scanned: int = 0
    n_hashed: int = 0

    @property
    def n_groups(self) -> int:
        return len(self.groups)

    @property
    def n_duplicate_files(self) -> int:
        return sum(len(g.duplicates) for g in self.groups)

    @property
    def n_intra_folder_groups(self) -> int:
        return sum(1 for g in self.groups if g.is_intra_folder)

    @property
    def n_cross_folder_groups(self) -> int:
        return sum(1 for g in self.groups if g.is_cross_folder)

    @property
    def wasted_bytes(self) -> int:
        return sum(g.wasted_bytes for g in self.groups)

    @property
    def n_moves(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MoveOp))

    @property
    def n_mkdirs(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MkdirOp))

    @property
    def move_destinations(self) -> list[tuple[str, str, str]]:
        """``(target_folder, source_name, destination_name)`` per planned move."""
        return [
            (str(op.dst.parent), op.src.name, op.dst.name)
            for op in self.ops
            if isinstance(op, MoveOp)
        ]

    def summary_dict(self) -> dict[str, Any]:
        """Machine-readable summary."""
        return {
            "root": str(self.root),
            "action": self.action.value,
            "files_scanned": self.n_scanned,
            "files_hashed": self.n_hashed,
            "duplicate_groups": self.n_groups,
            "duplicate_files": self.n_duplicate_files,
            "intra_folder_groups": self.n_intra_folder_groups,
            "cross_folder_groups": self.n_cross_folder_groups,
            "wasted_bytes": self.wasted_bytes,
            "moves": self.n_moves,
            "folders_created": self.n_mkdirs,
        }

    def write_csv(self, path: Path | str) -> int:
        """Write one row per duplicate copy to *path*.  Returns row count."""
        rows = 0
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "group",
                    "role",
                    "path",
                    "size",
                    "full_hash",
                    "n_copies",
                    "scope",
                ]
            )
            for i, group in enumerate(self.groups, start=1):
                scope = "same-folder" if group.is_intra_folder else "cross-folder"
                for f in group.files:
                    role = "keep" if f == group.canonical else "duplicate"
                    writer.writerow(
                        [
                            i,
                            role,
                            str(f),
                            group.size,
                            group.full_hash,
                            group.n_copies,
                            scope,
                        ]
                    )
                    rows += 1
        return rows


# ---------------------------------------------------------------------------
# Canonical-copy selection
# ---------------------------------------------------------------------------
def _canonical_sort_key(path: Path) -> tuple:
    """Rank a copy: lower sorts first and becomes the canonical one.

    Preference order: inside an event folder, no copy-suffix in the name,
    shallower path, shorter name, then alphabetical for a stable result.
    """
    in_event_folder = any(is_event_folder_name(part) for part in path.parts)
    has_copy_suffix = strip_copy_suffix(path.name) != path.name
    return (
        0 if in_event_folder else 1,
        1 if has_copy_suffix else 0,
        len(path.parts),
        len(path.name),
        str(path).lower(),
    )


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------
def find_duplicate_groups(
    root: Path,
    *,
    extensions: list[str] | None = None,
    recursive: bool = True,
    min_size: int = 1,
    progress: ProgressSink | None = None,
    use_catalog: bool = True,
    read_only: bool = False,
    exclude_dirs: Sequence[Path] = (),
) -> tuple[list[DuplicateGroup], int, int]:
    """Find groups of byte-identical media files under *root*.

    Args:
        root: Directory to scan.
        extensions: Media extensions to consider.
        recursive: Walk sub-directories.
        min_size: Ignore files smaller than this, in bytes.
        progress: Optional progress sink.
        use_catalog: Read and update ``<root>/.filecluster.db`` so repeated
            runs do not re-hash unchanged files.
        read_only: Reuse an existing catalog but write nothing back, so a
            preview leaves no ``.filecluster.db`` behind.
        exclude_dirs: Sub-trees to leave out, typically a quarantine folder
            that lives inside *root*.

    Returns:
        ``(groups, n_scanned, n_hashed)``.
    """
    progress = progress or NullProgress()
    exts = extensions or _ALL_EXTENSIONS
    files = walk_media_files(root, exts, recursive=recursive)
    if exclude_dirs:
        excluded = [Path(d).resolve() for d in exclude_dirs]
        files = [f for f in files if not _is_under(f, excluded)]

    sized: dict[int, list[Path]] = {}
    stats: dict[Path, tuple[int, float]] = {}
    for f in files:
        try:
            st = f.stat()
        except OSError:
            continue
        if st.st_size < min_size:
            continue
        stats[f] = (st.st_size, st.st_mtime)
        sized.setdefault(st.st_size, []).append(f)

    # Only same-size files can possibly be duplicates, so everything unique by
    # size is dismissed without reading a single byte.
    contenders = [f for group in sized.values() if len(group) > 1 for f in group]
    cache = _HashCache(root, stats, enabled=use_catalog, read_only=read_only)

    progress.start(len(contenders), "Hashing candidates")
    groups: list[DuplicateGroup] = []
    for size, same_size in sorted(sized.items()):
        if len(same_size) < 2:
            continue
        groups.extend(_group_same_size(same_size, size, cache, progress))

    cache.flush()
    logger.debug(
        f"Dedup scan: {len(files)} files, {len(contenders)} same-size candidates, "
        f"{len(groups)} duplicate groups"
    )
    return groups, len(files), cache.n_hashed


def _is_under(path: Path, roots: list[Path]) -> bool:
    """Whether *path* lies inside any of *roots*."""
    resolved = path.resolve()
    return any(resolved == r or r in resolved.parents for r in roots)


def _group_same_size(
    paths: list[Path],
    size: int,
    cache: _HashCache,
    progress: ProgressSink,
) -> list[DuplicateGroup]:
    """Split same-size files into byte-identical groups."""
    by_partial: dict[str, list[Path]] = {}
    for path in paths:
        progress.advance()
        partial = cache.partial_hash(path)
        if partial is None:
            continue
        by_partial.setdefault(partial, []).append(path)

    groups: list[DuplicateGroup] = []
    for same_partial in by_partial.values():
        if len(same_partial) < 2:
            continue
        by_full: dict[str, list[Path]] = {}
        for path in same_partial:
            full = cache.full_hash(path)
            if full is None:
                continue
            by_full.setdefault(full, []).append(path)
        for full, members in by_full.items():
            if len(members) < 2:
                continue
            groups.append(
                DuplicateGroup(
                    full_hash=full,
                    size=size,
                    files=sorted(members, key=_canonical_sort_key),
                )
            )
    return groups


class _HashCache:
    """Hash lookup for one tree, backed by the library catalog.

    A cached hash is trusted only while the file's size and mtime still match
    what was recorded, so editing a file never yields a stale hash.

    In read-only mode an existing catalog is still read, but nothing is created
    or written, so a dry run leaves the tree exactly as it found it.
    """

    def __init__(
        self,
        root: Path,
        stats: dict[Path, tuple[int, float]],
        *,
        enabled: bool = True,
        read_only: bool = False,
    ) -> None:
        self.root = root
        self._stats = stats
        self._partial: dict[Path, str | None] = {}
        self._full: dict[Path, str | None] = {}
        self._dirty: set[Path] = set()
        self.n_hashed = 0
        self.read_only = read_only
        self._catalog: LibraryCatalog | None = None
        if not enabled:
            return
        try:
            self._catalog = LibraryCatalog.open(root, read_only=read_only)
        except Exception as exc:
            logger.debug(f"Dedup running without catalog for {root}: {exc}")
            return
        for rel, record in self._catalog.get_file_records().items():
            path = root / rel
            st = stats.get(path)
            if st is None or not record.matches(*st):
                continue
            if record.partial_hash is not None:
                self._partial[path] = record.partial_hash
            if record.full_hash is not None:
                self._full[path] = record.full_hash

    def partial_hash(self, path: Path) -> str | None:
        if path not in self._partial:
            self._partial[path] = get_partial_hash(str(path))
            self._dirty.add(path)
            self.n_hashed += 1
        return self._partial[path]

    def full_hash(self, path: Path) -> str | None:
        if path not in self._full:
            try:
                self._full[path] = hash_file(str(path))
            except OSError:
                self._full[path] = None
            self._dirty.add(path)
        return self._full[path]

    def flush(self) -> None:
        """Persist newly computed hashes, then close the catalog."""
        if self._catalog is None:
            return
        if self.read_only:
            self._catalog.close()
            self._catalog = None
            return
        entries: list[tuple[str, int, float, str | None, str | None]] = []
        for path in self._dirty:
            st = self._stats.get(path)
            if st is None:
                continue
            try:
                rel = str(path.relative_to(self.root))
            except ValueError:
                continue
            entries.append(
                (rel, st[0], st[1], self._partial.get(path), self._full.get(path))
            )
        if entries:
            self._catalog.put_file_hashes(entries)
        self._catalog.close()
        self._catalog = None


# ---------------------------------------------------------------------------
# Planning and execution
# ---------------------------------------------------------------------------
def build_dedup_plan(
    root: Path,
    groups: list[DuplicateGroup],
    quarantine_dir: Path | None,
    action: DedupAction,
    *,
    n_scanned: int = 0,
    n_hashed: int = 0,
) -> DedupPlan:
    """Turn duplicate *groups* into a plan.

    In ``QUARANTINE`` mode every non-canonical copy is moved under
    *quarantine_dir*, keeping its path relative to *root* so the original
    layout stays recoverable.  Destination names are allocated collision-free.
    """
    plan = DedupPlan(
        root=root,
        groups=groups,
        action=action,
        n_scanned=n_scanned,
        n_hashed=n_hashed,
    )
    if action == DedupAction.REPORT or quarantine_dir is None:
        for group in groups:
            for dup in group.duplicates:
                plan.ops.append(
                    SkipOp(src=dup, reason=f"duplicate of {group.canonical}")
                )
        return plan

    allocator = DestinationAllocator()
    made: set[Path] = set()
    for group in groups:
        for dup in group.duplicates:
            try:
                rel_dir = dup.parent.relative_to(root)
            except ValueError:
                rel_dir = Path()
            dest_dir = quarantine_dir / rel_dir
            if dest_dir not in made:
                made.add(dest_dir)
                plan.ops.append(MkdirOp(path=dest_dir))
            plan.ops.append(MoveOp(src=dup, dst=allocator.allocate(dest_dir, dup.name)))
    return plan


def dedup(
    root: Path,
    quarantine_dir: Path | None = None,
    *,
    action: DedupAction = DedupAction.REPORT,
    execute: bool = False,
    extensions: list[str] | None = None,
    recursive: bool = True,
    min_size: int = 1,
    progress: ProgressSink | None = None,
    use_catalog: bool = True,
) -> DedupPlan:
    """Scan *root* for duplicates and return the resulting plan.

    Nothing is written unless *execute* is True and *action* is
    ``QUARANTINE`` -- not even the hash catalog, so a preview cannot be told
    apart from not having run at all.
    """
    writes = execute and action == DedupAction.QUARANTINE
    # A quarantine folder inside the scanned tree would otherwise show up as a
    # duplicate of the file it already holds.
    exclude = [quarantine_dir] if quarantine_dir is not None else []
    groups, n_scanned, n_hashed = find_duplicate_groups(
        root,
        extensions=extensions,
        recursive=recursive,
        min_size=min_size,
        progress=progress,
        use_catalog=use_catalog,
        read_only=not writes,
        exclude_dirs=exclude,
    )
    plan = build_dedup_plan(
        root,
        groups,
        quarantine_dir,
        action,
        n_scanned=n_scanned,
        n_hashed=n_hashed,
    )
    if writes:
        execute_plan(FileOperationPlan(ops=list(plan.ops)), progress)
    return plan
