"""Reconcile a source directory against a main photo library.

Checks whether files in a source directory (inbox or filecluster output dirs
with event folders) already exist in the main library, then either moves
duplicates aside or integrates new files into the library.

The matching uses the same 3-level cascade as ``mark_inbox_duplicates``:

1. **Size match** — filter library candidates by identical file size
2. **Partial hash** (first 1 MB, MD5) — cheap, eliminates most false positives
3. **Full hash** (SHA1) — definitive confirmation
"""

from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.catalog import LibraryCatalog
from filecluster.configuration import FileClusterSettings
from filecluster.file_operations import MkdirOp, MoveOp, SkipOp
from filecluster.image_grouper import get_partial_hash
from filecluster.ui import NullProgress, ProgressSink
from filecluster.utlis import hash_file, is_supported_filetype

# Regex for event-folder names produced by filecluster: ``[YYYY_MM_DD]…``
_EVENT_FOLDER_RE = re.compile(r"^\[(\d{4})_(\d{2})_(\d{2})\]")

_settings = FileClusterSettings()
_ALL_EXTENSIONS = _settings.image_extensions + _settings.video_extensions


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class FileStatus(StrEnum):
    """Classification of a single source file."""

    DUPLICATE = "DUPLICATE"
    NEW = "NEW"
    NAME_COLLISION = "NAME_COLLISION"


class FolderStatus(StrEnum):
    """Aggregate classification of an event folder."""

    ALL_DUPLICATE = "ALL_DUPLICATE"
    ALL_NEW = "ALL_NEW"
    PARTIAL = "PARTIAL"


class SourceMode(StrEnum):
    """How the source directory is structured."""

    EVENT_FOLDERS = "event-folders"
    FLAT = "flat"


# ---------------------------------------------------------------------------
# FileMatch — per-file result
# ---------------------------------------------------------------------------
@dataclass
class FileMatch:
    """Result of matching one source file against the library."""

    source_path: Path
    status: FileStatus
    library_match: Path | None = None
    name_collision_path: Path | None = None


# ---------------------------------------------------------------------------
# FolderResult — per-folder aggregate
# ---------------------------------------------------------------------------
@dataclass
class FolderResult:
    """Aggregate result for one event folder."""

    folder_name: str
    folder_path: Path
    status: FolderStatus
    files: list[FileMatch] = field(default_factory=list)

    @property
    def n_duplicates(self) -> int:
        return sum(1 for f in self.files if f.status == FileStatus.DUPLICATE)

    @property
    def n_new(self) -> int:
        return sum(1 for f in self.files if f.status == FileStatus.NEW)

    @property
    def n_name_collisions(self) -> int:
        return sum(1 for f in self.files if f.name_collision_path is not None)


# ---------------------------------------------------------------------------
# ReconcileOp — planned action
# ---------------------------------------------------------------------------
ReconcileOp = MoveOp | SkipOp | MkdirOp


# ---------------------------------------------------------------------------
# ReconcilePlan
# ---------------------------------------------------------------------------
@dataclass
class ReconcilePlan:
    """An ordered list of file operations produced by reconciliation."""

    ops: list[ReconcileOp] = field(default_factory=list)
    file_matches: list[FileMatch] = field(default_factory=list)
    folder_results: list[FolderResult] = field(default_factory=list)
    source_mode: SourceMode = SourceMode.FLAT

    @property
    def n_duplicates(self) -> int:
        return sum(1 for m in self.file_matches if m.status == FileStatus.DUPLICATE)

    @property
    def n_new(self) -> int:
        return sum(1 for m in self.file_matches if m.status == FileStatus.NEW)

    @property
    def n_name_collisions(self) -> int:
        return sum(1 for m in self.file_matches if m.name_collision_path is not None)

    @property
    def n_moves(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MoveOp))

    @property
    def n_skips(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, SkipOp))

    @property
    def n_mkdirs(self) -> int:
        return sum(1 for op in self.ops if isinstance(op, MkdirOp))

    @property
    def move_destinations(self) -> list[tuple[str, str, str]]:
        """``(target_folder, source_name, destination_name)`` for every move."""
        out: list[tuple[str, str, str]] = []
        for op in self.ops:
            if isinstance(op, MoveOp):
                out.append((str(op.dst.parent), op.src.name, op.dst.name))
        return out

    def summary_dict(self) -> dict[str, Any]:
        """Machine-readable summary."""
        return {
            "source_mode": self.source_mode.value,
            "total_files": len(self.file_matches),
            "duplicates": self.n_duplicates,
            "new": self.n_new,
            "name_collisions": self.n_name_collisions,
            "moves": self.n_moves,
            "skips": self.n_skips,
            "folders_created": self.n_mkdirs,
        }

    def write_csv(self, path: Path | str) -> int:
        """Write per-file results to *path* as CSV.  Returns row count."""
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "source_path",
                    "status",
                    "library_match",
                    "name_collision",
                ]
            )
            for m in self.file_matches:
                writer.writerow(
                    [
                        str(m.source_path),
                        m.status.value,
                        str(m.library_match) if m.library_match else "",
                        str(m.name_collision_path) if m.name_collision_path else "",
                    ]
                )
        return len(self.file_matches)


# ---------------------------------------------------------------------------
# LibraryIndex
# ---------------------------------------------------------------------------
class LibraryIndex:
    """In-memory index of a photo library, backed by the SQLite catalog.

    Walks the library once, building a ``{size: [(path, partial_hash,
    full_hash), …]}`` lookup.  Previously computed hashes are loaded from the
    catalog; new ones are written back.
    """

    def __init__(
        self,
        library_path: Path,
        *,
        force_reindex: bool = False,
        extensions: list[str] | None = None,
        progress: ProgressSink | None = None,
    ) -> None:
        self.library_path = library_path
        self._extensions = extensions or _ALL_EXTENSIONS
        self._by_size: dict[int, list[tuple[Path, str | None, str | None]]] = {}
        self._by_name: dict[str, list[Path]] = {}
        self._partial_cache: dict[str, str | None] = {}
        self._full_cache: dict[str, str | None] = {}
        self._catalog: LibraryCatalog | None = None
        self._new_hashes: list[tuple[str, int, float, str | None, str | None]] = []

        self._build(force_reindex=force_reindex, progress=progress or NullProgress())

    # -- construction -------------------------------------------------------

    def _build(self, *, force_reindex: bool, progress: ProgressSink) -> None:
        self._open_catalog(force_reindex)
        self._load_cached_hashes(force_reindex)

        all_files = self._walk_library()
        desc = "Rebuilding library index" if force_reindex else "Indexing library"
        progress.start(len(all_files), desc)

        for fpath in all_files:
            progress.advance()
            self._index_file(fpath, force_reindex)

        n_cached = len(self._partial_cache)
        logger.debug(f"Library index: {len(all_files)} files, {n_cached} cached hashes")

    def _open_catalog(self, force_reindex: bool) -> None:
        """Open (and optionally backup/clear) the SQLite catalog."""
        if force_reindex:
            backup_path = LibraryCatalog.backup(self.library_path)
            if backup_path is not None:
                logger.info(f"Catalog backed up to {backup_path}")

        try:
            self._catalog = LibraryCatalog.open(self.library_path)
        except Exception as exc:
            logger.debug(f"Could not open catalog for {self.library_path}: {exc}")

        if force_reindex and self._catalog is not None:
            cleared = self._catalog.clear_file_hashes()
            logger.info(f"Cleared {cleared} cached hashes for full reindex")

    def _load_cached_hashes(self, force_reindex: bool) -> None:
        """Seed in-memory caches from the catalog (skipped on force-reindex)."""
        if self._catalog is None or force_reindex:
            return
        for rel, (_size, p_hash, f_hash) in self._catalog.get_file_hashes().items():
            abs_path = str(self.library_path / rel)
            if p_hash is not None:
                self._partial_cache[abs_path] = p_hash
            if f_hash is not None:
                self._full_cache[abs_path] = f_hash

    def _walk_library(self) -> list[Path]:
        """Collect every supported media file under the library root."""
        all_files: list[Path] = []
        for root, _dirs, files in os.walk(self.library_path):
            for fname in files:
                if is_supported_filetype(fname, self._extensions):
                    all_files.append(Path(root) / fname)
        return all_files

    def _index_file(self, fpath: Path, force_reindex: bool) -> None:
        """Add a single file to the in-memory index."""
        key = str(fpath)
        try:
            st = fpath.stat()
        except OSError:
            return

        size = st.st_size
        p_hash: str | None = self._partial_cache.get(key)
        f_hash: str | None = self._full_cache.get(key)

        # When force-reindexing, eagerly compute partial hashes so the
        # catalog is fully populated for future runs.
        if force_reindex and p_hash is None:
            p_hash = get_partial_hash(key)
            if p_hash is not None:
                self._partial_cache[key] = p_hash
                self._new_hashes.append((key, size, st.st_mtime, p_hash, None))

        self._by_size.setdefault(size, []).append((fpath, p_hash, f_hash))
        self._by_name.setdefault(fpath.name.lower(), []).append(fpath)

    # -- hash helpers (lazy, with caching) ----------------------------------

    def partial_hash(self, filepath: Path) -> str | None:
        """Return the partial (first 1 MB) MD5 hash, computing if needed."""
        key = str(filepath)
        if key not in self._partial_cache:
            self._partial_cache[key] = get_partial_hash(key)
            self._record_new_hash(filepath, key)
        return self._partial_cache[key]

    def full_hash(self, filepath: Path) -> str | None:
        """Return the full SHA1 hash, computing if needed."""
        key = str(filepath)
        if key not in self._full_cache:
            try:
                self._full_cache[key] = hash_file(key)
            except OSError:
                self._full_cache[key] = None
            self._record_new_hash(filepath, key)
        return self._full_cache[key]

    def _record_new_hash(self, filepath: Path, key: str) -> None:
        try:
            st = filepath.stat()
            self._new_hashes.append(
                (
                    key,
                    st.st_size,
                    st.st_mtime,
                    self._partial_cache.get(key),
                    self._full_cache.get(key),
                )
            )
        except OSError:
            pass

    # -- queries ------------------------------------------------------------

    def candidates_by_size(
        self, size: int
    ) -> list[tuple[Path, str | None, str | None]]:
        """Return library files with the given size."""
        return self._by_size.get(size, [])

    def files_by_name(self, name: str) -> list[Path]:
        """Return library files matching *name* (case-insensitive)."""
        return self._by_name.get(name.lower(), [])

    @property
    def total_files(self) -> int:
        return sum(len(v) for v in self._by_size.values())

    # -- persistence --------------------------------------------------------

    def flush(self) -> None:
        """Write newly computed hashes back to the catalog."""
        if self._catalog is None or not self._new_hashes:
            return
        entries: list[tuple[str, int, float, str | None, str | None]] = []
        for abs_key, size, mtime, p_hash, f_hash in self._new_hashes:
            try:
                rel = str(Path(abs_key).relative_to(self.library_path))
            except ValueError:
                continue
            entries.append((rel, size, mtime, p_hash, f_hash))
        if entries:
            self._catalog.put_file_hashes(entries)
            logger.debug(f"Wrote {len(entries)} new hashes to library catalog")
        self._new_hashes.clear()

    def close(self) -> None:
        """Flush and close the catalog."""
        self.flush()
        if self._catalog is not None:
            self._catalog.close()
            self._catalog = None


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------
def detect_source_mode(source: Path) -> SourceMode:
    """Auto-detect whether *source* contains event folders or loose files."""
    for entry in source.iterdir():
        if entry.is_dir() and _EVENT_FOLDER_RE.match(entry.name):
            return SourceMode.EVENT_FOLDERS
    return SourceMode.FLAT


def _extract_year_from_folder(name: str) -> str | None:
    """Extract the year from an event folder name like ``[2024_01_15]…``."""
    m = _EVENT_FOLDER_RE.match(name)
    return m.group(1) if m else None


def _extract_date_from_folder(name: str) -> str | None:
    """Extract ``YYYY_MM_DD`` from an event folder name."""
    m = _EVENT_FOLDER_RE.match(name)
    if m:
        return f"{m.group(1)}_{m.group(2)}_{m.group(3)}"
    return None


# ---------------------------------------------------------------------------
# File matching
# ---------------------------------------------------------------------------
def _match_file(
    source_file: Path,
    index: LibraryIndex,
) -> FileMatch:
    """Match a single source file against the library index.

    Uses the 3-level cascade: size → partial hash → full hash.
    """
    try:
        src_size = source_file.stat().st_size
    except OSError:
        return FileMatch(source_path=source_file, status=FileStatus.NEW)

    # 1. Size match
    candidates = index.candidates_by_size(src_size)
    if not candidates:
        # Check for name collision (same name, different content)
        name_matches = index.files_by_name(source_file.name)
        collision = name_matches[0] if name_matches else None
        return FileMatch(
            source_path=source_file,
            status=FileStatus.NEW,
            name_collision_path=collision,
        )

    # 2. Partial hash
    src_partial = get_partial_hash(str(source_file))
    if src_partial is None:
        return FileMatch(source_path=source_file, status=FileStatus.NEW)

    for lib_path, _cached_p, _cached_f in candidates:
        lib_partial = index.partial_hash(lib_path)
        if lib_partial is None or lib_partial != src_partial:
            continue

        # 3. Full hash
        src_full = hash_file(str(source_file))
        lib_full = index.full_hash(lib_path)
        if src_full == lib_full:
            return FileMatch(
                source_path=source_file,
                status=FileStatus.DUPLICATE,
                library_match=lib_path,
            )

    # No full-hash match found — file is new
    name_matches = index.files_by_name(source_file.name)
    collision = name_matches[0] if name_matches else None
    return FileMatch(
        source_path=source_file,
        status=FileStatus.NEW,
        name_collision_path=collision,
    )


# ---------------------------------------------------------------------------
# Library placement helpers
# ---------------------------------------------------------------------------
def _library_dest_for_event_folder(
    folder_name: str,
    library: Path,
) -> Path:
    """Compute the library destination for an event folder.

    ``[2024_01_15]_event_name`` → ``library/2024/[2024_01_15]_event_name/``
    """
    year = _extract_year_from_folder(folder_name)
    if year is None:
        year = "unknown"
    return library / year / folder_name


def _library_dest_for_flat_file(
    source_file: Path,
    library: Path,
) -> Path:
    """Compute the library destination for a loose inbox file.

    Uses EXIF date or mtime to group into
    ``library/YYYY/[YYYY_MM_DD]_unsorted/filename``.
    """
    from filecluster.utlis import get_date_from_file

    try:
        m_time, _c_time, exif_date = get_date_from_file(str(source_file))
        dt = exif_date if exif_date is not None else m_time
    except Exception:
        dt = None

    if dt is not None:
        year = str(dt.year)
        date_str = dt.strftime("[%Y_%m_%d]_unsorted")
    else:
        year = "unknown"
        date_str = "[unknown]_unsorted"

    return library / year / date_str / source_file.name


# ---------------------------------------------------------------------------
# Main reconciliation
# ---------------------------------------------------------------------------
def reconcile(
    source: Path,
    library: Path,
    duplicates_dir: Path,
    *,
    execute: bool = False,
    force_reindex: bool = False,
    extensions: list[str] | None = None,
    progress: ProgressSink | None = None,
) -> ReconcilePlan:
    """Reconcile *source* against *library*.

    Args:
        source: Directory to reconcile (inbox or output dir with event folders).
        library: Main photo library root.
        duplicates_dir: Where to move confirmed duplicates.
        execute: If True, perform moves. Otherwise dry-run only.
        force_reindex: Rebuild the library index from scratch.
        extensions: Override the list of media extensions to consider.
        progress: Optional progress sink.

    Returns:
        A :class:`ReconcilePlan` describing (and optionally executing) the
        reconciliation.
    """
    progress = progress or NullProgress()

    # 1. Build library index
    index = LibraryIndex(
        library,
        force_reindex=force_reindex,
        extensions=extensions,
        progress=progress,
    )

    try:
        # 2. Detect source mode
        mode = detect_source_mode(source)
        exts = extensions or _ALL_EXTENSIONS

        # 3. Collect source files
        if mode == SourceMode.EVENT_FOLDERS:
            plan = _reconcile_event_folders(
                source, library, duplicates_dir, index, exts, progress
            )
        else:
            plan = _reconcile_flat(
                source, library, duplicates_dir, index, exts, progress
            )

        plan.source_mode = mode

        # 4. Execute if requested
        if execute:
            _execute_plan(plan)

        return plan
    finally:
        index.close()


def _reconcile_event_folders(
    source: Path,
    library: Path,
    duplicates_dir: Path,
    index: LibraryIndex,
    extensions: list[str],
    progress: ProgressSink,
) -> ReconcilePlan:
    """Reconcile event folders found in *source*."""
    plan = ReconcilePlan()

    # Discover event folders and collect their media files
    folders: list[Path] = sorted(
        d for d in source.iterdir() if d.is_dir() and _EVENT_FOLDER_RE.match(d.name)
    )
    folder_files = _collect_folder_files(folders, extensions)

    total = sum(len(v) for v in folder_files.values())
    progress.start(total, "Matching files")

    for folder in folders:
        files = folder_files.get(folder, [])
        if not files:
            continue
        _process_event_folder(
            folder, files, library, duplicates_dir, index, plan, progress
        )

    return plan


def _collect_folder_files(
    folders: list[Path], extensions: list[str]
) -> dict[Path, list[Path]]:
    """Map each event folder to its list of supported media files."""
    result: dict[Path, list[Path]] = {}
    for folder in folders:
        media = sorted(
            f
            for f in folder.iterdir()
            if f.is_file() and is_supported_filetype(f.name, extensions)
        )
        if media:
            result[folder] = media
    return result


def _classify_folder(matches: list[FileMatch]) -> FolderStatus:
    """Derive an aggregate folder status from its per-file matches."""
    n_dup = sum(1 for m in matches if m.status == FileStatus.DUPLICATE)
    if n_dup == len(matches):
        return FolderStatus.ALL_DUPLICATE
    n_new = sum(1 for m in matches if m.status == FileStatus.NEW)
    if n_new == len(matches):
        return FolderStatus.ALL_NEW
    return FolderStatus.PARTIAL


def _plan_folder_ops(
    folder_status: FolderStatus,
    matches: list[FileMatch],
    lib_dest: Path,
    dup_dest: Path,
    plan: ReconcilePlan,
) -> None:
    """Append mkdir/move operations to *plan* for one event folder."""
    if folder_status == FolderStatus.ALL_DUPLICATE:
        plan.ops.append(MkdirOp(path=dup_dest))
        for m in matches:
            plan.ops.append(
                MoveOp(src=m.source_path, dst=dup_dest / m.source_path.name)
            )
    elif folder_status == FolderStatus.ALL_NEW:
        plan.ops.append(MkdirOp(path=lib_dest))
        for m in matches:
            plan.ops.append(
                MoveOp(src=m.source_path, dst=lib_dest / m.source_path.name)
            )
    else:
        # Partial: new → library, duplicates → duplicates dir
        if any(m.status == FileStatus.NEW for m in matches):
            plan.ops.append(MkdirOp(path=lib_dest))
        if any(m.status == FileStatus.DUPLICATE for m in matches):
            plan.ops.append(MkdirOp(path=dup_dest))
        for m in matches:
            if m.status == FileStatus.DUPLICATE:
                plan.ops.append(
                    MoveOp(src=m.source_path, dst=dup_dest / m.source_path.name)
                )
            else:
                plan.ops.append(
                    MoveOp(src=m.source_path, dst=lib_dest / m.source_path.name)
                )


def _process_event_folder(
    folder: Path,
    files: list[Path],
    library: Path,
    duplicates_dir: Path,
    index: LibraryIndex,
    plan: ReconcilePlan,
    progress: ProgressSink,
) -> None:
    """Match every file in one event folder and append results to *plan*."""
    folder_matches: list[FileMatch] = []
    for fpath in files:
        progress.advance()
        match = _match_file(fpath, index)
        folder_matches.append(match)
        plan.file_matches.append(match)

    folder_status = _classify_folder(folder_matches)
    plan.folder_results.append(
        FolderResult(
            folder_name=folder.name,
            folder_path=folder,
            status=folder_status,
            files=folder_matches,
        )
    )

    lib_dest = _library_dest_for_event_folder(folder.name, library)
    dup_dest = duplicates_dir / folder.name
    _plan_folder_ops(folder_status, folder_matches, lib_dest, dup_dest, plan)

    return plan


def _reconcile_flat(
    source: Path,
    library: Path,
    duplicates_dir: Path,
    index: LibraryIndex,
    extensions: list[str],
    progress: ProgressSink,
) -> ReconcilePlan:
    """Reconcile loose files in *source*."""
    plan = ReconcilePlan()

    files = sorted(
        f
        for f in source.iterdir()
        if f.is_file() and is_supported_filetype(f.name, extensions)
    )

    progress.start(len(files), "Matching files")

    # Track which library destination folders we've already planned to create
    planned_dirs: set[Path] = set()

    for fpath in files:
        progress.advance()
        match = _match_file(fpath, index)
        plan.file_matches.append(match)

        if match.status == FileStatus.DUPLICATE:
            # Move to duplicates dir
            if duplicates_dir not in planned_dirs:
                plan.ops.append(MkdirOp(path=duplicates_dir))
                planned_dirs.add(duplicates_dir)
            plan.ops.append(
                MoveOp(
                    src=match.source_path,
                    dst=duplicates_dir / match.source_path.name,
                )
            )
        else:
            # Move to library, grouped by date
            dest = _library_dest_for_flat_file(fpath, library)
            dest_dir = dest.parent
            if dest_dir not in planned_dirs:
                plan.ops.append(MkdirOp(path=dest_dir))
                planned_dirs.add(dest_dir)
            plan.ops.append(MoveOp(src=match.source_path, dst=dest))

    return plan


# ---------------------------------------------------------------------------
# Plan execution
# ---------------------------------------------------------------------------
def _execute_plan(plan: ReconcilePlan) -> None:
    """Execute all move and mkdir operations in the plan."""
    from shutil import move

    for op in plan.ops:
        if isinstance(op, MkdirOp):
            os.makedirs(op.path, exist_ok=True)
        elif isinstance(op, MoveOp):
            os.makedirs(op.dst.parent, exist_ok=True)
            move(str(op.src), str(op.dst))
