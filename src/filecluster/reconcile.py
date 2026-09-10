"""Reconcile a source directory against one or more main photo libraries.

Checks whether files in a source directory (an inbox, or a filecluster output
directory with event folders) already exist in the library, then moves
duplicates aside and integrates new files.

The matching uses the same 3-level cascade as ``mark_inbox_duplicates``:

1. **Size match** — filter library candidates by identical file size
2. **Partial hash** (first 1 MB, MD5) — cheap, eliminates most false positives
3. **Full hash** (SHA1) — definitive confirmation

Source directories are walked recursively and may mix event folders with loose
files.  Files duplicated inside the source itself are detected too, so a
single pass never copies the same content into the library twice.  Every
destination is allocated through :class:`~filecluster.file_operations.\
DestinationAllocator`, so no planned operation can overwrite an existing file.
"""

from __future__ import annotations

import csv
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.catalog import LibraryCatalog
from filecluster.configuration import FileClusterSettings
from filecluster.exceptions import OverlappingPathsError
from filecluster.file_operations import (
    CopyOp,
    DestinationAllocator,
    FileOperationPlan,
    MkdirOp,
    MoveOp,
    SkipOp,
    execute_plan,
)
from filecluster.ui import NullProgress, ProgressSink
from filecluster.utlis import (
    EVENT_FOLDER_RE,
    extract_year_from_folder,
    find_sidecar_files,
    get_partial_hash,
    hash_file,
    is_sidecar_file,
    is_supported_filetype,
    walk_media_files,
)

_settings = FileClusterSettings()
_ALL_EXTENSIONS = _settings.image_extensions + _settings.video_extensions

#: Non-media files inside an event folder that still belong to it.
_FOLDER_METADATA_NAMES = frozenset({_settings.ini_filename})


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class FileStatus(StrEnum):
    """Classification of a single source file."""

    DUPLICATE = "DUPLICATE"
    NEW = "NEW"
    SOURCE_DUPLICATE = "SOURCE_DUPLICATE"
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
    MIXED = "mixed"


class ReconcileAction(StrEnum):
    """What to do with the files that were classified."""

    MOVE = "move"
    COPY = "copy"
    SCAN = "scan"


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
    library_matches: list[Path] = field(default_factory=list)
    source_duplicate_of: Path | None = None
    sidecars: list[Path] = field(default_factory=list)
    size: int = 0

    @property
    def is_duplicate(self) -> bool:
        """Whether this file is a duplicate of anything already accounted for."""
        return self.status in (FileStatus.DUPLICATE, FileStatus.SOURCE_DUPLICATE)

    @property
    def n_library_matches(self) -> int:
        return len(self.library_matches)


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
    def n_source_duplicates(self) -> int:
        return sum(1 for f in self.files if f.status == FileStatus.SOURCE_DUPLICATE)

    @property
    def n_new(self) -> int:
        return sum(1 for f in self.files if f.status == FileStatus.NEW)

    @property
    def n_name_collisions(self) -> int:
        return sum(1 for f in self.files if f.name_collision_path is not None)


# ---------------------------------------------------------------------------
# ReconcileOp — planned action
# ---------------------------------------------------------------------------
ReconcileOp = MoveOp | CopyOp | SkipOp | MkdirOp


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
    action: ReconcileAction = ReconcileAction.MOVE
    libraries: list[Path] = field(default_factory=list)
    n_sidecars: int = 0
    n_extra_files: int = 0

    @property
    def n_duplicates(self) -> int:
        return sum(1 for m in self.file_matches if m.status == FileStatus.DUPLICATE)

    @property
    def n_source_duplicates(self) -> int:
        return sum(
            1 for m in self.file_matches if m.status == FileStatus.SOURCE_DUPLICATE
        )

    @property
    def n_new(self) -> int:
        return sum(1 for m in self.file_matches if m.status == FileStatus.NEW)

    @property
    def n_name_collisions(self) -> int:
        return sum(1 for m in self.file_matches if m.name_collision_path is not None)

    @property
    def n_renamed(self) -> int:
        """Files whose destination name had to change to avoid a collision."""
        return sum(
            1
            for op in self.ops
            if isinstance(op, MoveOp | CopyOp) and op.src.name != op.dst.name
        )

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

    @property
    def move_destinations(self) -> list[tuple[str, str, str]]:
        """``(target_folder, source_name, destination_name)`` per file operation.

        Populated in scan mode too, so a preview shows exactly the destinations
        and renames a real run would produce.
        """
        out: list[tuple[str, str, str]] = []
        for op in self.ops:
            if isinstance(op, MoveOp | CopyOp | SkipOp) and op.dst is not None:
                out.append((str(op.dst.parent), op.src.name, op.dst.name))
        return out

    def summary_dict(self) -> dict[str, Any]:
        """Machine-readable summary."""
        return {
            "source_mode": self.source_mode.value,
            "action": self.action.value,
            "libraries": [str(p) for p in self.libraries],
            "total_files": len(self.file_matches),
            "duplicates": self.n_duplicates,
            "source_duplicates": self.n_source_duplicates,
            "new": self.n_new,
            "name_collisions": self.n_name_collisions,
            "renamed": self.n_renamed,
            "sidecars": self.n_sidecars,
            "extra_files": self.n_extra_files,
            "moves": self.n_moves,
            "copies": self.n_copies,
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
                    "size",
                    "library_match",
                    "n_library_matches",
                    "all_library_matches",
                    "source_duplicate_of",
                    "name_collision",
                    "sidecars",
                ]
            )
            for m in self.file_matches:
                writer.writerow(
                    [
                        str(m.source_path),
                        m.status.value,
                        m.size,
                        str(m.library_match) if m.library_match else "",
                        m.n_library_matches,
                        "|".join(str(p) for p in m.library_matches),
                        str(m.source_duplicate_of) if m.source_duplicate_of else "",
                        str(m.name_collision_path) if m.name_collision_path else "",
                        "|".join(p.name for p in m.sidecars),
                    ]
                )
        return len(self.file_matches)


# ---------------------------------------------------------------------------
# ContentIndex — the size → partial → full cascade, shared by both sides
# ---------------------------------------------------------------------------
class ContentIndex:
    """Content-addressed index of files, built on the 3-level cascade.

    Hashes are computed lazily: a partial hash only when another file has the
    same size, a full hash only when the partial hashes agree.
    """

    def __init__(self) -> None:
        self._by_size: dict[int, list[Path]] = {}
        self._partial: dict[str, str | None] = {}
        self._full: dict[str, str | None] = {}

    def add(self, path: Path, size: int) -> None:
        self._by_size.setdefault(size, []).append(path)

    def candidates_by_size(self, size: int) -> list[Path]:
        return self._by_size.get(size, [])

    def partial_hash(self, path: Path) -> str | None:
        key = str(path)
        if key not in self._partial:
            self._partial[key] = get_partial_hash(key)
        return self._partial[key]

    def full_hash(self, path: Path) -> str | None:
        key = str(path)
        if key not in self._full:
            try:
                self._full[key] = hash_file(key)
            except OSError:
                self._full[key] = None
        return self._full[key]

    def find_matches(self, path: Path, size: int) -> list[Path]:
        """Return every indexed file whose content equals *path*'s."""
        candidates = self.candidates_by_size(size)
        if not candidates:
            return []
        src_partial = get_partial_hash(str(path))
        if src_partial is None:
            return []
        src_full: str | None = None
        matches: list[Path] = []
        for candidate in candidates:
            if self.partial_hash(candidate) != src_partial:
                continue
            if src_full is None:
                try:
                    src_full = hash_file(str(path))
                except OSError:
                    return []
            if self.full_hash(candidate) == src_full:
                matches.append(candidate)
        return matches


def _unique_roots(paths: Sequence[Path]) -> list[Path]:
    """Drop roots that resolve to a directory already in the list, keeping order."""
    seen: set[Path] = set()
    out: list[Path] = []
    for path in paths:
        resolved = Path(os.path.realpath(path))
        if resolved in seen:
            logger.debug(f"Ignoring duplicate library root {path}")
            continue
        seen.add(resolved)
        out.append(path)
    return out


# ---------------------------------------------------------------------------
# LibraryIndex
# ---------------------------------------------------------------------------
class LibraryIndex:
    """In-memory index of one or more photo libraries, backed by SQLite.

    Walks every library once, building ``{size: [path, …]}`` and
    ``{name: [path, …]}`` lookups.  Previously computed hashes are loaded from
    each library's catalog and reused only while the file's size *and* mtime
    still match what was recorded, so an edited file is never mistaken for its
    former self.  Newly computed hashes are written back on close.
    """

    def __init__(
        self,
        library_path: Path | str | Sequence[Path | str],
        *,
        force_reindex: bool = False,
        extensions: list[str] | None = None,
        progress: ProgressSink | None = None,
        read_only: bool = False,
    ) -> None:
        paths = (
            [Path(library_path)]
            if isinstance(library_path, str | Path)
            else [Path(p) for p in library_path]
        )
        if not paths:
            raise ValueError("At least one library path is required")
        # Deduplicate roots that resolve to the same directory: indexing one
        # twice would report every file as its own duplicate.
        self.library_paths: list[Path] = _unique_roots(paths)
        paths = self.library_paths
        self.read_only = read_only
        self.library_path: Path = paths[0]
        self._extensions = extensions or _ALL_EXTENSIONS
        self._by_size: dict[int, list[tuple[Path, str | None, str | None]]] = {}
        self._by_name: dict[str, list[Path]] = {}
        self._partial_cache: dict[str, str | None] = {}
        self._full_cache: dict[str, str | None] = {}
        self._sizes: dict[str, int] = {}
        self._catalogs: dict[Path, LibraryCatalog] = {}
        self._new_hashes: dict[str, tuple[int, float]] = {}
        self.n_stale_cache_entries = 0

        self._build(force_reindex=force_reindex, progress=progress or NullProgress())

    # -- construction -------------------------------------------------------

    def _build(self, *, force_reindex: bool, progress: ProgressSink) -> None:
        desc = "Rebuilding library index" if force_reindex else "Indexing library"
        all_files: list[tuple[Path, Path]] = []
        for root in self.library_paths:
            self._open_catalog(root, force_reindex)
            all_files.extend(
                (root, f) for f in walk_media_files(root, self._extensions)
            )

        progress.start(len(all_files), desc)
        records = self._load_cached_records(force_reindex)

        for root, fpath in all_files:
            progress.advance()
            self._index_file(root, fpath, records, force_reindex)

        logger.debug(
            f"Library index: {len(all_files)} files, "
            f"{len(self._partial_cache)} cached partial hashes, "
            f"{self.n_stale_cache_entries} stale entries ignored"
        )

    def _open_catalog(self, root: Path, force_reindex: bool) -> None:
        """Open (and optionally back up / clear) one library's catalog.

        Read-only mode reuses cached hashes but writes nothing, so a dry run
        neither creates a catalog nor discards the hashes of an existing one.
        """
        if force_reindex and not self.read_only:
            backup_path = LibraryCatalog.backup(root)
            if backup_path is not None:
                logger.info(f"Catalog backed up to {backup_path}")

        try:
            catalog = LibraryCatalog.open(root, read_only=self.read_only)
        except Exception as exc:
            logger.debug(f"Could not open catalog for {root}: {exc}")
            return
        self._catalogs[root] = catalog

        if force_reindex and not self.read_only:
            cleared = catalog.clear_file_hashes()
            logger.info(f"Cleared {cleared} cached hashes for full reindex of {root}")

    def _load_cached_records(self, force_reindex: bool) -> dict[str, Any]:
        """Return ``{abs_path: FileRecord}`` from every catalog."""
        if force_reindex:
            return {}
        records: dict[str, Any] = {}
        for root, catalog in self._catalogs.items():
            for rel, record in catalog.get_file_records().items():
                records[str(root / rel)] = record
        return records

    def _index_file(
        self,
        root: Path,
        fpath: Path,
        records: dict[str, Any],
        force_reindex: bool,
    ) -> None:
        """Add a single file to the in-memory index."""
        key = str(fpath)
        try:
            st = fpath.stat()
        except OSError:
            return

        size = st.st_size
        self._sizes[key] = size
        p_hash: str | None = None
        f_hash: str | None = None

        record = records.get(key)
        if record is not None:
            if record.matches(size, st.st_mtime):
                p_hash = record.partial_hash
                f_hash = record.full_hash
            else:
                self.n_stale_cache_entries += 1

        if p_hash is not None:
            self._partial_cache[key] = p_hash
        if f_hash is not None:
            self._full_cache[key] = f_hash

        # When force-reindexing, eagerly compute partial hashes so the
        # catalog is fully populated for future runs.
        if force_reindex and p_hash is None:
            p_hash = get_partial_hash(key)
            if p_hash is not None:
                self._partial_cache[key] = p_hash
                self._new_hashes[key] = (size, st.st_mtime)

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
        except OSError:
            return
        self._new_hashes[key] = (st.st_size, st.st_mtime)

    # -- queries ------------------------------------------------------------

    def candidates_by_size(
        self, size: int
    ) -> list[tuple[Path, str | None, str | None]]:
        """Return library files with the given size."""
        return self._by_size.get(size, [])

    def files_by_name(self, name: str) -> list[Path]:
        """Return library files matching *name* (case-insensitive)."""
        return self._by_name.get(name.lower(), [])

    def size_of(self, filepath: Path) -> int | None:
        """Return the indexed size of *filepath*, or None if not indexed."""
        return self._sizes.get(str(filepath))

    @property
    def total_files(self) -> int:
        return sum(len(v) for v in self._by_size.values())

    def duplicate_groups(self) -> list[list[Path]]:
        """Return groups of library files that share identical content.

        Only same-size candidates are hashed, so this is cheap on a library
        where most files are unique.
        """
        groups: list[list[Path]] = []
        for entries in self._by_size.values():
            if len(entries) < 2:
                continue
            by_full: dict[str, list[Path]] = {}
            by_partial: dict[str, list[Path]] = {}
            for path, _p, _f in entries:
                partial = self.partial_hash(path)
                if partial is None:
                    continue
                by_partial.setdefault(partial, []).append(path)
            for same_partial in by_partial.values():
                if len(same_partial) < 2:
                    continue
                for path in same_partial:
                    full = self.full_hash(path)
                    if full is None:
                        continue
                    by_full.setdefault(full, []).append(path)
            groups.extend(g for g in by_full.values() if len(g) > 1)
        return groups

    # -- persistence --------------------------------------------------------

    def flush(self) -> None:
        """Write newly computed hashes back to the per-library catalogs."""
        if self.read_only or not self._new_hashes:
            return
        per_library: dict[
            Path, list[tuple[str, int, float, str | None, str | None]]
        ] = {}
        for abs_key, (size, mtime) in self._new_hashes.items():
            abs_path = Path(abs_key)
            for root, _catalog in self._catalogs.items():
                try:
                    rel = str(abs_path.relative_to(root))
                except ValueError:
                    continue
                per_library.setdefault(root, []).append(
                    (
                        rel,
                        size,
                        mtime,
                        self._partial_cache.get(abs_key),
                        self._full_cache.get(abs_key),
                    )
                )
                break

        for root, entries in per_library.items():
            self._catalogs[root].put_file_hashes(entries)
            logger.debug(f"Wrote {len(entries)} new hashes to catalog of {root}")
        self._new_hashes.clear()

    def close(self) -> None:
        """Flush and close every catalog."""
        self.flush()
        for catalog in self._catalogs.values():
            catalog.close()
        self._catalogs.clear()


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------
def _event_folder_for(path: Path, source: Path) -> Path | None:
    """Return the nearest ancestor of *path* that is an event folder.

    The search stops at *source*, so an event-folder-looking ancestor outside
    the scanned tree is ignored.
    """
    try:
        rel_parts = path.relative_to(source).parts[:-1]
    except ValueError:
        return None
    current = source
    found: Path | None = None
    for part in rel_parts:
        current = current / part
        if EVENT_FOLDER_RE.match(part):
            found = current
    return found


def detect_source_mode(
    source: Path,
    *,
    extensions: list[str] | None = None,
    recursive: bool = True,
) -> SourceMode:
    """Detect whether *source* holds event folders, loose files, or both."""
    exts = extensions or _ALL_EXTENSIONS
    files = walk_media_files(source, exts, recursive=recursive)
    has_event = False
    has_loose = False
    for f in files:
        if _event_folder_for(f, source) is not None:
            has_event = True
        else:
            has_loose = True
        if has_event and has_loose:
            return SourceMode.MIXED

    if has_event:
        return SourceMode.EVENT_FOLDERS
    if has_loose:
        return SourceMode.FLAT

    # Nothing to classify by content — fall back to the folder names, so an
    # empty set of event folders is still reported as event-folder mode.
    for entry in source.iterdir():
        if entry.is_dir() and EVENT_FOLDER_RE.match(entry.name):
            return SourceMode.EVENT_FOLDERS
    return SourceMode.FLAT


# ---------------------------------------------------------------------------
# File matching
# ---------------------------------------------------------------------------
def _match_file(
    source_file: Path,
    index: LibraryIndex,
    seen: ContentIndex | None = None,
) -> FileMatch:
    """Match a single source file against the library index.

    Uses the 3-level cascade: size → partial hash → full hash.  When *seen* is
    given, files already classified in this run are checked too, so duplicates
    inside the source itself are reported as ``SOURCE_DUPLICATE``.
    """
    try:
        src_size = source_file.stat().st_size
    except OSError:
        return FileMatch(source_path=source_file, status=FileStatus.NEW)

    library_matches = _find_library_matches(source_file, src_size, index)
    if library_matches:
        return FileMatch(
            source_path=source_file,
            status=FileStatus.DUPLICATE,
            library_match=library_matches[0],
            library_matches=library_matches,
            size=src_size,
        )

    if seen is not None:
        source_matches = seen.find_matches(source_file, src_size)
        if source_matches:
            return FileMatch(
                source_path=source_file,
                status=FileStatus.SOURCE_DUPLICATE,
                source_duplicate_of=source_matches[0],
                size=src_size,
            )

    name_matches = index.files_by_name(source_file.name)
    return FileMatch(
        source_path=source_file,
        status=FileStatus.NEW,
        name_collision_path=name_matches[0] if name_matches else None,
        size=src_size,
    )


def _find_library_matches(
    source_file: Path, src_size: int, index: LibraryIndex
) -> list[Path]:
    """Return every library file whose content equals *source_file*'s."""
    candidates = index.candidates_by_size(src_size)
    if not candidates:
        return []

    src_partial = get_partial_hash(str(source_file))
    if src_partial is None:
        return []

    src_full: str | None = None
    matches: list[Path] = []
    for lib_path, _cached_p, _cached_f in candidates:
        if index.partial_hash(lib_path) != src_partial:
            continue
        if src_full is None:
            try:
                src_full = hash_file(str(source_file))
            except OSError:
                return []
        if index.full_hash(lib_path) == src_full:
            matches.append(lib_path)
    return matches


# ---------------------------------------------------------------------------
# Library placement helpers
# ---------------------------------------------------------------------------
def _library_dest_for_event_folder(folder_name: str, library: Path) -> Path:
    """Compute the library destination for an event folder.

    ``[2024_01_15]_event_name`` → ``library/2024/[2024_01_15]_event_name/``
    """
    year = extract_year_from_folder(folder_name) or "unknown"
    return library / year / folder_name


def _library_dest_for_flat_file(source_file: Path, library: Path) -> Path:
    """Compute the library destination directory for a loose inbox file.

    Uses EXIF date or mtime to group into ``library/YYYY/[YYYY_MM_DD]_unsorted``.
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
# Planner
# ---------------------------------------------------------------------------
@dataclass
class _Planner:
    """Turns classified files into collision-free operations."""

    plan: ReconcilePlan
    action: ReconcileAction
    allocator: DestinationAllocator = field(default_factory=DestinationAllocator)
    _dirs: set[Path] = field(default_factory=set)
    _planned: set[Path] = field(default_factory=set)

    def mkdir(self, directory: Path) -> None:
        if directory in self._dirs:
            return
        self._dirs.add(directory)
        self.plan.ops.append(MkdirOp(path=directory))

    def transfer(self, src: Path, dest_dir: Path, reason: str = "") -> Path | None:
        """Plan moving/copying *src* into *dest_dir* under a free name.

        Returns the allocated destination, or None when *src* was already
        planned (which happens with nested event folders).
        """
        if src in self._planned:
            return None
        self._planned.add(src)

        # The name is claimed even in scan mode, so a preview shows exactly the
        # renames a real run would perform.
        dst = self.allocator.allocate(dest_dir, src.name)
        if self.action == ReconcileAction.SCAN:
            self.plan.ops.append(SkipOp(src=src, reason=reason or "scan only", dst=dst))
            return dst

        self.mkdir(dest_dir)
        op = (
            CopyOp(src=src, dst=dst)
            if self.action == ReconcileAction.COPY
            else MoveOp(src=src, dst=dst)
        )
        self.plan.ops.append(op)
        return dst

    def transfer_group(
        self, match: FileMatch, dest_dir: Path, reason: str = ""
    ) -> None:
        """Plan the file and its sidecars into the same destination folder."""
        self.transfer(match.source_path, dest_dir, reason)
        for sidecar in match.sidecars:
            if self.transfer(sidecar, dest_dir, reason) is not None:
                self.plan.n_sidecars += 1


# ---------------------------------------------------------------------------
# Path validation
# ---------------------------------------------------------------------------
def _resolve(path: Path) -> Path:
    """Resolve *path* through symlinks, tolerating a not-yet-created directory."""
    return Path(os.path.realpath(path))


def _paths_overlap(first: Path, second: Path) -> bool:
    """Whether *first* and *second* are the same directory or nested."""
    return first == second or first in second.parents or second in first.parents


def _validate_roots(
    source: Path, libraries: Sequence[Path], duplicates_dir: Path
) -> None:
    """Reject roots that overlap, since reconciling them would destroy data.

    Symlinks are resolved first, so an alias to a library is caught too.
    """
    resolved_source = _resolve(source)
    resolved_dups = _resolve(duplicates_dir)

    for lib in libraries:
        resolved_lib = _resolve(lib)
        if _paths_overlap(resolved_source, resolved_lib):
            raise OverlappingPathsError("source", source, "library", lib)
        if _paths_overlap(resolved_dups, resolved_lib):
            raise OverlappingPathsError(
                "duplicates dir", duplicates_dir, "library", lib
            )

    # The duplicates folder may sit next to the source, but not inside it: the
    # walk would then pick up files that were just moved there.
    if resolved_dups == resolved_source or resolved_source in resolved_dups.parents:
        raise OverlappingPathsError("duplicates dir", duplicates_dir, "source", source)


# ---------------------------------------------------------------------------
# Main reconciliation
# ---------------------------------------------------------------------------
def reconcile(
    source: Path,
    library: Path | Sequence[Path],
    duplicates_dir: Path,
    *,
    execute: bool = False,
    force_reindex: bool = False,
    extensions: list[str] | None = None,
    progress: ProgressSink | None = None,
    action: ReconcileAction = ReconcileAction.MOVE,
    recursive: bool = True,
    include_sidecars: bool = True,
    detect_source_duplicates: bool = True,
) -> ReconcilePlan:
    """Reconcile *source* against *library*.

    Args:
        source: Directory to reconcile (inbox, or output dir with event folders).
        library: Main photo library root, or several roots to match against.
            New files are placed in the first one.
        duplicates_dir: Where to move confirmed duplicates.
        execute: If True, perform the planned operations. Otherwise dry-run.
        force_reindex: Rebuild the library index from scratch (backs up first).
        extensions: Override the list of media extensions to consider.
        progress: Optional progress sink.
        action: Move, copy, or only scan and report.
        recursive: Walk the source tree recursively.
        include_sidecars: Keep companion files (``.xmp``, ``.aae``, …) with
            their media file.
        detect_source_duplicates: Also report files duplicated inside *source*.

    Returns:
        A :class:`ReconcilePlan` describing (and optionally executing) the
        reconciliation.

    Raises:
        OverlappingPathsError: When the source, a library or the duplicates
            directory are the same directory or nested inside one another.
    """
    progress = progress or NullProgress()
    libraries = (
        [Path(library)]
        if isinstance(library, str | Path)
        else [Path(p) for p in library]
    )
    exts = extensions or _ALL_EXTENSIONS

    # Before opening a catalog or touching a file: overlapping roots would make
    # every file match itself and a move would empty the library.
    _validate_roots(source, libraries, duplicates_dir)

    index = LibraryIndex(
        libraries,
        force_reindex=force_reindex,
        extensions=exts,
        progress=progress,
        read_only=not execute,
    )

    try:
        plan = ReconcilePlan(action=action, libraries=libraries)
        plan.source_mode = detect_source_mode(
            source, extensions=exts, recursive=recursive
        )
        planner = _Planner(plan=plan, action=action)
        seen = ContentIndex() if detect_source_duplicates else None

        media = walk_media_files(source, exts, recursive=recursive)
        # Sidecars travel with their media file, so they must not also be
        # planned as loose "extra" files.
        sidecars_by_media: dict[Path, list[Path]] = {}
        claimed_sidecars: set[Path] = set()
        if include_sidecars:
            for f in media:
                found = find_sidecar_files(f)
                if found:
                    sidecars_by_media[f] = found
                    claimed_sidecars.update(found)

        event_files, loose_files = _split_by_event_folder(media, source)

        progress.start(len(media), "Matching files")
        _reconcile_event_folders(
            event_files=event_files,
            library=libraries[0],
            duplicates_dir=duplicates_dir,
            index=index,
            planner=planner,
            progress=progress,
            seen=seen,
            sidecars_by_media=sidecars_by_media,
            claimed_sidecars=claimed_sidecars,
            extensions=exts,
            include_sidecars=include_sidecars,
        )
        _reconcile_loose_files(
            source=source,
            files=loose_files,
            library=libraries[0],
            duplicates_dir=duplicates_dir,
            index=index,
            planner=planner,
            progress=progress,
            seen=seen,
            sidecars_by_media=sidecars_by_media,
        )

        if execute and action != ReconcileAction.SCAN:
            _execute_plan(plan, progress)

        return plan
    finally:
        index.close()


def _split_by_event_folder(
    media: list[Path], source: Path
) -> tuple[dict[Path, list[Path]], list[Path]]:
    """Group media files by their event folder, keeping loose ones apart."""
    event_files: dict[Path, list[Path]] = {}
    loose: list[Path] = []
    for f in media:
        folder = _event_folder_for(f, source)
        if folder is None:
            loose.append(f)
        else:
            event_files.setdefault(folder, []).append(f)
    return event_files, loose


def _classify_folder(matches: list[FileMatch]) -> FolderStatus:
    """Derive an aggregate folder status from its per-file matches."""
    n_dup = sum(1 for m in matches if m.is_duplicate)
    if n_dup == len(matches):
        return FolderStatus.ALL_DUPLICATE
    n_new = sum(1 for m in matches if m.status == FileStatus.NEW)
    if n_new == len(matches):
        return FolderStatus.ALL_NEW
    return FolderStatus.PARTIAL


def _reconcile_event_folders(
    *,
    event_files: dict[Path, list[Path]],
    library: Path,
    duplicates_dir: Path,
    index: LibraryIndex,
    planner: _Planner,
    progress: ProgressSink,
    seen: ContentIndex | None,
    sidecars_by_media: dict[Path, list[Path]],
    claimed_sidecars: set[Path],
    extensions: list[str],
    include_sidecars: bool,
) -> None:
    """Classify and plan every event folder found in the source."""
    for folder in sorted(event_files):
        files = event_files[folder]
        matches: list[FileMatch] = []
        for fpath in files:
            progress.advance()
            match = _match_file(fpath, index, seen)
            match.sidecars = sidecars_by_media.get(fpath, [])
            if seen is not None and match.status == FileStatus.NEW:
                seen.add(fpath, match.size)
            matches.append(match)
            planner.plan.file_matches.append(match)

        status = _classify_folder(matches)
        planner.plan.folder_results.append(
            FolderResult(
                folder_name=folder.name,
                folder_path=folder,
                status=status,
                files=matches,
            )
        )

        lib_dest = _library_dest_for_event_folder(folder.name, library)
        dup_dest = duplicates_dir / folder.name

        for match in matches:
            rel_dir = match.source_path.parent.relative_to(folder)
            if match.is_duplicate:
                planner.transfer_group(match, dup_dest / rel_dir, "duplicate")
            else:
                planner.transfer_group(match, lib_dest / rel_dir, "new file")

        # Everything else in the folder (cluster metadata, unsupported files,
        # nested non-media content) follows the folder itself.
        default_dest = dup_dest if status == FolderStatus.ALL_DUPLICATE else lib_dest
        _plan_extra_files(
            folder=folder,
            dest_root=default_dest,
            planner=planner,
            extensions=extensions,
            claimed_sidecars=claimed_sidecars,
            nested_folders=set(event_files) - {folder},
            include_sidecars=include_sidecars,
        )


def _plan_extra_files(
    *,
    folder: Path,
    dest_root: Path,
    planner: _Planner,
    extensions: list[str],
    claimed_sidecars: set[Path],
    nested_folders: set[Path],
    include_sidecars: bool,
) -> None:
    """Plan the non-media files inside an event folder.

    Cluster metadata, unrecognised extensions and nested non-media content
    would otherwise be left behind when the folder's media moves away.

    With *include_sidecars* off, companion files are left behind here as well;
    otherwise they would be swept up as generic extra files and end up in a
    destination chosen by the folder rather than by their media file.
    """
    for dirpath, dirnames, filenames in os.walk(folder):
        here = Path(dirpath)
        # A nested event folder is planned on its own pass, with its own
        # destination, so it must not be swept up here.
        dirnames[:] = [d for d in dirnames if (here / d) not in nested_folders]
        rel_dir = here.relative_to(folder)
        for name in filenames:
            path = here / name
            if is_supported_filetype(name, extensions) or path in claimed_sidecars:
                continue
            if not include_sidecars and is_sidecar_file(name):
                continue
            reason = (
                "folder metadata" if name in _FOLDER_METADATA_NAMES else "extra file"
            )
            if planner.transfer(path, dest_root / rel_dir, reason) is not None:
                planner.plan.n_extra_files += 1


def _reconcile_loose_files(
    *,
    source: Path,
    files: list[Path],
    library: Path,
    duplicates_dir: Path,
    index: LibraryIndex,
    planner: _Planner,
    progress: ProgressSink,
    seen: ContentIndex | None,
    sidecars_by_media: dict[Path, list[Path]],
) -> None:
    """Classify and plan the media files that sit outside any event folder."""
    for fpath in files:
        progress.advance()
        match = _match_file(fpath, index, seen)
        match.sidecars = sidecars_by_media.get(fpath, [])
        if seen is not None and match.status == FileStatus.NEW:
            seen.add(fpath, match.size)
        planner.plan.file_matches.append(match)

        # Preserve any sub-directory the file was found in, so a nested inbox
        # does not collapse into one flat duplicates folder.
        rel_dir = fpath.parent.relative_to(source)
        if match.is_duplicate:
            planner.transfer_group(match, duplicates_dir / rel_dir, "duplicate")
        else:
            dest = _library_dest_for_flat_file(fpath, library)
            planner.transfer_group(match, dest.parent, "new file")


# ---------------------------------------------------------------------------
# Plan execution
# ---------------------------------------------------------------------------
def _execute_plan(plan: ReconcilePlan, progress: ProgressSink | None = None) -> None:
    """Execute all operations in the plan."""
    execute_plan(FileOperationPlan(ops=list(plan.ops)), progress)
