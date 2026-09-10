"""Per-library SQLite catalog for caching cluster metadata and file hashes.

Each watch-folder (library) gets a ``.filecluster.db`` file at its root.
The catalog stores:

* **Cluster rows** — the same data that lives in ``.cluster.ini`` files, keyed
  by the event-folder path relative to the library root.  A folder-mtime column
  lets the scanner skip unchanged folders on repeated runs.

* **File rows** — per-file size, mtime, partial hash and full hash so that
  duplicate detection across runs does not re-hash the entire library.

The catalog is transparent to the user: no new CLI flags are needed.
``--force-recalc`` / ``-f`` bypasses the mtime cache and forces a full rescan.
"""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from filecluster import logger

_SCHEMA_VERSION = 1

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS clusters (
    path          TEXT PRIMARY KEY,
    start_date    TEXT,
    end_date      TEXT,
    median        TEXT,
    is_continuous INTEGER,
    file_count    INTEGER,
    folder_mtime  REAL,
    scanned_at    TEXT
);

CREATE TABLE IF NOT EXISTS files (
    path          TEXT PRIMARY KEY,
    size          INTEGER,
    mtime         REAL,
    partial_hash  TEXT,
    full_hash     TEXT,
    exif_date     TEXT,
    scanned_at    TEXT
);
"""


#: Tolerance when comparing a stored mtime with the one on disk. SQLite keeps
#: mtimes as REAL, and some filesystems round them, so an exact compare would
#: invalidate the whole cache on every run.
MTIME_TOLERANCE_S = 1e-6


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class FileRecord:
    """A cached per-file row, including the fields needed to validate it."""

    path: str
    size: int
    mtime: float
    partial_hash: str | None = None
    full_hash: str | None = None

    def matches(self, size: int, mtime: float) -> bool:
        """Whether this row still describes a file with *size* and *mtime*."""
        return self.size == size and abs(self.mtime - mtime) <= MTIME_TOLERANCE_S


class LibraryCatalog:
    """SQLite-backed metadata cache for a single library directory.

    Usage::

        with LibraryCatalog.open(library_path) as catalog:
            row = catalog.get_cluster("2020/[2020_06_15]_event")
            ...

    The database file is ``<library>/.filecluster.db``.
    """

    DB_FILENAME = ".filecluster.db"

    def __init__(
        self,
        conn: sqlite3.Connection,
        db_path: Path,
        *,
        read_only: bool = False,
    ) -> None:
        self._conn = conn
        self.db_path = db_path
        self.read_only = read_only

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def open(
        cls, library_path: str | Path, *, read_only: bool = False
    ) -> LibraryCatalog:
        """Open (or create) the catalog for *library_path*.

        Args:
            library_path: Library root holding the catalog.
            read_only: Open an existing catalog without creating or modifying
                anything, so a dry run leaves no trace on disk. SQLite itself
                rejects writes on such a connection.

        Raises:
            FileNotFoundError: With *read_only*, when no catalog exists yet.
        """
        db_path = Path(library_path) / cls.DB_FILENAME
        if read_only:
            if not db_path.exists():
                raise FileNotFoundError(f"No catalog at {db_path}")
            # `mode=ro` keeps SQLite from creating the database or its WAL/SHM
            # sidecars, which a plain connect() would do even for a read.
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
            conn.row_factory = sqlite3.Row
            return cls(conn, db_path, read_only=True)

        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        catalog = cls(conn, db_path)
        catalog._ensure_schema()
        return catalog

    def close(self) -> None:
        self._conn.close()

    def _guard_writable(self) -> None:
        """Raise before a write when this catalog was opened read-only.

        SQLite would refuse the statement anyway; failing here names the cause
        instead of surfacing a bare "attempt to write a readonly database".
        """
        if self.read_only:
            raise sqlite3.OperationalError(
                f"Catalog {self.db_path} is open read-only (dry run)"
            )

    def __enter__(self) -> LibraryCatalog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        cur = self._conn.executescript(_SCHEMA_SQL)
        cur.close()
        row = self._conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (_SCHEMA_VERSION,)
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Cluster operations
    # ------------------------------------------------------------------

    def get_cluster(self, rel_path: str) -> dict[str, Any] | None:
        """Return a cached cluster row, or *None* if not present."""
        row = self._conn.execute(
            "SELECT * FROM clusters WHERE path = ?", (rel_path,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_all_clusters(self) -> list[dict[str, Any]]:
        """Return every cached cluster row."""
        rows = self._conn.execute("SELECT * FROM clusters").fetchall()
        return [dict(r) for r in rows]

    def put_cluster(
        self,
        rel_path: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        median: str | None = None,
        is_continuous: bool = True,
        file_count: int = 0,
        folder_mtime: float = 0.0,
    ) -> None:
        """Insert or update a cluster row."""
        self._guard_writable()
        self._conn.execute(
            """\
            INSERT INTO clusters
                (path, start_date, end_date, median, is_continuous,
                 file_count, folder_mtime, scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                start_date    = excluded.start_date,
                end_date      = excluded.end_date,
                median        = excluded.median,
                is_continuous = excluded.is_continuous,
                file_count    = excluded.file_count,
                folder_mtime  = excluded.folder_mtime,
                scanned_at    = excluded.scanned_at
            """,
            (
                rel_path,
                start_date,
                end_date,
                median,
                int(is_continuous),
                file_count,
                folder_mtime,
                _now_iso(),
            ),
        )
        self._conn.commit()

    def prune_clusters(self, existing_rel_paths: set[str]) -> int:
        """Delete cluster rows whose path is no longer on disk.

        Returns the number of rows deleted.
        """
        self._guard_writable()
        all_paths = {
            r["path"]
            for r in self._conn.execute("SELECT path FROM clusters").fetchall()
        }
        stale = all_paths - existing_rel_paths
        if not stale:
            return 0
        self._conn.executemany(
            "DELETE FROM clusters WHERE path = ?", [(p,) for p in stale]
        )
        self._conn.commit()
        logger.debug(f"Pruned {len(stale)} stale cluster rows from catalog")
        return len(stale)

    # ------------------------------------------------------------------
    # File-hash operations
    # ------------------------------------------------------------------

    def get_file_hashes(self) -> dict[str, tuple[int, str | None, str | None]]:
        """Return ``{path: (size, partial_hash, full_hash)}`` for every file."""
        rows = self._conn.execute(
            "SELECT path, size, partial_hash, full_hash FROM files"
        ).fetchall()
        return {r["path"]: (r["size"], r["partial_hash"], r["full_hash"]) for r in rows}

    def get_file_records(self) -> dict[str, FileRecord]:
        """Return ``{path: FileRecord}`` for every cached file.

        Unlike :meth:`get_file_hashes` this carries ``mtime``, which callers
        need to tell a still-valid cache entry from a stale one.
        """
        rows = self._conn.execute(
            "SELECT path, size, mtime, partial_hash, full_hash FROM files"
        ).fetchall()
        return {
            r["path"]: FileRecord(
                path=r["path"],
                size=r["size"] if r["size"] is not None else -1,
                mtime=r["mtime"] if r["mtime"] is not None else -1.0,
                partial_hash=r["partial_hash"],
                full_hash=r["full_hash"],
            )
            for r in rows
        }

    def delete_file_rows(self, rel_paths: Iterable[str]) -> int:
        """Delete the given file rows.  Returns the number of rows removed."""
        self._guard_writable()
        paths = [(p,) for p in rel_paths]
        if not paths:
            return 0
        cur = self._conn.executemany("DELETE FROM files WHERE path = ?", paths)
        self._conn.commit()
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else len(paths)

    def get_file_hashes_by_size(
        self,
    ) -> dict[int, list[tuple[str, str | None, str | None]]]:
        """Return ``{size: [(path, partial_hash, full_hash), ...]}``."""
        rows = self._conn.execute(
            "SELECT path, size, partial_hash, full_hash FROM files"
        ).fetchall()
        by_size: dict[int, list[tuple[str, str | None, str | None]]] = {}
        for r in rows:
            by_size.setdefault(r["size"], []).append(
                (r["path"], r["partial_hash"], r["full_hash"])
            )
        return by_size

    def put_file_hashes(
        self,
        entries: list[tuple[str, int, float, str | None, str | None]],
    ) -> None:
        """Bulk upsert file hash rows.

        Each entry is ``(path, size, mtime, partial_hash, full_hash)``.
        """
        self._guard_writable()
        now = _now_iso()
        self._conn.executemany(
            """\
            INSERT INTO files (path, size, mtime, partial_hash, full_hash, scanned_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size         = excluded.size,
                mtime        = excluded.mtime,
                partial_hash = excluded.partial_hash,
                full_hash    = excluded.full_hash,
                scanned_at   = excluded.scanned_at
            """,
            [(p, s, m, ph, fh, now) for p, s, m, ph, fh in entries],
        )
        self._conn.commit()

    def clear_file_hashes(self) -> int:
        """Delete every row from the files table.

        Returns the number of rows deleted.  Used by force-reindex to start
        from a clean slate after the database file has been backed up.
        """
        self._guard_writable()
        cursor = self._conn.execute("SELECT COUNT(*) FROM files")
        count = cursor.fetchone()[0]
        if count:
            self._conn.execute("DELETE FROM files")
            self._conn.commit()
            logger.debug(f"Cleared {count} file-hash rows from catalog")
        return count

    @staticmethod
    def backup(library_path: str | Path) -> Path | None:
        """Copy the catalog database to a timestamped backup file.

        Returns the backup path, or *None* if no catalog exists to back up.

        Uses SQLite's own backup API rather than a file copy: a plain copy of
        the main database file omits everything still sitting in the WAL, and a
        checkpoint can silently fail to fold it in while another connection
        holds a read snapshot.

        Raises:
            sqlite3.Error: When the database cannot be read consistently. The
                caller must not proceed as though a backup exists.
        """
        db_path = Path(library_path) / LibraryCatalog.DB_FILENAME
        if not db_path.exists():
            return None

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = db_path.with_suffix(f".{stamp}.bak")
        source = sqlite3.connect(str(db_path), timeout=30)
        try:
            target = sqlite3.connect(str(backup_path), timeout=30)
            try:
                # Copies committed WAL content too, and retries internally
                # while other connections are writing.
                source.backup(target)
            finally:
                target.close()
        except BaseException:
            # A partial .bak must not be left where list_backups() would offer
            # it to `catalog restore`.
            with suppress(OSError):
                backup_path.unlink()
            raise
        finally:
            source.close()
        logger.info(f"Backed up catalog to {backup_path}")
        return backup_path

    @staticmethod
    def list_backups(library_path: str | Path) -> list[Path]:
        """Return existing catalog backups, newest last."""
        root = Path(library_path)
        stem = Path(LibraryCatalog.DB_FILENAME).stem
        return sorted(root.glob(f"{stem}.*.bak"))

    @staticmethod
    def restore(library_path: str | Path, backup: str | Path | None = None) -> Path:
        """Restore the catalog from *backup* (newest one when omitted).

        The database currently in place is itself backed up first, so a restore
        is never a one-way door.

        Raises:
            FileNotFoundError: when there is nothing to restore from.
            sqlite3.DatabaseError: when the chosen backup is not a readable
                database, checked before the current one is touched.
        """
        root = Path(library_path)
        if backup is None:
            backups = LibraryCatalog.list_backups(root)
            if not backups:
                raise FileNotFoundError(f"No catalog backup found in {root}")
            src = backups[-1]
        else:
            src = Path(backup)
            if not src.exists():
                raise FileNotFoundError(f"Backup not found: {src}")

        _assert_readable(src)
        db_path = root / LibraryCatalog.DB_FILENAME
        LibraryCatalog.backup(root)
        # Stale WAL/SHM files would otherwise be replayed on top of the
        # restored database and undo the restore.
        for extra in (".db-wal", ".db-shm"):
            sidecar = db_path.with_name(db_path.name.replace(".db", extra))
            sidecar.unlink(missing_ok=True)
        shutil.copy2(str(src), str(db_path))
        logger.info(f"Restored catalog from {src}")
        return src

    def prune_files(self, existing_rel_paths: set[str]) -> int:
        """Delete file rows whose path is no longer on disk."""
        self._guard_writable()
        all_paths = {
            r["path"] for r in self._conn.execute("SELECT path FROM files").fetchall()
        }
        stale = all_paths - existing_rel_paths
        if not stale:
            return 0
        self._conn.executemany(
            "DELETE FROM files WHERE path = ?", [(p,) for p in stale]
        )
        self._conn.commit()
        logger.debug(f"Pruned {len(stale)} stale file rows from catalog")
        return len(stale)

    # ------------------------------------------------------------------
    # Maintenance / introspection
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        """Return counts and coverage figures for this catalog."""

        def _scalar(sql: str) -> int:
            row = self._conn.execute(sql).fetchone()
            return int(row[0] or 0)

        n_files = _scalar("SELECT COUNT(*) FROM files")
        return {
            "db_path": str(self.db_path),
            "db_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
            "schema_version": _scalar(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version"
            ),
            "clusters": _scalar("SELECT COUNT(*) FROM clusters"),
            "files": n_files,
            "partial_hashes": _scalar(
                "SELECT COUNT(*) FROM files WHERE partial_hash IS NOT NULL"
            ),
            "full_hashes": _scalar(
                "SELECT COUNT(*) FROM files WHERE full_hash IS NOT NULL"
            ),
            "total_bytes": _scalar("SELECT COALESCE(SUM(size), 0) FROM files"),
        }

    def verify(self, library_path: str | Path | None = None) -> dict[str, list[str]]:
        """Compare cached file rows against the files on disk.

        Returns a mapping with three lists of relative paths: ``missing`` for
        rows whose file is gone, ``stale`` for rows whose size or mtime no
        longer matches, and ``ok`` for the rest.
        """
        root = Path(library_path) if library_path is not None else self.db_path.parent
        result: dict[str, list[str]] = {"ok": [], "stale": [], "missing": []}
        for rel, record in self.get_file_records().items():
            try:
                st = (root / rel).stat()
            except OSError:
                result["missing"].append(rel)
                continue
            bucket = "ok" if record.matches(st.st_size, st.st_mtime) else "stale"
            result[bucket].append(rel)
        return result

    def vacuum(self) -> None:
        """Compact the database file."""
        self._guard_writable()
        self._conn.execute("VACUUM")
        self._conn.commit()


def _assert_readable(db_path: Path) -> None:
    """Raise unless *db_path* is a readable SQLite database.

    Restoring an unreadable backup would replace a working catalog with junk,
    so this runs before anything is overwritten.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        conn.execute("PRAGMA schema_version").fetchone()
    finally:
        conn.close()
