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

import sqlite3
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


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class LibraryCatalog:
    """SQLite-backed metadata cache for a single library directory.

    Usage::

        with LibraryCatalog.open(library_path) as catalog:
            row = catalog.get_cluster("2020/[2020_06_15]_event")
            ...

    The database file is ``<library>/.filecluster.db``.
    """

    DB_FILENAME = ".filecluster.db"

    def __init__(self, conn: sqlite3.Connection, db_path: Path) -> None:
        self._conn = conn
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def open(cls, library_path: str | Path) -> LibraryCatalog:
        """Open (or create) the catalog for *library_path*."""
        db_path = Path(library_path) / cls.DB_FILENAME
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        catalog = cls(conn, db_path)
        catalog._ensure_schema()
        return catalog

    def close(self) -> None:
        self._conn.close()

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
        The backup is a simple file copy (the WAL is checkpointed first when
        possible).
        """
        import shutil
        from datetime import UTC, datetime

        db_path = Path(library_path) / LibraryCatalog.DB_FILENAME
        if not db_path.exists():
            return None

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        backup_path = db_path.with_suffix(f".{stamp}.bak")
        shutil.copy2(str(db_path), str(backup_path))
        logger.info(f"Backed up catalog to {backup_path}")
        return backup_path

    def prune_files(self, existing_rel_paths: set[str]) -> int:
        """Delete file rows whose path is no longer on disk."""
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
