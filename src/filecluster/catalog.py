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
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.configuration import default_settings
from filecluster.exceptions import HashPolicyConflictError
from filecluster.media_integrity import (
    DECODABLE_IMAGE_EXTENSIONS,
    IntegrityStatus,
    verify_image,
    verify_video,
)
from filecluster.ui import NullProgress, ProgressSink
from filecluster.utlis import (
    crc32_file,
    get_exif_date,
    get_partial_hash,
    hash_file,
    walk_media_files,
)

# Default gap used when deciding whether an event folder is "continuous".
# Matches ``FileClusterSettings.time_granularity_minutes``.
_DEFAULT_TIME_GRANULARITY_S = default_settings.time_granularity_minutes * 60

_SCHEMA_VERSION = 3

#: Media extensions the catalog builder scans, matching the reconcile index so
#: a catalog built here is directly reusable by ``reconcile``.
_DEFAULT_EXTENSIONS = (
    default_settings.image_extensions + default_settings.video_extensions
)
_VIDEO_EXTENSIONS = {
    ext if ext.startswith(".") else f".{ext}"
    for ext in default_settings.video_extensions
}

#: Maps a content-check outcome to its result bucket in ``verify(deep=True)``.
_DEEP_BUCKET: dict[IntegrityStatus, str] = {
    IntegrityStatus.OK: "decoded_ok",
    IntegrityStatus.CORRUPT: "corrupt",
    IntegrityStatus.UNREADABLE: "unreadable",
    IntegrityStatus.SKIPPED: "skipped",
}


class HashMode(StrEnum):
    """How much file content the catalog builder hashes."""

    SHORT = "short"
    FULL = "full"


class HashAlgo(StrEnum):
    """Which digest the catalog builder uses for its content hashes.

    ``SHA1`` is the legacy default and keeps the historical fast-prefilter
    split (MD5 partial + SHA-1 full, recorded as ``hash_algo IS NULL`` so
    ``reconcile`` and ``dedup`` keep reusing those hashes). ``BLAKE3`` is a
    modern, fast, cryptographically strong digest used for *both* the partial
    and full hashes when the user opts in.
    """

    SHA1 = "sha1"
    BLAKE3 = "blake3"


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
    scanned_at    TEXT,
    hash_algo     TEXT,
    crc32         TEXT
);

CREATE TABLE IF NOT EXISTS library_settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

#: Keys used in the ``library_settings`` table to pin the hashing policy that
#: every file in the library must share.
_SETTING_HASH_ALGO = "hash_algo"
_SETTING_CRC32 = "crc32"

#: Columns added to the ``files`` table after schema v1. Older catalogs are
#: migrated in place with ``ALTER TABLE ADD COLUMN`` (SQLite fills existing
#: rows with NULL, which correctly means "legacy MD5 partial + SHA-1 full").
_FILES_COLUMNS_V2 = ("hash_algo", "crc32")


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
    crc32: str | None = None
    hash_algo: str | None = None

    def matches(self, size: int, mtime: float) -> bool:
        """Whether this row still describes a file with *size* and *mtime*."""
        return self.size == size and abs(self.mtime - mtime) <= MTIME_TOLERANCE_S

    @property
    def uses_legacy_hashes(self) -> bool:
        """Whether the stored hashes are the legacy MD5-partial/SHA-1-full pair.

        ``reconcile`` and ``dedup`` compute MD5 partial and SHA-1 full hashes,
        so they may only reuse a cached hash when it was produced the same way
        (``hash_algo IS NULL``). Rows built with an explicit ``--hash-algo``
        are recomputed by those tools instead of being trusted blindly.
        """
        return self.hash_algo is None


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
        self._migrate_files_columns()
        row = self._conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if row is None or row[0] < _SCHEMA_VERSION:
            self._conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (_SCHEMA_VERSION,)
            )
            self._conn.commit()

    def _migrate_files_columns(self) -> None:
        """Add columns introduced after schema v1 to a pre-existing catalog.

        ``CREATE TABLE IF NOT EXISTS`` never alters an existing table, so a
        catalog created under schema v1 keeps its old column set until this
        runs. Missing columns are added as nullable, so old rows read back as
        NULL (legacy hashing) and nothing else changes.
        """
        present = {r["name"] for r in self._conn.execute("PRAGMA table_info(files)")}
        added = False
        for column in _FILES_COLUMNS_V2:
            if column not in present:
                self._conn.execute(f"ALTER TABLE files ADD COLUMN {column} TEXT")
                added = True
        if added:
            self._conn.commit()
            logger.debug("Migrated files table to schema v2 (hash_algo, crc32)")

    # ------------------------------------------------------------------
    # Library settings (pinned hashing policy)
    # ------------------------------------------------------------------

    def get_setting(self, key: str) -> str | None:
        """Return one ``library_settings`` value, or None when unset."""
        row = self._conn.execute(
            "SELECT value FROM library_settings WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row is not None else None

    def set_setting(self, key: str, value: str | None) -> None:
        """Insert or update one ``library_settings`` value."""
        self._guard_writable()
        self._conn.execute(
            """\
            INSERT INTO library_settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        self._conn.commit()

    def get_hash_policy(self) -> dict[str, Any] | None:
        """Return the library's pinned hashing policy, or None if never set.

        The dict has ``hash_algo`` (``"sha1"``/``"blake3"``/``None`` for the
        legacy default) and ``crc32`` (bool). None means no build has recorded
        a policy yet, so the next build is free to choose one.
        """
        has_policy = self._conn.execute(
            "SELECT 1 FROM library_settings WHERE key IN (?, ?) LIMIT 1",
            (_SETTING_HASH_ALGO, _SETTING_CRC32),
        ).fetchone()
        if has_policy is None:
            return None
        return {
            "hash_algo": self.get_setting(_SETTING_HASH_ALGO),
            "crc32": self.get_setting(_SETTING_CRC32) == "1",
        }

    def set_hash_policy(self, *, hash_algo: str | None, crc32: bool) -> None:
        """Pin the library's hashing policy so later builds must match it."""
        self.set_setting(_SETTING_HASH_ALGO, hash_algo)
        self.set_setting(_SETTING_CRC32, "1" if crc32 else "0")

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
            "SELECT path, size, mtime, partial_hash, full_hash, crc32, hash_algo "
            "FROM files"
        ).fetchall()
        return {
            r["path"]: FileRecord(
                path=r["path"],
                size=r["size"] if r["size"] is not None else -1,
                mtime=r["mtime"] if r["mtime"] is not None else -1.0,
                partial_hash=r["partial_hash"],
                full_hash=r["full_hash"],
                crc32=r["crc32"],
                hash_algo=r["hash_algo"],
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

        Callers of this method (``reconcile`` and ``dedup``) always compute the
        legacy MD5-partial/SHA-1-full pair, so ``hash_algo`` is written NULL
        and any stored ``crc32`` is cleared: the checksum described the
        previous content and would no longer match after a re-hash.
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
                scanned_at   = excluded.scanned_at,
                hash_algo    = NULL,
                crc32        = NULL
            """,
            [(p, s, m, ph, fh, now) for p, s, m, ph, fh in entries],
        )
        self._conn.commit()

    def put_files(
        self,
        entries: list[
            tuple[
                str,
                int,
                float,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
            ]
        ],
    ) -> None:
        """Bulk upsert full file rows, including the EXIF date.

        Each entry is ``(path, size, mtime, partial_hash, full_hash,
        exif_date, hash_algo, crc32)``.  Unlike :meth:`put_file_hashes`, this
        also writes the ``exif_date``, ``hash_algo`` and ``crc32`` columns, so
        it is what the catalog builder uses.
        """
        self._guard_writable()
        now = _now_iso()
        self._conn.executemany(
            """\
            INSERT INTO files
                (path, size, mtime, partial_hash, full_hash, exif_date,
                 hash_algo, crc32, scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size         = excluded.size,
                mtime        = excluded.mtime,
                partial_hash = excluded.partial_hash,
                full_hash    = excluded.full_hash,
                exif_date    = excluded.exif_date,
                hash_algo    = excluded.hash_algo,
                crc32        = excluded.crc32,
                scanned_at   = excluded.scanned_at
            """,
            [
                (p, s, m, ph, fh, ex, ha, crc, now)
                for p, s, m, ph, fh, ex, ha, crc in entries
            ],
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

    def clear_clusters(self) -> int:
        """Delete every row from the clusters table.

        Returns the number of rows deleted. Used by force rebuild so cluster
        metadata is recomputed together with the file index.
        """
        self._guard_writable()
        cursor = self._conn.execute("SELECT COUNT(*) FROM clusters")
        count = cursor.fetchone()[0]
        if count:
            self._conn.execute("DELETE FROM clusters")
            self._conn.commit()
            logger.debug(f"Cleared {count} cluster rows from catalog")
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
        policy = self.get_hash_policy()
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
            "crc32_checksums": _scalar(
                "SELECT COUNT(*) FROM files WHERE crc32 IS NOT NULL"
            ),
            "hash_algo": (policy["hash_algo"] or "sha1") if policy else None,
            "crc32_policy": policy["crc32"] if policy else None,
            "total_bytes": _scalar("SELECT COALESCE(SUM(size), 0) FROM files"),
        }

    def verify(
        self,
        library_path: str | Path | None = None,
        *,
        deep: bool = False,
        progress: ProgressSink | None = None,
    ) -> dict[str, list[str]]:
        """Compare cached file rows against the files on disk.

        Returns a mapping with three lists of relative paths: ``missing`` for
        rows whose file is gone, ``stale`` for rows whose size or mtime no
        longer matches, and ``ok`` for the rest.

        When *deep* is set, every file still present on disk is additionally
        checked at the content level and four extra buckets are returned:
        ``decoded_ok`` for files that pass, ``corrupt`` for damaged files,
        ``unreadable`` for files that could not be read, and ``skipped`` for
        files no check could be applied to. The deep check combines two
        independent signals, and the stored baseline is never overwritten:

        * **Structural decode** — decodable images
          (:data:`~filecluster.media_integrity.DECODABLE_IMAGE_EXTENSIONS`) are
          fully decoded with Pillow and videos are validated with ``ffprobe``.
        * **Baseline re-hash** — for files that still match their cached
          size/mtime, the stored ``crc32`` and full hash are recomputed and
          compared, catching silent bit rot even in formats that cannot be
          decoded here (RAW/HEIC). A file whose size/mtime changed is only
          decoded, never compared against a now-outdated hash, so an edit is
          never mistaken for corruption.

        A file is ``corrupt`` if either signal fails, ``decoded_ok`` if either
        succeeds, and ``skipped`` only when neither applies (for example a RAW
        file with no stored checksum, or any video when ``ffprobe`` is absent).
        """
        root = Path(library_path) if library_path is not None else self.db_path.parent
        result: dict[str, list[str]] = {"ok": [], "stale": [], "missing": []}
        if deep:
            result.update(decoded_ok=[], corrupt=[], unreadable=[], skipped=[])
        sink = progress or NullProgress()
        records = self.get_file_records()
        if deep:
            sink.start(len(records), "Verifying files")
        for rel, record in records.items():
            try:
                st = (root / rel).stat()
            except OSError:
                result["missing"].append(rel)
                if deep:
                    sink.advance()
                continue
            matches = record.matches(st.st_size, st.st_mtime)
            result["ok" if matches else "stale"].append(rel)
            if deep:
                status = self._verify_file_deep(
                    root / rel, record, check_checksums=matches
                )
                result[_DEEP_BUCKET[status]].append(rel)
                sink.advance()
        return result

    def _verify_file_deep(
        self, path: Path, record: FileRecord, *, check_checksums: bool
    ) -> IntegrityStatus:
        """Combine a structural decode with a baseline re-hash for *path*."""
        content = self._verify_content(path)
        if content is IntegrityStatus.UNREADABLE:
            return content
        checksum = (
            self._verify_checksums(path, record)
            if check_checksums
            else IntegrityStatus.SKIPPED
        )
        if checksum is IntegrityStatus.UNREADABLE:
            return checksum
        if IntegrityStatus.CORRUPT in (content, checksum):
            return IntegrityStatus.CORRUPT
        if IntegrityStatus.OK in (content, checksum):
            return IntegrityStatus.OK
        return IntegrityStatus.SKIPPED

    @staticmethod
    def _verify_content(path: Path) -> IntegrityStatus:
        """Run the right content-level decode for *path* based on its suffix."""
        suffix = path.suffix.lower()
        if suffix in _VIDEO_EXTENSIONS:
            return verify_video(path)
        if suffix in DECODABLE_IMAGE_EXTENSIONS:
            return verify_image(path)
        return IntegrityStatus.SKIPPED

    @staticmethod
    def _verify_checksums(path: Path, record: FileRecord) -> IntegrityStatus:
        """Recompute the stored CRC32/full hash and compare against *record*.

        Returns ``OK`` when at least one stored checksum matches, ``CORRUPT``
        on any mismatch (bit rot), ``UNREADABLE`` when the file cannot be read,
        and ``SKIPPED`` when the row carries no checksum to compare against.
        """
        matched = False
        if record.crc32 is not None:
            got = crc32_file(path)
            if got is None:
                return IntegrityStatus.UNREADABLE
            if got != record.crc32:
                return IntegrityStatus.CORRUPT
            matched = True
        if record.full_hash is not None:
            algo = record.hash_algo or "sha1"
            try:
                got = hash_file(path, algo=algo)
            except OSError:
                return IntegrityStatus.UNREADABLE
            if got != record.full_hash:
                return IntegrityStatus.CORRUPT
            matched = True
        return IntegrityStatus.OK if matched else IntegrityStatus.SKIPPED

    def vacuum(self) -> None:
        """Compact the database file."""
        self._guard_writable()
        self._conn.execute("VACUUM")
        self._conn.commit()

    # ------------------------------------------------------------------
    # Building the catalog from an organised library on disk
    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        library_path: str | Path,
        *,
        rebuild: bool = False,
        image_hash: HashMode = HashMode.FULL,
        video_hash: HashMode = HashMode.SHORT,
        full_hash: bool | None = None,
        hash_algo: HashAlgo | None = None,
        crc32: bool = False,
        hash_algo_explicit: bool = True,
        crc32_explicit: bool = True,
        read_exif: bool = True,
        extensions: list[str] | None = None,
        progress: ProgressSink | None = None,
    ) -> dict[str, Any]:
        """Scan *library_path* and populate its catalog with file metadata.

        Two modes:

        * **update** (default): only files that are new or whose size/mtime
          changed since the last scan are re-read; everything else is kept.
        * **rebuild** (*rebuild=True*): the existing catalog is backed up to a
          timestamped ``.bak`` file, its file rows are cleared, and every file
          is re-read from scratch.

        Every file gets a short hash of its first 1 MiB. By default images also
        get a full hash while videos retain only the short hash. The two
        policies can be configured independently with *image_hash* and
        *video_hash*. *full_hash* is retained as a compatibility override:
        True makes both policies full and False makes both short.

        *hash_algo* selects the digest. Left as ``None`` the historical split
        is used (MD5 partial + SHA-1 full, stored with ``hash_algo`` NULL so
        ``reconcile``/``dedup`` keep reusing those hashes). :attr:`HashAlgo.SHA1`
        or :attr:`HashAlgo.BLAKE3` make *both* the partial and full hashes use
        that algorithm and record it on every row.

        *crc32*, independent of the hashing options, additionally stores a
        whole-file CRC32 checksum per file for cheap bit-rot detection on later
        ``verify --deep`` runs.

        The hashing policy (algorithm and CRC32 choice) is pinned to the
        library on its first build, so every file shares one comparable set of
        hashes. A later build that *explicitly* asks for a different policy is
        refused with :class:`~filecluster.exceptions.HashPolicyConflictError`
        unless *rebuild* is set, which backs up the catalog and re-hashes the
        whole library under the new policy. *hash_algo_explicit* and
        *crc32_explicit* tell the builder whether the caller actually chose
        those values (True) or is passing defaults (False); defaults never
        conflict, so a plain ``catalog build`` re-run keeps the stored policy.

        Event folders under the library are also indexed into the clusters
        table (start/end/median dates, file count, continuity).

        Returns a summary dict with the counts and the backup path (if any).
        """
        root = Path(library_path)
        image_hash = HashMode(image_hash)
        video_hash = HashMode(video_hash)
        if full_hash is not None:
            image_hash = video_hash = HashMode.FULL if full_hash else HashMode.SHORT
        if hash_algo is not None:
            hash_algo = HashAlgo(hash_algo)

        progress = progress or NullProgress()
        with cls.open(root) as catalog:
            # Reconcile the requested policy with the one already pinned to the
            # library. On a plain (non-rebuild) build a genuine conflict is
            # refused; a rebuild adopts the requested policy after re-hashing.
            requested = {
                "hash_algo": hash_algo.value if hash_algo is not None else None,
                "crc32": crc32,
            }
            stored = catalog.get_hash_policy()
            if stored is not None and not rebuild:
                conflict = (
                    hash_algo_explicit and stored["hash_algo"] != requested["hash_algo"]
                ) or (crc32_explicit and stored["crc32"] != requested["crc32"])
                if conflict:
                    raise HashPolicyConflictError(stored, requested)
                # Reuse the pinned policy so defaults never silently switch it.
                hash_algo = (
                    HashAlgo(stored["hash_algo"])
                    if stored["hash_algo"] is not None
                    else None
                )
                crc32 = bool(stored["crc32"])

            backup_path: Path | None = None
            if rebuild:
                # Back up before clearing so a rebuild is never a one-way door.
                backup_path = cls.backup(root)
                if backup_path is not None:
                    logger.info(f"Catalog backed up to {backup_path} before rebuild")
                catalog.clear_file_hashes()
                catalog.clear_clusters()

            catalog.set_hash_policy(
                hash_algo=hash_algo.value if hash_algo is not None else None,
                crc32=crc32,
            )
            result = catalog._scan_files(
                root,
                image_hash=image_hash,
                video_hash=video_hash,
                hash_algo=hash_algo,
                compute_crc32=crc32,
                read_exif=read_exif,
                extensions=extensions,
                progress=progress,
            )
            cluster_result = catalog._scan_clusters(
                root,
                force=rebuild,
                progress=progress,
            )
            catalog.vacuum()

        result.update(cluster_result)
        result["mode"] = "rebuild" if rebuild else "update"
        result["backup"] = str(backup_path) if backup_path else None
        return result

    def _scan_files(
        self,
        library_path: Path,
        *,
        image_hash: HashMode,
        video_hash: HashMode,
        hash_algo: HashAlgo | None,
        compute_crc32: bool,
        read_exif: bool,
        extensions: list[str] | None,
        progress: ProgressSink,
    ) -> dict[str, Any]:
        """Walk the library, upsert changed files, and prune vanished ones."""
        exts = extensions or list(_DEFAULT_EXTENSIONS)
        files = walk_media_files(library_path, exts)
        existing = self.get_file_records()

        # None keeps the historical split (MD5 partial + SHA-1 full stored with
        # a NULL hash_algo); an explicit algorithm is used for both hashes and
        # recorded on every row.
        stored_algo = hash_algo.value if hash_algo is not None else None
        partial_algo = hash_algo.value if hash_algo is not None else "md5"
        full_algo = hash_algo.value if hash_algo is not None else "sha1"

        progress.start(len(files), "Indexing files")
        added = updated = skipped = 0
        seen: set[str] = set()
        batch: list[
            tuple[
                str,
                int,
                float,
                str | None,
                str | None,
                str | None,
                str | None,
                str | None,
            ]
        ] = []

        for fpath in files:
            progress.advance()
            try:
                st = fpath.stat()
            except OSError:
                continue
            rel = str(fpath.relative_to(library_path))
            seen.add(rel)

            record = existing.get(rel)
            record_valid = record is not None and record.matches(
                st.st_size, st.st_mtime
            )
            hash_mode = (
                video_hash if fpath.suffix.lower() in _VIDEO_EXTENSIONS else image_hash
            )
            needs_full_hash = hash_mode is HashMode.FULL
            # A cached row can be reused only when it still matches on disk and
            # already carries everything this build asks for: the right hash
            # algorithm, a full hash when one is wanted, and a CRC32 when one
            # is wanted.
            unchanged = (
                record_valid
                and record.partial_hash is not None
                and record.hash_algo == stored_algo
                and (record.full_hash is not None or not needs_full_hash)
                and (not compute_crc32 or record.crc32 is not None)
            )
            if unchanged:
                skipped += 1
                continue

            key = str(fpath)
            p_hash = get_partial_hash(key, algo=partial_algo)
            if needs_full_hash:
                try:
                    f_hash: str | None = hash_file(key, algo=full_algo)
                except OSError:
                    f_hash = None
            else:
                # Preserve a valid full hash when the policy is downgraded, but
                # only if it was produced with the same algorithm and the file
                # itself has not changed.
                f_hash = (
                    record.full_hash
                    if record_valid and record.hash_algo == stored_algo
                    else None
                )
            if compute_crc32:
                crc = crc32_file(key)
            else:
                # Keep an existing checksum for an unchanged file; drop it once
                # the content has changed so verify never trusts a stale value.
                crc = record.crc32 if record_valid else None
            exif_date = _exif_iso(key) if read_exif else None

            batch.append(
                (
                    rel,
                    st.st_size,
                    st.st_mtime,
                    p_hash,
                    f_hash,
                    exif_date,
                    stored_algo,
                    crc,
                )
            )
            if record is None:
                added += 1
            else:
                updated += 1

            if len(batch) >= 500:
                self.put_files(batch)
                batch.clear()

        if batch:
            self.put_files(batch)

        pruned = self.prune_files(seen)

        return {
            "library": str(library_path),
            "db_path": str(self.db_path),
            "scanned": len(files),
            "added": added,
            "updated": updated,
            "skipped": skipped,
            "pruned": pruned,
            "image_hash": image_hash.value,
            "video_hash": video_hash.value,
            "hash_algo": stored_algo or "legacy",
            "crc32": compute_crc32,
            "read_exif": read_exif,
        }

    def _scan_clusters(
        self,
        library_path: Path,
        *,
        force: bool,
        progress: ProgressSink,
    ) -> dict[str, Any]:
        """Index event folders into the clusters table.

        Dates and continuity are derived from the file rows already stored for
        each event folder (EXIF date when present, otherwise the file mtime),
        so a ``catalog build`` pass fills both tables without a second EXIF
        walk. Folders whose mtime still matches the cached row are skipped
        unless *force* is set.
        """
        from filecluster.update_clusters import (
            fast_scandir,
            identify_folder_types,
            is_event,
        )

        root = str(library_path).rstrip("/").rstrip("\\")
        progress.update_description("Discovering event folders")
        subfolders = fast_scandir(root)
        subfolders_root = [s.replace(f"{root}/", "") for s in subfolders]
        event_dirs = [
            ed[0] for ed in filter(is_event, identify_folder_types(subfolders_root))
        ]

        progress.start(len(event_dirs), "Indexing clusters")
        existing = {row["path"]: row for row in self.get_all_clusters()}
        added = updated = skipped = 0
        seen: set[str] = set()

        for event_rel in event_dirs:
            progress.advance()
            pth = Path(root) / event_rel
            try:
                disk_mtime = pth.stat().st_mtime
            except OSError:
                continue
            seen.add(event_rel)

            cached = existing.get(event_rel)
            if (
                not force
                and cached is not None
                and cached.get("folder_mtime") == disk_mtime
            ):
                skipped += 1
                continue

            stats = self._cluster_stats_for_event(event_rel)
            if stats is None:
                # Empty or non-media event folder: drop a stale cache row if any.
                continue

            self.put_cluster(
                event_rel,
                start_date=stats["start_date"],
                end_date=stats["end_date"],
                median=stats["median"],
                is_continuous=stats["is_continuous"],
                file_count=stats["file_count"],
                folder_mtime=disk_mtime,
            )
            if cached is None:
                added += 1
            else:
                updated += 1

        pruned = self.prune_clusters(seen)
        return {
            "clusters_scanned": len(event_dirs),
            "clusters_added": added,
            "clusters_updated": updated,
            "clusters_skipped": skipped,
            "clusters_pruned": pruned,
        }

    def _cluster_stats_for_event(self, event_rel: str) -> dict[str, Any] | None:
        """Compute cluster dates from the indexed files under *event_rel*.

        Only direct children of the event folder are counted, matching how
        ``update_clusters.get_media_df`` scans an event directory.
        """
        rows = self._conn.execute(
            """\
            SELECT path, exif_date, mtime FROM files
            WHERE path LIKE ? AND instr(substr(path, length(?) + 2), '/') = 0
            """,
            (f"{event_rel}/%", event_rel),
        ).fetchall()
        if not rows:
            return None

        dates: list[datetime] = []
        for row in rows:
            dt = _parse_stored_date(row["exif_date"])
            if dt is None and row["mtime"] is not None:
                try:
                    dt = datetime.fromtimestamp(float(row["mtime"]))
                except (OSError, OverflowError, ValueError):
                    dt = None
            if dt is not None:
                dates.append(dt)

        if not dates:
            return {
                "start_date": None,
                "end_date": None,
                "median": None,
                "is_continuous": True,
                "file_count": len(rows),
            }

        dates.sort()
        start, end = dates[0], dates[-1]
        mid = dates[len(dates) // 2]
        # Continuity: no gap larger than the configured time granularity.
        is_continuous = True
        if len(dates) > 1:
            for earlier, later in pairwise(dates):
                if (later - earlier).total_seconds() > _DEFAULT_TIME_GRANULARITY_S:
                    is_continuous = False
                    break

        return {
            "start_date": str(start),
            "end_date": str(end),
            "median": str(mid),
            "is_continuous": is_continuous,
            "file_count": len(rows),
        }


def _exif_iso(path_name: str) -> str | None:
    """Return the file's EXIF capture date as an ISO string, or None.

    Videos and files without EXIF simply yield None; the builder stores that
    as a NULL ``exif_date`` rather than failing.
    """
    exif_date = get_exif_date(path_name)
    return exif_date.isoformat() if exif_date is not None else None


def _parse_stored_date(value: str | None) -> datetime | None:
    """Parse a date string previously written to the catalog, or None."""
    if not value:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    # Accept both ISO (…T…) and the space-separated form used by cluster rows.
    text = text.replace("T", " ", 1)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


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
