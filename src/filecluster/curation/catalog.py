"""SQLite cache for curation verdicts.

Kept separate from :class:`filecluster.catalog.LibraryCatalog` on purpose: that
one describes a destination library and its clusters, this one describes analyses
of inbox candidates and has its own schema version and lifetime.

A cached verdict is reused only when the content hash, the pipeline version, the
configuration fingerprint and the model fingerprint all match, so a changed
threshold or a swapped checkpoint invalidates exactly what it should.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.curation.types import (
    CurationDecision,
    CurationResult,
    MediaItem,
    StageResult,
)

SCHEMA_VERSION = 1

#: SQLite stores mtimes as REAL and some filesystems round them, so an exact
#: comparison would invalidate the whole cache on every run.
MTIME_TOLERANCE_S = 1e-6

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS files (
    sha256     TEXT PRIMARY KEY,
    last_path  TEXT NOT NULL,
    size       INTEGER NOT NULL,
    mtime      REAL NOT NULL,
    media_type TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS files_by_path ON files (last_path);

CREATE TABLE IF NOT EXISTS analyses (
    sha256             TEXT NOT NULL,
    pipeline_version   TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    model_fingerprint  TEXT NOT NULL,
    decision           TEXT NOT NULL,
    confidence         REAL NOT NULL,
    scores_json        TEXT NOT NULL,
    labels_json        TEXT NOT NULL,
    reasons_json       TEXT NOT NULL,
    stage_trace_json   TEXT NOT NULL,
    analyzed_at        TEXT NOT NULL,
    PRIMARY KEY (
        sha256,
        pipeline_version,
        config_fingerprint,
        model_fingerprint
    )
);

CREATE TABLE IF NOT EXISTS feedback (
    sha256        TEXT PRIMARY KEY,
    user_decision TEXT NOT NULL,
    source        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class CacheKey:
    """Everything besides the content that a cached verdict depends on."""

    pipeline_version: str
    config_fingerprint: str
    model_fingerprint: str


class CurationCatalog:
    """Verdict cache for one inbox.

    Usage::

        with CurationCatalog.open(db_path) as cache:
            cached = cache.get_analysis(item, key)
    """

    def __init__(self, conn: sqlite3.Connection, db_path: Path) -> None:
        self._conn = conn
        self.db_path = db_path

    # -- construction ------------------------------------------------------
    @classmethod
    def open(cls, db_path: str | Path) -> CurationCatalog:
        """Open (or create) the cache database at *db_path*."""
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        catalog = cls(conn, path)
        catalog._ensure_schema()
        return catalog

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()

    def __enter__(self) -> CurationCatalog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _ensure_schema(self) -> None:
        self._conn.executescript(_SCHEMA_SQL).close()
        row = self._conn.execute(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
        stored = int(row["version"]) if row is not None else None
        if stored is None:
            self._conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
        elif stored != SCHEMA_VERSION:
            # Verdicts written by another schema cannot be trusted, and they are
            # always recomputable, so the analyses are dropped rather than
            # migrated field by field.
            logger.info(
                f"Curation cache schema {stored} != {SCHEMA_VERSION}; "
                "discarding cached analyses"
            )
            self._conn.execute("DELETE FROM analyses")
            self._conn.execute("DELETE FROM schema_version")
            self._conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
            )
        self._conn.commit()

    @property
    def schema_version(self) -> int:
        """Schema version currently recorded in the database."""
        row = self._conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM schema_version"
        ).fetchone()
        return int(row["v"])

    # -- files -------------------------------------------------------------
    def lookup_sha256(self, relative_path: str, size: int, mtime: float) -> str | None:
        """Return a known digest for an unchanged file, avoiding a re-read."""
        row = self._conn.execute(
            "SELECT sha256, size, mtime FROM files WHERE last_path = ?",
            (relative_path,),
        ).fetchone()
        if row is None:
            return None
        if int(row["size"]) != int(size):
            return None
        if abs(float(row["mtime"]) - float(mtime)) > MTIME_TOLERANCE_S:
            return None
        return str(row["sha256"])

    def put_file(self, item: MediaItem) -> None:
        """Record where a digest was last seen, and with what size and mtime."""
        self._conn.execute(
            """\
            INSERT INTO files (sha256, last_path, size, mtime, media_type, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(sha256) DO UPDATE SET
                last_path  = excluded.last_path,
                size       = excluded.size,
                mtime      = excluded.mtime,
                media_type = excluded.media_type,
                updated_at = excluded.updated_at
            """,
            (
                item.sha256,
                item.relative_path,
                item.size,
                item.mtime,
                item.media_type.value,
                _now_iso(),
            ),
        )
        self._conn.commit()

    # -- analyses ----------------------------------------------------------
    def get_analysis(self, item: MediaItem, key: CacheKey) -> CurationResult | None:
        """Return the cached verdict for *item*, or *None* when it is absent."""
        row = self._conn.execute(
            """\
            SELECT * FROM analyses
            WHERE sha256 = ? AND pipeline_version = ?
              AND config_fingerprint = ? AND model_fingerprint = ?
            """,
            (
                item.sha256,
                key.pipeline_version,
                key.config_fingerprint,
                key.model_fingerprint,
            ),
        ).fetchone()
        if row is None:
            return None
        return _row_to_result(row, item)

    def put_analysis(self, result: CurationResult, key: CacheKey) -> None:
        """Store a verdict, replacing any earlier one for the same key."""
        self._conn.execute(
            """\
            INSERT INTO analyses (
                sha256, pipeline_version, config_fingerprint, model_fingerprint,
                decision, confidence, scores_json, labels_json, reasons_json,
                stage_trace_json, analyzed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sha256, pipeline_version, config_fingerprint,
                        model_fingerprint)
            DO UPDATE SET
                decision         = excluded.decision,
                confidence       = excluded.confidence,
                scores_json      = excluded.scores_json,
                labels_json      = excluded.labels_json,
                reasons_json     = excluded.reasons_json,
                stage_trace_json = excluded.stage_trace_json,
                analyzed_at      = excluded.analyzed_at
            """,
            (
                result.item.sha256,
                key.pipeline_version,
                key.config_fingerprint,
                key.model_fingerprint,
                result.decision.value,
                float(result.confidence),
                _dump(dict(result.scores)),
                _dump(list(result.labels)),
                _dump(list(result.reasons)),
                _dump([stage.as_dict() for stage in result.stage_trace]),
                _now_iso(),
            ),
        )
        self._conn.commit()

    # -- feedback ----------------------------------------------------------
    def put_feedback(
        self, sha256: str, decision: CurationDecision, source: str = "cli"
    ) -> None:
        """Record an explicit user correction for later preference training."""
        now = _now_iso()
        self._conn.execute(
            """\
            INSERT INTO feedback (sha256, user_decision, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(sha256) DO UPDATE SET
                user_decision = excluded.user_decision,
                source        = excluded.source,
                updated_at    = excluded.updated_at
            """,
            (sha256, decision.value, source, now, now),
        )
        self._conn.commit()

    def get_feedback(self) -> dict[str, CurationDecision]:
        """Return every recorded user correction, keyed by content hash."""
        rows = self._conn.execute("SELECT sha256, user_decision FROM feedback")
        return {
            str(r["sha256"]): CurationDecision(str(r["user_decision"])) for r in rows
        }

    # -- maintenance -------------------------------------------------------
    def stats(self) -> dict[str, Any]:
        """Return counts describing the cache contents."""

        def _scalar(sql: str) -> int:
            return int(self._conn.execute(sql).fetchone()[0] or 0)

        return {
            "db_path": str(self.db_path),
            "db_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
            "schema_version": self.schema_version,
            "files": _scalar("SELECT COUNT(*) FROM files"),
            "analyses": _scalar("SELECT COUNT(*) FROM analyses"),
            "feedback": _scalar("SELECT COUNT(*) FROM feedback"),
        }

    def prune_analyses(self, key: CacheKey) -> int:
        """Delete verdicts that no longer match *key*. Returns rows removed."""
        cur = self._conn.execute(
            """\
            DELETE FROM analyses
            WHERE pipeline_version != ? OR config_fingerprint != ?
               OR model_fingerprint != ?
            """,
            (key.pipeline_version, key.config_fingerprint, key.model_fingerprint),
        )
        self._conn.commit()
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def vacuum(self) -> None:
        """Compact the database file."""
        self._conn.execute("VACUUM")
        self._conn.commit()


def _dump(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _row_to_result(row: sqlite3.Row, item: MediaItem) -> CurationResult:
    scores_raw = json.loads(row["scores_json"])
    return CurationResult(
        item=item,
        decision=CurationDecision(str(row["decision"])),
        confidence=float(row["confidence"]),
        scores={
            str(k): (None if v is None else float(v)) for k, v in scores_raw.items()
        },
        labels=tuple(json.loads(row["labels_json"])),
        reasons=tuple(json.loads(row["reasons_json"])),
        stage_trace=tuple(
            StageResult.from_dict(entry)
            for entry in json.loads(row["stage_trace_json"])
        ),
        pipeline_version=str(row["pipeline_version"]),
        cache_hit=True,
    )
