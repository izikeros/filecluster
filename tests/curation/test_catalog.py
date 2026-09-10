"""Tests for the curation verdict cache."""

import sqlite3

from filecluster.curation.catalog import SCHEMA_VERSION, CacheKey, CurationCatalog
from filecluster.curation.types import (
    CurationDecision,
    CurationResult,
    StageResult,
)

from .conftest import make_item, write_photo

KEY = CacheKey(
    pipeline_version="curation-1",
    config_fingerprint="cfg",
    model_fingerprint="model",
)


def make_result(item, decision=CurationDecision.KEEP) -> CurationResult:
    return CurationResult(
        item=item,
        decision=decision,
        confidence=0.81,
        scores={"personal_probability": 0.9, "aesthetic_score": None},
        labels=("portrait",),
        reasons=("semantic.personal",),
        stage_trace=(
            StageResult(stage="metadata", scores={"rules.strong_signals": 0.0}),
        ),
        pipeline_version="curation-1",
    )


class TestSchema:
    def test_creates_its_schema(self, tmp_path):
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            assert catalog.schema_version == SCHEMA_VERSION

    def test_creates_missing_parent_directories(self, tmp_path):
        with CurationCatalog.open(tmp_path / "nested" / "cache.db") as catalog:
            assert catalog.db_path.exists()

    def test_a_foreign_schema_version_discards_analyses(self, tmp_path):
        """Verdicts are always recomputable, so migration is not worth the risk."""
        path = tmp_path / "cache.db"
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        with CurationCatalog.open(path) as catalog:
            catalog.put_analysis(make_result(item), KEY)

        conn = sqlite3.connect(path)
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version (version) VALUES (99)")
        conn.commit()
        conn.close()

        with CurationCatalog.open(path) as catalog:
            assert catalog.schema_version == SCHEMA_VERSION
            assert catalog.get_analysis(item, KEY) is None


class TestAnalyses:
    def test_round_trips_a_verdict(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        original = make_result(item)

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(original, KEY)
            cached = catalog.get_analysis(item, KEY)

        assert cached is not None
        assert cached.decision is original.decision
        assert cached.confidence == original.confidence
        assert cached.labels == original.labels
        assert cached.reasons == original.reasons
        assert cached.scores["personal_probability"] == 0.9
        assert cached.scores["aesthetic_score"] is None
        assert cached.stage_trace[0].stage == "metadata"
        assert cached.cache_hit

    def test_a_second_write_replaces_the_first(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(make_result(item, CurationDecision.KEEP), KEY)
            catalog.put_analysis(make_result(item, CurationDecision.REVIEW), KEY)
            cached = catalog.get_analysis(item, KEY)

        assert cached.decision is CurationDecision.REVIEW
        assert catalog_row_count(tmp_path / "cache.db") == 1

    def test_each_fingerprint_invalidates_independently(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        others = [
            CacheKey("curation-2", "cfg", "model"),
            CacheKey("curation-1", "cfg-changed", "model"),
            CacheKey("curation-1", "cfg", "model-changed"),
        ]

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(make_result(item), KEY)

            assert catalog.get_analysis(item, KEY) is not None
            for key in others:
                assert catalog.get_analysis(item, key) is None

    def test_content_identity_survives_a_rename(self, tmp_path):
        """A cached verdict follows the bytes, not the file name."""
        path = write_photo(tmp_path / "IMG_0001.jpg")
        item = make_item(path, sha256="a" * 64)
        renamed = make_item(path, relative_path="holiday.jpg", sha256="a" * 64)

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(make_result(item), KEY)

            assert catalog.get_analysis(renamed, KEY) is not None

    def test_prune_removes_foreign_keys(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(make_result(item), CacheKey("old", "cfg", "model"))
            catalog.put_analysis(make_result(item), KEY)

            removed = catalog.prune_analyses(KEY)

            assert removed == 1
            assert catalog.get_analysis(item, KEY) is not None


class TestFileRows:
    def test_a_known_unchanged_file_skips_rehashing(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"), sha256="b" * 64)

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_file(item)
            found = catalog.lookup_sha256(item.relative_path, item.size, item.mtime)

        assert found == item.sha256

    def test_a_changed_size_invalidates_the_row(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"), sha256="b" * 64)

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_file(item)

            assert (
                catalog.lookup_sha256(item.relative_path, item.size + 1, item.mtime)
                is None
            )

    def test_a_changed_mtime_invalidates_the_row(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"), sha256="b" * 64)

        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_file(item)

            assert (
                catalog.lookup_sha256(item.relative_path, item.size, item.mtime + 10)
                is None
            )

    def test_an_unknown_path_is_a_miss(self, tmp_path):
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            assert catalog.lookup_sha256("absent.jpg", 1, 1.0) is None


class TestFeedbackAndStats:
    def test_feedback_is_stored_and_updated(self, tmp_path):
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_feedback("c" * 64, CurationDecision.KEEP, source="test")
            catalog.put_feedback("c" * 64, CurationDecision.REJECT, source="test")

            assert catalog.get_feedback() == {"c" * 64: CurationDecision.REJECT}

    def test_stats_count_the_rows(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_file(item)
            catalog.put_analysis(make_result(item), KEY)

            stats = catalog.stats()

        assert stats["files"] == 1
        assert stats["analyses"] == 1
        assert stats["schema_version"] == SCHEMA_VERSION

    def test_vacuum_keeps_the_data(self, tmp_path):
        item = make_item(write_photo(tmp_path / "IMG_0001.jpg"))
        with CurationCatalog.open(tmp_path / "cache.db") as catalog:
            catalog.put_analysis(make_result(item), KEY)
            catalog.vacuum()

            assert catalog.get_analysis(item, KEY) is not None


def catalog_row_count(path) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM analyses").fetchone()[0])
    finally:
        conn.close()
