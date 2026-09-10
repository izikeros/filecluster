"""Tests for the curation data types."""

from pathlib import Path

from PIL import Image

from filecluster.curation.types import (
    SIGNAL_KEYS,
    CurationContext,
    CurationDecision,
    CurationResult,
    MediaKind,
    StageResult,
    iter_signals,
)

from .conftest import make_item


class TestStageResult:
    def test_round_trips_through_a_dict(self):
        """The cache stores stage traces as JSON, so this has to be lossless."""
        original = StageResult(
            stage="metadata",
            scores={"rules.utility_evidence": 0.5},
            labels=("screenshot",),
            reasons=("metadata.screenshot_filename",),
            terminal_decision=CurationDecision.REJECT,
            confidence=0.9,
            model_id="rules-v1",
            duration_ms=1.25,
            failed=False,
        )

        restored = StageResult.from_dict(original.as_dict())

        assert restored == original

    def test_missing_optional_fields_stay_none(self):
        restored = StageResult.from_dict({"stage": "features"})

        assert restored.terminal_decision is None
        assert restored.confidence is None
        assert restored.scores == {}


class TestCurationResult:
    def test_reports_the_last_stage_and_best_label(self, tmp_path):
        item = make_item(tmp_path / "a.jpg")
        result = CurationResult(
            item=item,
            decision=CurationDecision.REVIEW,
            confidence=0.4,
            scores={"personal_probability": 0.6, "aesthetic_score": None},
            labels=("portrait", "event"),
            reasons=(),
            stage_trace=(
                StageResult(stage="metadata", duration_ms=1.0),
                StageResult(stage="features", duration_ms=2.5),
            ),
            pipeline_version="curation-1",
        )

        assert result.completed_stage == "features"
        assert result.top_label == "portrait"
        assert result.duration_ms == 3.5
        assert result.signal("personal_probability") == 0.6
        assert result.signal("aesthetic_score") is None

    def test_missing_signals_are_absent_not_zero(self, tmp_path):
        """A missing signal must never be read as "measured, and it was zero"."""
        item = make_item(tmp_path / "a.jpg")
        result = CurationResult(
            item=item,
            decision=CurationDecision.REVIEW,
            confidence=0.0,
            scores={},
            labels=(),
            reasons=(),
            stage_trace=(),
            pipeline_version="curation-1",
        )

        assert all(result.signal(key) is None for key in SIGNAL_KEYS)


class TestMediaItem:
    def test_only_images_go_to_the_pixel_stages(self, tmp_path):
        image = make_item(tmp_path / "a.jpg")
        video = make_item(tmp_path / "a.mp4", media_type=MediaKind.VIDEO)

        assert image.is_image
        assert not video.is_image


class TestCurationContext:
    def test_release_drops_the_working_image(self, settings):
        context = CurationContext(settings)
        context.set_image(Image.new("RGB", (4, 4)))
        context.values["features"] = "something"

        context.release()

        assert context.image is None
        assert context.values == {}

    def test_failed_decode_is_remembered(self, settings):
        context = CurationContext(settings)
        context.set_image(None)

        assert context.image_failed

    def test_context_manager_releases(self, settings):
        with CurationContext(settings) as context:
            context.set_image(Image.new("RGB", (4, 4)))
        assert context.image is None


def test_iter_signals_skips_missing_values():
    scores: dict[str, float | None] = {
        "personal_probability": 0.7,
        "technical_quality": None,
        "features.entropy": 0.3,
    }

    assert list(iter_signals(scores)) == [("personal_probability", 0.7)]


def test_paths_are_pathlib(tmp_path: Path):
    assert isinstance(make_item(tmp_path / "a.jpg").path, Path)
