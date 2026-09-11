"""Tests for discovery, cascade orchestration and caching."""

import pytest

from filecluster.curation import reasons
from filecluster.curation.catalog import CurationCatalog
from filecluster.curation.configuration import CurationSettings, Thresholds
from filecluster.curation.pipeline import (
    CurationPipeline,
    build_stages,
    curate,
    discover_media,
)
from filecluster.curation.providers.base import (
    OcrAggregates,
    ProviderInfo,
    Providers,
    VlmJudgement,
)
from filecluster.curation.types import CurationDecision, MediaKind, StageResult

from .conftest import (
    BrokenSemanticProvider,
    FakeOcrProvider,
    FakeSemanticProvider,
    RecordingStage,
    TerminalStage,
    make_item,
    write_document,
    write_photo,
    write_screenshot,
)


class TestDiscovery:
    def test_finds_images_and_videos_recursively(self, tmp_path, settings):
        write_photo(tmp_path / "a.jpg")
        write_photo(tmp_path / "sub" / "b.jpg")
        (tmp_path / "clip.mp4").write_bytes(b"x")
        (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")

        found = discover_media(tmp_path, settings)

        assert [f.relative_path for f in found] == ["a.jpg", "clip.mp4", "sub/b.jpg"]
        assert found[1].media_type is MediaKind.VIDEO

    def test_skips_the_cache_database_and_hidden_entries(self, tmp_path, settings):
        write_photo(tmp_path / "a.jpg")
        (tmp_path / ".filecluster-curation.db").write_bytes(b"x")
        (tmp_path / ".filecluster-curation.db-wal").write_bytes(b"x")
        (tmp_path / ".hidden.jpg").write_bytes(b"x")
        write_photo(tmp_path / ".cache" / "thumb.jpg")

        found = discover_media(tmp_path, settings)

        assert [f.relative_path for f in found] == ["a.jpg"]

    def test_limit_is_deterministic(self, tmp_path, settings):
        for name in ("c.jpg", "a.jpg", "b.jpg"):
            write_photo(tmp_path / name)

        first = discover_media(tmp_path, settings, limit=2)
        second = discover_media(tmp_path, settings, limit=2)

        assert [f.relative_path for f in first] == ["a.jpg", "b.jpg"]
        assert first == second

    def test_an_empty_inbox_is_not_an_error(self, tmp_path, settings):
        assert discover_media(tmp_path, settings) == []

    def test_a_missing_inbox_is_reported(self, tmp_path, settings):
        with pytest.raises(NotADirectoryError):
            discover_media(tmp_path / "absent", settings)

    def test_a_symlink_out_of_the_inbox_is_ignored(self, tmp_path, settings):
        outside = write_photo(tmp_path / "outside" / "secret.jpg")
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        (inbox / "link.jpg").symlink_to(outside)

        assert discover_media(inbox, settings) == []


class TestStageAssembly:
    def test_only_cheap_stages_by_default(self, settings):
        names = [stage.name for stage in build_stages(settings)]

        assert names == ["metadata", "features"]

    def test_an_enabled_stage_without_a_provider_is_skipped(self):
        settings = CurationSettings(enable_semantic=True, enable_ocr=True)

        names = [stage.name for stage in build_stages(settings, Providers())]

        assert names == ["metadata", "features"]

    def test_requested_stages_without_providers_are_reported(self, inbox):
        settings = CurationSettings(enable_semantic=True, enable_ocr=True)

        run = CurationPipeline(settings).run(inbox)

        assert run.unavailable_stages == ("ocr", "semantic")

    def test_providers_are_added_in_cost_order(self):
        settings = CurationSettings(enable_semantic=True, enable_ocr=True)
        providers = Providers(
            semantic=FakeSemanticProvider({"portrait": 0.9}),
            ocr=FakeOcrProvider(OcrAggregates()),
        )

        names = [stage.name for stage in build_stages(settings, providers)]

        assert names == ["metadata", "features", "ocr", "semantic"]


class TestCascade:
    def test_a_terminal_decision_skips_the_remaining_stages(self, tmp_path, settings):
        """The expensive stage must not run once a cheap one is certain."""
        expensive = RecordingStage(name="expensive")
        pipeline = CurationPipeline(settings, stages=[TerminalStage(), expensive])
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = pipeline.analyze(item)

        assert result.decision is CurationDecision.REJECT
        assert expensive.calls == []

    def test_a_low_confidence_terminal_reject_is_not_honoured(self, tmp_path, settings):
        """The confidence floor lives in configuration, not inside a stage."""
        expensive = RecordingStage(name="expensive")
        pipeline = CurationPipeline(
            settings,
            stages=[TerminalStage(confidence=0.2), expensive],
        )
        item = make_item(write_photo(tmp_path / "a.jpg"))

        pipeline.analyze(item)

        assert expensive.calls == [item.relative_path]

    def test_a_video_reaches_review_without_pixel_work(self, tmp_path, settings):
        path = tmp_path / "clip.mp4"
        path.write_bytes(b"not a video")
        item = make_item(path, media_type=MediaKind.VIDEO)

        result = CurationPipeline(settings).analyze(item)

        assert result.decision is CurationDecision.REVIEW
        assert reasons.UNSUPPORTED_MEDIA_TYPE in result.reasons

    def test_a_broken_provider_lands_in_review(self, tmp_path):
        settings = CurationSettings(enable_semantic=True)
        broken = BrokenSemanticProvider()
        providers = Providers(semantic=broken)
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = CurationPipeline(settings, providers).analyze(item)

        assert broken.calls == 1
        assert result.decision is CurationDecision.REVIEW
        assert reasons.PROVIDER_UNAVAILABLE in result.reasons

    def test_a_stage_that_raises_does_not_end_the_run(self, tmp_path, settings):
        class Exploding:
            name = "exploding"

            def analyze(self, item, context):
                raise RuntimeError("boom")

        pipeline = CurationPipeline(settings, stages=[Exploding()])
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = pipeline.analyze(item)

        assert result.decision is CurationDecision.REVIEW
        assert reasons.STAGE_ERROR in result.reasons

    def test_semantic_signals_reach_the_verdict(self, tmp_path):
        settings = CurationSettings(enable_semantic=True)
        providers = Providers(
            semantic=FakeSemanticProvider(
                {"personal_people": 0.97, "portrait": 0.9, "document": 0.05}
            )
        )
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = CurationPipeline(settings, providers).analyze(item)

        assert result.top_label == "personal_people"
        assert result.signal("personal_probability") == 0.97
        assert result.decision is CurationDecision.KEEP

    def test_a_qualified_vlm_judgement_becomes_the_final_verdict(self, tmp_path):
        class FakeVlmProvider:
            def info(self):
                return ProviderInfo(name="fake-vlm", model_id="fake-vlm-1")

            def judge(self, image):
                return VlmJudgement(
                    decision=CurationDecision.KEEP,
                    confidence=0.9,
                    labels=("portrait",),
                    reasons=("clear personal photograph",),
                )

        settings = CurationSettings(
            enable_vlm=True,
            thresholds=Thresholds(keep=0.95, reject=0.05, vlm_band=(0.25, 0.75)),
        )
        providers = Providers(vlm=FakeVlmProvider())
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = CurationPipeline(settings, providers).analyze(item)

        assert result.decision is CurationDecision.KEEP
        assert result.confidence == 0.9
        assert result.top_label == "portrait"
        assert reasons.VLM_DECISION in result.reasons

    def test_ocr_contributes_only_aggregates(self, tmp_path):
        settings = CurationSettings(enable_ocr=True)
        providers = Providers(
            ocr=FakeOcrProvider(
                OcrAggregates(
                    blocks=30,
                    lines=30,
                    characters=900,
                    mean_confidence=0.95,
                    text_area_fraction=0.5,
                )
            )
        )
        item = make_item(write_document(tmp_path / "page.png"))

        result = CurationPipeline(settings, providers).analyze(item)

        assert reasons.HIGH_TEXT_DENSITY in result.reasons
        assert not any(key == "text" for key in result.scores)
        assert result.signal("utility_probability") > 0.5

    def test_named_signals_are_always_present_in_the_result(self, tmp_path, settings):
        item = make_item(write_photo(tmp_path / "a.jpg"))

        result = CurationPipeline(settings).analyze(item)

        assert "personal_probability" in result.scores
        assert result.scores["aesthetic_score"] is None


class TestRun:
    def test_reports_aggregate_counts(self, inbox, settings):
        run = CurationPipeline(settings).run(inbox)

        counts = run.decision_counts()
        assert set(counts) == {"keep", "review", "reject"}
        assert sum(counts.values()) == 3
        assert run.n_discovered == 3
        assert run.elapsed_seconds >= 0.0

    def test_an_unambiguous_screenshot_is_rejected(self, tmp_path, settings):
        write_screenshot(tmp_path / "Screenshot_2024-05-01.png")

        run = CurationPipeline(settings).run(tmp_path)

        assert run.results[0].decision is CurationDecision.REJECT

    def test_a_document_is_not_rejected_without_semantic_evidence(
        self, tmp_path, settings
    ):
        write_document(tmp_path / "page.png")

        run = CurationPipeline(settings).run(tmp_path)

        assert run.results[0].decision is CurationDecision.REVIEW

    def test_one_unreadable_file_does_not_stop_the_run(self, tmp_path, settings):
        write_photo(tmp_path / "a.jpg")
        (tmp_path / "b.jpg").write_bytes(b"garbage")

        run = CurationPipeline(settings).run(tmp_path)

        assert len(run.results) == 2
        assert run.n_errors == 1

    def test_reason_counts_are_aggregated(self, inbox, settings):
        run = CurationPipeline(settings).run(inbox)

        assert run.reason_counts().total() > 0

    def test_results_can_be_filtered_by_decision(self, inbox, settings):
        run = CurationPipeline(settings).run(inbox)

        rejected = run.results_for(CurationDecision.REJECT)

        assert all(r.decision is CurationDecision.REJECT for r in rejected)


class TestCaching:
    def test_a_second_run_reuses_the_cache(self, inbox, settings):
        first = curate(inbox, settings)
        second = curate(inbox, settings)

        assert first.n_cache_hits == 0
        assert second.n_cache_hits == len(second.results)

    def test_force_recompute_ignores_the_cache(self, inbox, settings):
        curate(inbox, settings)

        again = curate(inbox, settings, force_recompute=True)

        assert again.n_cache_hits == 0

    def test_changed_settings_invalidate_the_cache(self, inbox, settings):
        curate(inbox, settings)
        stricter = settings.model_copy(
            update={"thresholds": Thresholds(keep=0.9, reject=0.1)}
        )

        again = curate(inbox, stricter)

        assert again.n_cache_hits == 0

    def test_a_new_model_invalidates_the_cache(self, inbox):
        settings = CurationSettings(enable_semantic=True)
        providers = Providers(semantic=FakeSemanticProvider({"portrait": 0.8}))
        curate(inbox, settings, providers)

        class OtherModel(FakeSemanticProvider):
            def info(self):
                info = super().info()
                return info.__class__(name=info.name, model_id="fake-2", revision="2")

        again = curate(
            inbox, settings, Providers(semantic=OtherModel({"portrait": 0.8}))
        )

        assert again.n_cache_hits == 0

    def test_a_changed_file_is_reanalysed(self, tmp_path, settings):
        path = write_photo(tmp_path / "a.jpg", seed=1)
        curate(tmp_path, settings)

        write_photo(path, seed=2)
        again = curate(tmp_path, settings)

        assert again.n_cache_hits == 0

    def test_the_cache_can_be_switched_off(self, inbox, settings):
        curate(inbox, settings, use_cache=False)

        assert not (inbox / ".filecluster-curation.db").exists()

    def test_stage_traces_survive_a_cache_round_trip(self, inbox, settings):
        first = curate(inbox, settings)
        second = curate(inbox, settings)

        original = {r.item.relative_path: r for r in first.results}
        for result in second.results:
            before = original[result.item.relative_path].stage_trace
            assert [s.stage for s in result.stage_trace] == [s.stage for s in before]
            assert [s.reasons for s in result.stage_trace] == [
                s.reasons for s in before
            ]
            assert [s.terminal_decision for s in result.stage_trace] == [
                s.terminal_decision for s in before
            ]

    def test_the_cache_stores_one_row_per_file(self, inbox, settings):
        curate(inbox, settings)
        curate(inbox, settings)

        with CurationCatalog.open(settings.cache_path_for(inbox)) as catalog:
            assert catalog.stats()["analyses"] == 3


class TestProgress:
    def test_progress_is_advanced_once_per_file(self, inbox, settings):
        class Sink:
            def __init__(self):
                self.detail = ""
                self.total = 0
                self.steps = 0

            def start(self, total, description=""):
                self.total = total

            def advance(self, step=1):
                self.steps += step

            def update_description(self, text):
                pass

        sink = Sink()
        CurationPipeline(settings).run(inbox, progress=sink)

        assert sink.total == 3
        assert sink.steps == 3


def test_stage_results_are_recorded_in_order(tmp_path, settings):
    item = make_item(write_photo(tmp_path / "a.jpg"))

    result = CurationPipeline(settings).analyze(item)

    assert [stage.stage for stage in result.stage_trace] == ["metadata", "features"]
    assert all(isinstance(stage, StageResult) for stage in result.stage_trace)
