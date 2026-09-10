"""Tests for the metadata rule stage.

The important assertions here are negative: a PNG, a missing EXIF block or an
unusual aspect ratio must not be enough to reject a file.
"""

from filecluster.curation import reasons
from filecluster.curation.rules import (
    CAMERA_EXIF_PERSONAL_PRIOR,
    MetadataRuleStage,
    ScreenResolutions,
    load_screen_resolutions,
    read_metadata_facts,
)
from filecluster.curation.types import CurationDecision, MediaKind

from .conftest import make_item, write_document, write_photo, write_screenshot


class TestScreenResolutions:
    def test_matches_in_both_orientations(self):
        table = ScreenResolutions(version=1, sizes=frozenset({(1170, 2532)}))

        assert table.matches(1170, 2532)
        assert table.matches(2532, 1170)

    def test_tolerates_a_few_pixels(self):
        table = ScreenResolutions(version=1, sizes=frozenset({(1170, 2532)}))

        assert table.matches(1170, 2530, tolerance=4)
        assert not table.matches(1170, 2500, tolerance=4)

    def test_packaged_table_is_loadable(self):
        table = load_screen_resolutions()

        assert table.version >= 1
        assert table.matches(1170, 2532)

    def test_a_broken_table_degrades_to_empty(self, tmp_path):
        """Losing the table may only make the pipeline more cautious."""
        path = tmp_path / "screens.json"
        path.write_text("nonsense", encoding="utf-8")

        table = load_screen_resolutions(path)

        assert table.sizes == frozenset()


class TestMetadataFacts:
    def test_camera_exif_is_detected(self, tmp_path):
        path = write_photo(tmp_path / "IMG_0001.jpg", camera_exif=True)

        facts = read_metadata_facts(path)

        assert facts.has_camera_exif
        assert facts.width and facts.height

    def test_missing_exif_is_reported_without_error(self, tmp_path):
        path = write_photo(tmp_path / "IMG_0002.jpg", camera_exif=False)

        facts = read_metadata_facts(path)

        assert not facts.has_camera_exif

    def test_unreadable_file_is_flagged_not_raised(self, tmp_path):
        path = tmp_path / "broken.jpg"
        path.write_bytes(b"definitely not a jpeg")

        facts = read_metadata_facts(path)

        assert facts.unreadable


class TestRuleStage:
    def test_video_goes_straight_to_review(self, tmp_path, context):
        item = make_item(tmp_path / "clip.mp4", media_type=MediaKind.VIDEO)

        result = MetadataRuleStage().analyze(item, context)

        assert result.terminal_decision is CurationDecision.REVIEW
        assert reasons.UNSUPPORTED_MEDIA_TYPE in result.reasons

    def test_screenshot_name_and_resolution_reject(self, tmp_path, context):
        path = write_screenshot(tmp_path / "Screenshot_2024-05-01.png")

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert result.terminal_decision is CurationDecision.REJECT
        assert reasons.SCREENSHOT_FILENAME in result.reasons
        assert reasons.SCREEN_RESOLUTION_MATCH in result.reasons

    def test_png_without_exif_alone_never_rejects(self, tmp_path, context):
        """One weak signal is not evidence; a PNG can be a photograph."""
        path = write_document(tmp_path / "page.png")

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert result.terminal_decision is None
        assert reasons.PNG_WITHOUT_CAMERA_EXIF in result.reasons

    def test_camera_exif_raises_the_personal_prior(self, tmp_path, context):
        path = write_photo(tmp_path / "IMG_0003.jpg", camera_exif=True)

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert result.terminal_decision is None
        assert reasons.CAMERA_EXIF_PRESENT in result.reasons
        assert result.scores["rules.personal_prior"] == CAMERA_EXIF_PERSONAL_PRIOR

    def test_a_photograph_never_looks_like_a_screenshot_by_size(
        self, tmp_path, context
    ):
        """A camera file at screen size is still a camera file."""
        path = write_photo(tmp_path / "IMG_0004.jpg", camera_exif=True)

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert reasons.SCREEN_RESOLUTION_MATCH not in result.reasons

    def test_screenshot_software_alone_is_decisive(self, tmp_path, context):
        """A capture tool writing its own name is a system-level signal."""
        path = write_document(tmp_path / "capture.png")
        stage = MetadataRuleStage()
        facts = read_metadata_facts(path)
        patched = facts.__class__(
            width=facts.width,
            height=facts.height,
            has_camera_exif=False,
            has_any_exif=True,
            software="Greenshot 1.2",
            image_format="PNG",
        )

        signals = stage._collect_signals(make_item(path), patched)

        assert reasons.SCREENSHOT_SOFTWARE in signals
        assert reasons.SCREENSHOT_SOFTWARE in reasons.DECISIVE_UTILITY_SIGNALS

    def test_extreme_aspect_ratio_is_only_a_note(self, tmp_path, context):
        from PIL import Image

        path = tmp_path / "panorama.jpg"
        Image.new("RGB", (2000, 300), (120, 90, 60)).save(path)

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert reasons.EXTREME_ASPECT_RATIO in result.reasons
        assert result.terminal_decision is None

    def test_the_stage_records_its_data_version(self, tmp_path, context):
        path = write_photo(tmp_path / "IMG_0005.jpg")

        result = MetadataRuleStage().analyze(make_item(path), context)

        assert result.model_id and result.model_id.startswith("rules-v1")

    def test_facts_are_published_on_the_context(self, tmp_path, context):
        path = write_photo(tmp_path / "IMG_0006.jpg")

        MetadataRuleStage().analyze(make_item(path), context)

        assert "metadata_facts" in context.values
