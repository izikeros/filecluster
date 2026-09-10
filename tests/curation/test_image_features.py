"""Tests for the lightweight pixel feature stage."""

import numpy as np
import pytest
from PIL import Image

from filecluster.curation import reasons
from filecluster.curation.configuration import CurationSettings
from filecluster.curation.image_features import (
    ImageFeatureStage,
    compute_features,
    document_evidence,
    load_working_image,
    photographic_evidence,
    technical_quality,
)
from filecluster.curation.types import CurationDecision

from .conftest import make_item, page_array, scene_array, write_document, write_photo


class TestWorkingImage:
    def test_downscales_to_the_configured_side(self, tmp_path, settings):
        path = tmp_path / "big.jpg"
        Image.fromarray(scene_array(3000, 2000)).save(path)

        image = load_working_image(path, settings)

        assert max(image.size) <= settings.max_image_side

    def test_converts_grayscale_to_rgb(self, tmp_path, settings):
        path = tmp_path / "grey.png"
        Image.new("L", (64, 64), 128).save(path)

        assert load_working_image(path, settings).mode == "RGB"

    def test_flattens_transparency_onto_neutral_grey(self, tmp_path, settings):
        """Compositing onto white would make every transparent PNG a document."""
        path = tmp_path / "alpha.png"
        Image.new("RGBA", (64, 64), (255, 0, 0, 0)).save(path)

        image = load_working_image(path, settings)

        assert image.mode == "RGB"
        assert image.getpixel((0, 0)) == (128, 128, 128)

    def test_applies_the_exif_orientation(self, tmp_path, settings):
        path = tmp_path / "rotated.jpg"
        image = Image.fromarray(scene_array(400, 200))
        exif = image.getexif()
        exif[274] = 6  # rotate 90 degrees
        image.save(path, exif=exif)

        loaded = load_working_image(path, settings)

        assert loaded.size[1] > loaded.size[0]

    def test_refuses_a_decompression_bomb(self, tmp_path, settings):
        """A file declaring more pixels than allowed is refused before decoding."""
        path = tmp_path / "bomb.png"
        Image.new("RGB", (600, 600)).save(path)
        assert load_working_image(path, settings)

        strict = settings.model_copy(update={"max_pixels": 1_000})
        with pytest.raises(ValueError, match="pixels"):
            load_working_image(path, strict)


class TestFeatures:
    def test_flat_image_is_not_sharp_and_has_no_edges(self):
        flat = Image.new("RGB", (128, 128), (100, 100, 100))

        features = compute_features(flat)

        assert features.sharpness == 0.0
        assert features.edge_density == 0.0
        assert features.uniform_background == 1.0
        assert features.entropy == 0.0

    def test_noise_is_sharper_than_a_gradient(self):
        rng = np.random.default_rng(1)
        noise = Image.fromarray((rng.random((128, 128, 3)) * 255).astype("uint8"))
        gradient = Image.fromarray(
            np.broadcast_to(
                np.linspace(0, 255, 128, dtype="uint8")[None, :, None], (128, 128, 3)
            ).copy()
        )

        assert compute_features(noise).sharpness > compute_features(gradient).sharpness

    def test_clipping_is_measured(self):
        white = Image.new("RGB", (64, 64), (255, 255, 255))
        black = Image.new("RGB", (64, 64), (0, 0, 0))

        assert compute_features(white).overexposed_fraction == 1.0
        assert compute_features(black).underexposed_fraction == 1.0

    def test_a_colourful_scene_beats_a_page_on_colourfulness(self):
        scene = compute_features(Image.fromarray(scene_array()))
        page = compute_features(Image.fromarray(page_array()))

        assert scene.colorfulness > page.colorfulness

    def test_features_are_reported_under_a_namespace(self):
        features = compute_features(Image.new("RGB", (32, 32), (10, 20, 30)))

        assert all(key.startswith("features.") for key in features.as_scores())


class TestDerivedScores:
    def test_technical_quality_stays_in_range(self):
        for image in (
            Image.new("RGB", (64, 64), (255, 255, 255)),
            Image.new("RGB", (64, 64), (0, 0, 0)),
            Image.fromarray(scene_array()),
        ):
            assert 0.0 <= technical_quality(compute_features(image)) <= 1.0

    def test_a_page_looks_more_like_a_document_than_a_scene(self):
        page = document_evidence(compute_features(Image.fromarray(page_array())))
        scene = document_evidence(compute_features(Image.fromarray(scene_array())))

        assert page > 0.5
        assert scene < page

    def test_a_scene_looks_more_photographic_than_a_page(self):
        page = photographic_evidence(compute_features(Image.fromarray(page_array())))
        scene = photographic_evidence(compute_features(Image.fromarray(scene_array())))

        assert scene > page


class TestStage:
    def test_publishes_the_working_image_for_later_stages(
        self, tmp_path, settings, context
    ):
        path = write_photo(tmp_path / "IMG_0001.jpg")

        ImageFeatureStage(settings).analyze(make_item(path), context)

        assert context.image is not None

    def test_reports_quality_and_evidence_signals(self, tmp_path, settings, context):
        path = write_document(tmp_path / "page.png")

        result = ImageFeatureStage(settings).analyze(make_item(path), context)

        assert "technical_quality" in result.scores
        assert "features.document_evidence" in result.scores
        assert reasons.DOCUMENT_LIKE in result.reasons

    def test_never_ends_the_cascade_for_a_readable_file(
        self, tmp_path, settings, context
    ):
        """Low quality alone may not decide anything."""
        path = tmp_path / "blurry.jpg"
        Image.new("RGB", (200, 200), (40, 40, 42)).save(path)

        result = ImageFeatureStage(settings).analyze(make_item(path), context)

        assert result.terminal_decision is None
        assert reasons.LOW_SHARPNESS in result.reasons

    def test_a_corrupt_file_goes_to_review(self, tmp_path, settings, context):
        path = tmp_path / "broken.jpg"
        path.write_bytes(b"not an image")

        result = ImageFeatureStage(settings).analyze(make_item(path), context)

        assert result.terminal_decision is CurationDecision.REVIEW
        assert result.failed
        assert reasons.DECODE_ERROR in result.reasons

    def test_a_missing_file_goes_to_review(self, tmp_path, settings, context):
        item = make_item(tmp_path / "gone.jpg")

        result = ImageFeatureStage(settings).analyze(item, context)

        assert result.terminal_decision is CurationDecision.REVIEW
        assert reasons.FILE_MISSING in result.reasons

    def test_an_oversized_image_goes_to_review(self, tmp_path, context):
        path = write_photo(tmp_path / "IMG_0002.jpg")
        strict = CurationSettings(max_pixels=1_000_000).model_copy(
            update={"max_pixels": 1_000}
        )

        result = ImageFeatureStage(strict).analyze(make_item(path), context)

        assert result.terminal_decision is CurationDecision.REVIEW
        assert reasons.IMAGE_TOO_LARGE in result.reasons
