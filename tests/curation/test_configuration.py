"""Tests for curation settings, validation and the cache fingerprint."""

import json

import pytest

from filecluster.curation.configuration import (
    CACHE_FILENAME,
    CurationSettings,
    Thresholds,
    load_settings,
)
from filecluster.curation.exceptions import CurationConfigError


class TestValidation:
    def test_reject_threshold_must_be_below_keep(self):
        with pytest.raises(CurationConfigError):
            Thresholds(keep=0.4, reject=0.6)

    def test_equal_thresholds_are_refused(self):
        """Equal thresholds would leave no uncertainty band at all."""
        with pytest.raises(CurationConfigError):
            Thresholds(keep=0.5, reject=0.5)

    def test_thresholds_stay_within_zero_and_one(self):
        with pytest.raises(Exception):  # noqa: B017 - pydantic's own error type
            Thresholds(keep=1.4, reject=0.2)

    def test_batch_size_is_at_least_one(self):
        with pytest.raises(Exception):  # noqa: B017 - pydantic's own error type
            CurationSettings(batch_size=0)

    def test_remote_vlm_needs_explicit_permission(self):
        with pytest.raises(CurationConfigError):
            CurationSettings(
                enable_vlm=True,
                vlm_endpoint="https://example.invalid/v1",
                allow_remote_vlm=False,
            )

    def test_remote_vlm_is_allowed_once_opted_in(self):
        settings = CurationSettings(
            enable_vlm=True,
            vlm_endpoint="https://example.invalid/v1",
            allow_remote_vlm=True,
        )

        assert settings.vlm_endpoint

    def test_vlm_band_must_be_ordered(self):
        with pytest.raises(CurationConfigError):
            Thresholds(vlm_band=(0.8, 0.2))


class TestPaths:
    def test_cache_defaults_into_the_inbox(self, tmp_path):
        settings = CurationSettings()

        assert settings.cache_path_for(tmp_path) == tmp_path / CACHE_FILENAME

    def test_explicit_cache_path_wins(self, tmp_path):
        settings = CurationSettings(cache_path=tmp_path / "elsewhere.db")

        assert settings.cache_path_for(tmp_path).name == "elsewhere.db"

    def test_extensions_are_classified(self):
        settings = CurationSettings()

        assert settings.is_image_extension(".JPG")
        assert settings.is_video_extension(".MP4")
        assert not settings.is_image_extension(".txt")


class TestFingerprint:
    def test_same_settings_hash_the_same(self):
        assert CurationSettings().fingerprint() == CurationSettings().fingerprint()

    def test_a_threshold_change_invalidates_the_cache(self):
        base = CurationSettings()
        tweaked = CurationSettings(thresholds=Thresholds(keep=0.8, reject=0.3))

        assert base.fingerprint() != tweaked.fingerprint()

    def test_a_weight_change_invalidates_the_cache(self):
        base = CurationSettings()
        tweaked = base.model_copy(deep=True)
        tweaked.weights.personal = 0.9

        assert base.fingerprint() != tweaked.fingerprint()

    def test_enabling_a_stage_invalidates_the_cache(self):
        assert (
            CurationSettings().fingerprint()
            != CurationSettings(enable_ocr=True).fingerprint()
        )

    def test_cache_location_alone_does_not_invalidate(self, tmp_path):
        """Moving the database does not change what a stage would decide."""
        base = CurationSettings()
        moved = CurationSettings(cache_path=tmp_path / "other.db")

        assert base.fingerprint() == moved.fingerprint()

    def test_config_file_contents_are_part_of_the_fingerprint(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text(json.dumps({"max_image_side": 512}), encoding="utf-8")
        first = load_settings(path).fingerprint()

        path.write_text(json.dumps({"max_image_side": 512, "batch_size": 4}))
        second = load_settings(path).fingerprint()

        assert first != second


class TestLoadSettings:
    def test_reads_json(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text(
            json.dumps({"max_image_side": 512, "thresholds": {"keep": 0.8}}),
            encoding="utf-8",
        )

        settings = load_settings(path)

        assert settings.max_image_side == 512
        assert settings.keep_threshold == 0.8

    def test_cli_overrides_win_over_the_file(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text(json.dumps({"max_image_side": 512}), encoding="utf-8")

        settings = load_settings(path, max_image_side=256)

        assert settings.max_image_side == 256

    def test_none_overrides_are_ignored(self, tmp_path):
        """An unset CLI flag must not overwrite a configured value."""
        path = tmp_path / "curation.json"
        path.write_text(json.dumps({"enable_ocr": True}), encoding="utf-8")

        settings = load_settings(path, enable_ocr=None)

        assert settings.enable_ocr

    def test_invalid_json_reports_the_file(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text("{not json", encoding="utf-8")

        with pytest.raises(CurationConfigError, match="Invalid JSON"):
            load_settings(path)

    def test_a_top_level_list_is_refused(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text("[1, 2]", encoding="utf-8")

        with pytest.raises(CurationConfigError, match="mapping"):
            load_settings(path)

    def test_missing_file_is_reported(self, tmp_path):
        with pytest.raises(CurationConfigError, match="Cannot read"):
            load_settings(tmp_path / "absent.json")

    def test_invalid_values_are_reported_as_config_errors(self, tmp_path):
        path = tmp_path / "curation.json"
        path.write_text(json.dumps({"batch_size": -3}), encoding="utf-8")

        with pytest.raises(CurationConfigError):
            load_settings(path)
