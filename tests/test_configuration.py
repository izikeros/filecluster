"""Tests for the configuration module.

Covers FileClusterSettings validation, Config creation for different OS/mode
combinations, ConfigFactory behaviour, CLI override logic (including precedence
rules), and backwards-compatibility helper functions.
"""

from datetime import timedelta
from pathlib import Path

import pytest

from filecluster.configuration import (
    AssignDateToClusterMethod,
    ClusteringMethod,
    Config,
    ConfigFactory,
    CopyMode,
    FileClusterSettings,
    Status,
    default_factory,
    get_default_config,
    get_development_config,
    get_proper_mode_config,
    override_config_with_cli_params,
)


# ---------------------------------------------------------------------------
# FileClusterSettings
# ---------------------------------------------------------------------------
class TestFileClusterSettings:
    """Tests for Pydantic-based FileClusterSettings."""

    def test_default_image_extensions_include_common_formats(self):
        """
        Test Description: Verify default image extensions contain standard photo formats.

        Purpose: Users rely on defaults to capture .jpg, .jpeg, .heic, etc.

        Test Strategy:
        - Setup: Instantiate default settings
        - Verification: Check that common formats are present and lowercase
        """
        s = FileClusterSettings()
        assert ".jpg" in s.image_extensions
        assert ".jpeg" in s.image_extensions
        assert ".heic" in s.image_extensions

    def test_default_video_extensions_include_common_formats(self):
        """Verify default video extensions contain .mp4 and .3gp."""
        s = FileClusterSettings()
        assert ".mp4" in s.video_extensions
        assert ".3gp" in s.video_extensions

    def test_field_validator_lowercases_image_extensions(self):
        """
        Test Description: Extensions given in UPPER CASE are normalized to lowercase.

        Purpose: File matching must be case-insensitive; normalization at config
        level avoids bugs in downstream comparison logic.
        """
        s = FileClusterSettings(image_extensions=[".JPG", ".HEIC", ".Tiff"])
        assert all(ext == ext.lower() for ext in s.image_extensions)
        assert s.image_extensions == [".jpg", ".heic", ".tiff"]

    def test_field_validator_lowercases_video_extensions(self):
        """Same normalization applies to video extensions."""
        s = FileClusterSettings(video_extensions=[".MP4", ".MOV"])
        assert s.video_extensions == [".mp4", ".mov"]

    def test_default_time_granularity_is_60_minutes(self):
        """Business rule: default event gap is 60 minutes."""
        s = FileClusterSettings()
        assert s.time_granularity_minutes == 60

    def test_default_clustering_method_is_time_gap(self):
        """Only TIME_GAP clustering is currently supported."""
        s = FileClusterSettings()
        assert s.default_clustering_method == ClusteringMethod.TIME_GAP

    def test_default_assign_date_method_is_median(self):
        """Business rule: median date is the default representative for a cluster."""
        s = FileClusterSettings()
        assert s.default_assign_date_method == AssignDateToClusterMethod.MEDIAN

    def test_feature_flags_default_to_false(self):
        """All feature flags off by default to avoid unexpected behaviour."""
        s = FileClusterSettings()
        assert s.force_deep_scan is False
        assert s.assign_to_clusters_existing_in_libs is False
        assert s.skip_duplicated_existing_in_libs is False

    def test_cluster_df_columns_schema(self):
        """Verify the expected column schema for cluster DataFrames."""
        s = FileClusterSettings()
        expected = {
            "cluster_id",
            "start_date",
            "end_date",
            "median",
            "is_continuous",
            "path",
            "target_path",
            "file_count",
            "new_file_count",
        }
        assert set(s.cluster_df_columns) == expected

    def test_media_df_columns_schema(self):
        """Verify the expected column schema for media DataFrames."""
        s = FileClusterSettings()
        expected = {
            "file_name",
            "m_date",
            "c_date",
            "exif_date",
            "date",
            "size",
            "hash_value",
            "image",
            "is_image",
            "status",
            "duplicated_to",
            "duplicated_cluster",
        }
        assert set(s.media_df_columns) == expected


# ---------------------------------------------------------------------------
# Enum values
# ---------------------------------------------------------------------------
class TestEnumValues:
    """Verify enum members have distinct, expected values."""

    def test_copy_mode_members(self):
        assert CopyMode.COPY != CopyMode.MOVE != CopyMode.NOP

    def test_status_members(self):
        assert Status.UNKNOWN.value == 0
        assert Status.NEW_CLUSTER.value == 1
        assert Status.EXISTING_CLUSTER.value == 2
        assert Status.DUPLICATE.value == 3

    def test_assign_date_methods(self):
        assert AssignDateToClusterMethod.RANDOM != AssignDateToClusterMethod.MEDIAN


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------
class TestConfig:
    """Tests for the Config dataclass."""

    def test_repr_includes_all_fields(self):
        """
        Test Description: repr output lists every field for debugging.

        Purpose: Operators diagnosing mis-configuration need full visibility.
        """
        config = get_default_config()
        r = repr(config)
        for field in [
            "in_dir_name",
            "out_dir_name",
            "mode",
            "time_granularity",
            "watch_folders",
        ]:
            assert field in r

    def test_getitem_returns_attribute_value(self):
        """Config supports dict-style read access."""
        config = get_default_config()
        assert config["mode"] == config.mode

    def test_setitem_updates_attribute(self):
        """Config supports dict-style write access."""
        config = get_default_config()
        config["mode"] = CopyMode.NOP
        assert config.mode == CopyMode.NOP

    def test_time_granularity_is_timedelta(self):
        """time_granularity must be a timedelta for correct arithmetic."""
        config = get_default_config()
        assert isinstance(config.time_granularity, timedelta)
        assert config.time_granularity == timedelta(minutes=60)


# ---------------------------------------------------------------------------
# ConfigFactory
# ---------------------------------------------------------------------------
class TestConfigFactory:
    """Tests for ConfigFactory creation logic."""

    def test_production_mode_uses_move(self):
        """
        Test Description: Production config defaults to MOVE to free inbox space.

        Purpose: This is a critical business rule — production should move files,
        not copy, to avoid filling the disk.
        """
        config = default_factory.get_config(is_development_mode=False)
        assert config.mode == CopyMode.MOVE

    def test_development_mode_uses_copy(self):
        """
        Test Description: Dev config defaults to COPY to protect source files.

        Purpose: During development, source files must not be lost.
        """
        config = default_factory.get_config(is_development_mode=True)
        assert config.mode == CopyMode.COPY

    def test_windows_paths_used_on_nt(self):
        """
        Test Description: Windows-specific paths are used when os_name is 'nt'.

        Purpose: Cross-platform support requires OS-aware path selection.
        """
        settings = FileClusterSettings(
            windows_base_path=Path("C:\\photos\\"),
            linux_base_path=Path("/photos/"),
        )
        factory = ConfigFactory(settings)
        config = factory._create_config(is_development_mode=False, os_name="nt")
        assert "C:" in str(config.in_dir_name)

    def test_linux_paths_used_on_posix(self):
        """Linux paths are used when os_name is not 'nt'."""
        settings = FileClusterSettings(
            windows_base_path=Path("C:\\photos\\"),
            linux_base_path=Path("/photos/"),
        )
        factory = ConfigFactory(settings)
        config = factory._create_config(is_development_mode=False, os_name="posix")
        assert str(config.in_dir_name).startswith("/photos")

    def test_dev_mode_uses_dev_directories(self):
        """Dev mode uses dev_inbox_dir / dev_outbox_dir, not production dirs."""
        settings = FileClusterSettings(
            dev_inbox_dir=Path("dev_in"),
            dev_outbox_dir=Path("dev_out"),
            inbox_dir=Path("prod_in"),
            outbox_dir=Path("prod_out"),
        )
        factory = ConfigFactory(settings)
        config = factory._create_config(is_development_mode=True, os_name="posix")
        assert "dev_in" in str(config.in_dir_name)
        assert "dev_out" in str(config.out_dir_name)

    def test_prod_mode_uses_prod_directories(self):
        """Production mode uses inbox_dir / outbox_dir."""
        settings = FileClusterSettings(
            dev_inbox_dir=Path("dev_in"),
            dev_outbox_dir=Path("dev_out"),
            inbox_dir=Path("prod_in"),
            outbox_dir=Path("prod_out"),
        )
        factory = ConfigFactory(settings)
        config = factory._create_config(is_development_mode=False, os_name="posix")
        assert "prod_in" in str(config.in_dir_name)
        assert "prod_out" in str(config.out_dir_name)


# ---------------------------------------------------------------------------
# CLI Override Logic
# ---------------------------------------------------------------------------
class TestCliOverride:
    """Tests for ConfigFactory.override_from_cli and precedence rules."""

    def test_override_inbox_dir(self):
        """CLI --inbox-dir overrides the configured value."""
        config = get_default_config()
        updated = default_factory.override_from_cli(config, inbox_dir="/new/inbox")
        assert updated.in_dir_name == Path("/new/inbox")

    def test_override_output_dir(self):
        """CLI --output-dir overrides the configured value."""
        config = get_default_config()
        updated = default_factory.override_from_cli(config, output_dir="/new/out")
        assert updated.out_dir_name == Path("/new/out")

    def test_override_watch_dirs(self):
        """CLI --watch-dir replaces the watch folder list (converted to Paths)."""
        config = get_default_config()
        dirs = ["/w1", "/w2"]
        updated = default_factory.override_from_cli(config, watch_dir_list=dirs)
        assert updated.watch_folders == [Path(d) for d in dirs]

    def test_override_force_deep_scan(self):
        """CLI --force-deep-scan flag propagates."""
        config = get_default_config()
        updated = default_factory.override_from_cli(config, force_deep_scan=True)
        assert updated.force_deep_scan is True

    def test_nop_mode_overrides_copy_mode(self):
        """
        Test Description: --no-operation takes precedence over --copy-mode.

        Purpose: Business rule — dry-run must guarantee zero file operations
        regardless of other flags.

        Edge Cases Covered:
        - Both copy_mode and no_operation set to True simultaneously
        """
        config = get_default_config()
        updated = default_factory.override_from_cli(
            config, copy_mode=True, no_operation=True
        )
        assert updated.mode == CopyMode.NOP

    def test_copy_mode_without_nop(self):
        """--copy-mode alone sets COPY."""
        config = get_default_config()
        updated = default_factory.override_from_cli(config, copy_mode=True)
        assert updated.mode == CopyMode.COPY

    def test_drop_duplicates_requires_watch_folders(self):
        """
        Test Description: Enabling duplicate detection without watch folders is invalid.

        Purpose: Duplicates are detected against the library (watch folders).
        Without them, the feature cannot work and must raise ValueError.
        """
        config = get_default_config()
        config.watch_folders = []
        with pytest.raises(ValueError, match="Watch folders"):
            default_factory.override_from_cli(config, drop_duplicates=True)

    def test_use_existing_clusters_requires_watch_folders(self):
        """Enabling existing cluster assignment without watch folders is invalid."""
        config = get_default_config()
        config.watch_folders = []
        with pytest.raises(ValueError, match="Watch folders"):
            default_factory.override_from_cli(config, use_existing_clusters=True)

    def test_drop_duplicates_with_watch_folders_succeeds(self):
        """drop_duplicates + watch_folders is a valid combination."""
        config = get_default_config()
        config.watch_folders = ["/lib"]
        updated = default_factory.override_from_cli(config, drop_duplicates=True)
        assert updated.skip_duplicated_existing_in_libs is True

    def test_use_existing_clusters_with_watch_folders_succeeds(self):
        """use_existing_clusters + watch_folders is a valid combination."""
        config = get_default_config()
        config.watch_folders = ["/lib"]
        updated = default_factory.override_from_cli(config, use_existing_clusters=True)
        assert updated.assign_to_clusters_existing_in_libs is True

    def test_none_values_do_not_override(self):
        """Passing None for an override should leave the original value intact."""
        config = get_default_config()
        original_inbox = config.in_dir_name
        updated = default_factory.override_from_cli(config, inbox_dir=None)
        assert updated.in_dir_name == original_inbox

    def test_extra_kwargs_applied_if_attribute_exists(self):
        """Additional keyword arguments are applied when the Config has the attr."""
        config = get_default_config()
        updated = default_factory.override_from_cli(config, force_deep_scan=True)
        assert updated.force_deep_scan is True


# ---------------------------------------------------------------------------
# Backwards-compatibility functions
# ---------------------------------------------------------------------------
class TestBackwardsCompatibility:
    """Tests for legacy function wrappers."""

    def test_get_default_config_returns_production(self):
        """get_default_config returns a production (MOVE) config."""
        config = get_default_config()
        assert isinstance(config, Config)
        assert config.mode == CopyMode.MOVE

    def test_get_development_config_returns_dev(self):
        """get_development_config returns a dev (COPY) config."""
        config = get_development_config()
        assert isinstance(config, Config)
        assert config.mode == CopyMode.COPY

    def test_get_proper_mode_config_dispatches_correctly(self):
        """get_proper_mode_config delegates to production or dev as requested."""
        prod = get_proper_mode_config(is_development_mode=False)
        dev = get_proper_mode_config(is_development_mode=True)
        assert prod.mode == CopyMode.MOVE
        assert dev.mode == CopyMode.COPY

    def test_override_config_with_cli_params_wrapper(self):
        """
        Test Description: The legacy override wrapper delegates correctly.

        Purpose: Existing scripts use this wrapper; it must continue working.
        """
        config = get_default_config()
        updated = override_config_with_cli_params(
            config=config,
            inbox_dir="/test/in",
            output_dir="/test/out",
            watch_dir_list=["/test/lib"],
            no_operation=True,
            drop_duplicates=True,
            use_existing_clusters=True,
        )
        assert updated.in_dir_name == Path("/test/in")
        assert updated.out_dir_name == Path("/test/out")
        assert updated.mode == CopyMode.NOP
        assert updated.skip_duplicated_existing_in_libs is True
        assert updated.assign_to_clusters_existing_in_libs is True
