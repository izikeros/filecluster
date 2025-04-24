from pathlib import Path

import pytest

from filecluster.configuration import (
    Config,
    ConfigFactory,
    CopyMode,
    FileClusterSettings,
    default_factory,
    default_settings,
    get_default_config,
    get_development_config,
    get_proper_mode_config,
    override_config_with_cli_params,
)


def test_default_settings_instance():
    """Test that the default settings instance is properly created."""
    assert isinstance(default_settings, FileClusterSettings)
    assert default_settings.image_extensions[0] == ".jpg"


def test_config_factory_instance():
    """Test that the default factory instance is properly created."""
    assert isinstance(default_factory, ConfigFactory)
    assert default_factory.settings == default_settings


def test_config_creation():
    """Test that configurations are created correctly."""
    config = default_factory.get_config()
    assert isinstance(config, Config)
    assert config.mode == CopyMode.MOVE  # Default is MOVE for production


def test_development_config_creation():
    """Test development configuration creation."""
    config = default_factory.get_config(is_development_mode=True)
    assert isinstance(config, Config)
    assert config.mode == CopyMode.COPY  # Development mode uses COPY


def test_config_os_specific_paths():
    """Test that OS-specific paths are set correctly."""
    # Create a factory with custom settings for testing
    settings = FileClusterSettings(
        WINDOWS_BASE_PATH="C:\\test\\windows\\", LINUX_BASE_PATH="/test/linux/"
    )
    factory = ConfigFactory(settings)

    # Test Windows paths
    windows_config = factory._create_config(False, os_name="nt")
    assert "C:\\test\\windows" in str(windows_config.in_dir_name)

    # Test Linux paths
    linux_config = factory._create_config(False, os_name="posix")
    assert "/test/linux" in str(linux_config.in_dir_name)


def test_backwards_compatibility_functions():
    """Test that backwards compatibility functions work correctly."""
    # Test get_default_config
    config = get_default_config()
    assert isinstance(config, Config)
    assert config.mode == CopyMode.MOVE

    # Test get_development_config
    dev_config = get_development_config()
    assert isinstance(dev_config, Config)
    assert dev_config.mode == CopyMode.COPY

    # Test get_proper_mode_config
    prod_config = get_proper_mode_config(is_development_mode=False)
    assert prod_config.mode == CopyMode.MOVE
    dev2_config = get_proper_mode_config(is_development_mode=True)
    assert dev2_config.mode == CopyMode.COPY


def test_config_override_from_cli():
    """Test configuration overriding from CLI arguments."""
    config = get_default_config()

    # Override with specific parameters
    updated_config = default_factory.override_from_cli(
        config=config,
        inbox_dir="/test/inbox",
        output_dir="/test/output",
        watch_dir_list=["/test/watch1", "/test/watch2"],
        force_deep_scan=True,
        no_operation=True,
    )

    # Check that parameters were updated correctly
    assert updated_config.in_dir_name == Path("/test/inbox")
    assert updated_config.out_dir_name == Path("/test/output")
    assert updated_config.watch_folders == ["/test/watch1", "/test/watch2"]
    assert updated_config.force_deep_scan == True
    assert updated_config.mode == CopyMode.NOP  # No operation mode


def test_config_copy_vs_nop_mode_precedence():
    """Test that NOP mode takes precedence over COPY mode."""
    config = get_default_config()

    # Set both copy_mode and no_operation
    updated_config = default_factory.override_from_cli(
        config=config, copy_mode=True, no_operation=True
    )

    # NOP should win
    assert updated_config.mode == CopyMode.NOP


def test_config_validation():
    """Test that configuration validation works correctly."""
    config = get_default_config()

    # Try to enable features that require watch folders without providing them
    config.watch_folders = []

    # This should raise ValueError
    with pytest.raises(ValueError):
        default_factory.override_from_cli(
            config=config, skip_duplicated_existing_in_libs=True
        )

    with pytest.raises(ValueError):
        default_factory.override_from_cli(config=config, use_existing_clusters=True)


def test_backwards_compatibility_override_config():
    """Test the backwards compatibility override function."""
    config = get_default_config()

    updated_config = override_config_with_cli_params(
        config=config,
        inbox_dir="/test/inbox",
        output_dir="/test/output",
        watch_dir_list=["/test/library"],
        no_operation=True,
        copy_mode=False,  # This should be ignored when no_operation is True
        drop_duplicates=True,
        use_existing_clusters=True,
    )

    assert updated_config.in_dir_name == Path("/test/inbox")
    assert updated_config.out_dir_name == Path("/test/output")
    assert updated_config.watch_folders == ["/test/library"]
    assert updated_config.mode == CopyMode.NOP
    assert updated_config.skip_duplicated_existing_in_libs == True
    assert updated_config.assign_to_clusters_existing_in_libs == True


def test_field_validator():
    """Test that field validators work correctly."""
    # Create settings with mixed-case extensions
    settings = FileClusterSettings(
        image_extensions=[".JPG", ".JPEG"], video_extensions=[".MP4", ".MOV"]
    )

    # Check that extensions are converted to lowercase
    assert settings.image_extensions == [".jpg", ".jpeg"]
    assert settings.video_extensions == [".mp4", ".mov"]


def test_config_repr():
    """Test the string representation of Config."""
    config = get_default_config()
    repr_str = repr(config)

    # Check that the string contains key attributes
    assert "in_dir_name" in repr_str
    assert "out_dir_name" in repr_str
    assert "mode" in repr_str


def test_config_item_access():
    """Test dictionary-style access to Config."""
    config = get_default_config()

    # Test getter
    assert config["mode"] == config.mode
    assert config["in_dir_name"] == config.in_dir_name

    # Test setter
    config["mode"] = CopyMode.NOP
    assert config.mode == CopyMode.NOP
