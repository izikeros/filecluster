from pathlib import Path

import pytest

from filecluster.configuration import ConfigFactory, FileClusterSettings


@pytest.fixture
def assets_dir():
    return Path(__file__).parent / "assets"


@pytest.fixture
def test_settings():
    """Return settings configured for testing."""
    return FileClusterSettings(
        inbox_dir=Path("test_inbox"),
        outbox_dir="test_outbox",
        windows_base_path="./test_data",
        linux_base_path="./test_data",
        windows_library_paths=["./test_library"],
        linux_library_paths=["./test_library"],
    )


@pytest.fixture
def test_config_factory(test_settings):
    """Return a config factory using test settings."""
    return ConfigFactory(test_settings)


@pytest.fixture
def test_config(test_config_factory, assets_dir):
    """Return a configuration for testing."""
    config = test_config_factory.get_config(is_development_mode=True)
    # Override paths to use test assets
    config.in_dir_name = assets_dir / "test_inbox"
    config.out_dir_name = assets_dir / "test_outbox"
    config.watch_folders = [assets_dir / "test_library"]
    return config
