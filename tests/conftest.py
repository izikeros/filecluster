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
        INBOX_DIR="test_inbox",
        OUTBOX_DIR="test_outbox",
        WINDOWS_BASE_PATH="./test_data",
        LINUX_BASE_PATH="./test_data",
        WINDOWS_LIBRARY_PATHS=["./test_library"],
        LINUX_LIBRARY_PATHS=["./test_library"],
    )


@pytest.fixture
def test_config_factory(test_settings):
    """Return a config factory using test settings."""
    return ConfigFactory(test_settings)


@pytest.fixture
def test_config(test_config_factory):
    """Return a configuration for testing."""
    config = test_config_factory.get_config(is_dev_mode=True)
    # Override paths to use test assets
    config.in_dir_name = str(assets_dir() / "test_inbox")
    config.out_dir_name = str(assets_dir() / "test_outbox")
    config.watch_folders = [str(assets_dir() / "test_library")]
    return config
