"""Shared test fixtures for the filecluster test suite.

Provides reusable fixtures for configuration, test data, media DataFrames,
cluster DataFrames, and temporary directory structures used across all tests.
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from filecluster.configuration import (
    AssignDateToClusterMethod,
    ClusteringMethod,
    Config,
    ConfigFactory,
    CopyMode,
    FileClusterSettings,
    Status,
    default_settings,
)
from filecluster.filecluster_types import ClustersDataFrame, MediaDataFrame


@pytest.fixture
def assets_dir():
    """Path to the test assets directory containing sample media files."""
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
    """Return a configuration for testing with paths pointing to test assets."""
    config = test_config_factory.get_config(is_development_mode=True)
    config.in_dir_name = assets_dir / "set_1"
    config.out_dir_name = assets_dir / "test_outbox"
    config.watch_folders = [assets_dir / "zdjecia", assets_dir / "clusters"]
    return config


@pytest.fixture
def nop_config(test_config):
    """Return a config in NOP mode (no file operations)."""
    test_config.mode = CopyMode.NOP
    return test_config


@pytest.fixture
def sample_media_df():
    """Create a sample MediaDataFrame with known dates for deterministic testing.

    Contains 8 files across 3 distinct time clusters:
      - Cluster A: 2020-01-10 14:00 to 14:30 (3 files, within 1h gap)
      - Cluster B: 2020-03-15 10:00 to 10:45 (3 files, within 1h gap)
      - Cluster C: 2020-06-20 18:00 (1 file, isolated)
      - Cluster D: 2020-06-20 19:30 (1 file, 1.5h gap from C => separate cluster)
    """
    data = {
        "file_name": [
            "img_001.jpg",
            "img_002.jpg",
            "img_003.jpg",
            "img_004.jpg",
            "img_005.jpg",
            "img_006.jpg",
            "img_007.jpg",
            "img_008.jpg",
        ],
        "date": pd.to_datetime(
            [
                "2020-01-10 14:00:00",
                "2020-01-10 14:15:00",
                "2020-01-10 14:30:00",
                "2020-03-15 10:00:00",
                "2020-03-15 10:20:00",
                "2020-03-15 10:45:00",
                "2020-06-20 18:00:00",
                "2020-06-20 19:30:00",
            ]
        ),
        "size": [1000, 1100, 1200, 2000, 2100, 2200, 3000, 3100],
        "hash_value": [f"hash_{i}" for i in range(8)],
        "is_image": [True, True, True, True, True, True, True, False],
        "cluster_id": [None] * 8,
        "status": [Status.UNKNOWN] * 8,
        "duplicated_to": [[] for _ in range(8)],
        "duplicated_cluster": [[] for _ in range(8)],
    }
    return MediaDataFrame(pd.DataFrame(data))


@pytest.fixture
def sample_clusters_df():
    """Create a sample ClustersDataFrame with known clusters.

    Contains two existing clusters matching the sample_media_df time ranges.
    """
    data = {
        "cluster_id": [0, 1],
        "start_date": pd.to_datetime(["2020-01-10 13:50:00", "2020-03-15 09:50:00"]),
        "end_date": pd.to_datetime(["2020-01-10 14:40:00", "2020-03-15 10:50:00"]),
        "median": pd.to_datetime(["2020-01-10 14:15:00", "2020-03-15 10:20:00"]),
        "is_continuous": [True, True],
        "path": ["/lib/2020/[2020_01_10]_event_a", "/lib/2020/[2020_03_15]_event_b"],
        "target_path": [None, None],
        "file_count": [5, 3],
        "new_file_count": [None, None],
    }
    return ClustersDataFrame(pd.DataFrame(data))


@pytest.fixture
def empty_clusters_df():
    """Create an empty ClustersDataFrame with the correct column schema."""
    return ClustersDataFrame(pd.DataFrame(columns=default_settings.cluster_df_columns))


@pytest.fixture
def single_file_media_df():
    """A MediaDataFrame with exactly one file, for boundary testing."""
    data = {
        "file_name": ["solo.jpg"],
        "date": pd.to_datetime(["2021-07-04 12:00:00"]),
        "size": [5000],
        "hash_value": ["hash_solo"],
        "is_image": [True],
        "cluster_id": [None],
        "status": [Status.UNKNOWN],
        "duplicated_to": [[]],
        "duplicated_cluster": [[]],
    }
    return MediaDataFrame(pd.DataFrame(data))


@pytest.fixture
def media_df_with_timestamps():
    """MediaDataFrame with separate m_date, c_date, exif_date columns for timestamp
    disambiguation testing.
    """
    data = {
        "file_name": ["a.jpg", "b.jpg", "c.jpg", "d.jpg"],
        "m_date": [
            "2020-01-10 14:00:00",
            "2020-01-10 15:00:00",
            "2020-01-10 16:00:00",
            "2020-01-10 17:00:00",
        ],
        "c_date": [
            "2020-01-10 14:05:00",
            "2020-01-10 15:05:00",
            "2020-01-10 16:05:00",
            "2020-01-10 17:05:00",
        ],
        "exif_date": [
            "2020-01-10 13:55:00",
            None,
            "2020-01-10 15:55:00",
            None,
        ],
    }
    return MediaDataFrame(pd.DataFrame(data))


@pytest.fixture
def config_with_1h_granularity(assets_dir):
    """Config with 60-minute time granularity for predictable clustering."""
    return Config(
        in_dir_name=assets_dir / "set_1",
        out_dir_name=Path("/tmp/test_out"),
        watch_folders=[],
        image_extensions=default_settings.image_extensions,
        video_extensions=default_settings.video_extensions,
        time_granularity=timedelta(minutes=60),
        assign_date_to_clusters_method=AssignDateToClusterMethod.MEDIAN,
        clustering_method=ClusteringMethod.TIME_GAP,
        mode=CopyMode.NOP,
        force_deep_scan=False,
        assign_to_clusters_existing_in_libs=False,
        skip_duplicated_existing_in_libs=False,
    )
