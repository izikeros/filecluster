"""Tests for the dbase module.

Covers get_new_cluster_id_from_dataframe and get_existing_clusters_info,
including edge cases for empty DataFrames, gaps in IDs, and library scanning.
"""

import pandas as pd

from filecluster.configuration import default_settings
from filecluster.dbase import (
    get_existing_clusters_info,
    get_new_cluster_id_from_dataframe,
)


# ---------------------------------------------------------------------------
# get_new_cluster_id_from_dataframe
# ---------------------------------------------------------------------------
class TestGetNewClusterIdFromDataframe:
    """Tests for the cluster ID generator.

    Business rule: the returned ID must be greater than all existing IDs so
    that new clusters never collide with existing ones.
    """

    def test_empty_dataframe_returns_1(self):
        """
        Test Description: With no existing clusters, the first cluster ID is 1.

        Purpose: Starting from 1 (not 0) is a project convention that downstream
        code relies upon.
        """
        df = pd.DataFrame(columns=default_settings.cluster_df_columns)
        assert get_new_cluster_id_from_dataframe(df) == 1

    def test_sequential_ids(self):
        """Returns max + 1 for sequential IDs."""
        df = pd.DataFrame({"cluster_id": [1, 2, 3]})
        assert get_new_cluster_id_from_dataframe(df) == 4

    def test_with_gaps_returns_max_plus_one(self):
        """
        Test Description: Gaps in cluster IDs are ignored — always max + 1.

        Purpose: The function must not try to fill gaps (which would conflict
        with clusters that were deleted).
        """
        df = pd.DataFrame({"cluster_id": [1, 5, 10]})
        assert get_new_cluster_id_from_dataframe(df) == 11

    def test_with_nan_values_ignored(self):
        """NaN cluster IDs are ignored in the max calculation."""
        df = pd.DataFrame({"cluster_id": [1, 2, None, 5]})
        assert get_new_cluster_id_from_dataframe(df) == 6

    def test_single_cluster(self):
        """Single existing cluster returns ID + 1."""
        df = pd.DataFrame({"cluster_id": [42]})
        assert get_new_cluster_id_from_dataframe(df) == 43


# ---------------------------------------------------------------------------
# get_existing_clusters_info
# ---------------------------------------------------------------------------
class TestGetExistingClustersInfo:
    """Tests for scanning watch folders to build cluster DataFrames.

    Business rules:
    - When watch folder features are disabled, returns empty DF with correct schema
    - When enabled with valid watch folders, returns populated DF with unique IDs
    - Column schema must be consistent regardless of whether data is present
    """

    def test_empty_watch_folders_returns_empty_df(self):
        """
        Test Description: With no watch folders, an empty DF with the correct
        columns is returned.

        Purpose: Downstream code (ImageGrouper) expects the DF to always have
        the cluster column schema, even when empty.
        """
        df, empty_dirs, non_compliant = get_existing_clusters_info(
            watch_folders=[],
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=False,
            force_deep_scan=False,
        )
        assert len(df) == 0
        expected_cols = set(default_settings.cluster_df_columns)
        assert expected_cols.issubset(set(df.columns))

    def test_features_disabled_returns_empty_df(self):
        """Even with paths, if both features are disabled => empty DF."""
        df, _, _ = get_existing_clusters_info(
            watch_folders=["/some/path"],
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=False,
            force_deep_scan=False,
        )
        assert len(df) == 0

    def test_schema_consistency_between_empty_and_populated(self, assets_dir):
        """
        Test Description: Column sets of an empty DF and a populated DF are identical.

        Purpose: Code that merges or concatenates these DFs would break if
        schemas diverge.
        """
        df_blank, _, _ = get_existing_clusters_info(
            watch_folders=[],
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=False,
            force_deep_scan=False,
        )
        df_populated, _, _ = get_existing_clusters_info(
            watch_folders=[assets_dir / "zdjecia", assets_dir / "clusters"],
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=True,
            force_deep_scan=True,
        )
        assert sorted(df_blank.columns) == sorted(df_populated.columns)

    def test_cluster_ids_are_unique(self, assets_dir):
        """
        Test Description: All cluster IDs in the returned DF are unique.

        Purpose: Duplicate IDs would cause files to be mis-assigned to clusters.
        """
        df, _, _ = get_existing_clusters_info(
            watch_folders=[assets_dir / "zdjecia", assets_dir / "clusters"],
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=True,
            force_deep_scan=True,
        )
        ids = df.cluster_id.dropna().values
        assert len(ids) == len(set(ids))

    def test_returns_non_empty_for_real_library(self, assets_dir):
        """Real library folders with media produce a non-empty DF."""
        df, _, _ = get_existing_clusters_info(
            watch_folders=[assets_dir / "zdjecia"],
            skip_duplicated_existing_in_libs=True,
            assign_to_clusters_existing_in_libs=False,
            force_deep_scan=True,
        )
        assert len(df) > 0
