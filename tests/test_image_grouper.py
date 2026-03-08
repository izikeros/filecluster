"""Tests for the image_grouper module.

Covers ImageGrouper clustering logic, gap calculation, cluster creation,
target folder naming, duplicate handling, assignment to existing clusters,
file-move logic, and supporting helpers (TargetPathCreator, filter_by_substring_list,
check_df_has_all_expected_columns).
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from filecluster.configuration import (
    AssignDateToClusterMethod,
    CopyMode,
    Status,
    default_settings,
    get_development_config,
)
from filecluster.dbase import get_existing_clusters_info
from filecluster.exceptions import DateStringNoneError, MissingDfClusterColumnError
from filecluster.image_grouper import (
    ImageGrouper,
    TargetPathCreator,
    check_df_has_all_expected_columns,
    filter_by_substring_list,
    get_files_from_folder,
    get_watch_folders_files_path,
)
from filecluster.image_reader import InboxReader


# ---------------------------------------------------------------------------
# TargetPathCreator
# ---------------------------------------------------------------------------
class TestTargetPathCreator:
    """Tests for TargetPathCreator path generation.

    Business rules:
    - New clusters go under 'new/<date_string>'
    - Existing clusters go under 'existing/<folder_name>'
    - Duplicates go under 'duplicated/<folder_name>'
    """

    def test_new_cluster_path(self):
        """New cluster path is 'new/<date_string>'."""
        creator = TargetPathCreator(out_dir_name="/out")
        result = creator.for_new_cluster("[2020_01_01]_event")
        assert result == str(Path("new") / "[2020_01_01]_event")

    def test_existing_cluster_path(self):
        """Existing cluster path extracts the folder name and prefixes with 'existing'."""
        creator = TargetPathCreator(out_dir_name="/out")
        result = creator.for_existing_cluster("/lib/2020/[2020_01_01]_event")
        assert result == str(Path("existing") / "[2020_01_01]_event")

    def test_duplicates_path(self):
        """Duplicate path extracts folder name and prefixes with 'duplicated'."""
        creator = TargetPathCreator(out_dir_name="/out")
        result = creator.for_duplicates("/lib/2020/[2020_01_01]_event")
        assert result == str(Path("duplicated") / "[2020_01_01]_event")


# ---------------------------------------------------------------------------
# filter_by_substring_list
# ---------------------------------------------------------------------------
class TestFilterBySubstringList:
    """Tests for the substring filtering helper."""

    def test_filters_matching_strings(self):
        """Only strings containing at least one substring are returned."""
        strings = ["IMG_001.jpg", "IMG_002.jpg", "VIDEO_001.mp4"]
        result = filter_by_substring_list(strings, ["IMG_001"])
        assert result == ["IMG_001.jpg"]

    def test_returns_empty_for_no_matches(self):
        """No matches => empty list."""
        result = filter_by_substring_list(["a", "b"], ["x"])
        assert result == []

    def test_multiple_substrings_or_logic(self):
        """Any substring match includes the string."""
        strings = ["alpha.jpg", "beta.jpg", "gamma.jpg"]
        result = filter_by_substring_list(strings, ["alpha", "gamma"])
        assert set(result) == {"alpha.jpg", "gamma.jpg"}


# ---------------------------------------------------------------------------
# check_df_has_all_expected_columns
# ---------------------------------------------------------------------------
class TestCheckDfColumns:
    """Tests for DataFrame column validation."""

    def test_passes_when_all_columns_present(self):
        """No exception when DF has all expected columns."""
        df = pd.DataFrame(columns=["a", "b", "c"])
        check_df_has_all_expected_columns(df, ["a", "b"])

    def test_raises_when_column_missing(self):
        """MissingDfClusterColumnError when a required column is absent."""
        df = pd.DataFrame(columns=["a"])
        with pytest.raises(MissingDfClusterColumnError):
            check_df_has_all_expected_columns(df, ["a", "b"])


# ---------------------------------------------------------------------------
# get_files_from_folder / get_watch_folders_files_path
# ---------------------------------------------------------------------------
class TestFileDiscovery:
    """Tests for file listing helpers."""

    def test_get_files_from_folder_finds_files(self, assets_dir):
        """Recursively lists files with extensions in the watch folder."""
        files = list(get_files_from_folder(str(assets_dir / "zdjecia")))
        assert len(files) > 0
        assert all(isinstance(f, Path) for f in files)

    def test_get_watch_folders_files_path(self, assets_dir):
        """Returns both file names and full paths."""
        names, paths = get_watch_folders_files_path([str(assets_dir / "zdjecia")])
        assert len(names) > 0
        assert len(names) == len(paths)
        assert all(isinstance(n, str) for n in names)


# ---------------------------------------------------------------------------
# ImageGrouper - Gap Calculation
# ---------------------------------------------------------------------------
class TestImageGrouperGapCalculation:
    """Tests for ImageGrouper.calculate_gaps.

    Business rule: time deltas between consecutive photos determine cluster
    boundaries. Only unclustered (status=UNKNOWN) items participate.
    """

    def test_calculate_gaps_adds_delta_column(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: After calculate_gaps(), a 'date_delta' column exists.

        Purpose: The delta column is required by run_clustering to identify
        cluster boundaries.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        assert "date_delta" in grouper.inbox_media_df.columns

    def test_gaps_are_sorted_by_date(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """After gap calculation, the DataFrame is sorted ascending by date."""
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        dates = grouper.inbox_media_df["date"].values
        assert all(dates[i] <= dates[i + 1] for i in range(len(dates) - 1))

    def test_first_item_has_nat_delta(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """First item in sorted order has NaT delta (no previous item)."""
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        first_delta = grouper.inbox_media_df.iloc[0]["date_delta"]
        assert pd.isna(first_delta)


# ---------------------------------------------------------------------------
# ImageGrouper - run_clustering
# ---------------------------------------------------------------------------
class TestImageGrouperClustering:
    """Tests for the core run_clustering algorithm.

    Business rules:
    - Each group of consecutive files within the time granularity forms one cluster
    - Files separated by more than the time granularity start a new cluster
    - Every file gets assigned a cluster_id and status=NEW_CLUSTER
    """

    def test_correct_number_of_clusters_created(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: With 1h granularity, the 8-file sample yields exactly
        4 clusters (A: 3 files within 30min, B: 3 files within 45min,
        C: 1 file, D: 1 file at 1.5h from C).

        Purpose: This is the most fundamental business requirement — the number
        of clusters must match the actual number of events.

        Test Strategy:
        - Setup: sample_media_df with 4 time groups
        - Execution: calculate_gaps + run_clustering
        - Verification: new_cluster_df has exactly 4 rows
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        new_clusters = grouper.run_clustering()
        assert len(new_clusters) == 4

    def test_all_files_get_cluster_assignment(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """Every file must be assigned to a cluster after clustering."""
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()
        assert grouper.inbox_media_df["cluster_id"].notna().all()

    def test_all_files_marked_as_new_cluster(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """All newly clustered files have status=NEW_CLUSTER."""
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()
        statuses = grouper.inbox_media_df["status"].values
        assert all(s == Status.NEW_CLUSTER for s in statuses)

    def test_cluster_date_boundaries_are_correct(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: Each cluster's start_date and end_date match the
        actual min/max dates of its member files.

        Purpose: Wrong boundaries would cause incorrect assignment of new files
        to existing clusters.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        new_clusters = grouper.run_clustering()

        for _, cluster in new_clusters.iterrows():
            cid = cluster["cluster_id"]
            members = grouper.inbox_media_df[
                grouper.inbox_media_df["cluster_id"] == cid
            ]
            assert cluster["start_date"] == members["date"].min()
            assert cluster["end_date"] == members["date"].max()

    def test_files_within_same_event_share_cluster_id(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: The three files in 'Cluster A' (14:00, 14:15, 14:30)
        all get the same cluster_id.

        Purpose: If files from the same event get different IDs, they'll be
        split into separate folders — a critical user-facing bug.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()

        df = grouper.inbox_media_df
        cluster_a = df[
            df["file_name"].isin(["img_001.jpg", "img_002.jpg", "img_003.jpg"])
        ]
        assert cluster_a["cluster_id"].nunique() == 1

    def test_single_file_forms_one_cluster(
        self, config_with_1h_granularity, single_file_media_df, empty_clusters_df
    ):
        """
        Test Description: A single file should form exactly one cluster.

        Purpose: Boundary case — clustering must not crash or produce zero clusters.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=single_file_media_df,
        )
        grouper.calculate_gaps()
        new_clusters = grouper.run_clustering()
        assert len(new_clusters) == 1

    def test_smaller_granularity_produces_more_clusters(
        self, sample_media_df, empty_clusters_df, assets_dir
    ):
        """
        Test Description: With a 10-minute granularity the same data yields more
        clusters than with 60 minutes.

        Purpose: Verifies the granularity parameter actually controls cluster splitting.
        """
        from filecluster.configuration import (
            AssignDateToClusterMethod,
            ClusteringMethod,
            Config,
        )

        config_10m = Config(
            in_dir_name=assets_dir / "set_1",
            out_dir_name=Path("/tmp/test_out"),
            watch_folders=[],
            image_extensions=default_settings.image_extensions,
            video_extensions=default_settings.video_extensions,
            time_granularity=timedelta(minutes=10),
            assign_date_to_clusters_method=AssignDateToClusterMethod.MEDIAN,
            clustering_method=ClusteringMethod.TIME_GAP,
            mode=CopyMode.NOP,
            force_deep_scan=False,
            assign_to_clusters_existing_in_libs=False,
            skip_duplicated_existing_in_libs=False,
        )
        grouper = ImageGrouper(
            configuration=config_10m,
            df_clusters=empty_clusters_df.copy(),
            inbox_media_df=sample_media_df.copy(),
        )
        grouper.calculate_gaps()
        small_clusters = grouper.run_clustering()

        # With 10-min granularity, the 15-min and 20-min gaps within clusters A
        # and B will split them further, so we expect MORE than 4 clusters
        assert len(small_clusters) > 4


# ---------------------------------------------------------------------------
# ImageGrouper - Target Folder Naming
# ---------------------------------------------------------------------------
class TestTargetFolderNaming:
    """Tests for assign_target_folder_name_and_file_count_to_new_clusters.

    Business rules:
    - Folder name includes date, time, image count (IC_), and video count (VC_)
    - '_rich' suffix when >10 images or >10 videos
    """

    def test_folder_names_contain_date_and_counts(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: Each generated folder name includes a [YYYY_MM_DD]
        date prefix and IC_ / VC_ counts.

        Purpose: The folder name is the user-facing output — it must be
        informative enough to identify the event.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()
        folder_names = grouper.assign_target_folder_name_and_file_count_to_new_clusters(
            method=AssignDateToClusterMethod.MEDIAN
        )
        assert len(folder_names) > 0
        for name in folder_names:
            assert "IC_" in name
            assert "VC_" in name
            assert name.startswith("new/")

    def test_new_file_count_populated(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """new_file_count in clusters_df should be non-zero for new clusters."""
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()
        grouper.assign_target_folder_name_and_file_count_to_new_clusters()

        new_ids = grouper.get_new_cluster_ids()
        for cid in new_ids:
            row = grouper.df_clusters[grouper.df_clusters["cluster_id"] == cid]
            assert row["new_file_count"].values[0] > 0


# ---------------------------------------------------------------------------
# Integration: full pipeline with real files
# ---------------------------------------------------------------------------
class TestImageGrouperIntegration:
    """Integration tests using real image assets.

    No mocking — uses actual files from tests/assets/set_1 and real
    library scanning from tests/assets/zdjecia and tests/assets/clusters.
    """

    @pytest.fixture(autouse=True)
    def setup_grouper(self, assets_dir):
        """Set up a fully configured ImageGrouper with real data."""
        self.config = get_development_config()
        self.config.assign_to_clusters_existing_in_libs = True
        self.config.skip_duplicated_existing_in_libs = False

        watch_folders = [assets_dir / "zdjecia", assets_dir / "clusters"]
        self.df_clusters, _, _ = get_existing_clusters_info(
            watch_folders=watch_folders,
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=True,
            force_deep_scan=True,
        )

        reader = InboxReader(in_dir_name=assets_dir / "set_1")
        reader.get_media_files_info()

        self.grouper = ImageGrouper(
            configuration=self.config,
            df_clusters=self.df_clusters,
            inbox_media_df=reader.media_df,
        )

    def test_calculate_gaps_produces_deltas(self):
        """Real files produce non-null deltas (except the first)."""
        self.grouper.calculate_gaps()
        df = self.grouper.inbox_media_df
        assert "date_delta" in df.columns
        # All but first should have a delta
        non_first = df.iloc[1:]
        assert non_first["date_delta"].notna().all()

    def test_run_clustering_creates_at_least_one_cluster(self):
        """Real data always produces at least one cluster."""
        self.grouper.calculate_gaps()
        clusters = self.grouper.run_clustering()
        assert len(clusters) >= 1

    def test_run_clustering_assigns_all_files(self):
        """Every file gets a cluster assignment."""
        self.grouper.calculate_gaps()
        self.grouper.run_clustering()
        assert self.grouper.inbox_media_df["cluster_id"].notna().all()

    def test_get_new_cluster_ids_returns_unique_ids(self):
        """get_new_cluster_ids returns IDs for newly created clusters only."""
        self.grouper.calculate_gaps()
        self.grouper.run_clustering()
        ids = self.grouper.get_new_cluster_ids()
        assert len(ids) > 0
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# ImageGrouper - move_files_to_cluster_folder
# ---------------------------------------------------------------------------
class TestMoveFiles:
    """Tests for the file move/copy operation."""

    def test_none_target_path_raises_error(
        self, config_with_1h_granularity, sample_media_df, empty_clusters_df
    ):
        """
        Test Description: If target_path is None for any file, DateStringNoneError
        is raised.

        Purpose: Prevents attempting to create a directory with a None name.
        """
        grouper = ImageGrouper(
            configuration=config_with_1h_granularity,
            df_clusters=empty_clusters_df,
            inbox_media_df=sample_media_df,
        )
        grouper.calculate_gaps()
        grouper.run_clustering()
        # target_path is not set yet => should raise
        grouper.inbox_media_df["target_path"] = None
        with pytest.raises(DateStringNoneError):
            grouper.move_files_to_cluster_folder()
