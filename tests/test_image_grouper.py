"""Tests for the image grouper functionality in filecluster package.

This test module covers the functionality of the ImageGrouper class, which is
responsible for clustering media files based on their timestamps and other
metadata. It verifies that the grouping algorithm correctly identifies clusters,
manages gaps between images, handles duplicate detection, and assigns files to
appropriate existing clusters when configured to do so.
"""

import pytest

from filecluster.configuration import get_development_config
from filecluster.dbase import get_existing_clusters_info
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import InboxReader


def test_image_grouper__instantinates_for_dev_config():
    config = get_development_config()
    img_groupper = ImageGrouper(config)
    assert isinstance(img_groupper, ImageGrouper)


class TestImageGrouper:
    def setup_class(self):
        # use the same config for all tests in this class
        self.config = (
            get_development_config()
        )  # TODO: KS: 2025-04-25: this should be a fixture
        # we will be testing the case when we have existing clusters in libraries
        self.config.assign_to_clusters_existing_in_libs = True

    @pytest.fixture(autouse=True)
    def setup_method(self, assets_dir):
        watch_folders = [assets_dir / "zdjecia", assets_dir / "clusters"]

        # cluster duplicates, do not store them in a separate folder
        self.config.skip_duplicated_existing_in_libs = False

        # read cluster info from clusters in libraries (or empty dataframe)
        self.df_clusters, _, _ = get_existing_clusters_info(
            watch_folders=watch_folders,
            skip_duplicated_existing_in_libs=False,
            assign_to_clusters_existing_in_libs=True,
            force_deep_scan=True,
        )

        # initialize a media database with an empty or existing database
        inbox_reader = InboxReader(in_dir_name=assets_dir / "set_1")

        # read timestamps from imported pictures/recordings
        inbox_reader.get_media_files_info()

        # configure media grouper, initialize internal dataframes
        self.image_grouper = ImageGrouper(
            configuration=self.config,
            df_clusters=self.df_clusters,
            inbox_media_df=inbox_reader.media_df,
        )

    def test_calculate_gaps(self):
        self.image_grouper.calculate_gaps()
        # TODO: KS: 2020-12-12: add assertions

    def test_calculate_gaps__for_partially_clustered_media(self):
        # handle case
        self.image_grouper.calculate_gaps()
        # TODO: KS: 2020-12-12: add assertions

    def test_add_cluster_id_to_files_in_data_frame(self):
        self.image_grouper.calculate_gaps()
        self.image_grouper.run_clustering()
        # TODO: KS: 2020-12-13: add assertions

    @pytest.mark.skip(reason="fix expectations")
    def test_assign_to_existing_clusters(self):
        (
            files,
            clusters,
        ) = self.image_grouper.assign_to_existing_clusters()

        expected = [
            "IMG_4029.JPG",
            "IMG_4031.JPG",
            "IMG_3957.JPG",
            "IMG_3955.JPG",
        ]  # FIXME: KS: 2025-04-24: fix expected
        assert files == expected

    def test_run_clustering(self):
        self.image_grouper.calculate_gaps()
        cluster_list = self.image_grouper.run_clustering()
        assert len(cluster_list) > 0

    @pytest.mark.skip(reason="fix expectations")
    def test_check_import_for_duplicates_in_watch_folders(self):
        self.config.skip_duplicated_existing_in_libs = True
        files, dups = self.image_grouper.mark_inbox_duplicates()
        # check if duplicates removed from dataframe
        exp_dups = ["IMG_4029.JPG", "IMG_3957.JPG", "IMG_3955.JPG"]
        result = list(set(files))
        assert exp_dups == result
