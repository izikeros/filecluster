import pytest

from filecluster.configuration import get_development_config
from filecluster.dbase import get_existing_clusters_info
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import ImageReader


def test_image_grouper__instantinates_for_dev_config():
    config = get_development_config()
    img_groupper = ImageGrouper(config)
    assert isinstance(img_groupper, ImageGrouper)


class TestImageGrouper:
    def setup_class(self):
        self.config = get_development_config()
        self.config.assign_to_clusters_existing_in_libs = True

    @pytest.fixture(autouse=True)
    def setup_method(self, assets_dir):
        self.config.skip_duplicated_existing_in_libs = False
        # read cluster info from clusters in libraries (or empty dataframe)
        self.df_clusters, _, _ = get_existing_clusters_info(
            watch_folders=[assets_dir / "zdjecia", assets_dir / "clusters"],
            skip_duplicated_existing_in_libs=self.config.skip_duplicated_existing_in_libs,
            assign_to_clusters_existing_in_libs=self.config.assign_to_clusters_existing_in_libs,
            force_deep_scan=self.config.force_deep_scan,
        )

        # initialize a media database with an empty or existing database
        image_reader = ImageReader(in_dir_name=assets_dir / "set_1")

        # read timestamps from imported pictures/recordings
        image_reader.get_media_info_from_inbox_files()

        # configure media grouper, initialize internal dataframes
        self.image_grouper = ImageGrouper(
            configuration=self.config,
            df_clusters=self.df_clusters,
            inbox_media_df=image_reader.media_df,
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
