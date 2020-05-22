from unittest import TestCase

import pytest

from filecluster.configuration import get_default_config
from filecluster.image_groupper import ImageGroupper


class TestImageGroupper(TestCase):
    def test_image_groupper_instantinates(self):
        config = get_default_config()
        img_groupper = ImageGroupper(config)
        assert isinstance(img_groupper, ImageGroupper)

    @pytest.mark.skip()
    def test_calculate_gaps(self):
        self.fail()

    @pytest.mark.skip()
    def test_assign_images_to_existing_clusters(self):
        self.fail()

    @pytest.mark.skip()
    def test_add_tmp_cluster_id_to_files_in_data_frame(self):
        self.fail()

    @pytest.mark.skip()
    def test_save_cluster_data_to_data_frame(self):
        self.fail()

    @pytest.mark.skip()
    def test_get_num_of_clusters_in_df(self):
        self.fail()
