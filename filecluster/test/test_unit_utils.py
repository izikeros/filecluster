# Copyright (c) 2017 Krystian Safjan
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import unittest

from filecluster.configuration import get_default_config
from filecluster.file_cluster import ImageGroupper

assertions = unittest.TestCase('__init__')


class TestImageImporter():
    def get_data_from_files(self):
        config = get_default_config()
        img_importer = ImageGroupper(config)


class TestUtils(unittest.TestCase):

    def test_get_exif_date(self):
        assert False

    def create_folder_for_cluster(self):
        assert False

    def test_get_date_info_from_file(self):
        assert False
