import os

import pytest

from filecluster.configuration import get_default_config
from filecluster.image_reader import ImageReader

TEST_INBOX_DIR = 'inbox_test_a'


class TestImageReader:
    def setup_class(self):
        self.config = get_default_config()
        self.config.in_dir_name = TEST_INBOX_DIR

    def setup_method(self):
        self.imreader = ImageReader(self.config)
        self.media_list_of_rows = self.imreader.get_data_from_files_as_list_of_rows()

    def test_get_data_from_files_as_list_of_rows(self):
        files = os.listdir(TEST_INBOX_DIR)
        assert len(self.media_list_of_rows) == len(files)

    @pytest.mark.skip(reason='not implemented')
    def test_check_import_for_duplicates_in_watch_folders(self):
        # check if duplicates removed from dataframe
        assert False
    @pytest.mark.skip(reason='not implemented')
    def test_check_import_for_duplicates_in_existing_clusters(self):
        assert False
