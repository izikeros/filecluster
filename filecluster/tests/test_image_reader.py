import os

import pandas as pd
import pytest

from filecluster.configuration import get_default_config, get_development_config
from filecluster.image_reader import ImageReader, Metadata, multiple_timestamps_to_one, \
    initialize_row_dict, prepare_new_row_with_meta, get_files_from_watch_folder
from filecluster.types import MediaDataframe

TEST_INBOX_DIR = 'inbox_test_a'


class TestImageReader:
    def setup_class(self):
        self.config = get_default_config()
        self.config.in_dir_name = TEST_INBOX_DIR

    def setup_method(self):
        self.imreader = ImageReader(self.config)
        self.media_list_of_rows = self.imreader.get_data_from_files_as_list_of_rows(
        )

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

    def test_get_media_info_from_inbox_files(self):
        self.imreader.get_media_info_from_inbox_files()
        assert len(self.imreader.media_df) > 0


def test_metadata__intializes():
    _ = Metadata()


def test_multiple_timestamps_to_one():
    df = MediaDataframe(
        pd.DataFrame({
            'exif_date': [],
            'c_date': [],
            'm_date': []
        }))
    df = multiple_timestamps_to_one(df)
    assert len(df.columns) == 1 and df.columns[0] == 'date'


def test_initialize_row_dict():
    meta = Metadata()
    row_dict = initialize_row_dict(meta)
    assert isinstance(row_dict, dict)
    assert len(list(row_dict.keys())) == 12


@pytest.mark.skip(reason='not implemented')
def test_prepare_new_row_with_meta():
    # TODO: KS: 2020-12-09: Create dummy image or mock it
    empty_meta = Metadata()
    row = prepare_new_row_with_meta(fn='my_file_name',
                                    image_extensions=['jpg', 'png'],
                                    in_dir_name='my_dir_name',
                                    meta=empty_meta)


def test_get_files_from_watch_folder():
    config = get_development_config()
    watch_folder = config.watch_folders[0]
    f_list = get_files_from_watch_folder(watch_folder)
    assert len(list(f_list)) > 0
