import os

import pandas as pd
import pytest

from filecluster.configuration import get_development_config
from filecluster.filecluster_types import MediaDataFrame
from filecluster.image_reader import (
    ImageReader,
    Metadata,
    multiple_timestamps_to_one,
    initialize_row_dict,
    prepare_new_row_with_meta,
    get_files_from_folder,
    configure_im_reader,
    get_media_df,
    get_media_stats,
    mark_inbox_duplicates_vs_watch_folders,
    get_watch_folders_files_path,
)
from filecluster.tests.test_dbase import check_lists_equal

TEST_INBOX_DIR = "inbox_test_a"


class TestImageReader:
    def setup_class(self):
        self.config = get_development_config()
        self.config.in_dir_name = TEST_INBOX_DIR

    def setup_method(self):
        self.imreader = ImageReader(self.config)
        self.media_list_of_rows = self.imreader.get_data_from_files_as_list_of_rows()

    def test_get_data_from_files_as_list_of_rows(self):
        files = os.listdir(TEST_INBOX_DIR)
        assert len(self.media_list_of_rows) == len(files)

    def test_check_import_for_duplicates_in_watch_folders(self):
        watch_folders = self.config.watch_folders

        self.imreader.get_media_info_from_inbox_files()

        # get files in library
        watch_names, _ = get_watch_folders_files_path(watch_folders)
        # get files in inbox
        new_names = self.imreader.media_df.file_name.values.tolist()
        potential_dups = [f for f in new_names if f in watch_names]

        inbox_media_df, dups = mark_inbox_duplicates_vs_watch_folders(
            inbox_media_df=self.imreader.media_df,
            watch_folders=watch_folders,
            skip_duplicated_existing_in_libs=True,
        )
        # check if duplicates removed from dataframe
        exp_dups = ["IMG_4029.JPG", "IMG_3957.JPG", "IMG_3955.JPG"]
        assert check_lists_equal(exp_dups, list(set(dups)))

    @pytest.mark.skip(reason="not implemented")
    def test_check_import_for_duplicates_in_existing_clusters(self):
        assert False

    def test_get_media_info_from_inbox_files(self):
        self.imreader.get_media_info_from_inbox_files()
        assert len(self.imreader.media_df) > 0


def test_metadata__intializes():
    _ = Metadata()


def test_multiple_timestamps_to_one():
    df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
    df = multiple_timestamps_to_one(df)
    assert len(df.columns) == 1 and df.columns[0] == "date"


def test_initialize_row_dict():
    meta = Metadata()
    row_dict = initialize_row_dict(meta)
    assert isinstance(row_dict, dict)
    assert len(list(row_dict.keys())) == 12


@pytest.mark.skip(reason="not implemented")
def test_prepare_new_row_with_meta():
    # TODO: KS: 2020-12-09: Create dummy image or mock it
    empty_meta = Metadata()
    row = prepare_new_row_with_meta(
        fn="my_file_name",
        image_extensions=["jpg", "png"],
        in_dir_name="my_dir_name",
        meta=empty_meta,
    )


def test_get_files_from_watch_folder():
    config = get_development_config()
    watch_folder = config.watch_folders[0]
    f_list = get_files_from_folder(watch_folder)
    assert len(list(f_list)) > 0


def test_dir_scanner():
    conf = configure_im_reader(in_dir_name="inbox_test_a")
    media_df = get_media_df(conf)
    time_granularity = int(conf.time_granularity.total_seconds())
    media_stats = get_media_stats(media_df, time_granularity)
    pass
