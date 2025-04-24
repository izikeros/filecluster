import os

import pandas as pd
import pytest
from numpy import dtype

from filecluster.configuration import get_development_config
from filecluster.filecluster_types import MediaDataFrame
from filecluster.image_grouper import get_files_from_folder
from filecluster.image_reader import (
    ImageReader,
    Metadata,
    configure_im_reader,
    get_media_df,
    get_media_stats,
    initialize_row_dict,
    multiple_timestamps_to_one,
    prepare_new_row_with_meta,
)


class TestImageReader:
    @pytest.fixture(autouse=True)
    def setup_class(self, assets_dir):
        self.in_dir_name = assets_dir / "set_1"

    @pytest.fixture(autouse=True)
    def setup_method(self):
        self.imreader = ImageReader(in_dir_name=self.in_dir_name)
        self.media_list_of_rows = self.imreader.get_data_from_files_as_list_of_rows()

    def test_get_data_from_files_as_list_of_rows(self, assets_dir):
        files = os.listdir(str(assets_dir / "set_1"))
        assert len(self.media_list_of_rows) == len(files)

    @pytest.mark.skip(reason="not implemented")
    def test_check_import_for_duplicates_in_existing_clusters(self):
        raise AssertionError

    def test_get_media_info_from_inbox_files(self):
        self.imreader.get_media_info_from_inbox_files()
        assert len(self.imreader.media_df) > 0


def test_metadata__intializes():
    _ = Metadata()


def test_multiple_timestamps_to_one__columns_reduced():
    df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
    df = multiple_timestamps_to_one(df)
    assert len(df.columns) == 1 and df.columns[0] == "date"


def test_multiple_timestamps_to_one__date_is_datetime64():
    df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
    df = multiple_timestamps_to_one(df)
    dtypes = df.dtypes
    assert dtypes["date"] == dtype("<M8[ns]")


def test_multiple_timestamps_to_one__dates_are_datetime64():
    df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
    df = multiple_timestamps_to_one(df, drop_columns=False)
    dtypes = df.dtypes
    assert dtypes["date"] == dtype("<M8[ns]")
    assert dtypes["exif_date"] == dtype("<M8[ns]")
    assert dtypes["c_date"] == dtype("<M8[ns]")
    assert dtypes["m_date"] == dtype("<M8[ns]")


def test_multiple_timestamps_to_one(assets_dir):
    in_dir_name = assets_dir / "set_1"
    image_reader = ImageReader(in_dir_name)
    row_list = image_reader.get_data_from_files_as_list_of_rows()
    inbox_media_df_in = MediaDataFrame(pd.DataFrame(row_list))
    sel_cols = ["file_name", "m_date", "c_date", "exif_date", "date"]

    inbox_media_df_out = multiple_timestamps_to_one(
        inbox_media_df_in.copy(), drop_columns=False
    )
    # _ = ["file_name", "date"]

    # keep only most important results for analysis in testing
    _ = inbox_media_df_out[sel_cols]


def test_initialize_row_dict():
    meta = Metadata()
    row_dict = initialize_row_dict(meta)
    assert isinstance(row_dict, dict)
    assert len(list(row_dict.keys())) == 12


@pytest.mark.skip(reason="not implemented")
def test_prepare_new_row_with_meta():
    # TODO: KS: 2020-12-09: Create dummy image or mock it
    empty_meta = Metadata()
    _ = prepare_new_row_with_meta(
        media_file_name="my_file_name",
        accepted_media_file_extensions=["jpg", "png"],
        in_dir_name="my_dir_name",
        meta=empty_meta,
    )


def test_get_files_from_watch_folder(assets_dir):
    watch_folder = assets_dir / "zdjecia"
    f_list = get_files_from_folder(watch_folder)
    assert len(list(f_list)) > 0


def test_dir_scanner(assets_dir):
    conf = configure_im_reader(in_dir_name=str(assets_dir / "set_1"))
    media_df = get_media_df(conf.in_dir_name)
    time_granularity = int(conf.time_granularity.total_seconds())
    _ = get_media_stats(media_df, time_granularity)
