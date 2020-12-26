"""Module for reading media data on files from given folder."""
import logging
import os
from pathlib import PosixPath
from typing import Any, Dict, Union, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

import filecluster.utlis as ut
from filecluster.configuration import (
    Config,
    get_default_config,
    CopyMode,
    Driver,
    Status,
)
from filecluster.filecluster_types import MediaDataFrame
from filecluster.image_grouper import get_watch_folders_files_path

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Metadata:
    """ """

    def __init__(self) -> None:
        self.file_name: str = ""
        self.path_name: str = ""
        self.m_time: str = ""
        self.c_time: str = ""
        self.exif_date: str = ""
        self.date: str = ""
        self.file_size: int = 0
        self.hash_value: int = 0
        self.image: int = 0
        self.is_image: bool = True
        self.cluster_id: int = 0
        self.status: Status = Status.UNKNOWN
        self.duplicated_to: List[str] = []
        self.duplicated_cluster: List[str] = []


def multiple_timestamps_to_one(image_df: MediaDataFrame) -> MediaDataFrame:
    """Get timestamp from exif (primary) or m_date. Drop not needed date cols.

    Prepare single timestamp out of cdate, mdate and exif.

    Args:
      image_df: MediaDataFrame:

    Returns:
      media dataframe with selected single date out of cdate, mdate and exif

    """
    logger.debug("Cleaning-up timestamps in imported media.")
    # TODO: Ensure that any date is assigned to file
    # use exif date as base
    image_df["date"] = image_df["exif_date"]
    # unless is missing - then use modification date:
    # TODO: Alternatively, take earliest from m_date and c_date
    image_df["date"] = image_df["date"].fillna(image_df["m_date"])

    # infer date format  from strings
    image_df["date"] = pd.to_datetime(image_df["date"], infer_datetime_format=True)

    image_df.drop(["m_date", "c_date", "exif_date"], axis=1, inplace=True)
    return image_df


def initialize_row_dict(meta: Metadata) -> Dict[str, Any]:
    """Generate single row based on values defined in outer method

    Args:
      meta: Metadata:

    Returns:

    """
    # define structure of images dataframe and fill with data
    row = {
        "file_name": meta.file_name,
        "m_date": meta.m_time,
        "c_date": meta.c_time,
        "exif_date": meta.exif_date,
        "date": meta.date,
        "size": meta.file_size,
        "hash_value": meta.hash_value,
        "is_image": meta.is_image,
        "cluster_id": meta.cluster_id,
        "status": meta.status,
        "duplicated_to": meta.duplicated_to,
        "duplicated_cluster": meta.duplicated_cluster,
    }
    return row


def prepare_new_row_with_meta(
        media_file_name: str,
        accepted_media_file_extensions: List[str],
        in_dir_name: Union[str, PosixPath],  # FIXME: Needs harmonization
        meta: Metadata,
) -> Dict[str, Any]:
    """Prepare dictionary with metadata for input media file.

    Args:
      media_file_name:
      accepted_media_file_extensions:
      in_dir_name:
      meta:

    Returns:
        Dictionary with metadata.
    """
    meta.file_name = media_file_name
    # full path + file name
    path_name = os.path.join(in_dir_name, media_file_name)
    meta.path_name = path_name
    # get modification, creation and exif dates
    meta.m_time, meta.c_time, meta.exif_date = ut.get_date_from_file(path_name)
    # determine if media file is image or other type
    is_image = ut.is_image(path_name, accepted_media_file_extensions)
    meta.is_image = is_image
    if not is_image:
        pass
    if media_file_name.lower().endswith("mov"):
        pass
    # file size
    meta.file_size = os.path.getsize(path_name)
    # file hash
    meta.hash_value = ut.hash_file(path_name)
    # placeholder for date representative for file
    meta.date = None  # to be filled in later
    # placeholder for assignment to cluster
    meta.cluster_id = None
    # status
    meta.status = Status.UNKNOWN
    # duplication info
    meta.duplicated_to = []
    meta.duplicated_cluster = []
    # generate new row using data obtained above
    new_row = initialize_row_dict(meta)
    return new_row


class ImageReader(object):
    def __init__(
            self, config: Config, media_df: Optional[MediaDataFrame] = None
    ) -> None:
        """Initialize media database with existing media dataframe or create empty one."""
        # read the config
        self.config = config

        if media_df is None:
            logger.debug(
                f"Initializing empty media dataframe in ImageReader ({config.in_dir_name})"
            )
            self.media_df = MediaDataFrame(pd.DataFrame())
        else:
            msg = "Initializing media dataframe in ImageReader with provided df."
            logger.debug(msg + f"Num records: {len(media_df)}")
            self.media_df = media_df

    def get_data_from_files_as_list_of_rows(self) -> List[dict]:
        """Recursively read exif data from files given in path provided in config.

        Args:

        Returns:
          List of rows: list of rows with all information

        """
        list_of_rows = []
        in_dir_name = self.config.in_dir_name
        ext = self.config.image_extensions + self.config.video_extensions

        logger.debug(f"Reading data from: {in_dir_name}")
        list_dir = os.listdir(in_dir_name)
        n_files = len(list_dir)
        image_extensions = self.config.image_extensions
        meta = Metadata()
        file_list = list(os.listdir(in_dir_name))
        for i_file, file_name in tqdm(
                enumerate(file_list),
                total=n_files,
        ):
            if ut.is_supported_filetype(file_name, ext):
                new_row = prepare_new_row_with_meta(
                    file_name, image_extensions, in_dir_name, meta
                )
                list_of_rows.append(new_row)
        return list_of_rows

    def get_media_info_from_inbox_files(self) -> None:
        """Read data from files, return media info in dataframe."""
        row_list = self.get_data_from_files_as_list_of_rows()
        logger.debug(f"Read info from {len(row_list)} files.")
        # convert list of rows to data frame
        inbox_media_df = MediaDataFrame(pd.DataFrame(row_list))
        inbox_media_df = multiple_timestamps_to_one(inbox_media_df)
        self.media_df = inbox_media_df


def check_if_media_files_from_db_exists():
    """ """
    # TODO: KS: 2020-10-28: implement
    logger.info("Running media scan (not implemented yet)")


def configure_im_reader(in_dir_name: str) -> Config:
    """Customize configuration for purpose of scanning the media library.

    Args:
        in_dir_name: input directory name to be scanned for the media contents.

    Returns:
        Configuration object for the im_reader.
    """
    conf = get_default_config()
    # modify config
    conf.__setattr__("in_dir_name", in_dir_name)
    conf.__setattr__("out_dir_name", "")
    conf.__setattr__("mode", CopyMode.NOP)
    conf.__setattr__("db_driver", Driver.DATAFRAME)
    conf.__setattr__("delete_db", False)
    return conf


def get_media_df(conf: Config) -> Optional[MediaDataFrame]:
    """

    Args:
      conf:

    Returns:

    """
    im_reader = ImageReader(config=conf)
    row_list = im_reader.get_data_from_files_as_list_of_rows()
    if row_list:
        df = MediaDataFrame(pd.DataFrame(row_list))
        return multiple_timestamps_to_one(df)
    else:
        return None


def get_media_stats(df: pd.DataFrame, time_granularity: int) -> dict:
    """Get statistics of media data represented in data frame.

    Args:
      df: pd.DataFrame:
      time_granularity: int:

    Returns:
        Dictionary describing media in dataframe.
    """
    date_min = df.date.min()
    date_max = df.date.max()
    date_median = df.iloc[int(len(df) / 2)].date

    df = df[["file_name", "date"]].copy()
    df["date_int"] = df["date"].apply(lambda x: x.value / 10 ** 9)
    df = df.sort_values("date_int")
    df["delta"] = df.date_int.diff()
    is_normal = not (any(df.delta.values > time_granularity))
    if not isinstance(is_normal, bool):
        pass
    file_count = len(df)
    return {
        "date_min": date_min,
        "date_max": date_max,
        "date_median": date_median,
        "is_normal": is_normal,
        "file_count": file_count,
    }
