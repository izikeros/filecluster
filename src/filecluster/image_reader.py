"""Module for reading media data on files from given folder."""

import os
import struct
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

import filecluster.utlis as ut
from filecluster.configuration import default_settings
from filecluster import logger
from filecluster.configuration import Config, CopyMode, Status, get_default_config
from filecluster.filecluster_types import MediaDataFrame

# for extracting timestamp from MOV files
ATOM_HEADER_SIZE = 8
# difference between Unix epoch and QuickTime epoch, in seconds
EPOCH_ADJUSTER = 2082844800


class Metadata:
    """Class defining media metadata."""

    def __init__(self) -> None:
        self.file_name: str = ""
        self.path_name: str = ""
        self.m_time: str = ""
        self.c_time: str = ""
        self.exif_date: str = ""
        self.date: str | None = ""
        self.file_size: int = 0
        self.hash_value: int = 0
        self.image: int = 0
        self.is_image: bool = True
        self.cluster_id: int | None = 0
        self.status: Status = Status.UNKNOWN
        self.duplicated_to: list[str] = []
        self.duplicated_cluster: list[str] = []


def multiple_timestamps_to_one(
    image_df: MediaDataFrame, rule="m_date", drop_columns: bool = True
) -> MediaDataFrame:
    """Get timestamp from exif (primary) or m_date. Drop not needed date cols.

    Prepare single timestamp out of cdate, mdate and exif (disambiguation)

    Args:
      rule:                         rule used for disambiguation
      drop_columns:                 whether to drop columns with timestamps or not
      image_df: MediaDataFrame:     input dataframe with media data

    Returns:
      media dataframe with selected single date out of cdate, mdate and exif

    """
    # logger.trace("Cleaning-up timestamps in imported media.")

    # normalize date format
    image_df["m_date"] = pd.to_datetime(image_df["m_date"])
    image_df["c_date"] = pd.to_datetime(image_df["c_date"])
    image_df["exif_date"] = pd.to_datetime(image_df["exif_date"])

    # TODO: Ensure that any date is assigned to file
    # use exif date as base

    # unless is missing - then use modification date:
    if rule == "m_date":
        # use exif date if available
        image_df["date"] = image_df["exif_date"]
        # fill missing (no exif date) with m_date
        image_df["date"] = image_df["date"].fillna(image_df["m_date"])
    elif rule == "earliest":
        image_df["date"] = image_df[["m_date", "c_date", "exif_date"]].min(axis=1)

    if drop_columns:
        image_df.drop(["m_date", "c_date", "exif_date"], axis=1, inplace=True)
    return image_df


def initialize_row_dict(meta: Metadata) -> dict[str, Any]:
    """Generate single row based on values defined in outer method.

    Args:
      meta: Metadata:

    Returns:
        Dictionary filled-in data from the input Metadata object.
    """
    return {
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


def prepare_new_row_with_meta(
    media_file_name: str,
    accepted_media_file_extensions: list[str],
    in_dir_name: Path,
    meta: Metadata,
) -> dict[str, Any]:
    """Prepare dictionary with metadata for input media file.

    Args:
      media_file_name:                  name of the media file
      accepted_media_file_extensions:   list of accepted media file extensions
      in_dir_name:                      input directory name
      meta:                             Metadata object

    Returns:
        Dictionary with metadata.


    TODO: Better reading metadata from MOV video files - see:
    # https://stackoverflow.com/questions/21355316/getting-metadata-for-mov-video
    # data: [2021_10_11]_Hania_...
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
    if media_file_name.lower().endswith("mov"):
        try:
            meta.c_time, meta.m_time = get_mov_timestamps(path_name)
        except Exception:
            logger.error(f"Cannot get dates from MOV file: {path_name}")

    # file size
    meta.file_size = os.path.getsize(path_name)
    # file hash
    meta.hash_value = ut.hash_file(path_name)
    # placeholder for date representative for file
    meta.date = None  # to be filled in later in: multiple_timestamps_to_one()
    # placeholder for assignment to cluster
    meta.cluster_id = None
    # status
    meta.status = Status.UNKNOWN
    # duplication info
    meta.duplicated_to = []
    meta.duplicated_cluster = []
    return initialize_row_dict(meta)


def get_mov_timestamps(filename):
    """Get the creation and modification date-time from .mov metadata.

    Returns None if a value is not available.

    from: https://stackoverflow.com/a/54683292
    """
    creation_time = modification_time = None

    # search for moov item
    with open(filename, "rb") as f:
        while True:
            atom_header = f.read(ATOM_HEADER_SIZE)
            # ~ print('atom header:', atom_header)  # debug purposes
            if atom_header[4:8] == b"moov":
                break  # found
            else:
                atom_size = struct.unpack(">I", atom_header[0:4])[0]
                f.seek(atom_size - 8, 1)

        # found 'moov', look for 'mvhd' and timestamps
        atom_header = f.read(ATOM_HEADER_SIZE)
        if atom_header[4:8] == b"cmov":
            raise RuntimeError("moov atom is compressed")
        elif atom_header[4:8] != b"mvhd":
            raise RuntimeError('expected to find "mvhd" header.')
        else:
            f.seek(4, 1)
            creation_time = get_creation_time(struct, f)
            modification_time = creation_time
    return creation_time, modification_time


# TODO Rename this here and in `get_mov_timestamps`
def get_creation_time(struct, f):
    result = struct.unpack(">I", f.read(4))[0] - EPOCH_ADJUSTER
    result = dt.fromtimestamp(result)
    if result.year < 1990:  # invalid or censored data
        result = None

    return result


class ImageReader:
    """Initialize a media database with existing media dataframe or create empty one."""

    def __init__(self, in_dir_name, media_df: MediaDataFrame | None = None) -> None:
        # read the config

        self.in_dir_name = in_dir_name
        self.image_extensions = default_settings.image_extensions
        self.video_extensions = default_settings.video_extensions

        if media_df is None:
            logger.debug(
                f"Initializing empty media dataframe in ImageReader ({in_dir_name})"
            )
            self.media_df = MediaDataFrame(DataFrame())
        else:
            msg = "Initializing media dataframe in ImageReader with provided df."
            logger.debug(f"{msg}Num records: {len(media_df)}")
            self.media_df = media_df

    def get_data_from_files_as_list_of_rows(self) -> list[dict]:
        """Recursively read exif data from files given in path provided in config.

        Args:

        Returns:
          List of rows: list of rows with all information

        """
        list_of_rows = []
        in_dir_name = self.in_dir_name
        ext = self.image_extensions + self.video_extensions

        logger.debug(f"Reading data from: {in_dir_name}")
        image_extensions = self.image_extensions
        meta = Metadata()
        file_list = list(os.listdir(in_dir_name))
        for file_name in tqdm(file_list):
            if ut.is_supported_filetype(file_name, ext):
                new_row = prepare_new_row_with_meta(
                    file_name, image_extensions, Path(in_dir_name), meta
                )
                list_of_rows.append(new_row)
        return list_of_rows

    def get_media_info_from_inbox_files(self) -> None:
        """Read data from files, return media info in a dataframe."""
        row_list = self.get_data_from_files_as_list_of_rows()
        logger.debug(f"Read info from {len(row_list)} files.")
        # convert a list of rows to data frame
        inbox_media_df = MediaDataFrame(DataFrame(row_list))
        inbox_media_df = multiple_timestamps_to_one(inbox_media_df)
        self.media_df = inbox_media_df


def configure_im_reader(in_dir_name: str | Path) -> Config:
    """Customize configuration for purpose of scanning the media library.

    Args:
        in_dir_name: input directory name to be scanned for the media contents.

    Returns:
        Configuration object for the im_reader.
    """
    conf = get_default_config()
    # modify config
    conf.__setattr__("in_dir_name", str(in_dir_name))
    conf.__setattr__("out_dir_name", "")
    conf.__setattr__("mode", CopyMode.NOP)
    conf.__setattr__("delete_db", False)
    return conf


def get_media_df(in_dir_name) -> MediaDataFrame | None:
    """Get data frame with metadata description of media indicated in Config.

    Args:
      conf:

    Returns:
        Dataframe with metadata of the contents of directory.
    """
    if os.listdir(in_dir_name):
        im_reader = ImageReader(in_dir_name)
        if row_list := im_reader.get_data_from_files_as_list_of_rows():
            df = MediaDataFrame(DataFrame(row_list))
            return multiple_timestamps_to_one(df)
        else:
            logger.debug(f" - directory {in_dir_name} is empty?.")
            return None
    else:
        logger.debug(f" - directory {in_dir_name} is empty.")
        return None


def get_media_stats(df: DataFrame, time_granularity: int) -> dict:
    """Get statistics of media data represented in data frame.

        Get statistics of media folder.

    Args:
      df: pd.DataFrame:
      time_granularity: int:

    Returns:
        Dictionary describing media in dataframe.
    """
    date_min = df.date.min()
    date_max = df.date.max()
    # date_median = df["date"].iloc[int(len(df) / 2)]
    date_median = df["date"].median()

    df = df[["file_name", "date"]].copy()
    df["date_int"] = df["date"].apply(lambda x: x.value / 10**9)
    df = df.sort_values("date_int")
    df["delta"] = df.date_int.diff()

    # check all media in the folder are compliant with the assumption that
    # all the media from the same event are not more than 1 hour apart
    # from each other
    is_time_consistent = not (any(df.delta.values > time_granularity))

    file_count = len(df)
    return {
        "date_min": date_min,
        "date_max": date_max,
        "date_median": date_median,
        "is_time_consistent": is_time_consistent,
        "file_count": file_count,
    }
