import logging
import os
from pathlib import PosixPath, Path
from typing import Any, Dict, Iterator, Union, List, Optional, Tuple

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
from pandas.core.frame import DataFrame

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

    Prepare single timestamp out of few available.

    Args:
      image_df: MediaDataFrame:

    Returns:

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
    """generate single row based on values defined in outer method

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
    fn: str,
    image_extensions: List[str],
    in_dir_name: Union[str, PosixPath],
    meta: Metadata,
) -> Dict[str, Any]:
    """

    Args:
      fn:
      image_extensions:
      in_dir_name:
      meta:

    Returns:

    """
    meta.file_name = fn
    # full path + file name
    path_name = os.path.join(in_dir_name, fn)
    meta.path_name = path_name
    # get modification, creation and exif dates
    meta.m_time, meta.c_time, meta.exif_date = ut.get_date_from_file(path_name)
    # determine if media file is image or other type
    is_image = ut.is_image(path_name, image_extensions)
    meta.is_image = is_image
    if not is_image:
        pass
    if fn.lower().endswith("mov"):
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
        """Recursively read exif data from files given in path provided in config

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


def mark_inbox_duplicates_vs_watch_folders(
    watch_folders: List[str],
    inbox_media_df: MediaDataFrame,
    skip_duplicated_existing_in_libs: bool,
) -> Tuple[MediaDataFrame, List[str]]:
    """Check if imported files are not in the library already, if so - skip them.

    Args:
      watch_folders: List[str]:
      inbox_media_df: MediaDataFrame:
      skip_duplicated_existing_in_libs:

    Returns:

    """
    # TODO: KS: 2020-12-24: make it method of ImageReader class
    if not skip_duplicated_existing_in_libs:
        return inbox_media_df, []
    else:
        logger.debug("Checking import for duplicates in watch folders")

    if not any(watch_folders):
        logger.debug("No library folder defined. Skipping duplicate search.")
        return inbox_media_df, []

    # get files in library
    watch_names, lst = get_watch_folders_files_path(watch_folders)

    # get files in inbox
    new_names = inbox_media_df.file_name.values.tolist()

    # commons - list of new names that apear in watch folders
    potential_dups = [f for f in new_names if f in watch_names]

    # verify potential dups using size comparison
    file_already_in_library = []
    keys_to_remove_from_inbox_import = []

    logger.info("Confirm potential duplicates")
    for potential_duplicate in tqdm(potential_dups):
        # get inbox item info
        inbox_item = inbox_media_df[inbox_media_df.file_name == potential_duplicate]

        # get matching watch folder items
        lib_items = [path for path in lst if path.name == potential_duplicate]
        inbox_item_size = inbox_item["size"].values[0]

        for lib_item in lib_items:
            if inbox_item_size == os.path.getsize(lib_item):
                file_already_in_library.append(lib_item)
                in_file_name = inbox_item.file_name.values[0]
                keys_to_remove_from_inbox_import.append(in_file_name)
                logger.debug(
                    f"For inbox file {in_file_name} there is duplicate already in library: {lib_item}"
                )

    # mark confirmed duplicates in import batch
    logger.info("mark confirmed duplicates in import batch")
    # FIXME: KS: 2020-12-26: Very slow stage 1sec/it
    sel_dups = inbox_media_df.file_name.isin(keys_to_remove_from_inbox_import)
    for idx, _row in tqdm(inbox_media_df[sel_dups].iterrows(), total=sum(sel_dups)):
        inbox_media_df.loc[idx, "status"] = Status.DUPLICATE  # Fixme: copy of a slice
        dups_patch = list(filter(lambda x: _row.file_name in str(x), lst))
        dups_str = [str(x) for x in dups_patch]
        dups_clust = [x.parts[-2] for x in dups_patch]

        inbox_media_df["duplicated_to"][idx] = dups_str  # Fixme: copy of a slice
        inbox_media_df["duplicated_cluster"][idx] = dups_clust  # Fixme: copy of a slice
    return inbox_media_df, keys_to_remove_from_inbox_import


def check_if_media_files_from_db_exists():
    """ """
    # TODO: KS: 2020-10-28: implement
    logger.info("Running media scan (not implemented yet)")


def configure_im_reader(in_dir_name: str) -> Config:
    """

    Args:
      in_dir_name:

    Returns:

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
    """

    Args:
      df: pd.DataFrame:
      time_granularity: int:

    Returns:

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
