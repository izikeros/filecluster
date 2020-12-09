import logging
import os
from functools import lru_cache
from pathlib import Path
from pprint import pprint
from typing import List, Optional

import pandas as pd

import filecluster.utlis as ut
from filecluster.configuration import Config
from filecluster.types import MediaDataframe

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Metadata:
    def __init__(self):
        self.file_name: str = ''
        self.path_name: str = ''
        self.m_time: str = ''
        self.c_time: str = ''
        self.exif_date: str = ''
        self.date: str = ''
        self.file_size: int = 0
        self.hash_value: int = 0
        self.full_path: str = ''
        self.image: int = 0
        self.is_image: bool = True
        self.cluster_id: int = 0
        self.duplicate_to_ids: List[int] = [0]


def multiple_timestamps_to_one(image_df: MediaDataframe) -> MediaDataframe:
    """Get timestamp from exif (primary) or m_date. Drop not needed date cols.

    Prepare single timestamp out of few available."""

    logger.debug("Cleaning-up timestamps in imported media.")
    # use exif date as base
    image_df['date'] = image_df['exif_date']
    # unless is missing - then use modification date:
    image_df['date'] = image_df['date'].fillna(image_df['m_date'])
    # infer date format  from strings
    image_df['date'] = pd.to_datetime(image_df['date'],
                                      infer_datetime_format=True)

    image_df.drop(['m_date', 'c_date', 'exif_date'], axis=1, inplace=True)
    return image_df


def initialize_row_dict(meta: Metadata):
    """generate single row based on values defined in outer method"""
    thumbnail = None
    # if generate_thumbnails:
    #     thumbnail = ut.get_thumbnail(path_name)

    # define structure of images dataframe and fill with data
    row = {
        'file_name': meta.file_name,
        'm_date': meta.m_time,
        'c_date': meta.c_time,
        'exif_date': meta.exif_date,
        'date': meta.date,
        'size': meta.file_size,
        'hash_value': meta.hash_value,
        'full_path': meta.full_path,
        'image': meta.image,
        'is_image': thumbnail,
        'cluster_id': meta.cluster_id,
        'duplicate_to_ids': meta.duplicate_to_ids
    }
    return row


def prepare_new_row_with_meta(fn, image_extensions, in_dir_name, meta):
    meta.file_name = fn
    # full path + file name
    path_name = os.path.join(in_dir_name, fn)
    meta.path_name = path_name
    # get modification, creation and exif dates
    meta.m_time, meta.c_time, meta.exif_date = ut.get_date_from_file(path_name)
    # determine if media file is image or other type
    meta.is_image = ut.is_image(path_name, image_extensions)
    # file size
    meta.file_size = os.path.getsize(path_name)
    # file hash
    meta.hash_value = ut.hash_file(path_name)
    # placeholder for date representative for file
    meta.date = None  # to be filled in later
    # placeholder for assignment to cluster
    meta.cluster_id = None
    # placeholder for storing info on this file duplicates
    meta.duplicate_to_ids = []
    # generate new row using data obtained above
    new_row = initialize_row_dict(meta)
    return new_row


class ImageReader(object):
    def __init__(self,
                 config: Config,
                 image_df: Optional[MediaDataframe] = None):
        """Initialize media database with existing media dataframe or create empty one."""

        # read the config
        self.config = config

        if image_df is None:
            logger.debug("Initializing empty media dataframe in ImageReader")
            self.image_df = MediaDataframe(pd.DataFrame())
        else:
            logger.debug(
                f"Initializing media dataframe in ImageReader with provided df. Num records: {len(image_df)}"
            )
            self.image_df = image_df

    def get_data_from_files_as_list_of_rows(self) -> List[dict]:
        """Recursively read exif data from files given in path provided in config

        :return: list of rows with all information
        :rtype: List of rows
        """

        list_of_rows = []
        in_dir_name = self.config.in_dir_name
        ext = self.config.image_extensions + self.config.video_extensions

        print(f"Reading data from: {in_dir_name}")
        list_dir = os.listdir(in_dir_name)
        n_files = len(list_dir)
        image_extensions = self.config.image_extensions
        meta = Metadata()
        for i_file, fn in enumerate(os.listdir(in_dir_name)):
            if ut.is_supported_filetype(fn, ext):
                new_row = prepare_new_row_with_meta(fn, image_extensions,
                                                    in_dir_name, meta)

                list_of_rows.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return list_of_rows

    def get_media_info_from_inbox_files(self):
        """Read data from files, return media info in dataframe."""
        row_list = self.get_data_from_files_as_list_of_rows()
        logger.debug(f"Read info from {len(row_list)} files.")
        # convert list of rows to data frame
        new_media_df = MediaDataframe(pd.DataFrame(row_list))
        new_media_df = multiple_timestamps_to_one(new_media_df)
        self.image_df = new_media_df

    def check_import_for_duplicates_in_existing_clusters(
            self, new_media_df: MediaDataframe):
        if self.image_df.empty:
            logger.debug(
                'MediaDataframe db empty. Skipping duplicate analysis.')
            # TODO: KS: 2020-05-24: Consider checking for duplicates within import (file size and hash based)
            return None
        else:
            logger.debug("Checking newly imported files against database")
            # TODO: 1. check for duplicates: in newly imported files (file size and hash based)
            # TODO: 2. check for duplicates: newly imported files against database
            # TODO: mark duplicates if found any
            logger.warning("Duplicates check not implemented")
            return new_media_df


def check_import_for_duplicates_in_watch_folders(
        watch_folders, new_media_df: MediaDataframe) -> MediaDataframe:
    """Check if imported files are not in the library already, if so - skip them."""
    logger.debug(
        "Checking import for duplicates in watch folders (not implemented)")

    lst = []
    file_list_watch = None
    if not any(watch_folders):
        logger.debug("No library folder defined. Skipping duplicate search.")
        return new_media_df

    for w in watch_folders:
        file_list_watch = get_files_from_watch_folder(w)
        path_list = [path for path in file_list_watch]
        lst.extend(path_list)

    watch_names = [path.name for path in lst]
    new_names = new_media_df.file_name.values.tolist()
    potential_dups = [f for f in new_names if f in watch_names]

    # verify potential dups using size comparison
    confirmed_dups = []
    keys_to_remove = []

    for d in potential_dups:
        nd = new_media_df[new_media_df.file_name == d]
        wd = [path for path in file_list_watch if path.name == d]
        n = nd['size'].values[0]
        if n == os.path.getsize(wd[0]):
            confirmed_dups.append(wd[0])
            keys_to_remove.append(new_media_df.file_name.values[0])
    print("Found duplicates based on filename and size:")
    pprint(confirmed_dups)

    # remove confirmed duplicated from the import batch
    new_media_df = new_media_df[~new_media_df.file_name.isin(keys_to_remove)]
    return new_media_df


@lru_cache
def get_files_from_watch_folder(w: str):
    return Path(w).rglob('*.*')


def check_on_updates_in_watch_folders(config: Config):
    """Running media scan in library."""
    # TODO: KS: 2020-10-28: implement
    # TODO: KS: 2020-10-28: need another database or media will be sufficient?
    logger.info(
        f'Running media scan in {config.watch_folders} (not implemented yet)')


def check_if_media_files_from_db_exists():
    # TODO: KS: 2020-10-28: implement
    logger.info('Running media scan (not implemented yet)')
