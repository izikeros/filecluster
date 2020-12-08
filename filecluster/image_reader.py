import logging
import os
from functools import lru_cache
from pathlib import Path
from pprint import pprint
from typing import List, Optional

import pandas as pd

import filecluster.utlis as ut
from filecluster.configuration import Config
from filecluster.types import Media

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def remove_not_used_timestamp_columns(image_df: Media) -> Media:
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
    # image_df['m_date'] = pd.to_datetime(image_df['m_date'], infer_datetime_format=True)
    # image_df['c_date'] = pd.to_datetime(image_df['c_date'], infer_datetime_format=True)
    # image_df['exif_date'] = pd.to_datetime(image_df['exif_date'], infer_datetime_format=True)
    return image_df


class ImageReader(object):
    def __init__(self, config: Config, image_df: Optional[Media] = None):
        # read the config
        self.config = config

        # initialize media database with existing media dataframe or cereate empty dataframe
        if image_df is None:
            logger.debug("Initializing empty media dataframe in ImageReader")
            self.image_df = Media(pd.DataFrame())
        else:
            logger.debug(
                f"Initializing media dataframe in ImageReader with provided df. Num records: {len(image_df)}"
            )
            self.image_df = image_df

    def get_data_from_files_as_list_of_rows(self) -> List[dict]:
        """Recursively read exif data from files gived in path provided in config

        :return: list of rows with all information
        :rtype: List of rows
        """

        def _initialize_row_dict():
            """generate single row based on values defined in outer method"""
            thumbnail = None
            if self.config.generate_thumbnails:
                thumbnail = ut.get_thumbnail(path_name)

            # define structure of images dataframe and fill with data
            row = {
                'file_name': fn,
                'm_date': m_time,
                'c_date': c_time,
                'exif_date': exif_date,
                'date': date,
                'size': file_size,
                'hash_value': hash_value,
                'full_path': path_name,
                'image': thumbnail,
                'is_image': is_img,
                'cluster_id': cluster_id,
                'duplicate_to_ids': duplicate_to_ids
            }
            return row

        list_of_rows = []
        in_dir_name = self.config.in_dir_name
        ext = self.config.image_extensions + self.config.video_extensions

        print(f"Reading data from: {in_dir_name}")
        list_dir = os.listdir(in_dir_name)
        n_files = len(list_dir)
        image_extensions = self.config.image_extensions
        for i_file, fn in enumerate(os.listdir(in_dir_name)):
            if ut.is_supported_filetype(fn, ext):
                # full path + file name
                path_name = os.path.join(in_dir_name, fn)

                # get modification, creation and exif dates
                m_time, c_time, exif_date = ut.get_date_from_file(path_name)

                # determine if media file is image or other type
                is_img = ut.is_image(path_name, image_extensions)

                # file size
                file_size = os.path.getsize(path_name)

                # file hash
                hash_value = ut.hash_file(path_name)

                # placeholder for date representative for file
                date = None  # to be filled in later

                # placeholder for assignment to cluster
                cluster_id = None

                # placeholder for storing info on this file duplicates
                duplicate_to_ids = []

                # generate new row using data obtained above
                new_row = _initialize_row_dict()

                list_of_rows.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return list_of_rows

    # def save_image_data_to_data_frame(self, list_of_rows):
    #     """Convert list of rows to pandas dataframe."""
    #     # save image data: name, path, date, hash to data frame

    def check_import_for_duplicates_in_watch_folders(self, new_media_df: Media)->Media:
        """Check if imported files are not in the library already, if so - skip them."""
        logger.debug(
            "Checking import for duplicates in watch folders (not implemented)"
        )

        lst = []
        file_list_watch = None
        if not any(self.config.watch_folders):
            logger.debug("No library folder defined. Skipping duplicate search.")
            return new_media_df

        for w in self.config.watch_folders:
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
        new_media_df = new_media_df[~new_media_df.file_name.isin(keys_to_remove
                                                                 )]
        return new_media_df

    def check_import_for_duplicates_in_existing_clusters(self, new_media_df: Media):
        if self.image_df.empty:
            logger.debug('Media db empty. Skipping duplicate analysis.')
            # TODO: KS: 2020-05-24: Consider checking for duplicates within import (file size and hash based)
            return None
        else:
            logger.debug("Checking newly imported files against database")
            # TODO: 1. check for duplicates: in newly imported files (file size and hash based)
            # TODO: 2. check for duplicates: newly imported files against database
            # TODO: mark duplicates if found any
            logger.warning("Duplicates check not implemented")
            return new_media_df


@lru_cache
def get_files_from_watch_folder(w: str):
    return Path(w).rglob('*.*')


def check_on_updates_in_watch_folders(config: Config):
    """Running media scan in structured media repository."""
    # TODO: KS: 2020-10-28: implement
    # TODO: KS: 2020-10-28: need another database or media will be sufficient?
    logger.info(
        f'Running media scan in {config.watch_folders} (not implemented yet)')


def check_if_media_files_from_db_exists():
    # TODO: KS: 2020-10-28: implement
    logger.info('Running media scan (not implemented yet)')


def get_media_info_from_inbox_files(image_reader: ImageReader) -> Media:
    """Read data from files, return media info in dataframe."""
    row_list = image_reader.get_data_from_files_as_list_of_rows()
    logger.debug(f"Read info from {len(row_list)} files.")
    # convert list of rows to data frame
    new_media_df = Media(pd.DataFrame(row_list))
    new_media_df = remove_not_used_timestamp_columns(new_media_df)
    return new_media_df
