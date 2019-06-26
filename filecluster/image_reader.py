import logging
import os

import pandas as pd

import filecluster.utlis as ut
from filecluster.file_cluster import GENERATE_THUMBNAIL

logger = logging.getLogger(__name__)


class ImageReader(object):
    def __init__(self, config):
        # read the config
        self.config = config
        self.image_df = pd.DataFrame

    def get_data_from_files(self):
        """return files data as list of rows (each row represented by dict)

        :param pth: path to inbox directory with files (pictures, video)
        :type pth: basestring
        :param ext: list of filename extensions taken into account
        :type ext: list
        :return: dataframe with all information
        :rtype: pandas dataframe
        """

        def _add_new_row():
            """generate single row based on values defined in outer method"""
            thumbnail = None
            if GENERATE_THUMBNAIL:
                thumbnail = ut.get_thumbnail(path_name)

            # define structure of images dataframe and fill with data
            row = {'file_name': fn,
                   'm_date': m_time,
                   'c_date': c_time,
                   'exif_date': exif_date,
                   'date': date,
                   'size': file_size,
                   'hash_value': hash_value,
                   'full_path': path_name,
                   'image': thumbnail,
                   'is_image': media_type,
                   'cluster_id': cluster_id,
                   'duplicate_to_ids': duplicate_to_ids
                   }
            return row

        list_of_rows = []
        pth = self.config['inDirName']
        ext = self.config['image_extensions'] + self.config['video_extensions']

        print(f"Reading data from: {pth}")
        list_dir = os.listdir(pth)
        n_files = len(list_dir)
        for i_file, fn in enumerate(os.listdir(pth)):
            if ut.is_supported_filetype(fn, ext):
                # full path + file name
                path_name = os.path.join(pth, fn)

                # get modification, creation and exif dates
                m_time, c_time, exif_date = ut.get_date_from_file(path_name)

                # determine if media file is ana image or other type
                media_type = ut.get_media_type(path_name, self.config[
                    'image_extensions'], self.config['video_extensions'])

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
                new_row = _add_new_row()

                list_of_rows.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return list_of_rows

    def save_image_data_to_data_frame(self, list_of_rows):
        """convert list of rows to pandas dataframe"""
        self.image_df = pd.DataFrame(list_of_rows)

    def cleanup_data_frame_timestamps(self):
        """Decide on which timestamp use as representative for file"""

        # use exif date as base
        self.image_df['date'] = self.image_df['exif_date']
        # unless is missing - then use modification date:
        self.image_df['date'] = self.image_df['date'].fillna(
            self.image_df['m_date'])

        # infer dataformat  from strings
        self.image_df['date'] = pd.to_datetime(self.image_df['date'],
                                               infer_datetime_format=True)
        self.image_df['m_date'] = pd.to_datetime(self.image_df['m_date'],
                                                 infer_datetime_format=True)
        self.image_df['c_date'] = pd.to_datetime(self.image_df['c_date'],
                                                 infer_datetime_format=True)
        self.image_df['exif_date'] = pd.to_datetime(self.image_df['exif_date'],
                                                    infer_datetime_format=True)

    def compare_data_frame_to_image_database(self):
        print("(TODO): checking newly imported files against database")

        # TODO: 1. check for duplicates: in newly imported files
        # TODO: 2. check for duplicates: newly imported files against database
        # TODO: mark duplicates if found any
        pass
