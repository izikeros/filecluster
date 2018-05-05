# -*- coding: utf-8 -*-
# Copyright (c) 2017 Krystian Safjan
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import os
from shutil import copy2, move
import sqlite3
import logging

import pandas as pd

from filecluster import utlis as ut

# In debug mode thumbnails are generated
GENERATE_THUMBNAIL = False
DEV_MODE = True

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Using info log-level")


class ImageReader(object):
    def __init__(self, config):
        # read the config
        self.config = config
        self.df = pd.DataFrame

    def get_data_from_files(self):
        """return files data as list of rows

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

            row = {'file_name': fn,
                   'm_date': m_time,
                   'c_date': c_time,
                   'exif_date': exif_date,
                   'date': date,
                   'size': file_size,
                   'md5': hash,
                   'full_path': path_name,
                   'image': thumbnail,
                   'is_image': media_type
                   }
            return row

        list_of_rows = []
        pth = self.config['inDirName']
        ext = self.config['image_extensions'] + self.config['video_extensions']

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

                # placeholder for date representatice for file
                date = None # to be filled in later

                # file size
                file_size = os.path.getsize(path_name)

                # file hash
                hash = ut.hash_file(path_name)

                # generate new row using data obtained above
                new_row = _add_new_row()

                list_of_rows.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return list_of_rows

    def save_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        self.df = pd.DataFrame(row_list)

    def cleanup_data_frame_timestamps(self):
        """Decide on which timestamp use as representative for file"""

        # use exif date as base
        self.df['date'] = self.df['exif_date']
        # unless is missing - then use modification date:
        self.df['date'] = self.df['date'].fillna(self.df['m_date'])

        # infer dataformat  from strings
        self.df['date'] = pd.to_datetime(self.df['date'],
                                         infer_datetime_format=True)
        self.df['m_date'] = pd.to_datetime(self.df['m_date'],
                                           infer_datetime_format=True)
        self.df['c_date'] = pd.to_datetime(self.df['c_date'],
                                           infer_datetime_format=True)
        self.df['exif_date'] = pd.to_datetime(self.df['exif_date'],
                                              infer_datetime_format=True)

    def compare_data_frame_to_media_table(self):
        print("checking newly imported files against database")
        pass

class ImageGroupper(object):
    def __init__(self, configuration, data_frame=None):
        # read the config
        self.config = configuration
        if data_frame is not None:
            self.df = data_frame

    def calculate_gaps(self, date_col, delta_col):
        """Calculate gaps between consecutive shots, save delta to dataframe

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.df.sort_values(by=date_col, ascending=True, inplace=True)
        # calculate breaks between the shoots
        self.df[delta_col] = self.df[date_col].diff()

    def add_tmp_cluster_id_to_files_in_data_frame(self,
                                                  date_col='date_delta',
                                                  cluster_col='cluster_id',
                                                  clustering_method='time_gap'):
        """Add tmp cluster id information to each file
        """
        if clustering_method == 'time_gap':
            time_delta = self.config['granularity_minutes']
            cluster_idx = 0

            n_files = len(self.df)
            i_file = 0
            for index, _row in self.df.iterrows():
                d_previous = self.df.loc[index][date_col]
                if d_previous > time_delta:
                    cluster_idx += 1
                self.df.loc[index, cluster_col] = cluster_idx
                i_file += 1
                ut.print_progress(i_file, n_files, 'clustering: ')
            print("")
            print("{num_clusters} clusters identified".format(
                num_clusters=cluster_idx + 1))
        else:
            logger.error(f"Unknown clustering method: {clustering_method}")

    def get_num_of_clusters_in_df(self):
        return self.df['cluster_id'].value_counts()

    def get_cluster_ids(self):
        return self.df['cluster_id'].unique()

    def assign_date_to_clusters(self, method='random'):
        """ return date representing cluster
        """
        if method == 'random':
            clusters = self.get_cluster_ids()
            for cluster in clusters:
                mask = self.df['cluster_id'] == cluster
                df = self.df.loc[mask]

                exif_date = df.sample(n=1)['date']
                exif_date = exif_date.values[0]
                ts = pd.to_datetime(str(exif_date))
                date_str = ts.strftime('[%Y_%m_%d]')
                time_str = ts.strftime('%H%M%S')

                image_count = df.loc[df['is_image'] == True].shape[0]
                video_count = df.loc[df['is_image'] == False].shape[0]

                date_string = "_".join([
                    date_str,
                    time_str,
                    'IC_{ic}'.format(ic=image_count),
                    'VC_{vc}'.format(vc=video_count)])

                self.df.loc[mask, 'date_string'] = date_string
        return date_string

    def move_or_copy_pictures(self, mode='copy'):
        """ move or copy items to dedicated folder"""
        pth_out = self.config['outDirName']
        pth_in = self.config['inDirName']
        n_files = len(self.df)
        i_file = 0
        for idx, row in self.df.iterrows():
            date_string = row['date_string']
            file_name = row['file_name']
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == 'copy':
                copy2(src, dst)
            else:
                move(src, dst)
            i_file += 1
            ut.print_progress(i_file, n_files, 'move/copy: ')
        print("")

    def move_files_to_cluster_folder(self):
        dirs = self.df['date_string'].unique()

        for dir_name in dirs:
            ut.create_folder_for_cluster(self.config, dir_name)

        self.move_or_copy_pictures(mode='copy')

    def db_connect(self):
        connection = sqlite3.connect(self.config['db_file'])
        return connection

    def db_get_table_rowcount(self, table, connection=None):
        if not connection:
            connection = self.db_connect()
        cursor = connection.execute(f"SELECT * FROM {table};")
        num_records = len(cursor.fetchall())
        return num_records

    def db_save_images(self):
        """Import data frame with media information into database. Existing
        records will be replaced by new."""
        connection = self.db_connect()

        # TODO: consider insert or ignore
        query = '''INSERT OR REPLACE INTO media (file_name, date, size, md5, 
        full_path, image, is_image) 
        VALUES (?,?,?,?,?,?,?);'''

        # get number of rows before importing new media
        num_before = self.db_get_table_rowcount('media')

        # see: # https://stackoverflow.com/questions/23574614/appending
        # -pandas-dataframe-to
        # # -sqlite-table-by-primary-key
        connection.executemany(query, self.df[
            ['file_name', 'date',
             'size', 'md5', 'full_path', 'image', 'is_image']].to_records(
            index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount('media')
        print(f"{num_after-num_before} rows added (before: {num_before}, "
              f"after: {num_after}")

    def db_save_clusters(self):
        pass


def db_create_tables_if_not_exists(configuration):
    try:
        # Creates or opens a file called mydb with a SQLite3 DB
        conn = sqlite3.connect(configuration['db_file'])
        # Get a cursor object
        cursor = conn.cursor()
        # Check if table users does not exist and create it

        # table for individual media files
        cursor.execute('''CREATE TABLE IF NOT EXISTS media(
                             file_name TEXT(256) PRIMARY KEY,
                             m_date DATETIME, 
                             c_date DATETIME, 
                             exif_date DATETIME, 
                             date DATETIME, 
                             size INTEGER,
                             md5 TEXT,
                             full_path TEXT,
                             image BLOB,
                             is_image INTEGER)''')

        # table with cluster information
        cursor.execute('''CREATE TABLE IF NOT EXISTS clusters(
                          id INTEGER PRIMARY KEY, 
                          start_date DATETIME, 
                          end_date DATETIME, 
                          median DATETIME)''')
        # Commit the change
        conn.commit()
        print("Database created/opened")
    # Catch the exception
    except Exception as e:
        # Roll back any change if something goes wrong
        conn.rollback()
        raise e
    finally:
        # Close the db connection
        conn.close()


def read_clusters_from_database(config=None):
    clusters = None
    return clusters


if __name__ == '__main__':
    """Main routine to perform groupping process"""

    config = ut.get_default_config()
    if DEV_MODE:
        config = ut.get_development_config()

    # --- create database witch schema if not exist
    db_create_tables_if_not_exists(config)

    # --- Read date when pictures/recordings in inbox were taken
    image_reader = ImageReader(config)
    row_list = image_reader.get_data_from_files()
    image_reader.save_data_to_data_frame(row_list)
    image_reader.cleanup_data_frame_timestamps()

    image_reader.compare_data_frame_to_media_table()

    # --- Group media by time
    image_groupper = ImageGroupper(configuration=config,
                                   data_frame=image_reader.df)

    # --- Read clusters from database
    read_clusters_from_database()

    # --- Perform clustering
    print("calculating gaps")
    image_groupper.calculate_gaps(date_col='date', delta_col='date_delta')
    image_groupper.add_tmp_cluster_id_to_files_in_data_frame(
        clustering_method=config['clustering_method'])

    image_groupper.assign_date_to_clusters(
        method=config['assign_date_to_clusters_method'])
    image_groupper.move_files_to_cluster_folder()

    # -- Save info to database
    image_groupper.db_save_images()
    image_groupper.db_save_clusters()



# TODO: read config from yaml
