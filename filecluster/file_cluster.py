# -*- coding: utf-8 -*-
# Copyright (c) 2017 Krystian Safjan
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

from filecluster import utlis as ut
import os
from shutil import copy2, move
import pandas as pd
import sqlite3
import logging

# In debug mode there are more columns in dataframe, e.g. thumbnails are
# generated
DEBUG_MODE = True
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
        """
        intialize dataframe and fill it in with filenames and creation dates

        :param pth: path to inbox directory with files (pictures, video)
        :type pth: basestring
        :param ext: list of filename extensions taken into account
        :type ext: list
        :return: dataframe with all information
        :rtype: pandas dataframe
        """

        def _add_new_row():
            new_row = {'file_name': fn,
                       'm_date': m_time,
                       'c_date': c_time,
                       'exif_date': exif_date,
                       'date': None,
                       'size': None,
                       'md5': None,
                       'full_path': path_name,
                       'image': ut.get_thumbnail(path_name),
                       'is_image': media_type
                       }
            return new_row

        row_list = []
        pth = self.config['inDirName']
        ext = self.config['image_extensions'] + self.config['video_extensions']

        list_dir = os.listdir(pth)
        n_files = len(list_dir)
        for i_file, fn in enumerate(os.listdir(pth)):
            if ut.is_supported_filetype(fn, ext):
                path_name = os.path.join(pth, fn)
                m_time, c_time, exif_date = ut.get_date_from_file(path_name)
                media_type = ut.get_media_type(path_name, self.config[
                    'image_extensions'], self.config['video_extensions'])
                new_row = _add_new_row()
                row_list.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return row_list

    def save_data_to_data_frame(self, row_list):
        # save to dataframe
        self.df = pd.DataFrame(row_list)

    def cleanup_data_frame_timestamps(self):
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

    def get_data_frame(self):
        return self.df


class ImageGroupper(object):
    def __init__(self, config, data_frame):
        # read the config
        self.config = config
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

    def do_clustering(self, date_col='date_delta',
                      cluster_col='cluster_id', method='time_gap'):
        """Add tmp cluster id information to each file
        """
        if method == 'time_gap':
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
                num_clusters=cluster_idx+1))
        else:
            logger.error(f"Unknown clustering method: {method}")

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

                image_count = df.loc[df['media_type'] ==
                                     'image'].shape[0]
                video_count = df.loc[df['media_type'] ==
                                     'video'].shape[0]

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

    def run_clustering(self):
        # TODO: create 'date' column and map there exif date if available or
        #  m_date otherwise
        n_files = len(self.df)
        if n_files:
            print(f"{n_files} files found")
        else:
            logger.error("No files to cluster found")
            return True
        print("")
        print("calculating gaps")
        self.calculate_gaps('date', 'date_delta')
        self.do_clustering()
        return False

def db_open_or_create(config):

    try:
        # Creates or opens a file called mydb with a SQLite3 DB
        db = sqlite3.connect(config['db_file'])
        # Get a cursor object
        cursor = db.cursor()
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
                             full_path TEXT
                             image BLOB,
                             is_image INTEGER''')

        # table with cluster information
        cursor.execute('''CREATE TABLE IF NOT EXISTS clusters(
                          id INTEGER PRIMARY KEY, 
                          start_date DATETIME, 
                          end_date DATETIME, 
                          median DATETIME)''')
        # Commit the change
        db.commit()
        print("Database created/opened")
    # Catch the exception
    except Exception as e:
        # Roll back any change if something goes wrong
        db.rollback()
        raise e
    finally:
        # Close the db connection
        db.close()


def read_clusters_from_database(config):
    clusters = None
    return clusters

if __name__ == '__main__':
    """Main routine to perform groupping process"""

    config = ut.get_default_config()
    if DEV_MODE:
        config = ut.get_development_config()

    # read date when pictures/recordings in inbox were taken
    image_reader = ImageReader(config)
    row_list = image_reader.get_data_from_files()
    image_reader.save_data_to_data_frame(row_list)
    image_reader.cleanup_data_frame_timestamps()

    # open or create database
    db = db_open_or_create(config)

    # group media by time
    image_groupper = ImageGroupper(config=config,
                                   data_frame=image_reader.get_data_frame())

    # read clusters from database
    # TODO:

    return_code = image_groupper.run_clustering()
    if not return_code:
        image_groupper.assign_date_to_clusters(method='random')
        image_groupper.move_files_to_cluster_folder()
    else:
        logger.error("Clustering Failed")

# TODO: Break if error
# TODO: periodically check size of inbox and outbox if size is correct
# https://stackoverflow.com/questions/23574614/appending-pandas-dataframe-to-sqlite-table-by-primary-key