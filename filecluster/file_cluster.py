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
DEV_MODE = False
DELETE_DB = True

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Using info log-level")


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


class ImageGroupper(object):
    def __init__(self, configuration, image_df=None, cluster_df=None):
        # read the config
        self.config = configuration

        # initialize image data frame (if provided)
        if image_df is not None:
            self.image_df = image_df

        # initialize cluster data frame (if provided)
        if cluster_df is not None:
            self.cluster_df = cluster_df

    def calculate_gaps(self, date_col, delta_col):
        """Calculate gaps between consecutive shots, save delta to dataframe

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.image_df.sort_values(by=date_col, ascending=True, inplace=True)
        # calculate breaks between the shoots
        self.image_df[delta_col] = self.image_df[date_col].diff()

    def assign_images_to_existing_clusters(self, date_start, date_end,
                                           margin, conn):
        # TODO: finalize implemantation
        # --- check if image can be assigned to any of existing clusters
        run_again = True
        while run_again:
            # iterate over and over since new cluster members might drag
            # cluster boundaries that new images will fit now
            run_again = False
            # find images <existing_clusters_start, existing_clusters_end>
            # see pandas Query:
            # https://stackoverflow.com/questions/11869910/
            for index, _row in self.image_df[
                (self.image_df['cluster_id'].isnull() &
                 self.image_df['date'] > date_start - margin &
                 self.image_df['date'] < date_end + margin)].iterrows():

                # TODO: add query to the cluster
                fit = None
                # is in cluster range with margins:
                # where
                # date > (date_start - margin) and
                # date < (date_stop + margin)
                if fit:
                    run_again = True
                    # add cluster info to image
                    # update cluster range (start/end date)

    def add_tmp_cluster_id_to_files_in_data_frame(self):
        """Add tmp cluster id information to each file
        """

        # open connection to db
        conn = self.db_connect()

        # Hint on reading date: http://numericalexpert.com/blog/sqlite_blob_time/
        cursor = conn.execute(f"SELECT MIN(start_date) FROM clusters;")
        existing_clusters_start = cursor.fetchone()  # FIXME

        cursor = conn.execute(f"SELECT MAX(end_date) FROM clusters;")
        existing_clusters_end = cursor.fetchone()  # FIXME

        cursor = conn.execute(f"SELECT MAX(id) FROM clusters;")
        existing_clusters_last_id = cursor.fetchone()[0]

        if not existing_clusters_last_id:
            existing_clusters_last_id = 0  # FIXME: check if this is ok

        # entry for new potential record
        new_cluster_idx = existing_clusters_last_id + 1

        cluster = {'id': new_cluster_idx,
                   'start_date': None,
                   'stop_date': None}

        list_new_clusters = []

        max_time_delta = self.config['granularity_minutes']

        n_files = len(self.image_df)
        i_file = 0

        # TODO: uncomment when implemented
        if 0 == 1:
            self.assign_images_to_existing_clusters(
                date_start=existing_clusters_start,
                date_end=existing_clusters_end,
                margin=self.config['granularity_minutes'],
                conn=conn)

        # new_images_df
        # df.loc[df['column_name'] == some_value]
        for index, _row in self.image_df[
            self.image_df['cluster_id'].isnull()].iterrows():
            delta_from_previous = self.image_df.loc[index]['date_delta']

            # check if new cluster encountered
            if delta_from_previous > max_time_delta or index == 0:
                new_cluster_idx += 1

                # append previous cluster date to the list
                if index > 0:
                    # add previous cluster info to the list of clusters
                    list_new_clusters.append(cluster)

                # create record for new cluster
                cluster = {'id': new_cluster_idx,
                           'start_date': self.image_df.loc[index]['date'],
                           'end_date': None}

            # assign cluster id to image
            self.image_df.loc[index, 'cluster_id'] = new_cluster_idx

            # update cluster stop date
            cluster['end_date'] = self.image_df.loc[index]['date']

            i_file += 1
            ut.print_progress(i_file, n_files, 'clustering: ')

        # save last cluster (TODO: check border cases: one file,
        # one cluster, no-files,...)
        list_new_clusters.append(cluster)

        print("")
        print("{num_clusters} clusters identified".format(
            num_clusters=new_cluster_idx))

        return list_new_clusters

    def save_cluster_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        self.cluster_df = pd.DataFrame(row_list)

    def get_num_of_clusters_in_df(self):
        return self.image_df['cluster_id'].value_counts()

    def get_cluster_ids(self):
        return self.image_df['cluster_id'].unique()

    def assign_representative_date_to_clusters(self, method='random'):
        """ return date representing cluster
        """
        if method == 'random':
            clusters = self.get_cluster_ids()
            for cluster in clusters:
                mask = self.image_df['cluster_id'] == cluster
                df = self.image_df.loc[mask]

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

                self.image_df.loc[mask, 'date_string'] = date_string
        return date_string

    def move_or_copy_pictures(self, mode='copy'):
        """ move or copy items to dedicated folder"""
        pth_out = self.config['outDirName']
        pth_in = self.config['inDirName']
        n_files = len(self.image_df)
        i_file = 0
        for idx, row in self.image_df.iterrows():
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
        dirs = self.image_df['date_string'].unique()

        for dir_name in dirs:
            ut.create_folder_for_cluster(self.config, dir_name)
        # FIXME: read from config parameters
        self.move_or_copy_pictures(mode='move')

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
        """Export data frame with media information into database. Existing
        records will be replaced by new."""
        connection = self.db_connect()

        # TODO: consider insert or ignore
        query = '''INSERT OR REPLACE INTO media (file_name, date, size, 
        hash_value, full_path, image, is_image) 
        VALUES (?,?,?,?,?,?,?);'''

        # get number of rows before importing new media
        num_before = self.db_get_table_rowcount('media')

        # see: # https://stackoverflow.com/questions/23574614/appending
        # -pandas-dataframe-to
        # # -sqlite-table-by-primary-key
        connection.executemany(query, self.image_df[
            ['file_name', 'date',
             'size', 'hash_value', 'full_path', 'image',
             'is_image']].to_records(
            index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount('media')
        print(f"{num_after-num_before} image rows added, before: "
              f"{num_before}, "
              f"after: {num_after}")

    def db_save_clusters(self):
        """Export data frame with media information into database. Existing
        records will be replaced by new."""
        connection = self.db_connect()

        cluster_table_name = 'clusters'
        # TODO: consider insert or ignore
        query = f'''INSERT OR REPLACE INTO {cluster_table_name} (id, 
        start_date, end_date) VALUES (?,?,?);'''

        # get number of rows before importing new media
        num_before = self.db_get_table_rowcount(cluster_table_name)

        # see: # https://stackoverflow.com/questions/23574614/appending
        # -pandas-dataframe-to-sqlite-table-by-primary-key
        new_df = self.cluster_df[['id', 'start_date', 'end_date']]
        new_df.id = new_df.id.astype(float)  # FIXME:
        # temporal workaround
        connection.executemany(query, new_df.to_records(index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount(cluster_table_name)
        print(f"{num_after-num_before} cluster rows added, before: "
              f"{num_before}, "
              f"after: {num_after}")


def db_create_tables_if_not_exists(configuration):
    # TODO: remove when development will be done
    if DELETE_DB:
        try:
            os.remove(configuration['db_file'])
            print(f"Database: {configuration['db_file']} has been deteted")
        except Exception as ex:
            print(ex)

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
                             hash_value TEXT,
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


def read_clusters_database(config=None):
    clusters = None
    return clusters


def read_images_database():
    pass


if __name__ == '__main__':
    """Main routine to perform groupping process"""

    config = ut.get_default_config()
    if DEV_MODE:
        config = ut.get_development_config()

    # --- create database with schema if not exist (image and cluster tables)
    db_create_tables_if_not_exists(config)
    read_images_database()

    # --- TODO: run media scan if needed

    # --- Read date when pictures/recordings in inbox were taken
    image_reader = ImageReader(config)
    row_list = image_reader.get_data_from_files()
    # save image data to data frame (name, path, date, hash)
    image_reader.save_image_data_to_data_frame(row_list)
    # handle cases with missing exif data
    image_reader.cleanup_data_frame_timestamps()

    # TODO: mark duplicates
    image_reader.compare_data_frame_to_image_database()

    # --- initialize media grouper
    image_groupper = ImageGroupper(configuration=config,
                                   image_df=image_reader.image_df)

    # --- Read clusters from database
    # TODO: read clusters
    read_clusters_database()

    # --- Perform clustering
    print("calculating gaps")
    image_groupper.calculate_gaps(date_col='date', delta_col='date_delta')
    # actual clustering takes place here:
    cluster_list = image_groupper.add_tmp_cluster_id_to_files_in_data_frame()
    image_groupper.save_cluster_data_to_data_frame(cluster_list)

    image_groupper.assign_representative_date_to_clusters(
        method=config['assign_date_to_clusters_method'])

    # Physically move or copy files to folders
    image_groupper.move_files_to_cluster_folder()

    # -- Save info to database
    image_groupper.db_save_images()
    image_groupper.db_save_clusters()

# TODO: read config from yaml
