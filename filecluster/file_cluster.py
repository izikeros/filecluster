# Copyright (c) 2017 Krystian Safjan
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103


import os

import pandas as pd

from utils import (
    create_folder_for_cluster,
    get_default_config,
    is_supported_filetype,
    get_date_from_file,
    get_thumbnail,
    get_media_type
)

from shutil import copy2, move


class ImageGroupper(object):
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

        row_list = []
        pth = self.config['inDirName']
        ext = self.config['image_extensions'] + self.config['video_extensions']

        DEBUG_MODE = False

        for fn in os.listdir(pth):
            if is_supported_filetype(fn, ext):
                path_name = os.path.join(pth, fn)
                m_time, c_time, exif_date = get_date_from_file(path_name)
                media_type = get_media_type(path_name, self.config[
                    'image_extensions'], self.config['video_extensions'])
                if DEBUG_MODE:
                    new_row = {'file_name': fn,
                               'm_date': m_time,
                               'c_date': c_time,
                               'exif_date': exif_date,
                               'full_path': path_name,
                               'image': get_thumbnail(path_name),
                               'media_type': media_type
                               }
                else:
                    new_row = {'file_name': fn,
                               'm_date': m_time,
                               'c_date': c_time,
                               'exif_date': exif_date,
                               'media_type': media_type
                               }
                row_list.append(new_row)

        self.df = pd.DataFrame(row_list)

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

    def calculate_gaps(self, date_col, delta_col):
        """ calculate gaps between consecutive shots

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.df.sort_values(by=date_col, ascending=True, inplace=True)
        # calculate breaks between the shoots
        self.df[delta_col] = self.df[date_col].diff()

    def do_clustering(self, date_col='date_delta',
                      cluster_col='cluster_id', method=None):
        """ add cluster id to dataframe
        """
        if method == 'baseline':
            td = self.config['granularity_minutes']
            cluster_idx = 0

            for index, _row in self.df.iterrows():
                d_previous = self.df.loc[index][date_col]
                if d_previous > td:
                    cluster_idx += 1
                self.df.loc[index, cluster_col] = cluster_idx

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

                df_media = df.groupby('media_type')

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

        elif method == 'mean':
            pass
        return date_string

    def move_or_copy_pictures(self, mode='copy'):
        """ move or copy items to dedicated folder"""
        pth_out = self.config['outDirName']
        pth_in = self.config['inDirName']
        for idx, row in self.df.iterrows():
            date_string = row['date_string']
            file_name = row['file_name']
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == 'copy':
                copy2(src, dst)


    def move_files_to_cluster_folder(self):
        dirs = self.df['date_string'].unique()

        for dir_name in dirs:
            create_folder_for_cluster(self.config, dir_name)

        self.move_or_copy_pictures(mode='copy')

    def run_clustering(self):
        print('calculating gaps')
        # TODO: create 'date' column and map there exif date if available or
        #  m_date otherwise
        self.calculate_gaps('date', 'date_delta')
        print('clustering')
        self.do_clustering(method='baseline')
        n_files = len(self.df)
        print ("%d files found") % n_files
        print('Done')


if __name__ == '__main__':
    this_config = get_default_config()
    image_groupper = ImageGroupper(this_config)
    image_groupper.get_data_from_files()
    image_groupper.run_clustering()
    image_groupper.assign_date_to_clusters(method='random')
    image_groupper.move_files_to_cluster_folder()
