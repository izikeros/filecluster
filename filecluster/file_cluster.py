# -*- coding: utf-8 -*-
# Copyright (c) 2017 Krystian Safjan
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import base64
import os
import time
from datetime import datetime, timedelta
from io import BytesIO
from shutil import copy2, move

import exifread
import pandas as pd
from PIL import Image


def get_default_config():
    # path to files to be clustered
    inbox_path = "/home/izik/fc_data/mix2"

    # filename extensions in scope of clustering
    image_extensions = ['.jpg', '.cr2']
    video_extensions = ['.mp4', '.3gp', 'mov']

    # minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    config = {
        'inDirName': inbox_path,
        'outDirName': '/home/izik/fc_data/out',
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'granularity_minutes': max_gap,
        'move_instead_of_copy': False,
        'cluster_col': 'cluster_id'
    }

    # ensure extensions are lowercase
    config['image_extensions'] = [xx.lower() for xx in
                                  config['image_extensions']]
    config['video_extensions'] = [xx.lower() for xx in
                                  config['video_extensions']]
    return config


def is_supported_filetype(file_name, ext):
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext))


def get_media_type(file_name, ext_image, ext_video):
    fn_lower = file_name.lower()
    is_video = fn_lower.endswith(tuple(ext_video))
    is_image = fn_lower.endswith(tuple(ext_image))

    if is_image:
        return 'image'
    elif is_video:
        return 'video'
    else:
        return 'unknown'


def get_date_from_file(path_name):
    """
    get date information from photo file
    """

    mtime = time.ctime(os.path.getmtime(path_name))
    ctime = time.ctime(os.path.getctime(path_name))
    exif_date = get_exif_date(path_name)
    return mtime, ctime, exif_date


def get_exif_date(path_name):
    """
    return exif date or none
    """

    # Open image file for reading (binary mode)
    img_file = open(path_name, 'rb')

    # Return Exif tags
    tags = exifread.process_file(img_file, details=False,
                                 stop_tag='EXIF DateTimeOriginal')

    try:
        exif_date_str = tags['EXIF DateTimeOriginal'].values
        exif_date = datetime.strptime(exif_date_str, '%Y:%m:%d %H:%M:%S')
    except KeyError:
        exif_date = None

    return exif_date


def create_folder_for_cluster(config, date_string):
    """ create designation folder that for all pictures from the cluster
    """
    pth = config['outDirName']
    dir_name = os.path.join(pth, date_string)
    try:
        result = os.makedirs(dir_name)
    except OSError as err:
        pass


def get_thumbnail(path):
    i = Image.open(path)
    i.thumbnail((150, 150), Image.LANCZOS)
    return i


def image_base64(im):
    if isinstance(im, str):
        im = get_thumbnail(im)
    with BytesIO() as buffer:
        im.save(buffer, 'jpeg')
        return base64.b64encode(buffer.getvalue()).decode()


def image_formatter(im):
    return '<img src="data:image/jpeg;base64,{image_tn}">'.format(
        image_tn=image_base64(im))


# Print iterations progress
def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    import sys
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


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

        list_dir = os.listdir(pth)
        n_files = len(list_dir)
        for i_file, fn in enumerate(os.listdir(pth)):
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
            print_progress(i_file, n_files-1, 'reading files: ')
        print("")

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

            n_files = len(self.df)
            i_file = 0
            for index, _row in self.df.iterrows():
                d_previous = self.df.loc[index][date_col]
                if d_previous > td:
                    cluster_idx += 1
                self.df.loc[index, cluster_col] = cluster_idx
                i_file += 1
                print_progress(i_file, n_files, 'clustering: ')
            print("")

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
        n_files = len(self.df)
        i_file = 0
        for idx, row in self.df.iterrows():
            date_string = row['date_string']
            file_name = row['file_name']
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == 'copy':
                copy2(src, dst)
            i_file += 1
            print_progress(i_file, n_files, 'move/copy: ')
        print("")

    def move_files_to_cluster_folder(self):
        dirs = self.df['date_string'].unique()

        for dir_name in dirs:
            create_folder_for_cluster(self.config, dir_name)

        self.move_or_copy_pictures(mode='copy')

    def run_clustering(self):
        # TODO: create 'date' column and map there exif date if available or
        #  m_date otherwise
        n_files = len(self.df)
        print("%d files found") % n_files
        print("")
        print("calculating gaps")
        self.calculate_gaps('date', 'date_delta')
        self.do_clustering(method='baseline')


if __name__ == '__main__':
    this_config = get_default_config()
    image_groupper = ImageGroupper(this_config)
    image_groupper.get_data_from_files()
    image_groupper.run_clustering()
    image_groupper.assign_date_to_clusters(method='random')
    image_groupper.move_files_to_cluster_folder()

# TODO: Break if error
# TODO: periodically check size of inbox and outbox if size is correct
