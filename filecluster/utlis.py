#!/usr/bin/python3
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import base64
import logging
import os
import time
from datetime import datetime, timedelta
from io import BytesIO
import sys
import hashlib
import exifread
from PIL import Image

logger = logging.getLogger(__name__)

# TODO: optimize this parameter for speed
block_size_for_hashing = 4096 * 32

DELETE_DB = True
GENERATE_THUMBNAIL = False


def get_development_config():
    """ Configuration for development"""
    print("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    config['move_or_copy'] = 'copy'

    # overwrite defaults with development specific params
    if os.name == 'nt':
        pth = 'h:\\incomming'
        config['inDirName'] = os.path.join(pth, 'inbox_test_a')
        config['outDirName'] = os.path.join(pth, 'inbox_clust_test')
    else:
        pth = '/home/izik/bulk/fc_data'
        config['inDirName'] = os.path.join(pth, 'mix2a')
        config['outDirName'] = os.path.join(pth, 'out')

    config['db_file'] = os.path.join(pth, 'filecluster_db.sqlite3')
    config['db_file_media'] = os.path.join(pth, 'media.p')
    config['db_file_cluster'] = os.path.join(pth, 'clusters.p')
    return config


def get_default_config():
    # path to files to be clustered

    # Configure inbox
    inbox_path = '/media/root/Foto/incomming/inbox'

    # Configure outbox
    outbox_path = '/media/root/Foto/incomming/inbox_clust/'

    # Configure database
    db_file = '/media/root/Foto/zdjecia/filecluster_db.sqlite3'
    db_file_clusters = '/media/root/Foto/zdjecia/clusters.p'
    db_file_media = '/media/root/Foto/zdjecia/media.p'

    # Filename extensions in scope of clustering
    image_extensions = ['.jpg', '.jpeg', '.dng', '.cr2']
    video_extensions = ['.mp4', '.3gp', 'mov']

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    # method that is used to group images, default: assume different events
    # are separated by significant time gape (max_gap config parameter)
    clustering_method = 'time_gap'

    assign_date_to_clusters_method = 'random'

    config = {
        'inDirName': inbox_path,
        'outDirName': outbox_path,
        'db_file_clusters': db_file_clusters,
        'db_file_media': db_file_media,
        'db_file_sqlite': db_file,
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'granularity_minutes': max_gap,
        'cluster_col': 'cluster_id',
        'assign_date_to_clusters_method': assign_date_to_clusters_method,
        'clustering_method': clustering_method,
        'move_or_copy': 'move',
        'db_driver': 'sqlite',  # dataframe | sqlite
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
    return is_image


def get_date_from_file(path_name):
    """
    get date information from photo file
    """

    m_time = time.ctime(os.path.getmtime(path_name))
    c_time = time.ctime(os.path.getctime(path_name))
    exif_date = get_exif_date(path_name)
    return m_time, c_time, exif_date


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
        os.makedirs(dir_name)
    except OSError as err:
        logger.error(err)


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
# from: https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
def print_progress(iteration, total, prefix='', suffix='', decimals=1,
                   bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent
        complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """

    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write(
        '\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


# modified version of
# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def hash_file(fname, hash_funct=hashlib.sha1):
    """hash funct can be e.g.: md5, sha1, sha256,..."""
    hash_value = hash_funct()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(block_size_for_hashing), b""):
            hash_value.update(chunk)
    return hash_value.hexdigest()
