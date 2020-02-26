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
        try:
            exif_date = datetime.strptime(exif_date_str, '%Y:%m:%d %H:%M:%S')
        except ValueError:
            logger.error(f'Invalid date for file: {path_name}. Setting: None')
            exif_date = None
    except KeyError:
        exif_date = None

    return exif_date


def create_folder_for_cluster(config, date_string, mode):
    """ create designation folder that for all pictures from the cluster
    """
    if mode != 'nop':
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
