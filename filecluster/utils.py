# Copyright (c) 2017 Krystian Safjan
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import base64
import os
import time
from PIL import Image
from datetime import datetime, timedelta
from io import BytesIO

import exifread


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
    except OSError:
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
