import base64
import hashlib
import logging
import os
import sys
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Callable, List

import exifread
from PIL import Image

from filecluster.configuration import CopyMode, Config
from filecluster.exceptions import DateStringNoneException

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: optimize this parameter for speed
block_size_for_hashing = 4096 * 32


def is_supported_filetype(file_name: str, ext_list: List[str]) -> bool:
    """Check if filename has one of the allowed extensions from the list."""
    ext_list_lower = [ext.lower() for ext in ext_list]
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext_list_lower))


def is_image(file_name: str, ext_list_image: List[str]) -> bool:
    """Determine if file is image based on known file name extensions."""
    ext_list_lower = [ext.lower() for ext in ext_list_image]
    fn_lower = file_name.lower()
    is_image = fn_lower.endswith(tuple(ext_list_lower))
    return is_image


def get_date_from_file(path_name):
    """Get date information from photo file."""

    m_time = time.ctime(os.path.getmtime(path_name))
    c_time = time.ctime(os.path.getctime(path_name))
    exif_date = get_exif_date(path_name)
    return m_time, c_time, exif_date


def get_exif_date(path_name):
    """Return exif date or none."""

    # Open image file for reading (binary mode)
    img_file = open(path_name, "rb")

    # Return Exif tags
    try:
        tags = exifread.process_file(
            img_file, details=False, stop_tag="EXIF DateTimeOriginal"
        )
    except Exception as ex:
        tags = {}

    try:
        exif_date_str = tags["EXIF DateTimeOriginal"].values
        try:
            exif_date = datetime.strptime(exif_date_str, "%Y:%m:%d %H:%M:%S")
        except ValueError:
            logger.error(f"Invalid date for file: {path_name}. Setting: None")
            exif_date = None
    except KeyError:
        exif_date = None

    return exif_date


def create_folder_for_cluster(config: Config, date_string: str, mode: CopyMode):
    """Create destination folder that for all pictures from the cluster."""

    if date_string is None:
        raise DateStringNoneException()

    if mode != CopyMode.NOP:
        pth = Path(config.out_dir_name)
        dir_name = pth / date_string
        try:
            os.makedirs(dir_name)
        except OSError as err:
            logger.error(err)


def get_thumbnail(path, width=150, height=150):
    """Read image and create thumbnail of given size."""
    i = Image.open(path)
    i.thumbnail((width, height), Image.LANCZOS)
    return i


def image_base64(img):
    """Return image as base64."""
    if isinstance(img, str):
        img = get_thumbnail(img)
    with BytesIO() as buffer:
        img.save(buffer, "jpeg")
        return base64.b64encode(buffer.getvalue()).decode()


def image_formatter(im_base64):
    """HTML template to display base64 image"""
    return '<img src="data:image/jpeg;base64,{image_tn}">'.format(
        image_tn=image_base64(im_base64)
    )


# Print iterations progress
# from: https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
# TODO: KS: 2020-04-17: Consider using tqdm instead
def print_progress(iteration, total, prefix="", suffix="", decimals=1, bar_length=100):
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

    if total > 0:
        str_format = "{0:." + str(decimals) + "f}"
        percents = str_format.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = "█" * filled_length + "-" * (bar_length - filled_length)

        sys.stdout.write("\r%s |%s| %s%s %s" % (prefix, bar, percents, "%", suffix)),

        if iteration == total:
            sys.stdout.write("\n")
        sys.stdout.flush()


# modified version of
# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def hash_file(fname, hash_funct=hashlib.sha1):
    """Hash function can be e.g.: md5, sha1, sha256,..."""
    hash_value = hash_funct()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(block_size_for_hashing), b""):
            hash_value.update(chunk)
    return hash_value.hexdigest()
