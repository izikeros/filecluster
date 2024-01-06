"""Helper utilities to work with media files, exif data, hashing and base64 images."""
import base64
import hashlib
import os
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path

import exifread
from PIL import Image
from filecluster import logger
from filecluster.configuration import Config
from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneException

# TODO: optimize this parameter for speed
BLOCK_SIZE_FOR_HASHING = 4096 * 32


def is_supported_filetype(file_name: str, ext_list: list[str]) -> bool:
    """Check if filename has one of the allowed extensions from the list."""
    ext_list_lower = [ext.lower() for ext in ext_list]
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext_list_lower))


def is_image(file_name: str, ext_list_image: list[str]) -> bool:
    """Determine if file is image based on known file name extensions."""
    ext_list_lower = [ext.lower() for ext in ext_list_image]
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext_list_lower))


def get_date_from_file(path_name: str):
    """Get date information from photo file."""
    m_time = time.ctime(os.path.getmtime(path_name))
    c_time = time.ctime(os.path.getctime(path_name))
    exif_date = get_exif_date(path_name)
    return m_time, c_time, exif_date


def get_exif_date(path_name: str):
    """Return exif date or none."""
    # Open image file for reading (binary mode)
    img_file = open(path_name, "rb")

    # Return Exif tags
    try:
        tags = exifread.process_file(
            img_file, details=False, stop_tag="EXIF DateTimeOriginal"
        )
    except Exception:
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
        dir_name = (
            pth / date_string
        )  # fixme: can raise error: TypeError: unsupported operand type(s) for /: 'WindowsPath' and 'float'
        try:
            os.makedirs(dir_name, exist_ok=True)
        except OSError as err:
            logger.error(err)


def get_thumbnail(path, width: int = 150, height: int = 150):
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
    """HTML template to display base64 image."""
    return '<img src="data:image/jpeg;base64,{image_tn}">'.format(
        image_tn=image_base64(im_base64)
    )


def hash_file(fname, hash_funct=hashlib.sha1):
    """Hash function can be e.g.: md5, sha1, sha256,..."""
    # modified version of
    # https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    hash_value = hash_funct()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(BLOCK_SIZE_FOR_HASHING), b""):
            hash_value.update(chunk)
    return hash_value.hexdigest()
