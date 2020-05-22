import hashlib

import pytest

from filecluster.utlis import get_exif_date, is_supported_filetype, is_image, get_date_from_file, \
    create_folder_for_cluster, get_thumbnail, image_base64, image_formatter, hash_file

EXT_IMG = ['.jpg', '.CR2']
EXT_VID = ['.mp4', '.3gp']
IMG_PTH = 'inbox_test_a_orig/20181117_121813.jpg'


def test_is_supported_filetype_jpg_lower_case():
    assert is_supported_filetype('img.jpg', EXT_IMG) is True


def test_is_supported_filetype_jpg_upper_case():
    assert is_supported_filetype('img.JPG', EXT_IMG) is True


def test_is_supported_filetype_cr2():
    assert is_supported_filetype('img.cr2', EXT_IMG) is True


def test_is_supported_filetype_xyz():
    assert is_supported_filetype('img.xyz', EXT_IMG) is False


def test_is_supported_filetype_jpg_xyz():
    assert is_supported_filetype('img.jpg.xyz', EXT_IMG) is False


def test_is_image__image():
    assert is_image('img.jpg', EXT_IMG) is True


def test_is_image__image():
    assert is_image('img.xyz', EXT_IMG) is False


def test_is_image__video():
    assert is_image('img.mov', EXT_IMG) is False


def test_get_date_from_file():
    get_date_from_file(path_name=IMG_PTH)


def test_get_exif_date():
    get_exif_date(path_name=IMG_PTH)


@pytest.mark.skip()
def test_create_folder_for_cluster():
    create_folder_for_cluster()


def test_get_thumbnail():
    get_thumbnail(path=IMG_PTH)


def test_image_base64():
    image_base64(img=IMG_PTH)


def test_image_formatter():
    image_formatter(im=IMG_PTH)


def test_hash_file():
    hash_file(fname=IMG_PTH, hash_funct=hashlib.sha1)
