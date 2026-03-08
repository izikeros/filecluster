import hashlib

import pytest

from filecluster.configuration import CopyMode, get_default_config
from filecluster.utlis import (
    create_folder_for_cluster,
    get_date_from_file,
    get_exif_date,
    get_thumbnail,
    hash_file,
    image_base64,
    image_formatter,
    is_image,
    is_supported_filetype,
)

EXT_IMG = [".jpg", ".CR2"]
EXT_VID = [".mp4", ".3gp"]


@pytest.mark.parametrize(
    "filename, expected, label",
    [
        ("img.jpg", True, "jpg_lower_case"),
        ("img.JPG", True, "jpg_upper_case"),
        ("img.cr2", True, "cr2_file"),
        ("img.xyz", False, "unsupported_filetype"),
        ("img.jpg.xyz", False, "invalid_extension"),
    ],
)
def test_is_supported_filetype(filename, expected, label):
    assert is_supported_filetype(filename, EXT_IMG) is expected


@pytest.mark.parametrize(
    "filename, expected",
    [
        ("img.jpg", True),
        ("img.xyz", False),
        ("img.mov", False),
    ],
)
def test_is_image(filename, expected):
    assert is_image(filename, EXT_IMG) is expected


def test_get_date_from_file(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    get_date_from_file(path_name=img_pth)


def test_get_exif_date(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    get_exif_date(path_name=img_pth)


@pytest.mark.skip()
def test_create_folder_for_cluster():
    config = get_default_config()
    create_folder_for_cluster(
        config=config, date_string="[2020_11_21]", mode=CopyMode.NOP
    )


def test_get_thumbnail(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    get_thumbnail(path=img_pth)


def test_image_base64(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    image_base64(img=str(img_pth))


def test_image_formatter(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    image_formatter(im_base64=str(img_pth))


def test_hash_file(assets_dir):
    img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
    hash_file(fname=str(img_pth), hash_funct=hashlib.sha1)
