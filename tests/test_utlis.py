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
# img_pth = "inbox_test_a_orig/20181117_121813.jpg"


# img_pth = ASSETS_DIR / "set_1" / "IMG_3784.jpg"


def test_is_supported_filetype_jpg_lower_case():
    assert is_supported_filetype("img.jpg", EXT_IMG) is True


def test_is_supported_filetype_jpg_upper_case():
    assert is_supported_filetype("img.JPG", EXT_IMG) is True


def test_is_supported_filetype_cr2():
    assert is_supported_filetype("img.cr2", EXT_IMG) is True


def test_is_supported_filetype_xyz():
    assert is_supported_filetype("img.xyz", EXT_IMG) is False


def test_is_supported_filetype_jpg_xyz():
    assert is_supported_filetype("img.jpg.xyz", EXT_IMG) is False


def test_is_image__jpg():
    assert is_image("img.jpg", EXT_IMG) is True


def test_is_image__not_recognized_image_type():
    assert is_image("img.xyz", EXT_IMG) is False


def test_is_image__video():
    assert is_image("img.mov", EXT_IMG) is False


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
