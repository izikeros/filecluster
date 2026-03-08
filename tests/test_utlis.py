"""Tests for the utlis (utilities) module.

Covers file-type checking, EXIF date extraction, file hashing, folder creation,
thumbnail generation, and base64 image conversion.
"""

import hashlib
from datetime import datetime

import pytest

from filecluster.configuration import CopyMode, get_default_config
from filecluster.exceptions import DateStringNoneError
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


# ---------------------------------------------------------------------------
# is_supported_filetype
# ---------------------------------------------------------------------------
class TestIsSupportedFiletype:
    """Tests for the is_supported_filetype function.

    Business rule: only files whose extension is in the allowed list should be
    processed. Comparison must be case-insensitive.
    """

    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("img.jpg", True),
            ("img.JPG", True),
            ("img.Jpg", True),
            ("img.cr2", True),
            ("img.CR2", True),
            ("img.xyz", False),
            ("img.jpg.xyz", False),
            ("", False),
            ("no_extension", False),
            (".jpg", True),
        ],
        ids=[
            "lowercase_match",
            "uppercase_match",
            "mixed_case_match",
            "cr2_lower",
            "cr2_upper",
            "unsupported_ext",
            "double_ext_last_wins",
            "empty_string",
            "no_extension",
            "dot_only_filename",
        ],
    )
    def test_extension_matching(self, filename, expected):
        """Verify case-insensitive extension matching and rejection of unsupported."""
        assert is_supported_filetype(filename, EXT_IMG) is expected

    def test_empty_extension_list_rejects_everything(self):
        """With no allowed extensions, all files should be rejected."""
        assert is_supported_filetype("img.jpg", []) is False


# ---------------------------------------------------------------------------
# is_image
# ---------------------------------------------------------------------------
class TestIsImage:
    """Tests for the is_image function.

    Business rule: distinguish images from videos based on extension so the
    clustering folder names can include image/video counts.
    """

    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("photo.jpg", True),
            ("photo.JPG", True),
            ("video.mp4", False),
            ("video.mov", False),
            ("photo.cr2", True),
        ],
    )
    def test_image_vs_non_image(self, filename, expected):
        """Correctly distinguish images from non-images."""
        assert is_image(filename, EXT_IMG) is expected


# ---------------------------------------------------------------------------
# get_date_from_file / get_exif_date
# ---------------------------------------------------------------------------
class TestDateExtraction:
    """Tests for extracting dates from real image files.

    Uses actual JPEG files from test assets to verify that EXIF reading works
    end-to-end without mocking.
    """

    def test_get_date_from_file_returns_three_values(self, assets_dir):
        """
        Test Description: get_date_from_file returns (m_time, c_time, exif_date).

        Purpose: Downstream logic requires all three timestamps for disambiguation.
        m_time and c_time come from the OS and are always present. exif_date
        may be None if the file has no EXIF data.
        """
        img_pth = assets_dir / "set_1" / "IMG_4026.JPG"
        m_time, c_time, exif_date = get_date_from_file(path_name=str(img_pth))
        assert m_time is not None
        assert c_time is not None
        assert isinstance(exif_date, datetime)

    def test_get_date_from_file_no_exif(self, assets_dir):
        """Files without EXIF still return m_time and c_time; exif_date is None."""
        img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
        m_time, c_time, exif_date = get_date_from_file(path_name=str(img_pth))
        assert m_time is not None
        assert c_time is not None
        assert exif_date is None

    def test_get_exif_date_returns_datetime_for_jpeg_with_exif(self, assets_dir):
        """
        Test Description: EXIF date is a datetime for a file known to have EXIF data.

        Purpose: The clustering algorithm relies on EXIF dates being real datetimes.
        """
        img_pth = assets_dir / "set_1" / "IMG_4026.JPG"
        exif_date = get_exif_date(path_name=str(img_pth))
        assert isinstance(exif_date, datetime)
        assert exif_date.year == 2018

    def test_get_exif_date_returns_none_for_file_without_exif(self, assets_dir):
        """JPEG without EXIF data returns None."""
        img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
        exif_date = get_exif_date(path_name=str(img_pth))
        assert exif_date is None

    def test_get_exif_date_returns_none_for_non_image(self, assets_dir):
        """MOV file without standard EXIF returns None."""
        mov_pth = assets_dir / "set_1" / "IMG_2250.MOV"
        exif_date = get_exif_date(path_name=str(mov_pth))
        assert exif_date is None


# ---------------------------------------------------------------------------
# create_folder_for_cluster
# ---------------------------------------------------------------------------
class TestCreateFolderForCluster:
    """Tests for cluster folder creation.

    Mocking Strategy: We use tmp_path for file system operations. No external
    APIs are involved.
    """

    def test_creates_directory_in_copy_mode(self, tmp_path):
        """
        Test Description: In COPY mode, the target directory is created.

        Purpose: Files must have a destination before being copied.
        """
        config = get_default_config()
        config.out_dir_name = tmp_path
        create_folder_for_cluster(
            config=config, date_string="[2020_11_21]", mode=CopyMode.COPY
        )
        assert (tmp_path / "[2020_11_21]").is_dir()

    def test_nop_mode_does_not_create_directory(self, tmp_path):
        """
        Test Description: In NOP mode, no directory is created.

        Purpose: Dry-run must have zero side effects.
        """
        config = get_default_config()
        config.out_dir_name = tmp_path
        create_folder_for_cluster(
            config=config, date_string="[2020_11_21]", mode=CopyMode.NOP
        )
        assert not (tmp_path / "[2020_11_21]").exists()

    def test_none_date_string_raises_error(self, tmp_path):
        """
        Test Description: A None date_string raises DateStringNoneError.

        Purpose: Prevents creation of folders with meaningless names.
        """
        config = get_default_config()
        config.out_dir_name = tmp_path
        with pytest.raises(DateStringNoneError):
            create_folder_for_cluster(
                config=config, date_string=None, mode=CopyMode.COPY
            )

    def test_creates_nested_path(self, tmp_path):
        """
        Test Description: Nested paths (new/[date]) are created recursively.

        Purpose: ImageGrouper builds paths like 'new/[2020_01_01]_...'
        """
        config = get_default_config()
        config.out_dir_name = tmp_path
        create_folder_for_cluster(
            config=config, date_string="new/[2020_11_21]_event", mode=CopyMode.COPY
        )
        assert (tmp_path / "new" / "[2020_11_21]_event").is_dir()


# ---------------------------------------------------------------------------
# hash_file
# ---------------------------------------------------------------------------
class TestHashFile:
    """Tests for file hashing."""

    def test_returns_hex_string(self, assets_dir):
        """Hash output should be a hex string (40 chars for SHA-1)."""
        img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
        h = hash_file(fname=str(img_pth))
        assert isinstance(h, str)
        assert len(h) == 40  # SHA-1 hex digest length

    def test_same_file_same_hash(self, assets_dir):
        """Hashing the same file twice must produce identical results."""
        img_pth = str(assets_dir / "set_1" / "IMG_3784.jpg")
        h1 = hash_file(fname=img_pth)
        h2 = hash_file(fname=img_pth)
        assert h1 == h2

    def test_different_files_different_hash(self, assets_dir):
        """Different files should produce different hashes."""
        h1 = hash_file(fname=str(assets_dir / "set_1" / "IMG_3784.jpg"))
        h2 = hash_file(fname=str(assets_dir / "set_1" / "IMG_4128.jpg"))
        assert h1 != h2

    def test_custom_hash_function(self, assets_dir):
        """Allow overriding the hash algorithm (e.g. md5)."""
        img_pth = str(assets_dir / "set_1" / "IMG_3784.jpg")
        h = hash_file(fname=img_pth, hash_funct=hashlib.md5)
        assert isinstance(h, str)
        assert len(h) == 32  # MD5 hex digest length


# ---------------------------------------------------------------------------
# Thumbnail and base64
# ---------------------------------------------------------------------------
class TestImageConversion:
    """Tests for thumbnail generation and base64 encoding."""

    def test_get_thumbnail_returns_pil_image(self, assets_dir):
        """Thumbnail should return a PIL Image object with correct bounds."""
        from PIL import Image

        img_pth = assets_dir / "set_1" / "IMG_3784.jpg"
        thumb = get_thumbnail(path=img_pth, width=100, height=100)
        assert isinstance(thumb, Image.Image)
        assert thumb.size[0] <= 100
        assert thumb.size[1] <= 100

    def test_image_base64_returns_non_empty_string(self, assets_dir):
        """Base64 encoding should produce a non-empty string."""
        img_pth = str(assets_dir / "set_1" / "IMG_3784.jpg")
        b64 = image_base64(img_pth)
        assert isinstance(b64, str)
        assert len(b64) > 0

    def test_image_formatter_contains_img_tag(self, assets_dir):
        """HTML formatter should wrap the image in an <img> tag with data URI."""
        img_pth = str(assets_dir / "set_1" / "IMG_3784.jpg")
        html = image_formatter(im_base64=img_pth)
        assert html.startswith('<img src="data:image/jpeg;base64,')
        assert html.endswith('">')
