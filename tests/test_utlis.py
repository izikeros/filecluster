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

    def test_algo_selects_blake3(self, assets_dir):
        img_pth = str(assets_dir / "set_1" / "IMG_3784.jpg")
        sha1 = hash_file(fname=img_pth, algo="sha1")
        b3 = hash_file(fname=img_pth, algo="blake3")
        assert sha1 == hash_file(fname=img_pth)  # default is sha1
        assert b3 != sha1
        assert len(b3) == 64  # blake3 default 32-byte digest, hex


class TestCrc32File:
    def test_returns_eight_hex_digits(self, tmp_path):
        from filecluster.utlis import crc32_file

        f = tmp_path / "a.bin"
        f.write_bytes(b"hello world")
        crc = crc32_file(str(f))
        assert crc is not None
        assert len(crc) == 8
        assert int(crc, 16) >= 0

    def test_changes_when_content_changes(self, tmp_path):
        from filecluster.utlis import crc32_file

        f = tmp_path / "a.bin"
        f.write_bytes(b"hello world")
        before = crc32_file(str(f))
        f.write_bytes(b"hello worlD")
        assert crc32_file(str(f)) != before

    def test_missing_file_returns_none(self, tmp_path):
        from filecluster.utlis import crc32_file

        assert crc32_file(str(tmp_path / "nope.bin")) is None


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


# ---------------------------------------------------------------------------
# Partial hashing
# ---------------------------------------------------------------------------
class TestGetPartialHash:
    """Hashing only the leading bytes of a file."""

    def test_returns_hex_digest(self, tmp_path):
        from filecluster.utlis import get_partial_hash

        f = tmp_path / "a.jpg"
        f.write_bytes(b"some-photo-bytes")
        assert isinstance(get_partial_hash(str(f)), str)

    def test_same_prefix_same_hash(self, tmp_path):
        from filecluster.utlis import get_partial_hash

        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(b"prefix")
        b.write_bytes(b"prefix")
        assert get_partial_hash(str(a)) == get_partial_hash(str(b))

    def test_only_leading_bytes_are_read(self, tmp_path):
        from filecluster.utlis import get_partial_hash

        a = tmp_path / "a.bin"
        b = tmp_path / "b.bin"
        a.write_bytes(b"headXXXX")
        b.write_bytes(b"headYYYY")
        assert get_partial_hash(str(a), size=4) == get_partial_hash(str(b), size=4)

    def test_algo_selects_blake3(self, tmp_path):
        from filecluster.utlis import get_partial_hash

        f = tmp_path / "a.bin"
        f.write_bytes(b"some-photo-bytes")
        md5 = get_partial_hash(str(f))  # default md5
        b3 = get_partial_hash(str(f), algo="blake3")
        assert md5 != b3
        assert len(md5) == 32 and len(b3) == 64

    def test_missing_file_returns_none(self, tmp_path):
        from filecluster.utlis import get_partial_hash

        assert get_partial_hash(str(tmp_path / "nope.jpg")) is None

    def test_still_importable_from_image_grouper(self):
        """The old import path must keep working for existing callers."""
        from filecluster import image_grouper
        from filecluster.utlis import get_partial_hash

        assert image_grouper.get_partial_hash is get_partial_hash


# ---------------------------------------------------------------------------
# Media walking
# ---------------------------------------------------------------------------
class TestWalkMediaFiles:
    def test_finds_nested_media(self, tmp_path):
        from filecluster.utlis import walk_media_files

        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "top.jpg").write_bytes(b"1")
        (tmp_path / "a" / "mid.jpg").write_bytes(b"2")
        (tmp_path / "a" / "b" / "deep.mp4").write_bytes(b"3")

        found = walk_media_files(tmp_path, [".jpg", ".mp4"])
        assert [p.name for p in found] == ["deep.mp4", "mid.jpg", "top.jpg"]

    def test_non_recursive_stays_at_top(self, tmp_path):
        from filecluster.utlis import walk_media_files

        (tmp_path / "sub").mkdir()
        (tmp_path / "top.jpg").write_bytes(b"1")
        (tmp_path / "sub" / "deep.jpg").write_bytes(b"2")

        found = walk_media_files(tmp_path, [".jpg"], recursive=False)
        assert [p.name for p in found] == ["top.jpg"]

    def test_unsupported_extensions_are_skipped(self, tmp_path):
        from filecluster.utlis import walk_media_files

        (tmp_path / "a.jpg").write_bytes(b"1")
        (tmp_path / "notes.txt").write_bytes(b"2")

        assert [p.name for p in walk_media_files(tmp_path, [".jpg"])] == ["a.jpg"]

    def test_noise_directories_are_pruned(self, tmp_path):
        from filecluster.utlis import walk_media_files

        (tmp_path / "@eaDir").mkdir()
        (tmp_path / "@eaDir" / "thumb.jpg").write_bytes(b"1")
        (tmp_path / "keep.jpg").write_bytes(b"2")

        assert [p.name for p in walk_media_files(tmp_path, [".jpg"])] == ["keep.jpg"]

    def test_empty_tree(self, tmp_path):
        from filecluster.utlis import walk_media_files

        assert walk_media_files(tmp_path, [".jpg"]) == []

    def test_progress_callback_reports_running_totals(self, tmp_path):
        """The walk reports headway, so a slow scan does not look like a hang."""
        from filecluster.utlis import walk_media_files

        for i in range(3):
            sub = tmp_path / f"sub_{i}"
            sub.mkdir()
            (sub / f"a_{i}.jpg").write_bytes(b"1")

        seen: list[tuple[int, int]] = []
        walk_media_files(
            tmp_path, [".jpg"], on_progress=lambda f, n: seen.append((f, n))
        )

        # One call per visited directory: the root plus the three subfolders.
        assert len(seen) == 4
        # Folder and file counts only ever grow.
        assert [folders for folders, _ in seen] == [1, 2, 3, 4]
        assert seen[-1][1] == 3

    def test_progress_callback_is_optional(self, tmp_path):
        """Library callers that pass nothing keep the original behaviour."""
        from filecluster.utlis import walk_media_files

        (tmp_path / "a.jpg").write_bytes(b"1")
        assert [p.name for p in walk_media_files(tmp_path, [".jpg"])] == ["a.jpg"]


# ---------------------------------------------------------------------------
# Sidecar detection
# ---------------------------------------------------------------------------
class TestFindSidecarFiles:
    def test_replaced_extension_sidecar(self, tmp_path):
        from filecluster.utlis import find_sidecar_files

        media = tmp_path / "IMG_1.jpg"
        media.write_bytes(b"photo")
        (tmp_path / "IMG_1.xmp").write_bytes(b"meta")

        assert [p.name for p in find_sidecar_files(media)] == ["IMG_1.xmp"]

    def test_appended_extension_sidecar(self, tmp_path):
        from filecluster.utlis import find_sidecar_files

        media = tmp_path / "IMG_1.jpg"
        media.write_bytes(b"photo")
        (tmp_path / "IMG_1.jpg.xmp").write_bytes(b"meta")

        assert [p.name for p in find_sidecar_files(media)] == ["IMG_1.jpg.xmp"]

    def test_similar_stem_is_not_a_sidecar(self, tmp_path):
        from filecluster.utlis import find_sidecar_files

        media = tmp_path / "IMG_1.jpg"
        media.write_bytes(b"photo")
        (tmp_path / "IMG_12.xmp").write_bytes(b"other photo's meta")

        assert find_sidecar_files(media) == []

    def test_other_media_is_not_a_sidecar(self, tmp_path):
        from filecluster.utlis import find_sidecar_files

        media = tmp_path / "IMG_1.jpg"
        media.write_bytes(b"photo")
        (tmp_path / "IMG_1.mp4").write_bytes(b"live photo video")

        assert find_sidecar_files(media) == []

    def test_multiple_sidecars_are_sorted(self, tmp_path):
        from filecluster.utlis import find_sidecar_files

        media = tmp_path / "IMG_1.jpg"
        media.write_bytes(b"photo")
        (tmp_path / "IMG_1.xmp").write_bytes(b"a")
        (tmp_path / "IMG_1.aae").write_bytes(b"b")

        assert [p.name for p in find_sidecar_files(media)] == [
            "IMG_1.aae",
            "IMG_1.xmp",
        ]


# ---------------------------------------------------------------------------
# Event-folder names
# ---------------------------------------------------------------------------
class TestEventFolderNames:
    def test_recognises_event_folder(self):
        from filecluster.utlis import is_event_folder_name

        assert is_event_folder_name("[2024_01_15]_birthday")
        assert not is_event_folder_name("birthday")

    def test_extracts_year(self):
        from filecluster.utlis import extract_year_from_folder

        assert extract_year_from_folder("[2024_01_15]_birthday") == "2024"
        assert extract_year_from_folder("nope") is None

    def test_extracts_date(self):
        from filecluster.utlis import extract_date_from_folder

        assert extract_date_from_folder("[2024_01_15]_x") == "2024_01_15"
        assert extract_date_from_folder("nope") is None
