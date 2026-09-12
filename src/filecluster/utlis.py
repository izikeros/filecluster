"""Helper utilities to work with media files, exif data, hashing and base64 images."""

import base64
import hashlib
import logging
import os
import re
import zlib
from collections.abc import Callable
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import blake3
import exifread
from PIL import Image

from filecluster import initialize_image_support, logger
from filecluster.configuration import Config, CopyMode
from filecluster.exceptions import DateStringNoneError

# Suppress exifread's "File format not recognized" warnings
logging.getLogger("exifread").setLevel(logging.CRITICAL)

BLOCK_SIZE_FOR_HASHING = 4096 * 32

#: Hash algorithms the catalog can use, keyed by the name stored in the
#: ``hash_algo`` column. ``blake3`` is the modern default for opt-in builds;
#: ``md5``/``sha1`` remain for the legacy fast-prefilter/full-hash split.
HASH_CONSTRUCTORS: dict[str, Callable[[], Any]] = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "blake3": blake3.blake3,
}

#: Event-folder names produced by filecluster: ``[YYYY_MM_DD]_optional_name``.
EVENT_FOLDER_RE = re.compile(r"^\[(\d{4})_(\d{2})_(\d{2})\]")

#: How many leading bytes go into a partial hash. One megabyte is enough to
#: separate distinct photos while staying far cheaper than a full read.
PARTIAL_HASH_SIZE = 1024 * 1024

#: Extensions of companion files that belong to a media file and must travel
#: with it: editing sidecars, Apple adjustment data, GoPro low-res proxies,
#: thumbnails, subtitles and per-file JSON exports.
SIDECAR_EXTENSIONS: tuple[str, ...] = (
    ".xmp",
    ".aae",
    ".json",
    ".thm",
    ".lrv",
    ".srt",
    ".pp3",
    ".dop",
    ".on1",
    ".acr",
)

#: Directory names never worth walking into when scanning a media library.
SKIP_DIR_NAMES: frozenset[str] = frozenset(
    {
        ".git",
        ".svn",
        "@eaDir",
        ".Trash",
        ".Trashes",
        "#recycle",
        "$RECYCLE.BIN",
        ".thumbnails",
        "__pycache__",
    }
)


def is_supported_filetype(file_name: str, ext_list: list[str]) -> bool:
    """Check if the filename has one of the allowed extensions from the list."""
    ext_list_lower = [ext.lower() for ext in ext_list]
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext_list_lower))


def is_sidecar_file(file_name: str) -> bool:
    """Whether *file_name* looks like a companion file rather than media."""
    return file_name.lower().endswith(SIDECAR_EXTENSIONS)


def get_partial_hash(
    filepath, size: int = PARTIAL_HASH_SIZE, *, algo: str = "md5"
) -> str | None:
    """Hash the first *size* bytes of a file, or None if it cannot be read.

    *algo* selects the digest (``md5`` by default for the fast dedup
    prefilter; ``blake3`` when the catalog is built with that algorithm).
    """
    try:
        with open(filepath, "rb") as f:
            hasher = HASH_CONSTRUCTORS[algo]()
            hasher.update(f.read(size))
            return hasher.hexdigest()
    except OSError:
        return None


def walk_media_files(
    root: str | Path,
    ext_list: list[str],
    *,
    recursive: bool = True,
    skip_dir_names: frozenset[str] = SKIP_DIR_NAMES,
    on_progress: Callable[[int, int], None] | None = None,
) -> list[Path]:
    """Collect every supported media file under *root*.

    Args:
        root: Directory to scan.
        ext_list: Recognised media extensions.
        recursive: When False, only direct children of *root* are returned.
        skip_dir_names: Directory names to prune from the walk.
        on_progress: Called with ``(n_folders, n_files)`` found so far, once per
            visited directory. Walking a large or network-mounted tree takes
            long enough that the caller needs to show it is making headway.

    Returns:
        Sorted list of media file paths.
    """
    root = Path(root)
    found: list[Path] = []
    for n_folders, (dirpath, dirnames, filenames) in enumerate(os.walk(root), start=1):
        dirnames[:] = [d for d in dirnames if d not in skip_dir_names]
        if not recursive:
            dirnames.clear()
        here = Path(dirpath)
        found.extend(
            here / name for name in filenames if is_supported_filetype(name, ext_list)
        )
        if on_progress is not None:
            on_progress(n_folders, len(found))
    return sorted(found)


def is_event_folder_name(name: str) -> bool:
    """Whether *name* looks like a filecluster event folder."""
    return EVENT_FOLDER_RE.match(name) is not None


def extract_year_from_folder(name: str) -> str | None:
    """Extract the year from an event folder name like ``[2024_01_15]…``."""
    m = EVENT_FOLDER_RE.match(name)
    return m.group(1) if m else None


def extract_date_from_folder(name: str) -> str | None:
    """Extract ``YYYY_MM_DD`` from an event folder name."""
    m = EVENT_FOLDER_RE.match(name)
    return f"{m.group(1)}_{m.group(2)}_{m.group(3)}" if m else None


def find_sidecar_files(media_path: str | Path) -> list[Path]:
    """Return companion files that belong to *media_path*.

    Both naming conventions are recognised: ``IMG_001.xmp`` (extension
    replaced) and ``IMG_001.jpg.xmp`` (extension appended).
    """
    media_path = Path(media_path)
    parent = media_path.parent
    if not parent.is_dir():
        return []

    full_name = media_path.name.lower()
    stem = media_path.stem.lower()
    out: list[Path] = []
    for entry in parent.iterdir():
        name = entry.name
        if name == media_path.name or not is_sidecar_file(name):
            continue
        base = name[: len(name) - len(Path(name).suffix)].lower()
        if base in (stem, full_name) and entry.is_file():
            out.append(entry)
    return sorted(out)


def is_image(file_name: str, ext_list_image: list[str]) -> bool:
    """Determine if a file is an image based on known file name extensions."""
    ext_list_lower = [ext.lower() for ext in ext_list_image]
    fn_lower = file_name.lower()
    return fn_lower.endswith(tuple(ext_list_lower))


def get_date_from_file(path_name: str):
    """Get date information from a photo file.

    Returns:
        Tuple of (m_time, c_time, exif_date) as datetime objects.
        m_time and c_time are always present; exif_date may be None.
    """
    m_time = datetime.fromtimestamp(os.path.getmtime(path_name))
    c_time = datetime.fromtimestamp(os.path.getctime(path_name))
    exif_date = get_exif_date(path_name)
    return m_time, c_time, exif_date


def get_exif_date(path_name: str):
    """Return exif date or none."""
    initialize_image_support()
    # Open the image file for reading (binary mode)
    with open(path_name, "rb") as img_file:
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
            try:
                exif_date = datetime.strptime(exif_date_str, "%Y:%m:%d %H:%M:%S.%f")
            except ValueError:
                logger.error(
                    f"Invalid date {exif_date_str} for file: {path_name}. Setting: None"
                )
                exif_date = None
    except KeyError:
        exif_date = None
        logger.debug(f"No EXIF date for file: {path_name}")

    return exif_date


def create_folder_for_cluster(config: Config, date_string: str, mode: CopyMode):
    """Create a destination folder that for all pictures from the cluster."""
    if date_string is None:
        raise DateStringNoneError()

    if mode != CopyMode.NOP:
        pth = Path(config.out_dir_name)
        if not isinstance(date_string, str):
            logger.error(
                f"Expected date string got: {date_string} of type: {type(date_string)}"
            )
        dir_name = (
            pth / date_string
        )  # fixme: can raise error: TypeError: unsupported operand type(s) for /: 'WindowsPath' and 'float'
        try:
            os.makedirs(dir_name, exist_ok=True)
        except OSError as err:
            logger.error(err)


def get_thumbnail(path, width: int = 150, height: int = 150):
    """Read image and create thumbnail of given size."""
    initialize_image_support()
    i = Image.open(path)
    i.thumbnail((width, height), Image.Resampling.LANCZOS)
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
    return f'<img src="data:image/jpeg;base64,{image_base64(im_base64)}">'


def hash_file(fname, hash_funct=hashlib.sha1, *, algo: str | None = None):
    """Hash a whole file, streaming it in blocks.

    *hash_funct* is a ``hashlib``-style constructor (default SHA-1). Pass
    *algo* (``"sha1"`` or ``"blake3"``) to select the digest by name instead;
    it overrides *hash_funct* and is what the catalog uses to hash with the
    algorithm recorded for a build.
    """
    # modified version of
    # https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
    constructor = HASH_CONSTRUCTORS[algo] if algo is not None else hash_funct
    hash_value = constructor()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(BLOCK_SIZE_FOR_HASHING), b""):
            hash_value.update(chunk)
    return hash_value.hexdigest()


def crc32_file(fname) -> str | None:
    """Return the CRC32 of a whole file as 8 lowercase hex digits, or None.

    CRC32 is a cheap, non-cryptographic checksum: it cannot detect deliberate
    tampering but is a fast way to catch bit rot on repeated verifications.
    Returns None when the file cannot be read.
    """
    checksum = 0
    try:
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(BLOCK_SIZE_FOR_HASHING), b""):
                checksum = zlib.crc32(chunk, checksum)
    except OSError:
        return None
    return f"{checksum & 0xFFFFFFFF:08x}"


# def read_version():
#     with open(ROOT_DIR / "pyproject.toml", "rb") as f:
#         pyproject = tomllib.load(f)
#
#     return pyproject["project"]["version"]
