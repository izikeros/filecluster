"""Deep media integrity checks: full image decode and ffprobe validation.

These are the expensive, content-level checks used by ``catalog verify`` to
catch *structurally broken* media, not merely files whose size or mtime moved.
An image is fully decoded with Pillow; a video is validated with ``ffprobe``.
Both are opt-in because they read far more than the metadata layer does.
"""

from __future__ import annotations

import shutil
import subprocess
from enum import StrEnum
from pathlib import Path

from PIL import Image, ImageFile

from filecluster import logger

#: Image extensions whose pixels Pillow can fully decode in the base install.
#: RAW formats still need optional backends, so they are left to the hash check
#: and reported as ``SKIPPED`` by the decode pass.
DECODABLE_IMAGE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".jpg",
        ".jpeg",
        ".png",
        ".tif",
        ".tiff",
        ".bmp",
        ".gif",
        ".webp",
        ".heic",
        ".heif",
    }
)

#: How long a single ffprobe call may run before it is treated as unreadable.
FFPROBE_TIMEOUT_S = 60


class IntegrityStatus(StrEnum):
    """Outcome of a single content-level integrity check."""

    OK = "ok"
    CORRUPT = "corrupt"
    UNREADABLE = "unreadable"
    SKIPPED = "skipped"


def ffprobe_available() -> bool:
    """Whether an ``ffprobe`` binary is on PATH."""
    return shutil.which("ffprobe") is not None


def verify_image(path: str | Path) -> IntegrityStatus:
    """Fully decode *path* and report whether the pixel data is intact.

    Returns ``CORRUPT`` when the file cannot be decoded (truncated JPEG, broken
    data stream, unsupported/garbage content) and ``UNREADABLE`` when the file
    cannot be read at all (permissions). The decompression-bomb guard is lifted
    for the duration so a legitimately huge panorama is not flagged.
    """
    prev_truncated = ImageFile.LOAD_TRUNCATED_IMAGES
    prev_max_pixels = Image.MAX_IMAGE_PIXELS
    # A truncated file must raise here rather than silently loading partial
    # pixels, which is exactly the corruption we want to catch.
    ImageFile.LOAD_TRUNCATED_IMAGES = False
    Image.MAX_IMAGE_PIXELS = None
    try:
        with Image.open(path) as im:
            im.load()
    except PermissionError as exc:
        logger.debug(f"Image unreadable (permissions): {path}: {exc}")
        return IntegrityStatus.UNREADABLE
    except (OSError, SyntaxError, ValueError) as exc:
        logger.debug(f"Image failed to decode: {path}: {exc}")
        return IntegrityStatus.CORRUPT
    finally:
        ImageFile.LOAD_TRUNCATED_IMAGES = prev_truncated
        Image.MAX_IMAGE_PIXELS = prev_max_pixels
    return IntegrityStatus.OK


def verify_video(
    path: str | Path, *, timeout: int = FFPROBE_TIMEOUT_S
) -> IntegrityStatus:
    """Validate *path* with ``ffprobe``.

    Returns ``SKIPPED`` when ffprobe is not installed, ``CORRUPT`` when ffprobe
    reports an error (non-zero exit, or any error-level diagnostics on stderr),
    ``UNREADABLE`` when ffprobe cannot be run or times out, and ``OK`` otherwise.
    """
    if not ffprobe_available():
        return IntegrityStatus.SKIPPED
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "stream=codec_type",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.debug(f"ffprobe timed out on {path}")
        return IntegrityStatus.UNREADABLE
    except OSError as exc:
        logger.debug(f"Could not run ffprobe on {path}: {exc}")
        return IntegrityStatus.UNREADABLE

    # `-v error` keeps stderr empty for a clean file; any content there means
    # ffprobe found a container/stream problem worth surfacing.
    if proc.returncode != 0 or proc.stderr.strip():
        logger.debug(
            f"ffprobe reported problems on {path}: "
            f"rc={proc.returncode} err={proc.stderr.strip()[:200]}"
        )
        return IntegrityStatus.CORRUPT
    return IntegrityStatus.OK
