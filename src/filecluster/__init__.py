"""Image and video clustering by the event time."""

from loguru import logger
from pillow_heif import register_heif_opener

__all__ = ["initialize_image_support", "logger"]


_image_support_initialized = False


def initialize_image_support() -> None:
    """Register optional Pillow image plugins before opening media files.

    Importing ``filecluster`` deliberately has no process-wide side effects.
    Image readers call this idempotent function immediately before using
    Pillow, which keeps HEIC/HEIF support available to every workflow.
    """
    global _image_support_initialized
    if not _image_support_initialized:
        register_heif_opener()
        _image_support_initialized = True
