"""Reason codes explaining a curation verdict.

Codes are namespaced by the stage that emits them and are part of the report
format, so they are treated as an API: rename one only together with the cache
``pipeline_version``.
"""

from __future__ import annotations

from typing import Final

# -- metadata rules ---------------------------------------------------------
SCREENSHOT_FILENAME: Final = "metadata.screenshot_filename"
SCREEN_RESOLUTION_MATCH: Final = "metadata.screen_resolution_match"
SCREENSHOT_SOFTWARE: Final = "metadata.screenshot_software"
PNG_WITHOUT_CAMERA_EXIF: Final = "metadata.png_without_camera_exif"
CAMERA_EXIF_PRESENT: Final = "metadata.camera_exif_present"
NO_CAMERA_EXIF: Final = "metadata.no_camera_exif"
APP_GENERATED_FORMAT: Final = "metadata.app_generated_format"
EXTREME_ASPECT_RATIO: Final = "metadata.extreme_aspect_ratio"
UNSUPPORTED_MEDIA_TYPE: Final = "metadata.unsupported_media_type"

#: Signals strong enough to count towards an automatic reject. Two of these
#: agreeing, or one decisive signal, is the bar set by the specification.
STRONG_UTILITY_SIGNALS: Final[frozenset[str]] = frozenset(
    {
        SCREENSHOT_FILENAME,
        SCREEN_RESOLUTION_MATCH,
        SCREENSHOT_SOFTWARE,
        PNG_WITHOUT_CAMERA_EXIF,
    }
)

#: Signals produced by the operating system itself, trusted on their own.
DECISIVE_UTILITY_SIGNALS: Final[frozenset[str]] = frozenset({SCREENSHOT_SOFTWARE})

# -- lightweight image features --------------------------------------------
LOW_SHARPNESS: Final = "features.low_sharpness"
OVEREXPOSED: Final = "features.overexposed"
UNDEREXPOSED: Final = "features.underexposed"
LOW_CONTRAST: Final = "features.low_contrast"
LOW_INFORMATION: Final = "features.low_information"
DOCUMENT_LIKE: Final = "features.document_like"
UNIFORM_BACKGROUND: Final = "features.uniform_background"
PHOTOGRAPHIC_COLOR: Final = "features.photographic_color"

# -- OCR --------------------------------------------------------------------
HIGH_TEXT_DENSITY: Final = "ocr.high_text_density"
TEXT_ABSENT: Final = "ocr.text_absent"

# -- semantic / model stages ------------------------------------------------
SEMANTIC_PERSONAL: Final = "semantic.personal"
SEMANTIC_UTILITY: Final = "semantic.utility"
SEMANTIC_SCREENSHOT: Final = "semantic.screenshot"
SEMANTIC_LOW_MARGIN: Final = "semantic.low_margin"
VLM_DECISION: Final = "vlm.decision"

#: Reject needs at least one of these, so a purely technical or metadata-only
#: signal can never discard a photograph on its own.
SEMANTIC_REJECT_REASONS: Final[frozenset[str]] = frozenset(
    {
        SEMANTIC_UTILITY,
        SEMANTIC_SCREENSHOT,
        VLM_DECISION,
    }
)

# -- fusion -----------------------------------------------------------------
SCORE_ABOVE_KEEP: Final = "fusion.score_above_keep"
SCORE_BELOW_REJECT: Final = "fusion.score_below_reject"
SCORE_IN_BAND: Final = "fusion.score_in_uncertainty_band"
LOW_CONFIDENCE: Final = "fusion.low_confidence"
SIGNAL_CONFLICT: Final = "fusion.signal_conflict"
PROTECTED_SUBJECT: Final = "fusion.protected_subject"
NO_SEMANTIC_EVIDENCE: Final = "fusion.no_semantic_evidence"
MISSING_SIGNALS: Final = "fusion.missing_signals"

# -- processing failures ----------------------------------------------------
DECODE_ERROR: Final = "processing.decode_error"
FILE_MISSING: Final = "processing.file_missing"
IMAGE_TOO_LARGE: Final = "processing.image_too_large"
STAGE_ERROR: Final = "processing.stage_error"
PROVIDER_UNAVAILABLE: Final = "processing.provider_unavailable"
