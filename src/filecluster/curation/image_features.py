"""Stage 2: lightweight image features computed from the pixels.

One decode per file, downscaled once, and every feature derived from that single
working copy. The stage also publishes the working image on the context so the
optional OCR and semantic stages never decode the same file again.

None of the features here may reject a file on their own: a blurry photo of a
child is still a photo of a child. They contribute a technical-quality signal
and evidence for the "this is a document" hypothesis.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter

import numpy as np

from filecluster import logger
from filecluster.curation import reasons
from filecluster.curation.configuration import CurationSettings
from filecluster.curation.types import (
    CurationContext,
    CurationDecision,
    MediaItem,
    StageResult,
)

STAGE_NAME = "features"

#: Laplacian variance is mapped through a log scale: it spans several orders of
#: magnitude between a smooth gradient and a detailed frame, so a linear scale
#: would put almost every photograph at the top of the range.
_SHARPNESS_LOG_MIN = -4.0
_SHARPNESS_LOG_SPAN = 2.5

#: Colourfulness of a saturated outdoor photograph, used to normalise the
#: Hasler-Süsstrunk metric into [0, 1].
_COLORFULNESS_REF = 110.0

_CLIP_HIGH = 0.98
_CLIP_LOW = 0.02
_EDGE_THRESHOLD = 0.10
_HIST_BINS = 32

#: Neutral grey used to flatten transparency. Compositing onto white would make
#: every transparent PNG look like a document.
_ALPHA_BACKGROUND = (128, 128, 128)


@dataclass(frozen=True)
class ImageFeatures:
    """Cheap, interpretable measurements of one downscaled image."""

    width: int
    height: int
    aspect_ratio: float
    sharpness: float
    brightness: float
    brightness_p05: float
    brightness_p95: float
    overexposed_fraction: float
    underexposed_fraction: float
    contrast: float
    percentile_range: float
    entropy: float
    colorfulness: float
    edge_density: float
    uniform_background: float

    def as_scores(self) -> dict[str, float]:
        """Return the features under their ``features.*`` score keys."""
        return {f"features.{k}": float(v) for k, v in asdict(self).items()}


def load_working_image(path: Path, settings: CurationSettings):
    """Decode *path* into a small, upright RGB image ready for analysis.

    Applies the EXIF orientation, flattens transparency onto neutral grey,
    converts CMYK/grayscale to RGB and downscales the long side to
    ``settings.max_image_side``.

    Raises:
        ValueError: when the file declares more pixels than the configured
            limit, which is how a decompression bomb is refused.
        OSError: when the file cannot be decoded.
    """
    from PIL import Image, ImageOps

    with Image.open(path) as raw:
        pixels = (raw.size[0] or 0) * (raw.size[1] or 0)
        if pixels > settings.max_pixels:
            raise ValueError(
                f"{path} declares {pixels} pixels, above the "
                f"{settings.max_pixels} limit"
            )
        # ``draft`` lets the JPEG decoder skip most of the work when the target
        # is a thumbnail; it is a no-op for other formats.
        raw.draft("RGB", (settings.max_image_side, settings.max_image_side))
        image = ImageOps.exif_transpose(raw) or raw
        image = _to_rgb(image)
        image.thumbnail(
            (settings.max_image_side, settings.max_image_side),
            Image.Resampling.BILINEAR,
        )
        return image


def _to_rgb(image):
    from PIL import Image

    if image.mode in {"RGBA", "LA", "PA"} or (
        image.mode == "P" and "transparency" in image.info
    ):
        rgba = image.convert("RGBA")
        background = Image.new("RGB", rgba.size, _ALPHA_BACKGROUND)
        background.paste(rgba, mask=rgba.split()[-1])
        return background
    if image.mode != "RGB":
        return image.convert("RGB")
    return image


def compute_features(image) -> ImageFeatures:
    """Measure *image* (an RGB Pillow image) without modifying it."""
    rgb = np.asarray(image, dtype=np.float32) / 255.0
    if rgb.ndim == 2:  # defensive: a grayscale array still has to work
        rgb = np.repeat(rgb[:, :, None], 3, axis=2)
    lum = rgb @ np.array([0.299, 0.587, 0.114], dtype=np.float32)
    height, width = lum.shape

    hist, _ = np.histogram(lum, bins=_HIST_BINS, range=(0.0, 1.0))
    probabilities = hist / max(hist.sum(), 1)
    nonzero = probabilities[probabilities > 0]
    entropy = float(-(nonzero * np.log2(nonzero)).sum() / np.log2(_HIST_BINS))

    p05, p95 = (float(v) for v in np.percentile(lum, [5, 95]))

    return ImageFeatures(
        width=width,
        height=height,
        aspect_ratio=float(max(width, height) / max(min(width, height), 1)),
        sharpness=_sharpness(lum),
        brightness=float(lum.mean()),
        brightness_p05=p05,
        brightness_p95=p95,
        overexposed_fraction=float((lum > _CLIP_HIGH).mean()),
        underexposed_fraction=float((lum < _CLIP_LOW).mean()),
        contrast=float(lum.std()),
        percentile_range=p95 - p05,
        entropy=entropy,
        colorfulness=_colorfulness(rgb),
        edge_density=_edge_density(lum),
        uniform_background=float(probabilities.max()),
    )


def _sharpness(lum: np.ndarray) -> float:
    """Laplacian variance of the luminance, mapped to [0, 1]."""
    if lum.shape[0] < 3 or lum.shape[1] < 3:
        return 0.0
    centre = lum[1:-1, 1:-1]
    laplacian = (
        4.0 * centre - lum[:-2, 1:-1] - lum[2:, 1:-1] - lum[1:-1, :-2] - lum[1:-1, 2:]
    )
    variance = float(laplacian.var())
    scaled = (np.log10(variance + 1e-8) - _SHARPNESS_LOG_MIN) / _SHARPNESS_LOG_SPAN
    return float(np.clip(scaled, 0.0, 1.0))


def _colorfulness(rgb: np.ndarray) -> float:
    """Hasler-Süsstrunk colourfulness, normalised to [0, 1]."""
    scaled = rgb * 255.0
    red, green, blue = scaled[:, :, 0], scaled[:, :, 1], scaled[:, :, 2]
    rg = red - green
    yb = 0.5 * (red + green) - blue
    std = float(np.sqrt(rg.std() ** 2 + yb.std() ** 2))
    mean = float(np.sqrt(rg.mean() ** 2 + yb.mean() ** 2))
    return float(np.clip((std + 0.3 * mean) / _COLORFULNESS_REF, 0.0, 1.0))


def _edge_density(lum: np.ndarray) -> float:
    """Fraction of pixels whose Sobel gradient magnitude clears a threshold."""
    if lum.shape[0] < 3 or lum.shape[1] < 3:
        return 0.0
    gx = lum[1:-1, 2:] - lum[1:-1, :-2]
    gy = lum[2:, 1:-1] - lum[:-2, 1:-1]
    magnitude = np.sqrt(gx * gx + gy * gy)
    return float((magnitude > _EDGE_THRESHOLD).mean())


def technical_quality(features: ImageFeatures) -> float:
    """Fuse the exposure, contrast and sharpness features into one score."""
    exposure = 1.0 - min(
        1.0,
        features.overexposed_fraction * 4.0 + features.underexposed_fraction * 4.0,
    )
    contrast = float(np.clip(features.percentile_range / 0.6, 0.0, 1.0))
    brightness_penalty = 0.0
    if features.brightness < 0.12 or features.brightness > 0.92:
        brightness_penalty = 0.2
    score = 0.5 * features.sharpness + 0.25 * exposure + 0.25 * contrast
    return float(np.clip(score - brightness_penalty, 0.0, 1.0))


def document_evidence(features: ImageFeatures) -> float:
    """How much the pixels look like a scanned page rather than a scene.

    Three weak layout indicators are combined and then gated by how colourless
    the frame is. The gate is what keeps a bright, busy photograph of a city
    from looking like a page of text: paper is close to grey, a scene is not.
    """
    bright_background = float(np.clip((features.brightness_p95 - 0.75) / 0.2, 0.0, 1.0))
    colourless = float(np.clip((0.25 - features.colorfulness) / 0.25, 0.0, 1.0))
    flat_background = float(
        np.clip((features.uniform_background - 0.25) / 0.4, 0.0, 1.0)
    )
    texty_edges = float(np.clip((features.edge_density - 0.05) / 0.25, 0.0, 1.0))
    layout = 0.35 * bright_background + 0.35 * flat_background + 0.30 * texty_edges
    return float(np.clip(layout * colourless, 0.0, 1.0))


def photographic_evidence(features: ImageFeatures) -> float:
    """How much the pixels look like a camera frame of a real scene."""
    colour = float(np.clip(features.colorfulness / 0.35, 0.0, 1.0))
    texture = float(np.clip(features.entropy / 0.8, 0.0, 1.0))
    varied = 1.0 - float(np.clip((features.uniform_background - 0.2) / 0.5, 0.0, 1.0))
    return float(np.clip(0.4 * colour + 0.35 * texture + 0.25 * varied, 0.0, 1.0))


class ImageFeatureStage:
    """Decode once, measure cheap features, publish the working image."""

    name = STAGE_NAME

    def __init__(self, settings: CurationSettings) -> None:
        self._settings = settings

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Measure *item* and report quality plus document/photo evidence."""
        started = perf_counter()
        try:
            image = load_working_image(item.path, self._settings)
        except FileNotFoundError:
            return self._failed(item, started, reasons.FILE_MISSING)
        except ValueError:
            return self._failed(item, started, reasons.IMAGE_TOO_LARGE)
        except Exception as exc:  # Pillow's decode failures are a wide family
            logger.debug(f"Could not decode {item.relative_path}: {exc}")
            return self._failed(item, started, reasons.DECODE_ERROR)

        context.set_image(image)
        features = compute_features(image)
        context.values["features"] = features

        scores = features.as_scores()
        scores["technical_quality"] = technical_quality(features)
        scores["features.document_evidence"] = document_evidence(features)
        scores["features.photographic_evidence"] = photographic_evidence(features)

        return StageResult(
            stage=self.name,
            scores=scores,
            reasons=tuple(_feature_reasons(features, scores)),
            model_id="features-v1",
            duration_ms=(perf_counter() - started) * 1000,
        )

    def _failed(self, item: MediaItem, started: float, reason: str) -> StageResult:
        return StageResult(
            stage=self.name,
            reasons=(reason,),
            terminal_decision=CurationDecision.REVIEW,
            confidence=1.0,
            duration_ms=(perf_counter() - started) * 1000,
            failed=True,
        )


def _feature_reasons(features: ImageFeatures, scores: dict[str, float]) -> list[str]:
    out: list[str] = []
    if features.sharpness < 0.25:
        out.append(reasons.LOW_SHARPNESS)
    if features.overexposed_fraction > 0.20:
        out.append(reasons.OVEREXPOSED)
    if features.underexposed_fraction > 0.20:
        out.append(reasons.UNDEREXPOSED)
    if features.percentile_range < 0.15:
        out.append(reasons.LOW_CONTRAST)
    if features.entropy < 0.35 and features.edge_density < 0.02:
        out.append(reasons.LOW_INFORMATION)
    if features.uniform_background > 0.6:
        out.append(reasons.UNIFORM_BACKGROUND)
    if scores["features.document_evidence"] > 0.6:
        out.append(reasons.DOCUMENT_LIKE)
    if scores["features.photographic_evidence"] > 0.6:
        out.append(reasons.PHOTOGRAPHIC_COLOR)
    return out
