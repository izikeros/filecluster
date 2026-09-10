"""Optional OCR provider returning aggregates only.

Recognised text from a personal photo library is sensitive, so the words never
leave this module: callers receive counts, a mean confidence and the fraction of
the frame covered by text boxes.
"""

from __future__ import annotations

from importlib.util import find_spec
from typing import Any

from filecluster.curation.exceptions import (
    MissingDependencyError,
    ProviderUnavailableError,
)
from filecluster.curation.providers.base import OcrAggregates, ProviderInfo

_EXTRA = "curation"

#: Text covering this much of the frame is a strong document signal on its own
#: scale; the fusion step still needs a second signal to act on it.
_SATURATION_AREA = 0.35


def ocr_extra_available() -> bool:
    """Whether a supported OCR runtime is importable."""
    return find_spec("rapidocr_onnxruntime") is not None


def text_density_evidence(aggregates: OcrAggregates) -> float:
    """Turn OCR aggregates into one ``[0, 1]`` document-likeness signal."""
    area = min(1.0, aggregates.text_area_fraction / _SATURATION_AREA)
    lines = min(1.0, aggregates.lines / 12.0)
    confidence = min(1.0, max(0.0, aggregates.mean_confidence))
    return max(0.0, min(1.0, (0.6 * area + 0.4 * lines) * confidence))


class RapidOcrProvider:
    """Lightweight ONNX OCR backend, loaded on first use."""

    def __init__(self, model_version: str = "rapidocr-onnxruntime") -> None:
        if not ocr_extra_available():
            raise MissingDependencyError("OCR", _EXTRA)
        self._model_version = model_version
        self._engine: Any = None

    def info(self) -> ProviderInfo:
        """Return the pinned identity of the OCR backend."""
        return ProviderInfo(
            name="ocr",
            model_id=self._model_version,
            revision=_installed_version("rapidocr_onnxruntime"),
        )

    def analyze(self, image: object) -> OcrAggregates:
        """Return aggregate text statistics for *image*.

        Raises:
            ProviderUnavailableError: when the backend cannot process the image.
        """
        self._ensure_loaded()
        import numpy as np

        array = np.asarray(image)
        try:
            result, _ = self._engine(array)
        except Exception as exc:
            raise ProviderUnavailableError(f"OCR backend failed: {exc}") from exc
        if not result:
            return OcrAggregates()

        height, width = array.shape[0], array.shape[1]
        frame_area = float(max(height * width, 1))
        confidences: list[float] = []
        characters = 0
        text_area = 0.0
        for box, text, score in result:
            characters += len(str(text))
            confidences.append(float(score))
            text_area += _polygon_area(box)

        return OcrAggregates(
            blocks=len(result),
            lines=len(result),
            characters=characters,
            mean_confidence=sum(confidences) / len(confidences),
            text_area_fraction=min(1.0, text_area / frame_area),
        )

    def _ensure_loaded(self) -> None:
        if self._engine is not None:
            return
        try:
            from rapidocr_onnxruntime import RapidOCR  # ty: ignore[unresolved-import]
        except ImportError as exc:  # pragma: no cover - guarded in __init__
            raise MissingDependencyError("OCR", _EXTRA) from exc
        self._engine = RapidOCR()


def _polygon_area(box: object) -> float:
    """Shoelace area of a quadrilateral text box, zero when malformed."""
    try:
        points = [(float(p[0]), float(p[1])) for p in box]  # ty: ignore[not-iterable]
    except (TypeError, ValueError, IndexError):
        return 0.0
    if len(points) < 3:
        return 0.0
    total = 0.0
    for i, (x1, y1) in enumerate(points):
        x2, y2 = points[(i + 1) % len(points)]
        total += x1 * y2 - x2 * y1
    return abs(total) / 2.0


def _installed_version(module: str) -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(module.replace("_", "-"))
    except PackageNotFoundError:  # pragma: no cover - depends on install
        return "unknown"
