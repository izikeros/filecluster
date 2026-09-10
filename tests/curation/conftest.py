"""Fixtures for the curation tests.

Images are synthesised rather than committed: the pipeline reasons about pixel
statistics, so a generator that can produce "colourful scene", "page of text" and
"phone screen capture" on demand is more useful than a handful of sample files.

No test in this suite downloads model weights or needs a GPU. Model providers are
replaced by the fakes below.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from filecluster.curation.configuration import CurationSettings
from filecluster.curation.providers.base import (
    OcrAggregates,
    ProviderInfo,
    SemanticPrediction,
)
from filecluster.curation.types import (
    CurationContext,
    CurationDecision,
    MediaItem,
    MediaKind,
    StageResult,
)


# ---------------------------------------------------------------------------
# Image builders
# ---------------------------------------------------------------------------
def scene_array(width: int = 640, height: int = 480, seed: int = 0) -> np.ndarray:
    """A colourful, textured gradient standing in for a camera photograph."""
    rng = np.random.default_rng(seed)
    y = np.broadcast_to(np.linspace(0.0, 1.0, height)[:, None], (height, width))
    x = np.broadcast_to(np.linspace(0.0, 1.0, width)[None, :], (height, width))
    base = np.stack([0.15 + 0.7 * x, 0.30 + 0.5 * y, 0.75 - 0.5 * x], axis=2)
    detail = rng.normal(0.0, 0.06, (height, width, 3))
    return ((base + detail).clip(0, 1) * 255).astype("uint8")


def page_array(width: int = 600, height: int = 800) -> np.ndarray:
    """A grey-on-white block of text lines, standing in for a document."""
    page = np.full((height, width, 3), 246, dtype="uint8")
    for row in range(60, height - 60, 22):
        page[row : row + 7, 50 : width - 50] = 28
    return page


def write_photo(path: Path, *, camera_exif: bool = True, seed: int = 0) -> Path:
    """Write a JPEG scene, optionally with camera EXIF tags."""
    image = Image.fromarray(scene_array(seed=seed))
    exif = image.getexif()
    if camera_exif:
        exif[271] = "Canon"
        exif[272] = "EOS 80D"
        exif[36867] = "2024:05:01 12:00:00"
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, exif=exif)
    return path


def write_document(path: Path) -> Path:
    """Write a PNG page of text."""
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(page_array()).save(path)
    return path


def write_screenshot(path: Path) -> Path:
    """Write a PNG at a known phone screen resolution."""
    shot = np.full((2532, 1170, 3), 250, dtype="uint8")
    shot[120:220, 60:1000] = 20
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(shot).save(path)
    return path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def settings() -> CurationSettings:
    """Default settings with every optional stage switched off."""
    return CurationSettings()


@pytest.fixture
def context(settings) -> CurationContext:
    """A fresh per-file context."""
    return CurationContext(settings)


@pytest.fixture
def inbox(tmp_path) -> Path:
    """An inbox holding one photo, one document and one screenshot."""
    root = tmp_path / "inbox"
    write_photo(root / "IMG_0001.jpg")
    write_document(root / "notes" / "receipt.png")
    write_screenshot(root / "Screenshot_2024-05-01.png")
    return root


@pytest.fixture
def out_dir(tmp_path) -> Path:
    """An empty output directory."""
    target = tmp_path / "curated"
    target.mkdir()
    return target


def make_item(
    path: Path,
    *,
    relative_path: str | None = None,
    sha256: str = "0" * 64,
    media_type: MediaKind = MediaKind.IMAGE,
) -> MediaItem:
    """Build a :class:`MediaItem` for *path* without touching the cache."""
    stat = path.stat() if path.exists() else None
    return MediaItem(
        path=path,
        relative_path=relative_path or path.name,
        size=stat.st_size if stat else 0,
        mtime=stat.st_mtime if stat else 0.0,
        sha256=sha256,
        media_type=media_type,
        extension=path.suffix.lower(),
    )


# ---------------------------------------------------------------------------
# Fake providers and stages
# ---------------------------------------------------------------------------
@dataclass
class FakeSemanticProvider:
    """Semantic provider returning fixed label scores."""

    scores: dict[str, float]
    calls: int = 0

    def info(self) -> ProviderInfo:
        return ProviderInfo(name="semantic", model_id="fake", revision="1")

    def classify(self, images):
        self.calls += 1
        return [SemanticPrediction(scores=dict(self.scores)) for _ in images]


@dataclass
class BrokenSemanticProvider:
    """Semantic provider that always fails, as an unavailable model would."""

    calls: int = 0

    def info(self) -> ProviderInfo:
        return ProviderInfo(name="semantic", model_id="broken", revision="1")

    def classify(self, images):
        self.calls += 1
        raise RuntimeError("weights are missing")


@dataclass
class FakeOcrProvider:
    """OCR provider returning fixed aggregates and never any text."""

    aggregates: OcrAggregates
    calls: int = 0

    def info(self) -> ProviderInfo:
        return ProviderInfo(name="ocr", model_id="fake-ocr", revision="1")

    def analyze(self, image):
        self.calls += 1
        return self.aggregates


@dataclass
class RecordingStage:
    """Stage that records its calls, used to prove early exit works."""

    name: str = "recording"
    result: StageResult | None = None
    calls: list[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.calls is None:
            self.calls = []

    def analyze(self, item, context):
        self.calls.append(item.relative_path)
        return self.result or StageResult(stage=self.name)


@dataclass
class TerminalStage:
    """Stage that ends the cascade with a fixed decision."""

    decision: CurationDecision = CurationDecision.REJECT
    confidence: float = 0.99
    name: str = "terminal"

    def analyze(self, item, context):
        return StageResult(
            stage=self.name,
            reasons=("metadata.screenshot_software",),
            terminal_decision=self.decision,
            confidence=self.confidence,
        )
