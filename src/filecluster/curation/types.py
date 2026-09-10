"""Core types of the cascaded curation pipeline.

Nothing in here touches the filesystem, a model or the terminal: the types are
plain data so that every stage can be tested in isolation and so that a result
can be serialised into the cache without a second representation.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters for typing
    from PIL.Image import Image

    from filecluster.curation.configuration import CurationSettings


class CurationDecision(StrEnum):
    """Where a media file should go once the cascade has run."""

    KEEP = "keep"
    REVIEW = "review"
    REJECT = "reject"


class MediaKind(StrEnum):
    """Coarse media class, decided from the file extension."""

    IMAGE = "image"
    VIDEO = "video"
    OTHER = "other"


class SemanticLabel(StrEnum):
    """Stable identifiers for the semantic classes of the prompt bank."""

    PERSONAL_PEOPLE = "personal_people"
    FAMILY_HOME = "family_home"
    PORTRAIT = "portrait"
    LANDSCAPE = "landscape"
    CITY_TRAVEL = "city_travel"
    EVENT = "event"
    PET = "pet"
    ARTISTIC_PHOTO = "artistic_photo"
    SCREENSHOT = "screenshot"
    DOCUMENT = "document"
    RECEIPT_INVOICE = "receipt_invoice"
    BOOK_PAGE = "book_page"
    LABEL_PACKAGING = "label_packaging"
    PRODUCT_REFERENCE = "product_reference"
    WHITEBOARD_NOTES = "whiteboard_notes"
    LOW_INFORMATION = "low_information"
    OTHER = "other"


#: Classes that mark a file as a personal photograph worth keeping.
PERSONAL_LABELS: frozenset[SemanticLabel] = frozenset(
    {
        SemanticLabel.PERSONAL_PEOPLE,
        SemanticLabel.FAMILY_HOME,
        SemanticLabel.PORTRAIT,
        SemanticLabel.LANDSCAPE,
        SemanticLabel.CITY_TRAVEL,
        SemanticLabel.EVENT,
        SemanticLabel.PET,
        SemanticLabel.ARTISTIC_PHOTO,
    }
)

#: Classes that mark a file as utility material.
UTILITY_LABELS: frozenset[SemanticLabel] = frozenset(
    {
        SemanticLabel.SCREENSHOT,
        SemanticLabel.DOCUMENT,
        SemanticLabel.RECEIPT_INVOICE,
        SemanticLabel.BOOK_PAGE,
        SemanticLabel.LABEL_PACKAGING,
        SemanticLabel.PRODUCT_REFERENCE,
        SemanticLabel.WHITEBOARD_NOTES,
        SemanticLabel.LOW_INFORMATION,
    }
)

#: Classes whose presence blocks an automatic reject. Losing one of these is
#: the most expensive mistake the pipeline can make, so they outrank a low
#: linear score and send the file to review instead.
PROTECTED_LABELS: frozenset[SemanticLabel] = frozenset(
    {
        SemanticLabel.PERSONAL_PEOPLE,
        SemanticLabel.FAMILY_HOME,
        SemanticLabel.PET,
        SemanticLabel.EVENT,
    }
)

#: Named signals carried in :attr:`CurationResult.scores`. Everything else in
#: that mapping is a raw feature and stays out of the report columns.
SIGNAL_KEYS: tuple[str, ...] = (
    "personal_probability",
    "utility_probability",
    "technical_quality",
    "aesthetic_score",
    "preference_score",
)


def _as_float(value: object) -> float:
    """Coerce a cached JSON value to a float, defaulting to zero."""
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _as_str_tuple(value: object) -> tuple[str, ...]:
    """Coerce a cached JSON value to a tuple of strings."""
    if isinstance(value, str | bytes) or not isinstance(value, Iterable):
        return ()
    return tuple(str(item) for item in value)


@dataclass(frozen=True)
class MediaItem:
    """One discovered file, identified by content rather than by name."""

    path: Path
    relative_path: str
    size: int
    mtime: float
    sha256: str
    media_type: MediaKind
    extension: str

    @property
    def is_image(self) -> bool:
        """Whether the pixel stages are expected to be able to read this."""
        return self.media_type is MediaKind.IMAGE


@dataclass(frozen=True)
class StageResult:
    """What one stage learned about one file.

    ``terminal_decision`` is the only way a stage can end the cascade, and the
    pipeline honours it only when ``confidence`` clears the configured floor,
    so early exits stay visible in configuration rather than buried in a stage.
    """

    stage: str
    scores: Mapping[str, float] = field(default_factory=dict)
    labels: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    terminal_decision: CurationDecision | None = None
    confidence: float | None = None
    model_id: str | None = None
    duration_ms: float = 0.0
    failed: bool = False

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable view, used by the cache and the trace."""
        return {
            "stage": self.stage,
            "scores": {k: round(float(v), 6) for k, v in self.scores.items()},
            "labels": list(self.labels),
            "reasons": list(self.reasons),
            "terminal_decision": (
                self.terminal_decision.value if self.terminal_decision else None
            ),
            "confidence": self.confidence,
            "model_id": self.model_id,
            "duration_ms": round(self.duration_ms, 3),
            "failed": self.failed,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> StageResult:
        """Rebuild a stage result stored by :meth:`as_dict`."""
        decision = data.get("terminal_decision")
        confidence = data.get("confidence")
        raw_scores = data.get("scores")
        scores = (
            {str(k): _as_float(v) for k, v in raw_scores.items()}
            if isinstance(raw_scores, Mapping)
            else {}
        )
        return cls(
            stage=str(data.get("stage", "")),
            scores=scores,
            labels=_as_str_tuple(data.get("labels")),
            reasons=_as_str_tuple(data.get("reasons")),
            terminal_decision=(
                CurationDecision(str(decision)) if decision is not None else None
            ),
            confidence=_as_float(confidence) if confidence is not None else None,
            model_id=(
                str(data["model_id"]) if data.get("model_id") is not None else None
            ),
            duration_ms=_as_float(data.get("duration_ms")),
            failed=bool(data.get("failed", False)),
        )


@dataclass(frozen=True)
class CurationResult:
    """The pipeline verdict for one file, with the trail that produced it."""

    item: MediaItem
    decision: CurationDecision
    confidence: float
    scores: Mapping[str, float | None]
    labels: tuple[str, ...]
    reasons: tuple[str, ...]
    stage_trace: tuple[StageResult, ...]
    pipeline_version: str
    cache_hit: bool = False

    @property
    def completed_stage(self) -> str:
        """Name of the last stage that ran, or an empty string."""
        return self.stage_trace[-1].stage if self.stage_trace else ""

    @property
    def top_label(self) -> str:
        """First label, which every producer orders by descending score."""
        return self.labels[0] if self.labels else ""

    def signal(self, name: str) -> float | None:
        """Return one named signal, or *None* when it was never measured."""
        value = self.scores.get(name)
        return None if value is None else float(value)

    @property
    def duration_ms(self) -> float:
        """Total time spent in the stages that ran for this file."""
        return sum(stage.duration_ms for stage in self.stage_trace)


class CurationContext:
    """Per-file scratch space shared by the stages of one cascade run.

    The decoded working image is the expensive artefact here: several stages
    want it and a 50k-file run cannot afford to decode more than once, nor to
    keep more than one full image alive at a time.
    """

    def __init__(self, settings: CurationSettings) -> None:
        self.settings = settings
        self.values: dict[str, object] = {}
        self._image: Image | None = None
        self._image_failed = False

    def set_image(self, image: Image | None) -> None:
        """Publish the decoded, EXIF-corrected working image."""
        self._image = image
        self._image_failed = image is None

    @property
    def image(self) -> Image | None:
        """The working image, or *None* when decoding failed or never ran."""
        return self._image

    @property
    def image_failed(self) -> bool:
        """Whether an attempt to decode the working image has already failed."""
        return self._image_failed

    def release(self) -> None:
        """Drop the working image so peak memory stays at one image."""
        if self._image is not None:
            self._image.close()
        self._image = None
        self._image_failed = False
        self.values.clear()

    def __enter__(self) -> CurationContext:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class CurationStage(Protocol):
    """Contract every cascade stage implements."""

    name: str

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Inspect *item* and report what this stage measured."""
        ...


def iter_signals(scores: Mapping[str, float | None]) -> Iterator[tuple[str, float]]:
    """Yield the named signals present in *scores*, skipping missing ones."""
    for key in SIGNAL_KEYS:
        value = scores.get(key)
        if value is not None:
            yield key, float(value)
