"""Provider contracts for the model-backed stages.

Providers are injected, never imported by the pipeline directly, so a model can
be swapped, mocked or left out entirely. A provider may load weights, but only
on first use: importing this package must not pull a machine-learning runtime
into a plain ``filecluster run``.

A provider never moves files and never writes to the terminal.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from filecluster.curation.types import CurationDecision


@dataclass(frozen=True)
class ProviderInfo:
    """Identity of one provider, pinned tightly enough to cache against.

    A verdict produced by a different model revision, preprocessor or prompt
    bank is a different verdict, so all of it goes into the fingerprint.
    """

    name: str
    model_id: str
    revision: str = "unpinned"
    preprocessor_version: str = ""
    prompt_bank_version: str = ""
    weights_checksum: str = ""

    def as_dict(self) -> dict[str, str]:
        """Return the identity as plain strings, for hashing and reports."""
        return {
            "name": self.name,
            "model_id": self.model_id,
            "revision": self.revision,
            "preprocessor_version": self.preprocessor_version,
            "prompt_bank_version": self.prompt_bank_version,
            "weights_checksum": self.weights_checksum,
        }


@dataclass(frozen=True)
class SemanticPrediction:
    """Per-label similarity scores for one image."""

    scores: Mapping[str, float] = field(default_factory=dict)
    #: Only populated when preference learning is enabled; never reported.
    embedding: tuple[float, ...] | None = None

    def ranked(self, limit: int = 3) -> tuple[tuple[str, float], ...]:
        """Return the highest-scoring labels, best first."""
        ordered = sorted(self.scores.items(), key=lambda kv: kv[1], reverse=True)
        return tuple(ordered[:limit])


@dataclass(frozen=True)
class OcrAggregates:
    """Aggregate text statistics for one image.

    Recognised strings are deliberately absent: OCR output of a personal photo
    library is sensitive, and nothing downstream needs the words themselves.
    """

    blocks: int = 0
    lines: int = 0
    characters: int = 0
    mean_confidence: float = 0.0
    text_area_fraction: float = 0.0

    def as_scores(self) -> dict[str, float]:
        """Return the aggregates under their ``ocr.*`` score keys."""
        return {
            "ocr.blocks": float(self.blocks),
            "ocr.lines": float(self.lines),
            "ocr.characters": float(self.characters),
            "ocr.mean_confidence": float(self.mean_confidence),
            "ocr.text_area_fraction": float(self.text_area_fraction),
        }


@dataclass(frozen=True)
class VlmJudgement:
    """Validated structured answer from a vision-language model."""

    decision: CurationDecision
    confidence: float
    labels: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()


class SemanticProvider(Protocol):
    """Classifies images into the semantic label set."""

    def info(self) -> ProviderInfo:
        """Return the pinned identity of this provider."""
        ...

    def classify(self, images: Sequence[object]) -> list[SemanticPrediction]:
        """Score every image in *images* against the label set."""
        ...


class OcrProvider(Protocol):
    """Measures how much text an image contains."""

    def info(self) -> ProviderInfo:
        """Return the pinned identity of this provider."""
        ...

    def analyze(self, image: object) -> OcrAggregates:
        """Return aggregate text statistics for *image*."""
        ...


class QualityProvider(Protocol):
    """Scores aesthetic or perceptual quality in ``[0, 1]``."""

    def info(self) -> ProviderInfo:
        """Return the pinned identity of this provider."""
        ...

    def score(self, image: object) -> float:
        """Return the aesthetic score of *image*."""
        ...


class PreferenceProvider(Protocol):
    """Scores how well an image matches the user's own past decisions."""

    def info(self) -> ProviderInfo:
        """Return the pinned identity of this provider."""
        ...

    def score(self, embedding: Sequence[float]) -> float:
        """Return the preference score for one image embedding."""
        ...


class VlmProvider(Protocol):
    """Escalation path for files the cheaper stages could not settle."""

    def info(self) -> ProviderInfo:
        """Return the pinned identity of this provider."""
        ...

    def judge(self, image: object) -> VlmJudgement:
        """Return a structured verdict for *image*."""
        ...


@dataclass
class Providers:
    """The optional providers available to one run."""

    semantic: SemanticProvider | None = None
    ocr: OcrProvider | None = None
    quality: QualityProvider | None = None
    preference: PreferenceProvider | None = None
    vlm: VlmProvider | None = None

    def infos(self) -> list[ProviderInfo]:
        """Return the identity of every provider that is present."""
        candidates = (self.semantic, self.ocr, self.quality, self.preference, self.vlm)
        return [p.info() for p in candidates if p is not None]


def model_fingerprint(providers: Providers | None) -> str:
    """Hash the identity of every active provider.

    With no providers this is a stable constant, so heuristic-only runs still
    share a cache namespace.
    """
    infos = providers.infos() if providers is not None else []
    payload = [info.as_dict() for info in sorted(infos, key=lambda i: i.name)]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]
