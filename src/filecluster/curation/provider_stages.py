"""Adapters that turn a provider into a cascade stage.

Each adapter owns the same three jobs: hand the working image to the provider,
translate its answer into signals and reason codes, and contain its failures. A
provider that raises produces a *failed* stage result, which the fusion step
turns into ``review`` - a broken model may never reject a photograph.

The semantic provider takes a sequence of images so a future batched pipeline can
feed it several at a time; the current per-file loop passes one.
"""

from __future__ import annotations

from time import perf_counter

from filecluster import logger
from filecluster.curation import reasons
from filecluster.curation.configuration import CurationSettings
from filecluster.curation.providers.base import (
    OcrProvider,
    QualityProvider,
    SemanticPrediction,
    SemanticProvider,
    VlmProvider,
)
from filecluster.curation.providers.ocr import text_density_evidence
from filecluster.curation.scoring import semantic_reason_for
from filecluster.curation.types import (
    PERSONAL_LABELS,
    UTILITY_LABELS,
    CurationContext,
    MediaItem,
    StageResult,
)


class SemanticStage:
    """Zero-shot semantic classification of the working image."""

    name = "semantic"

    def __init__(self, provider: SemanticProvider, settings: CurationSettings) -> None:
        self._provider = provider
        self._settings = settings

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Score *item* against the semantic label set."""
        started = perf_counter()
        if context.image is None:
            return _failed(self.name, started, reasons.DECODE_ERROR)
        try:
            predictions = self._provider.classify([context.image])
        except Exception as exc:
            logger.warning(f"Semantic provider failed on {item.relative_path}: {exc}")
            return _failed(self.name, started, reasons.PROVIDER_UNAVAILABLE)
        if not predictions:
            return _failed(self.name, started, reasons.PROVIDER_UNAVAILABLE)

        prediction = predictions[0]
        scores = self._signals(prediction)
        ranked = prediction.ranked(3)
        labels = tuple(label for label, _ in ranked)
        notes = [semantic_reason_for(labels[0])] if labels else []
        if len(ranked) >= 2 and (
            ranked[0][1] - ranked[1][1] < self._settings.thresholds.conflict_margin
        ):
            notes.append(reasons.SEMANTIC_LOW_MARGIN)

        return StageResult(
            stage=self.name,
            scores=scores,
            labels=labels,
            reasons=tuple(notes),
            model_id=self._provider.info().model_id,
            duration_ms=(perf_counter() - started) * 1000,
        )

    @staticmethod
    def _signals(prediction: SemanticPrediction) -> dict[str, float]:
        scores = {f"semantic.{label}": v for label, v in prediction.scores.items()}
        personal = [
            prediction.scores[label.value]
            for label in PERSONAL_LABELS
            if label.value in prediction.scores
        ]
        utility = [
            prediction.scores[label.value]
            for label in UTILITY_LABELS
            if label.value in prediction.scores
        ]
        if personal:
            scores["personal_probability"] = max(personal)
        if utility:
            scores["utility_probability"] = max(utility)
        return scores


class OcrStage:
    """Text-density measurement, reported as aggregates only."""

    name = "ocr"

    #: Text covering this much of the frame, with lines to match, is reported
    #: as a high-density signal.
    HIGH_DENSITY = 0.55

    def __init__(self, provider: OcrProvider) -> None:
        self._provider = provider

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Measure how much text *item* contains."""
        started = perf_counter()
        if context.image is None:
            return _failed(self.name, started, reasons.DECODE_ERROR)
        try:
            aggregates = self._provider.analyze(context.image)
        except Exception as exc:
            logger.warning(f"OCR failed on {item.relative_path}: {exc}")
            return _failed(self.name, started, reasons.PROVIDER_UNAVAILABLE)

        evidence = text_density_evidence(aggregates)
        scores = aggregates.as_scores()
        scores["ocr.text_density_evidence"] = evidence
        notes: list[str] = []
        if evidence >= self.HIGH_DENSITY:
            notes.append(reasons.HIGH_TEXT_DENSITY)
        elif aggregates.characters == 0:
            notes.append(reasons.TEXT_ABSENT)

        return StageResult(
            stage=self.name,
            scores=scores,
            reasons=tuple(notes),
            model_id=self._provider.info().model_id,
            duration_ms=(perf_counter() - started) * 1000,
        )


class QualityStage:
    """Aesthetic scoring, kept separate from technical quality."""

    name = "quality"

    def __init__(self, provider: QualityProvider) -> None:
        self._provider = provider

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Score the aesthetics of *item*."""
        started = perf_counter()
        if context.image is None:
            return _failed(self.name, started, reasons.DECODE_ERROR)
        try:
            score = float(self._provider.score(context.image))
        except Exception as exc:
            logger.warning(f"Quality provider failed on {item.relative_path}: {exc}")
            return _failed(self.name, started, reasons.PROVIDER_UNAVAILABLE)

        return StageResult(
            stage=self.name,
            scores={"aesthetic_score": max(0.0, min(1.0, score))},
            model_id=self._provider.info().model_id,
            duration_ms=(perf_counter() - started) * 1000,
        )


class VlmStage:
    """Last-resort escalation for files still sitting in the uncertain band."""

    name = "vlm"

    def __init__(self, provider: VlmProvider) -> None:
        self._provider = provider

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Ask the VLM for a structured verdict on *item*."""
        started = perf_counter()
        if context.image is None:
            return _failed(self.name, started, reasons.DECODE_ERROR)
        try:
            judgement = self._provider.judge(context.image)
        except Exception as exc:
            logger.warning(f"VLM failed on {item.relative_path}: {exc}")
            return _failed(self.name, started, reasons.PROVIDER_UNAVAILABLE)

        return StageResult(
            stage=self.name,
            scores={"vlm.confidence": judgement.confidence},
            labels=judgement.labels,
            reasons=(reasons.VLM_DECISION,),
            terminal_decision=judgement.decision,
            confidence=judgement.confidence,
            model_id=self._provider.info().model_id,
            duration_ms=(perf_counter() - started) * 1000,
        )


def _failed(stage: str, started: float, reason: str) -> StageResult:
    return StageResult(
        stage=stage,
        reasons=(reason,),
        duration_ms=(perf_counter() - started) * 1000,
        failed=True,
    )
