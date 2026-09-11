"""Signal fusion, thresholds and the safety rules around them.

The linear score is only a proposal. The rules that follow it exist because the
two error types are not symmetric: an extra file in ``review`` costs a click,
while a rejected family photo costs something that cannot be clicked back.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from filecluster.curation import reasons
from filecluster.curation.configuration import CurationSettings
from filecluster.curation.types import (
    PERSONAL_LABELS,
    PROTECTED_LABELS,
    CurationDecision,
    SemanticLabel,
)

#: A signal at or above this level counts as "strong" for conflict detection.
STRONG_SIGNAL = 0.55

#: Weight of the metadata prior when no semantic model has spoken and the
#: personal probability has to be assembled from heuristics instead.
_PRIOR_WEIGHT = 0.7

#: Confidence multiplier applied while the personal signal is heuristic.
#: Uncalibrated evidence should not be able to produce a fully certain verdict.
_HEURISTIC_CONFIDENCE_FACTOR = 0.95


@dataclass(frozen=True)
class Signals:
    """The five fusion inputs, with *None* for anything never measured."""

    personal_probability: float | None = None
    utility_probability: float | None = None
    technical_quality: float | None = None
    aesthetic_score: float | None = None
    preference_score: float | None = None
    personal_source: str = "none"
    utility_source: str = "none"

    def as_scores(self) -> dict[str, float | None]:
        """Return the named signals, ready for the report and the cache."""
        return {
            "personal_probability": self.personal_probability,
            "utility_probability": self.utility_probability,
            "technical_quality": self.technical_quality,
            "aesthetic_score": self.aesthetic_score,
            "preference_score": self.preference_score,
        }


@dataclass(frozen=True)
class Verdict:
    """Outcome of the fusion step for one file."""

    decision: CurationDecision
    confidence: float
    keep_score: float | None
    reasons: tuple[str, ...]


def resolve_signals(scores: Mapping[str, float]) -> Signals:
    """Pick the best available source for each fusion input.

    A semantic model, when present, wins outright. Without it the personal and
    utility signals are assembled from the metadata prior and the pixel
    evidence, and the source is recorded so the confidence step can discount an
    uncalibrated guess.
    """
    personal = scores.get("personal_probability")
    personal_source = "semantic"
    if personal is None:
        personal, personal_source = _heuristic_personal(scores)

    utility = scores.get("utility_probability")
    utility_source = "semantic"
    if utility is None:
        utility, utility_source = _heuristic_utility(scores)

    return Signals(
        personal_probability=_clamp(personal),
        utility_probability=_clamp(utility),
        technical_quality=_clamp(scores.get("technical_quality")),
        aesthetic_score=_clamp(scores.get("aesthetic_score")),
        preference_score=_clamp(scores.get("preference_score")),
        personal_source=personal_source,
        utility_source=utility_source,
    )


def _heuristic_personal(scores: Mapping[str, float]) -> tuple[float | None, str]:
    prior = scores.get("rules.personal_prior")
    pixels = scores.get("features.photographic_evidence")
    if prior is None and pixels is None:
        return None, "none"
    if pixels is None:
        return prior, "metadata"
    if prior is None:
        return pixels, "pixels"
    return _PRIOR_WEIGHT * prior + (1.0 - _PRIOR_WEIGHT) * pixels, "heuristic"


def _heuristic_utility(scores: Mapping[str, float]) -> tuple[float | None, str]:
    candidates = {
        "metadata": scores.get("rules.utility_evidence"),
        "pixels": scores.get("features.document_evidence"),
        "ocr": scores.get("ocr.text_density_evidence"),
    }
    present = {k: v for k, v in candidates.items() if v is not None}
    if not present:
        return None, "none"
    source = max(present, key=lambda key: present[key])
    return present[source], source


def keep_score(signals: Signals, settings: CurationSettings) -> float | None:
    """Compute the linear keep score, or *None* without a positive signal.

    Missing components are dropped and the remaining positive weights are
    renormalised, so adding an aesthetic model later shifts the mix rather than
    the overall scale.
    """
    weights = settings.weights
    active = [
        (weights.personal, signals.personal_probability),
        (weights.technical_quality, signals.technical_quality),
        (weights.aesthetic, signals.aesthetic_score),
        (weights.preference, signals.preference_score),
    ]
    usable = [(w, v) for w, v in active if v is not None and w > 0.0]
    if not usable:
        return None

    total_weight = sum(w for w, _ in usable)
    positive = sum(w * v for w, v in usable) / total_weight
    penalty = 0.0
    if signals.utility_probability is not None:
        penalty = weights.utility_penalty * signals.utility_probability
    return max(0.0, min(1.0, positive - penalty))


def confidence_for(
    score: float | None,
    signals: Signals,
    settings: CurationSettings,
) -> float:
    """How far the score sits from the nearest threshold, in band units.

    Not the maximum model similarity: a file just past the keep threshold is
    barely decided, however certain a single model was about its label. Because
    this is a margin rather than independent evidence, the confidence floor in
    :func:`_apply_safety_rules` is applied to ``reject`` only; see the comment
    there.
    """
    if score is None:
        return 0.0
    keep, reject = settings.keep_threshold, settings.reject_threshold
    band = max(keep - reject, 1e-6)
    if score >= keep:
        distance = score - keep
    elif score <= reject:
        distance = reject - score
    else:
        distance = -min(score - reject, keep - score)

    value = 0.5 + distance / band
    if signals.personal_source not in {"semantic", "vlm"}:
        value *= _HEURISTIC_CONFIDENCE_FACTOR
    return max(0.0, min(1.0, value))


def has_protected_subject(
    labels: Sequence[str],
    scores: Mapping[str, float],
) -> bool:
    """Whether a person, pet, home or event is a likely subject of the file."""
    protected = {label.value for label in PROTECTED_LABELS}
    for label in labels:
        if label not in protected:
            continue
        score = scores.get(f"semantic.{label}")
        if score is None or score >= 0.5:
            return True
    return False


def fuse(
    scores: Mapping[str, float],
    labels: Sequence[str],
    stage_reasons: Sequence[str],
    settings: CurationSettings,
    *,
    stage_failed: bool = False,
) -> Verdict:
    """Turn the collected signals into a decision plus its justification."""
    signals = resolve_signals(scores)
    score = keep_score(signals, settings)
    confidence = confidence_for(score, signals, settings)
    notes: list[str] = []

    decision = _threshold_decision(score, settings, notes)
    decision, confidence = _apply_safety_rules(
        decision,
        confidence,
        signals=signals,
        labels=labels,
        scores=scores,
        stage_reasons=stage_reasons,
        settings=settings,
        stage_failed=stage_failed,
        notes=notes,
    )
    return Verdict(
        decision=decision,
        confidence=round(confidence, 4),
        keep_score=score,
        reasons=tuple(dict.fromkeys(notes)),
    )


def _threshold_decision(
    score: float | None,
    settings: CurationSettings,
    notes: list[str],
) -> CurationDecision:
    if score is None:
        notes.append(reasons.MISSING_SIGNALS)
        return CurationDecision.REVIEW
    if score >= settings.keep_threshold:
        notes.append(reasons.SCORE_ABOVE_KEEP)
        return CurationDecision.KEEP
    if score <= settings.reject_threshold:
        notes.append(reasons.SCORE_BELOW_REJECT)
        return CurationDecision.REJECT
    notes.append(reasons.SCORE_IN_BAND)
    return CurationDecision.REVIEW


def _apply_safety_rules(
    decision: CurationDecision,
    confidence: float,
    *,
    signals: Signals,
    labels: Sequence[str],
    scores: Mapping[str, float],
    stage_reasons: Sequence[str],
    settings: CurationSettings,
    stage_failed: bool,
    notes: list[str],
) -> tuple[CurationDecision, float]:
    reason_set = set(stage_reasons)
    screenshot_certain = bool(
        reason_set & (reasons.DECISIVE_UTILITY_SIGNALS | {reasons.SEMANTIC_SCREENSHOT})
    )

    # A stage that could not do its job must not influence the outcome, in
    # either direction: "unknown means review" covers a confident-looking keep
    # assembled from partial evidence just as much as it covers a reject. This
    # used to exempt `keep`, and the guarantee was delivered only as a side
    # effect of the confidence floor below.
    if stage_failed:
        notes.append(reasons.STAGE_ERROR)
        return CurationDecision.REVIEW, min(confidence, 0.5)

    # Two strong, opposing signals mean the file needs a human, whichever way
    # the arithmetic happened to land.
    if (
        signals.personal_probability is not None
        and signals.utility_probability is not None
        and signals.personal_probability >= STRONG_SIGNAL
        and signals.utility_probability >= STRONG_SIGNAL
    ):
        notes.append(reasons.SIGNAL_CONFLICT)
        return CurationDecision.REVIEW, min(confidence, 0.5)

    if decision is CurationDecision.REJECT:
        if has_protected_subject(labels, scores) and not screenshot_certain:
            notes.append(reasons.PROTECTED_SUBJECT)
            return CurationDecision.REVIEW, min(confidence, 0.6)
        # Technical quality and metadata alone are never enough to discard a
        # file: something has to have recognised *what* it is.
        if (
            not (reason_set & reasons.SEMANTIC_REJECT_REASONS)
            and not screenshot_certain
        ):
            notes.append(reasons.NO_SEMANTIC_EVIDENCE)
            return CurationDecision.REVIEW, min(confidence, 0.6)

        # The floor is deliberately one-sided. For a fused verdict, confidence
        # is a monotone function of the distance from the threshold, so asking
        # for it on both sides only re-imposes stricter thresholds - and does it
        # invisibly, because the configured numbers are then not the ones in
        # force. Charging that extra margin to `reject` alone matches the rest
        # of the system: a stage may only ever hand down a terminal `reject`,
        # and a wrong keep leaves clutter in the library while a wrong reject
        # buries a photograph.
        if confidence < settings.minimum_confidence:
            notes.append(reasons.LOW_CONFIDENCE)
            return CurationDecision.REVIEW, confidence

    return decision, confidence


def needs_vlm_escalation(verdict: Verdict, settings: CurationSettings) -> bool:
    """Whether a file is uncertain enough to be worth a VLM call."""
    if verdict.decision is not CurationDecision.REVIEW:
        return False
    low, high = settings.thresholds.vlm_band
    return low <= verdict.confidence <= high


def semantic_reason_for(label: str) -> str:
    """Map a semantic label to the reason code that justifies using it."""
    if label == SemanticLabel.SCREENSHOT.value:
        return reasons.SEMANTIC_SCREENSHOT
    if label in {member.value for member in PERSONAL_LABELS}:
        return reasons.SEMANTIC_PERSONAL
    return reasons.SEMANTIC_UTILITY


def _clamp(value: float | None) -> float | None:
    if value is None:
        return None
    return max(0.0, min(1.0, float(value)))
