"""Optional vision-language escalation for files nothing else could settle.

A VLM answer is untrusted input. It is parsed as JSON, validated against a fixed
shape, and clamped to the known decision and label vocabulary. Free text from the
model is never used as a path, a command or a report field.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from filecluster.curation.exceptions import (
    MissingDependencyError,
    ProviderUnavailableError,
)
from filecluster.curation.providers.base import ProviderInfo, VlmJudgement
from filecluster.curation.types import CurationDecision, SemanticLabel

_EXTRA = "curation-vlm"

#: Reasons are stored for explanation only, so they are length-capped rather
#: than trusted; an unbounded model string has no business in a CSV cell.
MAX_REASON_LENGTH = 200
MAX_REASONS = 4

PROMPT = (
    "Classify this image for a personal photo library. Answer with JSON only, "
    'using the shape {"decision": "keep|review|reject", "confidence": 0.0-1.0, '
    '"labels": [], "reasons": []}.'
)


def parse_vlm_response(payload: str | Mapping[str, Any]) -> VlmJudgement:
    """Validate a VLM answer and turn it into a judgement.

    Raises:
        ProviderUnavailableError: when the answer is not valid JSON, is missing
            required fields, or uses an unknown decision value. The pipeline
            turns that into ``review``.
    """
    if isinstance(payload, str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ProviderUnavailableError(f"VLM returned invalid JSON: {exc}") from exc
    else:
        data = payload

    if not isinstance(data, Mapping):
        raise ProviderUnavailableError("VLM answer is not a JSON object")

    try:
        decision = CurationDecision(str(data["decision"]).strip().lower())
    except (KeyError, ValueError) as exc:
        raise ProviderUnavailableError(
            f"VLM answer has no usable decision: {data.get('decision')!r}"
        ) from exc

    try:
        confidence = float(data.get("confidence", 0.0))
    except (TypeError, ValueError) as exc:
        raise ProviderUnavailableError("VLM confidence is not a number") from exc
    confidence = max(0.0, min(1.0, confidence))

    known = {label.value for label in SemanticLabel}
    labels = tuple(
        str(item) for item in _as_sequence(data.get("labels")) if str(item) in known
    )
    reasons = tuple(
        str(item)[:MAX_REASON_LENGTH]
        for item in _as_sequence(data.get("reasons"))[:MAX_REASONS]
    )
    return VlmJudgement(
        decision=decision,
        confidence=confidence,
        labels=labels,
        reasons=reasons,
    )


def _as_sequence(value: object) -> list[Any]:
    if isinstance(value, str | bytes | Mapping) or not isinstance(value, Iterable):
        return []
    return list(value)


class LocalVlmProvider:
    """Escalation through a locally hosted vision-language model.

    Left unimplemented on purpose: choosing a runtime and a checkpoint is a
    measured decision (licence, install size, throughput on the target machine)
    that belongs to the phase that enables this stage.
    """

    def __init__(self, model_id: str = "unset", revision: str = "unset") -> None:
        self._model_id = model_id
        self._revision = revision
        self._model: Any = None
        raise MissingDependencyError(
            "VLM escalation is not implemented yet; it is planned for a later "
            "phase and",
            _EXTRA,
        )

    def info(self) -> ProviderInfo:  # pragma: no cover - unreachable for now
        """Return the pinned identity of the VLM."""
        return ProviderInfo(
            name="vlm", model_id=self._model_id, revision=self._revision
        )

    def judge(self, image: object) -> VlmJudgement:  # pragma: no cover
        """Return a structured verdict for *image*."""
        raise ProviderUnavailableError("No VLM backend is configured")
