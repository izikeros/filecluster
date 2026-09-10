"""Semantic classification with a vision-language encoder.

The encoder of choice is a multilingual SigLIP 2 checkpoint, pinned to a
revision. Nothing here is imported unless the semantic stage is switched on, and
the weights load on the first image that actually reaches the stage.

Raw cosine similarity is not a calibrated probability. The aggregated per-label
score is treated as evidence, and the fusion step is where it turns into a
decision; calibration on a private evaluation set is still required before these
numbers deserve the name "probability".
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from importlib.util import find_spec
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.curation.exceptions import (
    MissingDependencyError,
    ProviderUnavailableError,
)
from filecluster.curation.providers.base import ProviderInfo, SemanticPrediction

#: Pinned checkpoint. Never resolve to "latest": a silent model update would
#: change verdicts without invalidating anything the user can see.
DEFAULT_MODEL_ID = "google/siglip2-base-patch16-224"
DEFAULT_REVISION = "main"

#: How many prompts per label are averaged into the label score. Averaging the
#: best two is more stable than trusting a single phrasing.
PROMPTS_PER_LABEL = 2

_DATA_PACKAGE = "filecluster.curation.data"
_PROMPT_BANK_FILE = "prompt_bank.json"

_EXTRA = "curation"


@dataclass(frozen=True)
class PromptBank:
    """Versioned prompts, one list per semantic label."""

    version: int
    prompts: dict[str, tuple[str, ...]]

    @property
    def labels(self) -> tuple[str, ...]:
        """Labels in a stable order."""
        return tuple(sorted(self.prompts))

    def flat(self) -> tuple[list[str], list[str]]:
        """Return parallel lists of prompt texts and their labels."""
        texts: list[str] = []
        owners: list[str] = []
        for label in self.labels:
            for prompt in self.prompts[label]:
                texts.append(prompt)
                owners.append(label)
        return texts, owners


@lru_cache(maxsize=4)
def load_prompt_bank(path: Path | None = None) -> PromptBank:
    """Load the prompt bank from the packaged data file or *path*.

    Raises:
        ProviderUnavailableError: when the file is missing or malformed. A
            broken prompt bank must fail loudly rather than silently classify
            everything as ``other``.
    """
    try:
        if path is None:
            text = (
                resources.files(_DATA_PACKAGE)
                .joinpath(_PROMPT_BANK_FILE)
                .read_text(encoding="utf-8")
            )
        else:
            text = Path(path).read_text(encoding="utf-8")
        data = json.loads(text)
        labels = data["labels"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ProviderUnavailableError(f"Unusable prompt bank: {exc}") from exc

    prompts = {
        str(label): tuple(str(p) for p in texts)
        for label, texts in labels.items()
        if texts
    }
    if not prompts:
        raise ProviderUnavailableError("Prompt bank contains no prompts")
    return PromptBank(version=int(data.get("version", 0)), prompts=prompts)


def semantic_extra_available() -> bool:
    """Whether the optional encoder runtime is importable."""
    return all(find_spec(mod) is not None for mod in ("torch", "transformers"))


class SigLipSemanticProvider:
    """SigLIP-style zero-shot classifier over the prompt bank."""

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        revision: str = DEFAULT_REVISION,
        device: str = "auto",
        prompt_bank: PromptBank | None = None,
        batch_size: int = 8,
    ) -> None:
        if not semantic_extra_available():
            raise MissingDependencyError("Semantic classification", _EXTRA)
        self._model_id = model_id
        self._revision = revision
        self._device_preference = device
        self._batch_size = max(1, batch_size)
        self._bank = prompt_bank or load_prompt_bank()
        self._model: Any = None
        self._processor: Any = None
        self._device: Any = None
        self._prompt_embeddings: Any = None
        self._prompt_owners: list[str] = []

    # -- identity ----------------------------------------------------------
    def info(self) -> ProviderInfo:
        """Return the pinned identity of the encoder and its prompts."""
        return ProviderInfo(
            name="semantic",
            model_id=self._model_id,
            revision=self._revision,
            preprocessor_version=self._model_id,
            prompt_bank_version=str(self._bank.version),
        )

    # -- inference ---------------------------------------------------------
    def classify(self, images: Sequence[object]) -> list[SemanticPrediction]:
        """Score every image against every label of the prompt bank."""
        if not images:
            return []
        self._ensure_loaded()
        import torch  # ty: ignore[unresolved-import]

        out: list[SemanticPrediction] = []
        for start in range(0, len(images), self._batch_size):
            batch = list(images[start : start + self._batch_size])
            inputs = self._processor(images=batch, return_tensors="pt").to(self._device)
            with torch.no_grad():
                features = self._model.get_image_features(**inputs)
                features = features / features.norm(dim=-1, keepdim=True)
                similarity = features @ self._prompt_embeddings.T
            out.extend(self._aggregate(row) for row in similarity.cpu().tolist())
        return out

    def _aggregate(self, similarities: Sequence[float]) -> SemanticPrediction:
        """Average the best prompts of each label into one score per label."""
        per_label: dict[str, list[float]] = {}
        for value, label in zip(similarities, self._prompt_owners, strict=True):
            per_label.setdefault(label, []).append(float(value))
        scores = {}
        for label, values in per_label.items():
            best = sorted(values, reverse=True)[:PROMPTS_PER_LABEL]
            # Cosine similarity lives in [-1, 1]; the shift to [0, 1] keeps the
            # score comparable with the other signals without pretending to be
            # calibrated.
            scores[label] = max(0.0, min(1.0, (sum(best) / len(best) + 1.0) / 2.0))
        return SemanticPrediction(scores=scores)

    # -- lazy loading ------------------------------------------------------
    def _ensure_loaded(self) -> None:
        if self._model is not None:
            return
        try:
            import torch  # ty: ignore[unresolved-import]
            from transformers import (  # ty: ignore[unresolved-import]
                AutoModel,
                AutoProcessor,
            )
        except ImportError as exc:  # pragma: no cover - guarded in __init__
            raise MissingDependencyError("Semantic classification", _EXTRA) from exc

        self._device = torch.device(resolve_device(self._device_preference))
        logger.info(f"Loading {self._model_id} on {self._device}")
        try:
            self._model = AutoModel.from_pretrained(
                self._model_id, revision=self._revision
            )
            self._processor = AutoProcessor.from_pretrained(
                self._model_id, revision=self._revision
            )
        except Exception as exc:
            raise ProviderUnavailableError(
                f"Could not load {self._model_id}@{self._revision}: {exc}"
            ) from exc
        self._model.eval().to(self._device)
        self._embed_prompts()

    def _embed_prompts(self) -> None:
        import torch  # ty: ignore[unresolved-import]

        texts, owners = self._bank.flat()
        self._prompt_owners = owners
        inputs = self._processor(
            text=texts, padding="max_length", return_tensors="pt"
        ).to(self._device)
        with torch.no_grad():
            embeddings = self._model.get_text_features(**inputs)
        self._prompt_embeddings = embeddings / embeddings.norm(dim=-1, keepdim=True)


def resolve_device(preference: str = "auto") -> str:
    """Pick a torch device name, honouring an explicit preference first."""
    if preference != "auto":
        return preference
    try:
        import torch  # ty: ignore[unresolved-import]
    except ImportError:  # pragma: no cover - only reachable without the extra
        return "cpu"
    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    return "cpu"
