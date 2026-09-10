"""Personal-preference model over frozen image embeddings.

A small linear model is the right size here: the training signal is a few
hundred hand-made decisions, so logistic regression over an embedding the
semantic stage already computed generalises better than anything deeper.

Training data must be split by event rather than at random. Photos from one
burst are near-duplicates, and splitting them across train and validation turns
the score into a memorisation check. :func:`split_by_group` enforces that.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import numpy as np

from filecluster.curation.providers.base import ProviderInfo

#: Fewer examples than this per class produce a model that mostly reproduces
#: the class balance, so training refuses to run.
MIN_EXAMPLES_PER_CLASS = 100


@dataclass(frozen=True)
class TrainingReport:
    """What one training run produced, for logging and versioning."""

    n_train: int
    n_validation: int
    validation_accuracy: float
    epochs: int
    dimensions: int


class LogisticPreferenceProvider:
    """Logistic regression over L2-normalised embeddings.

    Implemented directly on numpy so that enabling preference learning does not
    drag in a full machine-learning stack next to the encoder.
    """

    def __init__(
        self,
        weights: Sequence[float] | None = None,
        bias: float = 0.0,
        model_version: str = "preference-logreg-1",
    ) -> None:
        self._weights = (
            None if weights is None else np.asarray(weights, dtype=np.float64)
        )
        self._bias = float(bias)
        self._model_version = model_version

    # -- provider contract -------------------------------------------------
    def info(self) -> ProviderInfo:
        """Return the identity of the trained preference model."""
        return ProviderInfo(
            name="preference",
            model_id=self._model_version,
            revision=f"dims-{0 if self._weights is None else self._weights.size}",
        )

    @property
    def is_trained(self) -> bool:
        """Whether the model has coefficients to score with."""
        return self._weights is not None

    def score(self, embedding: Sequence[float]) -> float:
        """Return the probability that the user would keep this image."""
        if self._weights is None:
            raise ValueError("Preference model has not been trained")
        vector = _normalise(np.asarray(embedding, dtype=np.float64))
        if vector.size != self._weights.size:
            raise ValueError(
                f"Embedding has {vector.size} dimensions, model expects "
                f"{self._weights.size}"
            )
        return float(_sigmoid(float(vector @ self._weights) + self._bias))

    # -- training ----------------------------------------------------------
    def fit(
        self,
        embeddings: Sequence[Sequence[float]],
        keep_labels: Sequence[int],
        groups: Sequence[str],
        *,
        epochs: int = 400,
        learning_rate: float = 0.5,
        l2: float = 1e-3,
        validation_fraction: float = 0.25,
        seed: int = 0,
    ) -> TrainingReport:
        """Fit the model on user feedback, splitting validation by group.

        Raises:
            ValueError: when the inputs disagree in length or either class has
                fewer than :data:`MIN_EXAMPLES_PER_CLASS` examples.
        """
        x = np.asarray(embeddings, dtype=np.float64)
        y = np.asarray(keep_labels, dtype=np.float64)
        if x.ndim != 2 or x.shape[0] != y.size or len(groups) != y.size:
            raise ValueError("embeddings, keep_labels and groups must align")
        _check_class_balance(y)

        x = np.apply_along_axis(_normalise, 1, x)
        train_idx, val_idx = split_by_group(groups, validation_fraction, seed)

        weights = np.zeros(x.shape[1], dtype=np.float64)
        bias = 0.0
        x_train, y_train = x[train_idx], y[train_idx]
        for _ in range(epochs):
            predictions = _sigmoid(x_train @ weights + bias)
            error = predictions - y_train
            weights -= learning_rate * (
                x_train.T @ error / max(len(train_idx), 1) + l2 * weights
            )
            bias -= learning_rate * float(error.mean())

        self._weights = weights
        self._bias = bias
        accuracy = 1.0
        if len(val_idx):
            predicted = _sigmoid(x[val_idx] @ weights + bias) >= 0.5
            accuracy = float((predicted == (y[val_idx] >= 0.5)).mean())
        return TrainingReport(
            n_train=len(train_idx),
            n_validation=len(val_idx),
            validation_accuracy=accuracy,
            epochs=epochs,
            dimensions=int(x.shape[1]),
        )

    def state_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable form of the trained coefficients."""
        return {
            "model_version": self._model_version,
            "bias": self._bias,
            "weights": [] if self._weights is None else self._weights.tolist(),
        }

    @classmethod
    def from_state_dict(cls, state: dict[str, object]) -> LogisticPreferenceProvider:
        """Rebuild a provider stored by :meth:`state_dict`."""
        weights = state.get("weights") or None
        return cls(
            weights=weights,  # ty: ignore[invalid-argument-type]
            bias=float(state.get("bias") or 0.0),  # ty: ignore[invalid-argument-type]
            model_version=str(state.get("model_version") or "preference-logreg-1"),
        )


def split_by_group(
    groups: Sequence[str],
    validation_fraction: float = 0.25,
    seed: int = 0,
) -> tuple[list[int], list[int]]:
    """Split indices so that no group appears in both halves."""
    unique = sorted(set(groups))
    rng = np.random.default_rng(seed)
    rng.shuffle(unique)
    n_val_groups = round(len(unique) * validation_fraction)
    val_groups = set(unique[:n_val_groups])
    train = [i for i, g in enumerate(groups) if g not in val_groups]
    validation = [i for i, g in enumerate(groups) if g in val_groups]
    return train, validation


def _check_class_balance(labels: Iterable[float]) -> None:
    values = list(labels)
    keeps = sum(1 for v in values if v >= 0.5)
    rejects = len(values) - keeps
    if min(keeps, rejects) < MIN_EXAMPLES_PER_CLASS:
        raise ValueError(
            f"Need at least {MIN_EXAMPLES_PER_CLASS} keep and reject examples, "
            f"got {keeps} keep / {rejects} reject"
        )


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def _normalise(vector: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    return vector if norm == 0.0 else vector / norm
