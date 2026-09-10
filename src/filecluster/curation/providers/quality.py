"""Optional aesthetic-quality provider.

The first version of the pipeline scores technical quality from the cheap pixel
features and leaves aesthetics unmeasured, which the fusion step handles by
dropping the weight rather than substituting a zero.

This module is the seam for a later NIMA / MUSIQ / TOPIQ style predictor: it has
to satisfy :class:`~filecluster.curation.providers.base.QualityProvider` and
nothing else in the pipeline changes.
"""

from __future__ import annotations

from typing import Any

from filecluster.curation.exceptions import MissingDependencyError
from filecluster.curation.providers.base import ProviderInfo

_EXTRA = "curation"


class AestheticProvider:
    """Placeholder for a learned aesthetic predictor.

    Constructing it fails with an install hint instead of silently scoring
    everything the same, because a constant aesthetic signal would shift every
    verdict by the same amount and look like a working model.
    """

    def __init__(self, model_id: str = "unset", revision: str = "unset") -> None:
        self._model_id = model_id
        self._revision = revision
        self._model: Any = None
        raise MissingDependencyError(
            "Aesthetic scoring is not implemented yet; it is planned for a "
            "later phase and",
            _EXTRA,
        )

    def info(self) -> ProviderInfo:  # pragma: no cover - unreachable for now
        """Return the pinned identity of the aesthetic model."""
        return ProviderInfo(
            name="quality", model_id=self._model_id, revision=self._revision
        )

    def score(self, image: object) -> float:  # pragma: no cover - unreachable
        """Return the aesthetic score of *image* in ``[0, 1]``."""
        raise NotImplementedError
