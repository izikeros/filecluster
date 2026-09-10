"""Model providers for the optional stages of the curation cascade.

Only :mod:`filecluster.curation.providers.base` is imported here: the concrete
providers import machine-learning runtimes lazily, and pulling them in at package
import time would make ``filecluster run`` pay for a feature it does not use.
"""

from filecluster.curation.providers.base import (
    OcrAggregates,
    OcrProvider,
    PreferenceProvider,
    ProviderInfo,
    Providers,
    QualityProvider,
    SemanticPrediction,
    SemanticProvider,
    VlmJudgement,
    VlmProvider,
    model_fingerprint,
)

__all__ = [
    "OcrAggregates",
    "OcrProvider",
    "PreferenceProvider",
    "ProviderInfo",
    "Providers",
    "QualityProvider",
    "SemanticPrediction",
    "SemanticProvider",
    "VlmJudgement",
    "VlmProvider",
    "model_fingerprint",
]
