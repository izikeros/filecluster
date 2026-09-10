"""Cascaded curation of inbox media.

Classifies photos and videos as ``keep``, ``review`` or ``reject`` before the
usual event clustering runs, so utility material (screenshots, receipts,
documents, product shots) does not end up in a personal photo library.

The cascade runs cheapest-signal-first and stops as soon as one stage is
confident enough:

1. metadata and filename rules,
2. lightweight pixel features (and optional OCR),
3. semantic classification with a vision-language encoder,
4. quality, aesthetics and learned personal preference,
5. optional VLM escalation for whatever is still uncertain.

Two properties hold at every stage: nothing is ever deleted, and an unknown or
failed file goes to ``review`` rather than ``reject``.

Library use, which renders nothing and writes no media files::

    from filecluster.curation import CurationSettings, curate

    run = curate("inbox", CurationSettings())
    print(run.decision_counts())

The command line entry point lives in :mod:`filecluster.curation.cli`.
"""

from filecluster.curation.configuration import (
    PIPELINE_VERSION,
    CurationSettings,
    load_settings,
)
from filecluster.curation.exceptions import (
    CurationConfigError,
    CurationError,
    MissingDependencyError,
    ProviderUnavailableError,
    UnsafeRelativePathError,
)
from filecluster.curation.operations import (
    CurationFileOp,
    CurationOperationPlan,
    OperationMode,
    OperationStatus,
    build_operation_plan,
    execute_plan,
)
from filecluster.curation.pipeline import (
    CurationPipeline,
    CurationRun,
    build_stages,
    curate,
    discover_media,
)
from filecluster.curation.providers.base import Providers
from filecluster.curation.reporting import json_summary, report_rows, write_report
from filecluster.curation.types import (
    CurationDecision,
    CurationResult,
    MediaItem,
    MediaKind,
    SemanticLabel,
    StageResult,
)

__all__ = [
    "PIPELINE_VERSION",
    "CurationConfigError",
    "CurationDecision",
    "CurationError",
    "CurationFileOp",
    "CurationOperationPlan",
    "CurationPipeline",
    "CurationResult",
    "CurationRun",
    "CurationSettings",
    "MediaItem",
    "MediaKind",
    "MissingDependencyError",
    "OperationMode",
    "OperationStatus",
    "ProviderUnavailableError",
    "Providers",
    "SemanticLabel",
    "StageResult",
    "UnsafeRelativePathError",
    "build_operation_plan",
    "build_stages",
    "curate",
    "discover_media",
    "execute_plan",
    "json_summary",
    "load_settings",
    "report_rows",
    "write_report",
]
