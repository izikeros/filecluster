"""Report rows and summaries for a curation run.

This module builds data, never terminal output: a 50k-file run produces 50k rows
in a CSV file and a handful of aggregate numbers on screen.

Only the named signals reach the report. OCR text, embeddings and raw model
answers stay inside the pipeline, because a report file of a personal photo
library is easy to share by accident.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from pathlib import Path

from filecluster.curation.operations import CurationOperationPlan, OperationStatus
from filecluster.curation.pipeline import CurationRun
from filecluster.curation.types import SIGNAL_KEYS, CurationResult

REPORT_COLUMNS: tuple[str, ...] = (
    "source_path",
    "destination_path",
    "sha256",
    "decision",
    "confidence",
    "top_label",
    "labels",
    "personal_probability",
    "utility_probability",
    "technical_quality",
    "aesthetic_score",
    "preference_score",
    "reasons",
    "completed_stage",
    "cache_hit",
    "operation_status",
    "duration_ms",
    "pipeline_version",
    "model_fingerprint",
)

#: How many reason codes the terminal summary lists.
MAX_TOP_REASONS = 10


def report_row(
    result: CurationResult,
    plan: CurationOperationPlan | None,
    model_fingerprint: str,
) -> dict[str, object]:
    """Build one report row for *result*."""
    destination = plan.destination_for(result.item.path) if plan else None
    status = (
        plan.status_for(result.item.path).value
        if plan
        else OperationStatus.PLANNED.value
    )
    row: dict[str, object] = {
        "source_path": result.item.relative_path,
        "destination_path": str(destination) if destination else "",
        "sha256": result.item.sha256,
        "decision": result.decision.value,
        "confidence": round(result.confidence, 4),
        "top_label": result.top_label,
        "labels": _json_list(result.labels),
        "reasons": _json_list(result.reasons),
        "completed_stage": result.completed_stage,
        "cache_hit": int(result.cache_hit),
        "operation_status": status,
        "duration_ms": round(result.duration_ms, 3),
        "pipeline_version": result.pipeline_version,
        "model_fingerprint": model_fingerprint,
    }
    for key in SIGNAL_KEYS:
        value = result.signal(key)
        row[key] = "" if value is None else round(value, 4)
    return row


def report_rows(
    run: CurationRun,
    plan: CurationOperationPlan | None = None,
) -> list[dict[str, object]]:
    """Build a report row for every result of *run*."""
    fingerprint = run.cache_key.model_fingerprint
    return [report_row(result, plan, fingerprint) for result in run.results]


def write_report(
    path: str | Path,
    run: CurationRun,
    plan: CurationOperationPlan | None = None,
) -> int:
    """Write the per-file report to *path* as CSV. Returns the row count."""
    rows = report_rows(run, plan)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REPORT_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def json_summary(
    run: CurationRun,
    plan: CurationOperationPlan | None = None,
) -> dict[str, object]:
    """Build the aggregate summary printed by ``--json``.

    Aggregates only, so the payload is the same size for 50 files and 50,000.
    """
    summary: dict[str, object] = {
        "files": len(run.results),
        "discovered": run.n_discovered,
        "skipped": run.n_skipped,
        "cache_hits": run.n_cache_hits,
        "decisions": run.decision_counts(),
        "errors": run.n_errors,
        "elapsed_seconds": round(run.elapsed_seconds, 3),
        "executed": run.executed,
        "pipeline_version": run.cache_key.pipeline_version,
        "config_fingerprint": run.cache_key.config_fingerprint,
        "model_fingerprint": run.cache_key.model_fingerprint,
        "top_reasons": [
            {"reason": reason, "files": count}
            for reason, count in run.reason_counts().most_common(MAX_TOP_REASONS)
        ],
    }
    if plan is not None:
        summary["operations"] = {
            "mode": plan.mode.value,
            "planned": len(plan.ops),
            "copies": plan.n_copies,
            "moves": plan.n_moves,
            "renamed": plan.n_renamed,
            "completed": plan.n_completed,
            "failed": plan.n_failed,
            "by_decision": plan.counts_by_decision(),
        }
    return summary


def top_reasons(
    run: CurationRun, limit: int = MAX_TOP_REASONS
) -> list[tuple[str, int]]:
    """Return the most frequent reason codes of *run*."""
    return run.reason_counts().most_common(limit)


def _json_list(values: Sequence[str]) -> str:
    """Serialise a list into one stable CSV cell."""
    return json.dumps(list(values), separators=(",", ":"))
