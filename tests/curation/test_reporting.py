"""Tests for the CSV report and the JSON summary."""

import csv
import json

from filecluster.curation.configuration import CurationSettings
from filecluster.curation.operations import (
    OperationMode,
    build_operation_plan,
    execute_plan,
)
from filecluster.curation.pipeline import CurationPipeline
from filecluster.curation.providers.base import OcrAggregates, Providers
from filecluster.curation.reporting import (
    REPORT_COLUMNS,
    json_summary,
    report_rows,
    top_reasons,
    write_report,
)

from .conftest import FakeOcrProvider


def run_over(inbox, settings, providers=None):
    return CurationPipeline(settings, providers).run(inbox)


class TestCsvReport:
    def test_has_the_documented_columns(self, inbox, settings, tmp_path):
        run = run_over(inbox, settings)
        path = tmp_path / "report.csv"

        rows = write_report(path, run)

        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            assert tuple(reader.fieldnames) == REPORT_COLUMNS
            assert len(list(reader)) == rows == 3

    def test_lists_are_stored_as_json(self, inbox, settings):
        run = run_over(inbox, settings)

        row = report_rows(run)[0]

        assert isinstance(json.loads(str(row["labels"])), list)
        assert isinstance(json.loads(str(row["reasons"])), list)

    def test_missing_signals_are_blank_not_zero(self, inbox, settings):
        run = run_over(inbox, settings)

        rows = report_rows(run)

        assert all(row["aesthetic_score"] == "" for row in rows)

    def test_destination_and_status_come_from_the_plan(self, inbox, settings, out_dir):
        run = run_over(inbox, settings)
        plan = build_operation_plan(run.results, out_dir, OperationMode.COPY)
        execute_plan(plan)

        rows = report_rows(run, plan)

        assert all(row["destination_path"] for row in rows)
        assert {row["operation_status"] for row in rows} == {"completed"}

    def test_a_dry_run_reports_planned_operations(self, inbox, settings, out_dir):
        run = run_over(inbox, settings)
        plan = build_operation_plan(run.results, out_dir, OperationMode.SKIP)

        rows = report_rows(run, plan)

        assert {row["operation_status"] for row in rows} == {"planned"}

    def test_no_ocr_text_or_embeddings_are_written(self, inbox):
        """A report of a photo library must not carry recognised text."""
        settings = CurationSettings(enable_ocr=True)
        providers = Providers(
            ocr=FakeOcrProvider(
                OcrAggregates(blocks=4, lines=4, characters=80, mean_confidence=0.9)
            )
        )
        run = run_over(inbox, settings, providers)

        rows = report_rows(run)

        for row in rows:
            assert set(row) == set(REPORT_COLUMNS)
            assert not any("embedding" in key or "ocr" in key for key in row)

    def test_cache_hits_are_flagged(self, inbox, settings):
        run_over(inbox, settings)
        from filecluster.curation.pipeline import curate

        curate(inbox, settings)
        second = curate(inbox, settings)

        assert all(row["cache_hit"] == 1 for row in report_rows(second))


class TestJsonSummary:
    def test_is_aggregate_only(self, inbox, settings):
        run = run_over(inbox, settings)

        summary = json_summary(run)

        assert summary["files"] == 3
        assert set(summary["decisions"]) == {"keep", "review", "reject"}
        assert summary["executed"] is False
        assert "results" not in summary
        # The payload has to stay the same size for 50 or 50,000 files.
        assert len(summary["top_reasons"]) <= 10

    def test_is_json_serialisable(self, inbox, settings, out_dir):
        run = run_over(inbox, settings)
        plan = build_operation_plan(run.results, out_dir, OperationMode.COPY)

        text = json.dumps(json_summary(run, plan))

        assert "operations" in json.loads(text)

    def test_reports_the_fingerprints(self, inbox, settings):
        run = run_over(inbox, settings)

        summary = json_summary(run)

        assert summary["config_fingerprint"]
        assert summary["model_fingerprint"]

    def test_operation_counts_come_from_the_plan(self, inbox, settings, out_dir):
        run = run_over(inbox, settings)
        plan = build_operation_plan(run.results, out_dir, OperationMode.MOVE)
        execute_plan(plan)

        summary = json_summary(run, plan)

        assert summary["operations"]["moves"] == 3
        assert summary["operations"]["completed"] == 3


def test_top_reasons_are_capped(inbox, settings):
    run = run_over(inbox, settings)

    assert len(top_reasons(run, limit=2)) <= 2
