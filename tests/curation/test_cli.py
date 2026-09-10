"""Integration tests for ``filecluster curate``.

The pipeline runs for real over synthesised images; only the terminal is faked,
by way of ``CliRunner``.
"""

import csv
import json

from typer.testing import CliRunner

from filecluster.curation.cli import EXIT_OK, EXIT_USAGE, app
from filecluster.curation.reporting import REPORT_COLUMNS

runner = CliRunner()


def files_under(path) -> list[str]:
    """Media files below *path*, ignoring the cache database and other dot-files."""
    return sorted(
        p.name for p in path.rglob("*") if p.is_file() and not p.name.startswith(".")
    )


class TestHelp:
    def test_help_works_without_configuration(self):
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == EXIT_OK
        assert "keep" in result.output

    def test_copy_and_move_are_mutually_exclusive(self, inbox, out_dir):
        result = runner.invoke(
            app,
            ["-i", str(inbox), "-o", str(out_dir), "--execute", "--copy", "--move"],
        )

        assert result.exit_code == EXIT_USAGE

    def test_a_missing_inbox_is_a_usage_error(self, tmp_path, out_dir):
        result = runner.invoke(
            app, ["-i", str(tmp_path / "absent"), "-o", str(out_dir)]
        )

        assert result.exit_code == EXIT_USAGE


class TestDryRun:
    def test_writes_nothing_by_default(self, inbox, out_dir):
        before = files_under(inbox)

        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir)])

        assert result.exit_code == EXIT_OK
        assert files_under(inbox) == before
        assert files_under(out_dir) == []

    def test_an_empty_inbox_succeeds(self, tmp_path, out_dir):
        empty = tmp_path / "empty"
        empty.mkdir()

        result = runner.invoke(app, ["-i", str(empty), "-o", str(out_dir), "--json"])

        assert result.exit_code == EXIT_OK
        assert json.loads(result.stdout)["files"] == 0

    def test_output_is_bounded_and_never_per_file(self, inbox, out_dir):
        """A 50k-file inbox may not print 50k lines, so no path may appear."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir)])

        assert "Results" in result.output
        assert result.output.count("\n") < 60


class TestExecute:
    def test_copy_sorts_into_decision_folders(self, inbox, out_dir):
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--execute", "--copy", "-Y"]
        )

        assert result.exit_code == EXIT_OK
        assert (out_dir / "reject" / "Screenshot_2024-05-01.png").exists()
        assert (out_dir / "review" / "notes" / "receipt.png").exists()
        assert len(files_under(inbox)) == 3  # sources untouched

    def test_move_empties_the_inbox(self, inbox, out_dir):
        runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--execute", "--move", "-Y"]
        )

        assert files_under(inbox) == []
        assert len(files_under(out_dir)) == 3

    def test_execute_defaults_to_copying(self, inbox, out_dir):
        """The safer of the two operations is the default."""
        runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--execute", "-Y"])

        assert len(files_under(inbox)) == 3
        assert len(files_under(out_dir)) == 3

    def test_nothing_is_overwritten(self, inbox, out_dir):
        (out_dir / "reject").mkdir()
        target = out_dir / "reject" / "Screenshot_2024-05-01.png"
        target.write_bytes(b"existing")

        runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--execute", "--copy", "-Y"]
        )

        assert target.read_bytes() == b"existing"
        assert (out_dir / "reject" / "Screenshot_2024-05-01 (1).png").exists()


class TestOutputs:
    def test_json_is_the_only_thing_on_stdout(self, inbox, out_dir):
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--json"])

        payload = json.loads(result.stdout)
        assert payload["decisions"]["reject"] == 1
        assert payload["executed"] is False

    def test_report_has_the_documented_columns(self, inbox, out_dir, tmp_path):
        report = tmp_path / "curation.csv"

        runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--report", str(report)]
        )

        with open(report, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            assert tuple(reader.fieldnames) == REPORT_COLUMNS
            assert len(list(reader)) == 3

    def test_limit_selects_a_deterministic_subset(self, inbox, out_dir):
        first = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--limit", "2", "--json"]
        )
        second = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--limit", "2", "--json"]
        )

        assert json.loads(first.stdout)["files"] == 2
        assert (
            json.loads(second.stdout)["decisions"]
            == json.loads(first.stdout)["decisions"]
        )

    def test_the_second_run_uses_the_cache(self, inbox, out_dir):
        runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--json"])

        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--json"])

        assert json.loads(result.stdout)["cache_hits"] == 3

    def test_force_recompute_bypasses_the_cache(self, inbox, out_dir):
        runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--json"])

        result = runner.invoke(
            app,
            ["-i", str(inbox), "-o", str(out_dir), "--json", "--force-recompute"],
        )

        assert json.loads(result.stdout)["cache_hits"] == 0

    def test_no_cache_leaves_no_database(self, inbox, out_dir):
        runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "--no-cache"])

        assert not (inbox / ".filecluster-curation.db").exists()

    def test_a_broken_config_file_is_a_usage_error(self, inbox, out_dir, tmp_path):
        config = tmp_path / "curation.json"
        config.write_text('{"thresholds": {"keep": 0.2, "reject": 0.8}}')

        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--config", str(config)]
        )

        assert result.exit_code == EXIT_USAGE

    def test_a_config_file_changes_the_thresholds(self, inbox, out_dir, tmp_path):
        config = tmp_path / "curation.json"
        config.write_text(json.dumps({"thresholds": {"keep": 0.05, "reject": 0.01}}))

        result = runner.invoke(
            app,
            ["-i", str(inbox), "-o", str(out_dir), "--config", str(config), "--json"],
        )

        assert json.loads(result.stdout)["decisions"]["keep"] >= 1

    def test_a_missing_extra_does_not_stop_the_run(self, inbox, out_dir):
        """Without the semantic extra installed the run continues with fewer signals."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "--with-semantic", "--json"]
        )

        assert result.exit_code == EXIT_OK
        assert json.loads(result.stdout)["files"] == 3
