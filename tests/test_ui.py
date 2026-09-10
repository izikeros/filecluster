"""Tests for the terminal presentation layer.

The point of these tests is not "does it look nice" but "does it stay bounded".
A session may process tens of thousands of files, so every renderer must
produce output whose size is independent of the input size.

Mocking Strategy: none. A recording ``Console`` with a fixed width captures
exactly what a user would see.
"""

import io
import json
from pathlib import Path

import pytest
from rich.console import Console

from filecluster import ui
from filecluster.configuration import CopyMode, get_default_config
from filecluster.file_operations import CopyOp, FileOperationPlan, MkdirOp, SkipOp


@pytest.fixture
def console():
    """A console that records output at a fixed width."""
    return Console(record=True, width=100, force_terminal=False, no_color=True)


def output(console) -> str:
    """Return everything printed to *console* so far.

    ``clear=False`` keeps the recording intact so a test can assert on the
    output more than once.
    """
    return console.export_text(clear=False)


def lines(console) -> list[str]:
    """Return the non-empty output lines."""
    return [line for line in output(console).splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
class TestFormatting:
    """Numbers are the main thing a user reads, so they get separators."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (0, "0"),
            (7, "7"),
            (1000, "1,000"),
            (50000, "50,000"),
            (1234567, "1,234,567"),
        ],
    )
    def test_fmt_count_uses_thousands_separators(self, value, expected):
        """Large counts are readable at a glance."""
        assert ui.fmt_count(value) == expected

    def test_fmt_count_tolerates_non_numbers(self):
        """A missing value renders as zero rather than crashing the summary."""
        assert ui.fmt_count(None) == "0"
        assert ui.fmt_count("abc") == "0"

    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [(0, "0:00:00"), (59.4, "0:00:59"), (61, "0:01:01"), (3725, "1:02:05")],
    )
    def test_fmt_duration(self, seconds, expected):
        """Durations render as h:mm:ss."""
        assert ui.fmt_duration(seconds) == expected

    def test_fmt_files_is_singular_for_one(self):
        """One file is not "1 files"."""
        assert ui.fmt_files(1) == "1 file"
        assert ui.fmt_files(2) == "2 files"
        assert ui.fmt_files(50000) == "50,000 files"


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
class TestDiagnostics:
    """Per-file problems must collapse into one line per kind."""

    def test_thousands_of_problems_render_as_one_line(self, console):
        """5,000 identical problems produce a single line, not 5,000."""
        collector = ui.Diagnostics()
        for i in range(5000):
            collector.add("no EXIF date", f"file_{i}.jpg")

        ui.render_diagnostics(console, collector, verbose=0)

        rendered = lines(console)
        assert len(rendered) == 2  # the aggregate line plus the "-v" hint
        assert "5,000 files: no EXIF date" in rendered[0]

    def test_samples_are_capped_even_at_scale(self):
        """Only a bounded number of examples is retained in memory."""
        collector = ui.Diagnostics()
        for i in range(5000):
            collector.add("unreadable video timestamp", f"file_{i}.mov")

        item = collector.items[0]
        assert item.count == 5000
        assert len(item.samples) == ui.MAX_DIAGNOSTIC_SAMPLES

    def test_verbose_lists_samples_and_says_how_many_are_hidden(self, console):
        """At -v the retained examples are shown, with the remainder counted."""
        collector = ui.Diagnostics()
        for i in range(100):
            collector.add("no EXIF date", f"file_{i}.jpg")

        ui.render_diagnostics(console, collector, verbose=1)

        text = output(console)
        assert "file_0.jpg" in text
        assert f"… {100 - ui.MAX_DIAGNOSTIC_SAMPLES} more" in text

    def test_multiple_kinds_are_ordered_by_impact(self):
        """The problem affecting most files is reported first."""
        collector = ui.Diagnostics()
        collector.add_count("rare", 3)
        collector.add_count("common", 900)

        assert [item.kind for item in collector.items] == ["common", "rare"]

    def test_nothing_is_printed_when_there_are_no_problems(self, console):
        """A clean run adds no diagnostic noise."""
        ui.render_diagnostics(console, ui.Diagnostics())
        assert output(console) == ""

    def test_add_count_ignores_non_positive(self):
        """Zero occurrences do not create an empty diagnostic."""
        collector = ui.Diagnostics()
        collector.add_count("nothing", 0)
        assert len(collector) == 0

    def test_reset_clears_state(self):
        """The module-level collector can be reused between runs."""
        collector = ui.Diagnostics()
        collector.add("a", "x")
        collector.reset()
        assert len(collector) == 0


# ---------------------------------------------------------------------------
# Cluster listing
# ---------------------------------------------------------------------------
class TestLargestClusters:
    """Cluster listings are capped, with the tail summarized."""

    def test_800_clusters_render_within_a_fixed_line_budget(self, console):
        """A large run lists only the biggest clusters."""
        clusters = [(f"[2024_01_{i:02d}]_cluster", i) for i in range(1, 801)]

        ui.largest_clusters(console, clusters)

        rendered = lines(console)
        # heading + header row + capped rows + the "and N more" line
        assert len(rendered) <= ui.MAX_CLUSTER_ROWS + 4
        assert "and 780 more clusters" in rendered[-1]

    def test_tail_summary_counts_the_hidden_files(self, console):
        """The collapsed tail reports how many files it stands for."""
        clusters = [(f"c{i}", 10) for i in range(30)]

        ui.largest_clusters(console, clusters, limit=5)

        # 25 hidden clusters holding 10 files each
        assert "25 more clusters (250 files)" in output(console)

    def test_biggest_cluster_is_listed_first(self, console):
        """Ordering is by file count, not by name."""
        ui.largest_clusters(console, [("small", 1), ("huge", 999), ("mid", 50)])

        rendered = output(console)
        assert rendered.index("huge") < rendered.index("mid") < rendered.index("small")

    def test_limit_zero_shows_everything(self, console):
        """`--show 0` opts out of the cap."""
        ui.largest_clusters(console, [(f"c{i}", 1) for i in range(40)], limit=0)

        assert "more clusters" not in output(console)

    def test_empty_input_prints_nothing(self, console):
        """No clusters means no section."""
        ui.largest_clusters(console, [])
        assert output(console) == ""


# ---------------------------------------------------------------------------
# Plan preview
# ---------------------------------------------------------------------------
class TestPlanPreview:
    """The dry-run tree is bounded in both folders and files per folder."""

    def test_50k_files_render_within_a_fixed_line_budget(self, console):
        """A 50,000-file plan previews in a screenful, not 50,000 lines."""
        destinations = [
            (f"/out/[2024_01_{i % 500:03d}]_cluster", f"IMG_{i}.jpg", f"IMG_{i}.jpg")
            for i in range(50_000)
        ]

        ui.plan_preview(console, destinations, "/out")

        rendered = lines(console)
        # heading + tree root + tail line, and per folder: its name, up to
        # MAX_TREE_SAMPLES examples and one "… N more" line
        max_lines = 3 + ui.MAX_TREE_FOLDERS * (2 + ui.MAX_TREE_SAMPLES)
        assert len(rendered) <= max_lines
        assert "more folders" in rendered[-1]

    def test_renames_are_shown_with_an_arrow(self, console):
        """A collision rename is visible before anything is written."""
        destinations = [("/out/[2024]_x", "IMG_1.jpg", "IMG_1 (1).jpg")]

        ui.plan_preview(console, destinations, "/out")

        assert "IMG_1.jpg" in output(console)
        assert "IMG_1 (1).jpg" in output(console)

    def test_folder_labels_are_relative_to_the_output_dir(self, console):
        """The tree root carries the prefix, so branches stay short."""
        ui.plan_preview(console, [("/out/new/[2024]_x", "a.jpg", "a.jpg")], "/out")

        assert "new/[2024]_x" in output(console)

    def test_empty_plan_prints_nothing(self, console):
        """Nothing to do means no preview."""
        ui.plan_preview(console, [], "/out")
        assert output(console) == ""


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------
class TestProgress:
    """Progress plumbing must be safe in every environment."""

    def test_null_progress_accepts_everything(self):
        """The default sink is a no-op, so library callers stay silent."""
        progress = ui.NullProgress()
        progress.start(0, "nothing")
        progress.advance()
        progress.advance(100)

    def test_zero_items_does_not_fail(self, console):
        """An empty inbox must not crash the progress bar."""
        reporter = ui.RichReporter(console)
        with reporter.phase("Read inbox") as phase:
            phase.start(0, "Reading media")
        assert "Read inbox" in output(console)

    def test_progress_is_suppressed_when_not_a_terminal(self, console):
        """Redirected output must not receive one bar redraw per refresh."""
        reporter = ui.RichReporter(console)
        with reporter.phase("Read inbox") as phase:
            phase.start(50_000, "Reading media")
            for _ in range(1000):
                phase.advance()

        # Only the phase summary line survives.
        assert len(lines(console)) == 1

    def test_forced_colour_alone_does_not_enable_animation(self):
        """`FORCE_COLOR` asks for colour in a pipe, not for a redrawing bar.

        Rich reports such a console as a terminal, which would otherwise leave
        one line of debris per refresh in the redirected output.
        """
        piped = Console(file=io.StringIO(), force_terminal=True, width=100)

        assert piped.is_terminal is True
        assert ui.supports_animation(piped) is False

    def test_animation_is_allowed_on_a_real_tty(self):
        """A genuine terminal still gets its progress bar."""

        class FakeTty(io.StringIO):
            def isatty(self) -> bool:
                return True

        assert ui.supports_animation(Console(file=FakeTty(), force_terminal=True))

    def test_advances_are_batched_for_large_totals(self, console):
        """A 50k loop is not allowed to trigger a redraw per file."""
        reporter = ui.RichReporter(console)
        with reporter.phase("Read inbox") as phase:
            phase._batch = 0  # recomputed by start()
            phase.start(50_000, "Reading media")
            assert phase._batch == ui.MAX_PROGRESS_BATCH

    def test_phase_reports_its_detail_and_elapsed_time(self, console):
        """Each phase collapses to a single labelled line."""
        reporter = ui.RichReporter(console)
        with reporter.phase("Read inbox") as phase:
            phase.detail = "48,213 files"

        rendered = lines(console)
        assert len(rendered) == 1
        assert "Read inbox" in rendered[0]
        assert "48,213 files" in rendered[0]
        assert "0:00:00" in rendered[0]

    def test_failing_phase_is_marked_and_the_error_propagates(self, console):
        """A crash inside a phase is visible, and not swallowed."""
        reporter = ui.RichReporter(console)
        with pytest.raises(ValueError), reporter.phase("Read inbox"):
            raise ValueError("boom")

        assert "Read inbox" in output(console)

    def test_null_reporter_renders_nothing(self, console):
        """Library use produces no terminal output at all."""
        reporter = ui.NullReporter()
        with reporter.phase("Read inbox") as phase:
            phase.start(10)
            phase.advance()
        reporter.note("ignored")
        assert output(console) == ""


# ---------------------------------------------------------------------------
# Summary rendering
# ---------------------------------------------------------------------------
def _config(mode: CopyMode, tmp_path: Path):
    config = get_default_config()
    config.mode = mode
    config.in_dir_name = str(tmp_path / "inbox")
    config.out_dir_name = str(tmp_path / "out")
    config.watch_folders = []
    config.skip_duplicated_existing_in_libs = False
    config.assign_to_clusters_existing_in_libs = False
    config.restore_original_names = False
    return config


def _plan(n_files: int = 3, renamed: bool = False) -> FileOperationPlan:
    ops = [MkdirOp(path=Path("/out/[2024]_x"))]
    for i in range(n_files):
        dst_name = f"IMG_{i} (1).jpg" if renamed else f"IMG_{i}.jpg"
        ops.append(
            CopyOp(src=Path(f"/in/IMG_{i}.jpg"), dst=Path("/out/[2024]_x") / dst_name)
        )
    return FileOperationPlan(ops=ops)


class TestResultsPanel:
    """The summary is aggregate-only and adapts to the enabled features."""

    def test_summary_size_is_independent_of_file_count(self, console, tmp_path):
        """50,000 files produce the same number of summary lines as 3 do."""
        config = _config(CopyMode.COPY, tmp_path)
        results = {
            "new_folder_names": [f"c{i}" for i in range(487)],
            "file_operation_plan": _plan(n_files=2),
            "cluster_sizes": [(f"c{i}", 100) for i in range(487)],
        }

        ui.results_panel(console, results, config, elapsed=428.0)

        rendered = lines(console)
        assert len(rendered) <= 6
        assert "487" in output(console)
        assert "0:07:08" in output(console)

    def test_disabled_features_are_not_listed(self, console, tmp_path):
        """A run without duplicate detection does not report "0 duplicates"."""
        config = _config(CopyMode.COPY, tmp_path)

        ui.results_panel(console, {"file_operation_plan": _plan()}, config, 1.0)

        assert "Duplicates" not in output(console)

    def test_enabled_features_are_listed(self, console, tmp_path):
        """When duplicate detection runs, its result is always shown."""
        config = _config(CopyMode.COPY, tmp_path)
        config.skip_duplicated_existing_in_libs = True

        ui.results_panel(
            console,
            {"dup_files": ["a.jpg"], "file_operation_plan": _plan()},
            config,
            1.0,
        )

        assert "Duplicates" in output(console)

    def test_renames_are_reported(self, console, tmp_path):
        """Collision renames are surfaced, not silent."""
        config = _config(CopyMode.COPY, tmp_path)

        ui.results_panel(
            console, {"file_operation_plan": _plan(renamed=True)}, config, 1.0
        )

        assert "Renamed on collision" in output(console)

    def test_files_left_out_by_the_limit_are_reported(self, console, tmp_path):
        """The user is told the run covered only part of the inbox."""
        config = _config(CopyMode.NOP, tmp_path)
        config.inbox_limit = 100
        results = {
            "n_files_read": 100,
            "n_files_available": 48_213,
            "file_operation_plan": _plan(),
        }

        ui.results_panel(console, results, config, 1.0)

        assert "48,113 files not ingested" in output(console)
        assert "--limit reached" in output(console)

    def test_no_limit_means_no_truncation_warning(self, console, tmp_path):
        """Without a limit, a count mismatch must not be blamed on --limit."""
        results = {
            "n_files_read": 6,
            "n_files_available": 8,
            "file_operation_plan": _plan(),
        }

        ui.results_panel(console, results, _config(CopyMode.COPY, tmp_path), 1.0)

        assert "not ingested" not in output(console)

    def test_nothing_is_reported_when_the_whole_inbox_was_read(self, console, tmp_path):
        """A complete run gets no truncation warning."""
        results = {
            "n_files_read": 8,
            "n_files_available": 8,
            "file_operation_plan": _plan(),
        }

        ui.results_panel(console, results, _config(CopyMode.COPY, tmp_path), 1.0)

        assert "not ingested" not in output(console)

    def test_dry_run_wording_does_not_claim_files_were_touched(self, console, tmp_path):
        """In NOP mode the count is what *would* be processed."""
        config = _config(CopyMode.NOP, tmp_path)
        plan = FileOperationPlan(
            ops=[SkipOp(src=Path("/in/a.jpg"), reason="NOP mode") for _ in range(8)]
        )

        ui.results_panel(console, {"file_operation_plan": plan}, config, 1.0)

        assert "Files to process" in output(console)
        assert "Files moved" not in output(console)


class TestBanner:
    """The banner confirms the destination before a long run starts."""

    def test_shows_resolved_paths_and_mode(self, console, tmp_path):
        """Inbox, output and mode are visible up front."""
        config = _config(CopyMode.MOVE, tmp_path)

        ui.banner(console, config, "1.2.3")

        text = output(console)
        assert "1.2.3" in text
        assert "MOVE" in text
        assert "gap 60 min" in text

    def test_an_ingestion_limit_is_stated_up_front(self, console, tmp_path):
        """A truncated run says so before the results are read."""
        config = _config(CopyMode.NOP, tmp_path)
        config.inbox_limit = 500

        ui.banner(console, config, "1.2.3")

        text = output(console)
        assert "Limit" in text
        assert "first 500 files" in text

    def test_no_limit_adds_no_row(self, console, tmp_path):
        """A full run says nothing about limits."""
        ui.banner(console, _config(CopyMode.NOP, tmp_path), "1.2.3")

        assert "Limit" not in output(console)

    def test_many_watch_folders_are_summarized(self, console, tmp_path):
        """A long watch list collapses instead of wrapping over many lines."""
        config = _config(CopyMode.MOVE, tmp_path)
        config.watch_folders = [f"/lib/{i}" for i in range(50)]

        ui.banner(console, config, "1.2.3")

        assert "+49 more" in output(console)
        assert len(lines(console)) <= 7


# ---------------------------------------------------------------------------
# Machine-readable output
# ---------------------------------------------------------------------------
class TestJsonSummary:
    """`--json` carries aggregates only, so it stays small at any scale."""

    def test_summary_is_serializable_and_aggregate(self, tmp_path):
        """Counts, not file lists, and it survives a JSON round trip."""
        config = _config(CopyMode.COPY, tmp_path)
        results = {
            "new_folder_names": ["a", "b"],
            "dup_files": ["d.jpg"],
            "n_files_read": 50_000,
            "cluster_sizes": [("a", 30_000), ("b", 20_000)],
            "file_operation_plan": _plan(n_files=2),
        }

        summary = ui.json_summary(results, config, elapsed=12.3456)
        restored = json.loads(json.dumps(summary))

        assert restored["files_read"] == 50_000
        assert restored["new_clusters"] == 2
        assert restored["duplicates"] == 1
        assert restored["copied"] == 2
        assert restored["elapsed_seconds"] == 12.346
        assert restored["mode"] == "COPY"

    def test_missing_keys_default_to_zero(self, tmp_path):
        """An aborted or empty run still produces a valid summary."""
        summary = ui.json_summary({}, _config(CopyMode.NOP, tmp_path), 0.0)

        assert summary["files_read"] == 0
        assert summary["new_clusters"] == 0
        assert summary["aborted"] is False


class TestWriteReport:
    """The report file is where per-file detail belongs."""

    def test_every_file_is_written_with_a_header(self, tmp_path):
        """The CSV holds one row per file, plus a header."""
        target = tmp_path / "report.csv"

        n_rows = ui.write_report(
            target, _plan(n_files=1000), _config(CopyMode.COPY, tmp_path)
        )

        written = target.read_text().splitlines()
        assert n_rows == 1000
        assert len(written) == 1001
        assert written[0] == "operation,source,destination_folder,destination"
        assert written[1].startswith("copy,IMG_0.jpg,")

    def test_dry_run_plan_still_reports_destinations(self, tmp_path):
        """A preview can be exported without writing any media."""
        target = tmp_path / "report.csv"
        plan = FileOperationPlan(
            ops=[
                SkipOp(
                    src=Path("/in/a.jpg"),
                    reason="NOP mode",
                    dst=Path("/out/[2024]_x/a.jpg"),
                )
            ]
        )

        n_rows = ui.write_report(target, plan, _config(CopyMode.NOP, tmp_path))

        assert n_rows == 1
        assert "a.jpg" in target.read_text()


class TestErrorPanel:
    """Fatal errors are short and free of tracebacks."""

    def test_message_and_hint_are_shown(self, console):
        """The user gets the problem and what to do about it."""
        ui.error_panel(console, "Inbox does not exist", "Check the -i path.")

        text = output(console)
        assert "Inbox does not exist" in text
        assert "Check the -i path." in text
        assert "Traceback" not in text


class TestConfigureLogging:
    """Verbosity maps to log levels, and logs never reach stdout."""

    @pytest.mark.parametrize(
        ("verbosity", "quiet", "expected"),
        [
            (0, False, "WARNING"),
            (1, False, "INFO"),
            (2, False, "DEBUG"),
            (0, True, "ERROR"),
        ],
    )
    def test_level_selection(self, verbosity, quiet, expected, capsys):
        """Each flag combination selects the documented level."""
        from filecluster import logger

        ui.configure_logging(verbosity=verbosity, quiet=quiet)
        logger.log(expected, "probe message")

        captured = capsys.readouterr()
        assert "probe message" in captured.err
        assert captured.out == ""


# ---------------------------------------------------------------------------
# Byte formatting
# ---------------------------------------------------------------------------
class TestFmtBytes:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (0, "0 B"),
            (512, "512 B"),
            (1024, "1.0 KiB"),
            (1536, "1.5 KiB"),
            (1024**2, "1.0 MiB"),
            (1024**3, "1.0 GiB"),
            (1024**4, "1.0 TiB"),
        ],
    )
    def test_binary_units(self, value, expected):
        assert ui.fmt_bytes(value) == expected


# ---------------------------------------------------------------------------
# Dedup renderers
# ---------------------------------------------------------------------------
class TestDedupRenderers:
    def _plan(self, tmp_path, with_duplicates=True):
        from filecluster.dedup import DedupAction, DedupPlan, DuplicateGroup

        groups = []
        if with_duplicates:
            groups = [
                DuplicateGroup(
                    full_hash="h",
                    size=2048,
                    files=[tmp_path / "a.jpg", tmp_path / "sub" / "b.jpg"],
                )
            ]
        return DedupPlan(
            root=tmp_path, groups=groups, action=DedupAction.REPORT, n_scanned=2
        )

    def test_results_show_counts_and_reclaimable_space(self, console, tmp_path):
        ui.dedup_results(console, self._plan(tmp_path))
        out = output(console)
        assert "Duplicate groups" in out
        assert "Reclaimable" in out
        assert "2.0 KiB" in out

    def test_groups_tree_marks_the_kept_copy(self, console, tmp_path):
        ui.dedup_groups(console, self._plan(tmp_path))
        out = output(console)
        assert "keep" in out
        assert "dup" in out

    def test_clean_tree_message(self, console, tmp_path):
        ui.dedup_groups(console, self._plan(tmp_path, with_duplicates=False))
        assert "No duplicates found" in output(console)

    def test_banner_shows_dry_run(self, console, tmp_path):
        ui.dedup_banner(console, tmp_path, None, "report", execute=False)
        assert "DRY RUN" in output(console)


# ---------------------------------------------------------------------------
# Catalog renderers
# ---------------------------------------------------------------------------
class TestCatalogRenderers:
    def test_stats_table(self, console, tmp_path):
        ui.catalog_stats(
            console,
            tmp_path,
            {
                "db_path": str(tmp_path / ".filecluster.db"),
                "db_bytes": 4096,
                "schema_version": 1,
                "clusters": 3,
                "files": 10,
                "partial_hashes": 10,
                "full_hashes": 4,
                "total_bytes": 1024,
            },
        )
        out = output(console)
        assert "File rows" in out
        assert "4.0 KiB" in out
        assert "none" in out

    def test_stats_names_the_newest_backup(self, console, tmp_path):
        ui.catalog_stats(
            console,
            tmp_path,
            {
                "db_path": str(tmp_path / ".filecluster.db"),
                "backups": [
                    str(tmp_path / ".filecluster.20240101T000000Z.bak"),
                    str(tmp_path / ".filecluster.20240202T000000Z.bak"),
                ],
            },
        )
        out = output(console)
        assert "Backups" in out
        assert ".filecluster.20240202T000000Z.bak" in out

    def test_message_rows(self, console):
        ui.catalog_message(console, "Catalog restored", [("Library", "/photos")])
        out = output(console)
        assert "Catalog restored" in out
        assert "/photos" in out
