"""Tests for the command line interface.

Covers option parsing, backwards compatibility with the previous flag set,
error handling, the confirmation gate and the machine-readable outputs.

Mocking Strategy: none for the pipeline — the tests run real clustering over
the image assets into temp directories. ``CliRunner`` captures the output the
same way a shell would, which also means the terminal-only progress bars are
correctly absent.
"""

import json
import shutil
from importlib.metadata import version as pkg_version

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from filecluster.cli import EXIT_OK, EXIT_USAGE, app

runner = CliRunner()


@pytest.fixture
def inbox(tmp_path, assets_dir):
    """A writable copy of the test inbox, so MOVE mode is safe."""
    target = tmp_path / "inbox"
    shutil.copytree(assets_dir / "set_1", target)
    return target


@pytest.fixture
def out_dir(tmp_path):
    """An empty output directory."""
    target = tmp_path / "out"
    target.mkdir()
    return target


def files_under(path) -> list[str]:
    """Names of all files below *path*."""
    return [p.name for p in path.rglob("*") if p.is_file()]


# ---------------------------------------------------------------------------
# Basics
# ---------------------------------------------------------------------------
class TestVersionAndHelp:
    """Version and help must work without any configuration."""

    def test_version_matches_the_installed_package(self):
        """`--version` cannot drift from pyproject: both read the metadata."""
        result = runner.invoke(app, ["--version"])

        assert result.exit_code == EXIT_OK
        assert pkg_version("filecluster") in result.stdout

    def test_short_version_flag(self):
        """`-V` is accepted too."""
        assert runner.invoke(app, ["-V"]).exit_code == EXIT_OK

    def test_help_is_available(self):
        """Both help forms work and describe the tool."""
        for flag in ("-h", "--help"):
            result = runner.invoke(app, [flag])
            assert result.exit_code == EXIT_OK
            assert "event folders" in result.stdout

    def test_every_documented_option_exists(self):
        """The documented flag set is asserted on the parser, not on rendered
        help, which the terminal width would otherwise truncate."""
        group = get_command(app)
        # ``run`` is a subcommand of the default-group; inspect it directly.
        run_cmd = group.commands["run"]
        declared = {opt for param in run_cmd.params for opt in param.opts}

        assert declared >= {
            # inherited from the previous argparse CLI
            "-i", "--inbox-dir",
            "-o", "--output-dir",
            "-w", "--watch-dir",
            "-t", "--development-mode",
            "-n", "--no-operation",
            "-y", "--copy-mode",
            "-f", "--force-deep-scan",
            "-d", "--drop-duplicates",
            "-c", "--use-existing-clusters",
            "-r", "--restore-original-names",
            # added by the new CLI
            "-l", "--limit",
            "--flat", "--no-recursive",
            "-Y", "--yes",
            "-V", "--version",
            "-v", "--verbose",
            "-q", "--quiet",
            "--show", "--report", "--json", "--color",
        }  # fmt: skip


class TestBackwardsCompatibility:
    """The previous single-letter flags must keep working unchanged."""

    def test_short_flags_from_the_old_argparse_cli_are_accepted(
        self, inbox, out_dir, assets_dir
    ):
        """A command written for the old CLI still runs."""
        paths = [
            "-i",
            str(inbox),
            "-o",
            str(out_dir),
            "-w",
            str(assets_dir / "zdjecia"),
        ]
        result = runner.invoke(app, [*paths, "-n", "-f", "-d", "-c", "-r"])

        assert result.exit_code == EXIT_OK

    def test_duplicate_detection_without_a_watch_dir_is_a_usage_error(
        self, inbox, out_dir
    ):
        """`-d` and `-c` need something to compare against."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n", "-d"])

        assert result.exit_code == EXIT_USAGE
        assert "Watch folders are required" in result.output
        assert "Traceback" not in result.output

    def test_no_subcommand_name_is_required(self, inbox, out_dir):
        """`filecluster -i ... -o ...` works without naming a command."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])

        assert result.exit_code == EXIT_OK
        assert "filecluster" in result.stdout


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------
class TestErrorHandling:
    """Bad input produces a short message, never a traceback."""

    def test_missing_inbox_is_rejected_before_any_work(self, tmp_path, out_dir):
        """A non-existent inbox exits 2 with a readable message."""
        result = runner.invoke(
            app, ["-i", str(tmp_path / "nope"), "-o", str(out_dir), "-n"]
        )

        assert result.exit_code == EXIT_USAGE
        assert "does not exist" in result.output
        assert "Traceback" not in result.output

    def test_inbox_pointing_at_a_file_is_rejected(self, tmp_path, out_dir):
        """A file where a directory is expected is a usage error."""
        a_file = tmp_path / "photo.jpg"
        a_file.write_bytes(b"x")

        result = runner.invoke(app, ["-i", str(a_file), "-o", str(out_dir), "-n"])

        assert result.exit_code == EXIT_USAGE
        assert "Traceback" not in result.output

    def test_missing_watch_dir_is_rejected(self, inbox, out_dir, tmp_path):
        """Watch folders are validated the same way as the inbox."""
        result = runner.invoke(
            app,
            ["-i", str(inbox), "-o", str(out_dir), "-w", str(tmp_path / "gone"), "-n"],
        )

        assert result.exit_code == EXIT_USAGE

    def test_negative_show_is_rejected(self, inbox, out_dir):
        """`--show` must be a non-negative count."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--show", "-1"]
        )

        assert result.exit_code != EXIT_OK


# ---------------------------------------------------------------------------
# Dry run and confirmation
# ---------------------------------------------------------------------------
class TestDryRun:
    """`--no-operation` must be provably inert."""

    def test_nothing_is_written_and_the_inbox_is_untouched(self, inbox, out_dir):
        """A preview leaves both directories exactly as they were."""
        before = sorted(files_under(inbox))

        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])

        assert result.exit_code == EXIT_OK
        assert files_under(out_dir) == []
        assert sorted(files_under(inbox)) == before

    def test_preview_shows_the_planned_layout(self, inbox, out_dir):
        """The user sees where files would land before committing."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])

        assert "Planned layout" in result.stdout
        assert "nothing written" in result.stdout

    def test_preview_output_is_bounded(self, inbox, out_dir):
        """Even a preview never prints one line per file."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])

        assert len(result.stdout.splitlines()) < 60


class TestConfirmation:
    """Writing files is gated, but automation must not be blocked by it."""

    def test_yes_skips_the_prompt_and_moves_files(self, inbox, out_dir):
        """`--yes` proceeds without asking."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-Y"])

        assert result.exit_code == EXIT_OK
        assert len(files_under(out_dir)) == 8
        assert files_under(inbox) == []

    def test_non_interactive_use_proceeds_without_a_prompt(self, inbox, out_dir):
        """A pipeline with no tty is not left hanging on a question."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir)], input="")

        assert result.exit_code == EXIT_OK
        assert len(files_under(out_dir)) == 8

    def test_copy_mode_keeps_the_inbox(self, inbox, out_dir):
        """`-y` copies rather than moves."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-y", "-Y"])

        assert result.exit_code == EXIT_OK
        assert len(files_under(inbox)) == 8
        assert len(files_under(out_dir)) == 8


# ---------------------------------------------------------------------------
# Ingestion limit
# ---------------------------------------------------------------------------
class TestIngestionLimit:
    """`--limit` trades completeness for speed on a large inbox."""

    def test_only_the_limited_files_are_processed(self, inbox, out_dir):
        """Three files in, three files planned."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--limit", "3", "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["limit"] == 3
        assert payload["files_read"] == 3
        assert payload["files_available"] == 8

    def test_short_flag_works(self, inbox, out_dir):
        """`-l` is the shorthand."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "-l", "2", "--json"]
        )

        assert json.loads(result.stdout)["files_read"] == 2

    def test_truncation_is_visible_in_the_summary(self, inbox, out_dir):
        """A partial run must not look like a complete one."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--limit", "3"]
        )

        # Long temp paths make the banner wrap, so compare on collapsed
        # whitespace rather than on exact line breaks.
        rendered = " ".join(result.stdout.split())
        assert "Limit first 3 files" in rendered
        assert "5 files not ingested" in rendered

    def test_repeated_runs_pick_the_same_files(self, inbox, out_dir, tmp_path):
        """A limited preview is reproducible."""
        reports = []
        paths = ["-i", str(inbox), "-o", str(out_dir)]
        for name in ("first.csv", "second.csv"):
            report = tmp_path / name
            runner.invoke(app, [*paths, "-n", "--limit", "4", "--report", str(report)])
            reports.append(sorted(report.read_text().splitlines()))

        assert reports[0] == reports[1]

    def test_limit_applies_to_writes_too(self, inbox, out_dir):
        """Only the ingested files are moved; the rest stay in the inbox."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-Y", "--limit", "3"]
        )

        assert result.exit_code == EXIT_OK
        assert len(files_under(out_dir)) == 3
        assert len(files_under(inbox)) == 5

    def test_zero_is_rejected(self, inbox, out_dir):
        """A limit of zero would ingest nothing, so it is a usage error."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--limit", "0"]
        )

        assert result.exit_code != EXIT_OK
        assert files_under(out_dir) == []

    def test_without_the_flag_everything_is_ingested(self, inbox, out_dir):
        """The limit is opt-in."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["limit"] is None
        assert payload["files_read"] == payload["files_available"] == 8


# ---------------------------------------------------------------------------
# Machine-readable output
# ---------------------------------------------------------------------------
class TestJsonOutput:
    """`--json` output must be parseable and free of decoration."""

    def test_stdout_is_valid_json_only(self, inbox, out_dir):
        """No banner, table or ANSI codes get mixed into the payload."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["files_read"] == 8
        assert payload["new_clusters"] == 4
        assert payload["mode"] == "DRY RUN"
        assert "\x1b[" not in result.stdout

    def test_summary_stays_aggregate_at_scale(self, inbox, out_dir):
        """The payload lists clusters, never individual files."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--json"]
        )

        payload = json.loads(result.stdout)
        assert sum(c["files"] for c in payload["clusters"]) == 8
        assert "files" not in payload  # no per-file list


class TestReportFile:
    """`--report` is where per-file detail goes instead of the terminal."""

    def test_report_holds_every_file_while_the_terminal_stays_short(
        self, inbox, out_dir, tmp_path
    ):
        """Full detail on disk, bounded summary on screen."""
        report = tmp_path / "report.csv"

        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--report", str(report)]
        )

        rows = report.read_text().splitlines()
        assert result.exit_code == EXIT_OK
        assert len(rows) == 9  # header plus 8 files
        assert len(result.stdout.splitlines()) < 60

    def test_report_records_the_actual_operation(self, inbox, out_dir, tmp_path):
        """The report reflects the mode that ran."""
        report = tmp_path / "report.csv"

        runner.invoke(
            app,
            ["-i", str(inbox), "-o", str(out_dir), "-Y", "--report", str(report)],
        )

        assert report.read_text().splitlines()[1].startswith("move,")


# ---------------------------------------------------------------------------
# Verbosity
# ---------------------------------------------------------------------------
class TestVerbosity:
    """Logs are diagnostics: stderr only, and off by default."""

    def test_quiet_prints_nothing_on_success(self, inbox, out_dir):
        """`-q` is silent when everything worked."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n", "-q"])

        assert result.exit_code == EXIT_OK
        assert result.stdout.strip() == ""

    def test_default_run_reports_diagnostics_as_a_summary(self, inbox, out_dir):
        """Files without EXIF are counted, not listed one by one."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])

        assert "no EXIF date" in result.stdout
        assert "Re-run with -v" in result.stdout

    def test_verbose_lists_the_affected_files(self, inbox, out_dir):
        """At -v the examples behind a diagnostic become visible."""
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n", "-v"])

        assert ".jpg" in result.stdout or ".JPG" in result.stdout

    def test_show_limits_the_cluster_listing(self, inbox, out_dir):
        """`--show 2` names two clusters and summarizes the rest."""
        result = runner.invoke(
            app, ["-i", str(inbox), "-o", str(out_dir), "-n", "--show", "2"]
        )

        assert "2 more clusters" in result.stdout


# ---------------------------------------------------------------------------
# Scale
# ---------------------------------------------------------------------------
class TestScale:
    """Output length must not grow with the number of files."""

    def test_output_length_is_stable_as_the_inbox_grows(self, inbox, out_dir):
        """Eleven times the files must not mean eleven times the output."""
        small = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])
        small_lines = len(small.stdout.splitlines())

        # Multiply the inbox by copying every asset under new names.
        originals = [p for p in inbox.iterdir() if p.is_file()]
        for i in range(10):
            for src in originals:
                shutil.copy2(src, inbox / f"{src.stem}_copy{i}{src.suffix}")

        big = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])
        big_lines = len(big.stdout.splitlines())

        assert len(files_under(inbox)) == 88
        assert big.exit_code == EXIT_OK
        # Growth comes only from the capped cluster table and preview tree, so
        # 11x the input must stay well under 2x the output, and under the
        # absolute ceiling those caps impose.
        assert big_lines < small_lines * 2
        assert big_lines < 100


# ---------------------------------------------------------------------------
# Subcommand registration
# ---------------------------------------------------------------------------
def _media(path, content=b"a-photo"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


class TestSubcommands:
    """The documented command names must be the ones Typer registers."""

    def test_registered_command_names(self):
        group = get_command(app)
        assert sorted(group.commands) == ["catalog", "dedup", "reconcile", "run"]

    @pytest.mark.parametrize("name", ["run", "reconcile", "dedup", "catalog"])
    def test_help_is_available(self, name):
        result = runner.invoke(app, [name, "--help"])
        assert result.exit_code == EXIT_OK

    def test_bare_options_still_default_to_run(self, inbox, out_dir):
        result = runner.invoke(app, ["-i", str(inbox), "-o", str(out_dir), "-n"])
        assert result.exit_code == EXIT_OK


class TestReconcileCommand:
    def test_dry_run_reports_without_writing(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        source = tmp_path / "src"
        photo = _media(source / "[2024_01_01]_ev" / "a.jpg")

        result = runner.invoke(app, ["reconcile", "-s", str(source), "-l", str(lib)])

        assert result.exit_code == EXIT_OK
        assert "DRY RUN" in result.stdout
        assert photo.exists()
        assert not list(lib.rglob("a.jpg"))

    def test_execute_moves_new_files(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        source = tmp_path / "src"
        _media(source / "[2024_01_01]_ev" / "a.jpg")

        result = runner.invoke(
            app, ["reconcile", "-s", str(source), "-l", str(lib), "--execute"]
        )

        assert result.exit_code == EXIT_OK
        assert (lib / "2024" / "[2024_01_01]_ev" / "a.jpg").exists()

    def test_json_summary_is_the_only_stdout(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        source = tmp_path / "src"
        _media(source / "a.jpg")

        result = runner.invoke(
            app, ["reconcile", "-s", str(source), "-l", str(lib), "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["new"] == 1
        assert payload["action"] == "move"

    def test_report_writes_csv(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        source = tmp_path / "src"
        _media(source / "a.jpg")
        report = tmp_path / "report.csv"

        runner.invoke(
            app,
            [
                "reconcile",
                "-s",
                str(source),
                "-l",
                str(lib),
                "--report",
                str(report),
                "--json",
            ],
        )

        assert report.read_text().count("\n") == 2

    def test_scan_only_writes_nothing(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        source = tmp_path / "src"
        photo = _media(source / "a.jpg")

        result = runner.invoke(
            app,
            [
                "reconcile",
                "-s",
                str(source),
                "-l",
                str(lib),
                "--execute",
                "--scan-only",
                "--json",
            ],
        )

        assert json.loads(result.stdout)["moves"] == 0
        assert photo.exists()

    def test_repeated_library_option(self, tmp_path):
        lib_a = tmp_path / "a"
        lib_b = tmp_path / "b"
        lib_a.mkdir()
        _media(lib_b / "2024" / "[2024_01_01]_ev" / "x.jpg", b"already-stored")
        source = tmp_path / "src"
        _media(source / "x.jpg", b"already-stored")

        result = runner.invoke(
            app,
            [
                "reconcile",
                "-s",
                str(source),
                "-l",
                str(lib_a),
                "-l",
                str(lib_b),
                "--json",
            ],
        )

        assert json.loads(result.stdout)["duplicates"] == 1

    def test_missing_source_is_a_usage_error(self, tmp_path):
        lib = tmp_path / "lib"
        lib.mkdir()
        result = runner.invoke(
            app, ["reconcile", "-s", str(tmp_path / "nope"), "-l", str(lib)]
        )
        assert result.exit_code == EXIT_USAGE

    def test_source_equal_to_library_is_refused(self, tmp_path):
        lib = tmp_path / "lib"
        photo = _media(lib / "2024" / "[2024_01_01]_ev" / "a.jpg")

        result = runner.invoke(
            app, ["reconcile", "-s", str(lib), "-l", str(lib), "--execute"]
        )

        assert result.exit_code == EXIT_USAGE
        assert photo.exists()

    def test_dry_run_leaves_no_catalog_behind(self, tmp_path):
        lib = tmp_path / "lib"
        _media(lib / "2024" / "[2024_01_01]_ev" / "a.jpg", b"library-file")
        source = tmp_path / "src"
        _media(source / "b.jpg", b"incoming-file")

        result = runner.invoke(
            app, ["reconcile", "-s", str(source), "-l", str(lib), "--json"]
        )

        assert result.exit_code == EXIT_OK
        assert list(lib.glob(".filecluster*")) == []


class TestDedupCommand:
    def test_reports_duplicates(self, tmp_path):
        root = tmp_path / "lib"
        _media(root / "a.jpg", b"same-bytes")
        _media(root / "b.jpg", b"same-bytes")

        result = runner.invoke(app, ["dedup", "-d", str(root), "--json"])

        payload = json.loads(result.stdout)
        assert payload["duplicate_groups"] == 1
        assert payload["duplicate_files"] == 1
        assert payload["action"] == "report"
        assert list(root.glob(".filecluster*")) == []

    def test_clean_tree_says_so(self, tmp_path):
        root = tmp_path / "lib"
        _media(root / "a.jpg", b"one")
        _media(root / "b.jpg", b"two-longer")

        result = runner.invoke(app, ["dedup", "-d", str(root)])

        assert result.exit_code == EXIT_OK
        assert "No duplicates found" in result.stdout

    def test_quarantine_execute_moves_copies(self, tmp_path):
        root = tmp_path / "lib"
        _media(root / "IMG_1.jpg", b"same-bytes")
        _media(root / "IMG_1-Kopiuj.jpg", b"same-bytes")
        quarantine = tmp_path / "quarantine"

        result = runner.invoke(
            app,
            ["dedup", "-d", str(root), "-q", str(quarantine), "--execute", "--json"],
        )

        assert json.loads(result.stdout)["moves"] == 1
        assert (root / "IMG_1.jpg").exists()
        assert (quarantine / "IMG_1-Kopiuj.jpg").exists()

    def test_execute_without_quarantine_is_rejected(self, tmp_path):
        root = tmp_path / "lib"
        _media(root / "a.jpg", b"same-bytes")

        result = runner.invoke(app, ["dedup", "-d", str(root), "--execute"])

        assert result.exit_code == EXIT_USAGE

    def test_report_writes_one_row_per_copy(self, tmp_path):
        root = tmp_path / "lib"
        _media(root / "a.jpg", b"same-bytes")
        _media(root / "b.jpg", b"same-bytes")
        report = tmp_path / "dupes.csv"

        runner.invoke(
            app, ["dedup", "-d", str(root), "--report", str(report), "--json"]
        )

        assert report.read_text().count("\n") == 3


class TestCatalogCommand:
    def test_stats_on_a_fresh_library(self, tmp_path):
        result = runner.invoke(app, ["catalog", "stats", "-l", str(tmp_path), "--json"])

        payload = json.loads(result.stdout)
        assert payload["files"] == 0
        assert payload["clusters"] == 0
        assert payload["backups"] == []

    def test_stats_renders_a_table(self, tmp_path):
        result = runner.invoke(app, ["catalog", "stats", "-l", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        assert "Catalog" in result.stdout

    def test_backup_then_restore_round_trip(self, tmp_path):
        from filecluster.catalog import LibraryCatalog

        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("a.jpg", 1, 1.0, "p", "f")])

        backup = runner.invoke(
            app, ["catalog", "backup", "-l", str(tmp_path), "--json"]
        )
        assert json.loads(backup.stdout)["backup"] is not None

        with LibraryCatalog.open(tmp_path) as cat:
            cat.clear_file_hashes()

        restore = runner.invoke(
            app, ["catalog", "restore", "-l", str(tmp_path), "--json"]
        )
        assert restore.exit_code == EXIT_OK

        with LibraryCatalog.open(tmp_path) as cat:
            assert "a.jpg" in cat.get_file_hashes()

    def test_restore_without_backup_fails_cleanly(self, tmp_path):
        result = runner.invoke(app, ["catalog", "restore", "-l", str(tmp_path)])
        assert result.exit_code == EXIT_USAGE

    def test_build_scans_library(self, tmp_path):
        event = tmp_path / "2020" / "[2020_06_15]_trip"
        event.mkdir(parents=True)
        (event / "a.jpg").write_bytes(b"aaaa")
        (event / "b.jpg").write_bytes(b"bbbb")

        result = runner.invoke(app, ["catalog", "build", "-l", str(tmp_path), "--json"])
        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["mode"] == "update"
        assert payload["scanned"] == 2
        assert payload["added"] == 2
        assert payload["clusters_scanned"] == 1
        assert payload["clusters_added"] == 1
        assert payload["image_hash"] == "full"
        assert payload["video_hash"] == "short"

    def test_build_accepts_independent_hash_modes(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"image")
        (tmp_path / "clip.mp4").write_bytes(b"video")

        result = runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--image-hash",
                "short",
                "--video-hash",
                "full",
                "--json",
            ],
        )

        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["image_hash"] == "short"
        assert payload["video_hash"] == "full"

        from filecluster.catalog import LibraryCatalog

        with LibraryCatalog.open(tmp_path) as catalog:
            hashes = catalog.get_file_hashes()
        assert hashes["a.jpg"][2] is None
        assert hashes["clip.mp4"][2] is not None

    def test_build_rejects_invalid_hash_mode(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--image-hash",
                "invalid",
            ],
        )
        assert result.exit_code == EXIT_USAGE
        assert "Hash mode must be 'full' or 'short'." in result.stderr

    def test_build_full_hash_shortcut_hashes_both_media_types(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"image")
        (tmp_path / "clip.mp4").write_bytes(b"video")

        result = runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--full-hash",
                "--json",
            ],
        )

        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["image_hash"] == "full"
        assert payload["video_hash"] == "full"

    def test_build_rebuild_backs_up(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        runner.invoke(app, ["catalog", "build", "-l", str(tmp_path), "--json"])

        result = runner.invoke(
            app, ["catalog", "build", "-l", str(tmp_path), "-f", "--json"]
        )
        payload = json.loads(result.stdout)
        assert payload["mode"] == "rebuild"
        assert payload["backup"] is not None

    def test_build_renders_a_table(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        result = runner.invoke(app, ["catalog", "build", "-l", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        assert "Catalog built" in result.stdout

    def test_build_conflicting_hash_algo_is_refused(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--hash-algo",
                "blake3",
                "--json",
            ],
        )

        # A plain (non-rebuild) build that explicitly asks for a different
        # algorithm must refuse in JSON mode and point at --rebuild.
        result = runner.invoke(
            app,
            ["catalog", "build", "-l", str(tmp_path), "--hash-algo", "sha1", "--json"],
        )
        assert result.exit_code == EXIT_USAGE
        assert "--rebuild" in result.stderr

    def test_build_conflicting_crc32_is_refused(self, tmp_path):
        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        runner.invoke(
            app,
            ["catalog", "build", "-l", str(tmp_path), "--crc32", "--json"],
        )

        result = runner.invoke(
            app,
            ["catalog", "build", "-l", str(tmp_path), "--json"],
        )
        # No explicit --crc32 flag here, so the stored crc32=True policy is
        # reused silently rather than treated as a conflict.
        assert result.exit_code == EXIT_OK

    def test_build_rebuild_overrides_conflicting_policy(self, tmp_path):
        from filecluster.catalog import LibraryCatalog

        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--hash-algo",
                "blake3",
                "--json",
            ],
        )

        result = runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--hash-algo",
                "sha1",
                "-f",
                "--json",
            ],
        )
        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["mode"] == "rebuild"

        # The CLI maps --hash-algo sha1 onto the legacy (NULL) policy so
        # reconcile/dedup keep reusing those hashes.
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": None, "crc32": False}

    def test_build_default_rerun_reuses_pinned_policy(self, tmp_path):
        from filecluster.catalog import LibraryCatalog

        (tmp_path / "a.jpg").write_bytes(b"aaaa")
        runner.invoke(
            app,
            [
                "catalog",
                "build",
                "-l",
                str(tmp_path),
                "--hash-algo",
                "blake3",
                "--crc32",
                "--json",
            ],
        )

        # Plain re-run with no hashing flags keeps the pinned blake3+crc32.
        (tmp_path / "b.jpg").write_bytes(b"bbbb")
        result = runner.invoke(app, ["catalog", "build", "-l", str(tmp_path), "--json"])
        assert result.exit_code == EXIT_OK

        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_hash_policy() == {"hash_algo": "blake3", "crc32": True}
            assert cat.get_file_records()["b.jpg"].hash_algo == "blake3"

    def test_verify_reports_missing_rows(self, tmp_path):
        from filecluster.catalog import LibraryCatalog

        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("gone.jpg", 1, 1.0, "p", "f")])

        result = runner.invoke(
            app, ["catalog", "verify", "-l", str(tmp_path), "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["missing"] == 1
        assert payload["pruned"] == 0

    def test_verify_prune_removes_stale_rows(self, tmp_path):
        from filecluster.catalog import LibraryCatalog

        with LibraryCatalog.open(tmp_path) as cat:
            cat.put_file_hashes([("gone.jpg", 1, 1.0, "p", "f")])

        result = runner.invoke(
            app, ["catalog", "verify", "-l", str(tmp_path), "--prune", "--json"]
        )

        assert json.loads(result.stdout)["pruned"] == 1
        with LibraryCatalog.open(tmp_path) as cat:
            assert cat.get_file_hashes() == {}

    def test_verify_deep_reports_corruption(self, tmp_path):
        from PIL import Image

        from filecluster.catalog import LibraryCatalog

        good = tmp_path / "good.jpg"
        Image.new("RGB", (16, 16), (10, 20, 30)).save(good, "JPEG")
        broken = tmp_path / "broken.jpg"
        data = good.read_bytes()
        broken.write_bytes(data[: len(data) // 2])

        LibraryCatalog.build(tmp_path)

        result = runner.invoke(
            app, ["catalog", "verify", "-l", str(tmp_path), "--deep", "--json"]
        )

        payload = json.loads(result.stdout)
        assert payload["decoded_ok"] == 1
        assert payload["corrupt"] == 1


class TestRecursiveInbox:
    """Tests for recursive vs flat inbox reading in CLI."""

    def test_recursive_by_default(self, tmp_path, assets_dir):
        inbox_dir = tmp_path / "inbox"
        inbox_dir.mkdir()
        sub_dir = inbox_dir / "sub"
        sub_dir.mkdir()
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        shutil.copy(assets_dir / "set_1" / "IMG_3784.jpg", inbox_dir / "top.jpg")
        shutil.copy(assets_dir / "set_1" / "IMG_4026.JPG", sub_dir / "nested.jpg")

        result = runner.invoke(
            app, ["-i", str(inbox_dir), "-o", str(out_dir), "-y", "-Y", "--json"]
        )
        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["files_read"] == 2

    def test_flat_option_ignores_subdirectories(self, tmp_path, assets_dir):
        inbox_dir = tmp_path / "inbox"
        inbox_dir.mkdir()
        sub_dir = inbox_dir / "sub"
        sub_dir.mkdir()
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        shutil.copy(assets_dir / "set_1" / "IMG_3784.jpg", inbox_dir / "top.jpg")
        shutil.copy(assets_dir / "set_1" / "IMG_4026.JPG", sub_dir / "nested.jpg")

        result = runner.invoke(
            app,
            [
                "-i",
                str(inbox_dir),
                "-o",
                str(out_dir),
                "-y",
                "-Y",
                "--flat",
                "--json",
            ],
        )
        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["files_read"] == 1

    def test_no_recursive_alias_ignores_subdirectories(self, tmp_path, assets_dir):
        inbox_dir = tmp_path / "inbox"
        inbox_dir.mkdir()
        sub_dir = inbox_dir / "sub"
        sub_dir.mkdir()
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        shutil.copy(assets_dir / "set_1" / "IMG_3784.jpg", inbox_dir / "top.jpg")
        shutil.copy(assets_dir / "set_1" / "IMG_4026.JPG", sub_dir / "nested.jpg")

        result = runner.invoke(
            app,
            [
                "-i",
                str(inbox_dir),
                "-o",
                str(out_dir),
                "-y",
                "-Y",
                "--no-recursive",
                "--json",
            ],
        )
        assert result.exit_code == EXIT_OK
        payload = json.loads(result.stdout)
        assert payload["files_read"] == 1
