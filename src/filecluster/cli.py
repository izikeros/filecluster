"""Command line interface.

Thin wrapper around :func:`filecluster.file_cluster.main` and
:func:`filecluster.reconcile.reconcile`: it parses options, sets up
rendering, and turns exceptions into readable messages.  All layout decisions
live in :mod:`filecluster.ui`, and all orchestration in ``file_cluster`` /
``reconcile``, so this module stays free of both.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, NoReturn

import click
import typer
from typer.core import TyperGroup

from filecluster import ui
from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneError
from filecluster.file_cluster import main
from filecluster.version import get_version

# Exit codes, matching the usual shell conventions.
EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130


class _DefaultRunGroup(TyperGroup):
    """Typer group that falls back to the ``run`` subcommand.

    When the first CLI token is not a known subcommand name, ``run`` is
    prepended so that ``filecluster -i … -o …`` keeps working after
    ``reconcile`` was added as a second command.
    """

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        # If the first token is not a registered command name, assume ``run``.
        if args and args[0] not in self.commands:
            args = ["run", *args]
        # Bare invocation (no args at all) also defaults to ``run``.
        if not args:
            args = ["run"]
        return super().parse_args(ctx, args)


app = typer.Typer(
    cls=_DefaultRunGroup,
    add_completion=False,
    no_args_is_help=False,
    invoke_without_command=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Group photos and videos into event folders based on their timestamps.",
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"filecluster {get_version()}")
        raise typer.Exit(EXIT_OK)


@app.command()
def run(  # noqa: C901 - a CLI entry point is a flat list of options by nature
    inbox_dir: Annotated[
        Path | None,
        typer.Option(
            "-i",
            "--inbox-dir",
            help="Directory with input media files to process.",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "-o",
            "--output-dir",
            help="Directory where clustered media will be placed.",
            file_okay=False,
        ),
    ] = None,
    watch_dirs: Annotated[
        list[Path] | None,
        typer.Option(
            "-w",
            "--watch-dir",
            help="Existing media library to match against. Repeatable.",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ] = None,
    development_mode: Annotated[
        bool,
        typer.Option(
            "-t", "--development-mode", help="Use the development test directories."
        ),
    ] = False,
    no_operation: Annotated[
        bool,
        typer.Option(
            "-n", "--no-operation", help="Dry run: show the plan, change nothing."
        ),
    ] = False,
    copy_mode: Annotated[
        bool, typer.Option("-y", "--copy-mode", help="Copy files instead of moving.")
    ] = False,
    force_deep_scan: Annotated[
        bool,
        typer.Option(
            "-f",
            "--force-deep-scan",
            help="Recompute cluster info for every existing cluster.",
        ),
    ] = False,
    drop_duplicates: Annotated[
        bool,
        typer.Option(
            "-d",
            "--drop-duplicates",
            help="Put duplicates in a separate folder instead of clustering them.",
        ),
    ] = False,
    use_existing_clusters: Annotated[
        bool,
        typer.Option(
            "-c",
            "--use-existing-clusters",
            help="Assign media to matching clusters already in the watch folders.",
        ),
    ] = False,
    restore_original_names: Annotated[
        bool,
        typer.Option(
            "-r",
            "--restore-original-names",
            help="Strip copy suffixes such as '-Kopiuj(1)' or ' - Copy' from names.",
        ),
    ] = False,
    limit: Annotated[
        int | None,
        typer.Option(
            "-l",
            "--limit",
            help=(
                "Ingest at most this many inbox files, in name order. "
                "Handy with --no-operation to try a large inbox quickly."
            ),
            min=1,
        ),
    ] = None,
    yes: Annotated[
        bool,
        typer.Option("-Y", "--yes", help="Do not ask for confirmation before writing."),
    ] = False,
    show: Annotated[
        int,
        typer.Option(
            "--show",
            help="How many of the largest clusters to list. 0 lists all of them.",
            min=0,
        ),
    ] = ui.MAX_CLUSTER_ROWS,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            help="Write the full per-file operation list to this CSV file.",
            dir_okay=False,
            writable=True,
        ),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print a machine-readable summary instead."),
    ] = False,
    color: Annotated[
        bool | None,
        typer.Option("--color/--no-color", help="Force colour on or off."),
    ] = None,
    verbose: Annotated[
        int,
        typer.Option("-v", "--verbose", count=True, help="-v for info, -vv for debug."),
    ] = 0,
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Only report errors.")
    ] = False,
    _version: Annotated[
        bool,
        typer.Option(
            "-V",
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the version and exit.",
        ),
    ] = False,
) -> None:
    """Group photos and videos into event folders based on their timestamps."""
    ui.configure_logging(verbosity=verbose, quiet=quiet)
    ui.diagnostics.reset()

    # With --json the summary is the payload, so nothing else may touch stdout.
    render = not as_json and not quiet
    console = ui.make_console(color=False if as_json else color)
    err_console = ui.make_console(stderr=True, color=color)
    reporter = (
        ui.RichReporter(console, verbose=verbose) if render else ui.NullReporter()
    )

    try:
        results = main(
            inbox_dir=str(inbox_dir) if inbox_dir else None,
            output_dir=str(output_dir) if output_dir else None,
            watch_dir_list=[str(w) for w in (watch_dirs or [])],
            development_mode=development_mode,
            no_operation=no_operation,
            copy_mode=copy_mode,
            force_deep_scan=force_deep_scan,
            drop_duplicates=drop_duplicates,
            use_existing_clusters=use_existing_clusters,
            restore_original_names=restore_original_names,
            limit=limit,
            reporter=reporter,
            confirm=None if yes else _make_confirm(console, render),
            banner=_make_banner(console) if render else None,
        )
    except KeyboardInterrupt:
        ui.error_panel(err_console, "Interrupted. No further files were touched.")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (FileNotFoundError, NotADirectoryError) as exc:
        _fail(err_console, exc, verbose, hint="Check the -i, -o and -w paths.")
    except PermissionError as exc:
        _fail(err_console, exc, verbose, hint="Check the file and folder permissions.")
    except DateStringNoneError as exc:
        _fail(
            err_console,
            exc,
            verbose,
            message="Could not determine a date for at least one cluster.",
            hint="Re-run with -vv to see which files lack a usable timestamp.",
        )
    except ValueError as exc:
        _fail(err_console, exc, verbose)

    elapsed = float(results.get("elapsed") or 0.0)
    plan = results.get("file_operation_plan")
    config = results["config"]  # always present once main() has returned

    if report is not None:
        n_rows = ui.write_report(report, plan, config)
        if render:
            console.print(
                f"\n  [dim]Wrote {ui.fmt_count(n_rows)} rows to {report}[/]",
                highlight=False,
            )

    if as_json:
        typer.echo(json.dumps(ui.json_summary(results, config, elapsed), indent=2))
        raise typer.Exit(EXIT_OK)

    if results.get("aborted"):
        if render:
            console.print("\n  [yellow]Aborted.[/] [dim]Nothing was written.[/]\n")
        raise typer.Exit(EXIT_OK)

    if render:
        ui.results_panel(console, results, config, elapsed)
        ui.largest_clusters(console, results.get("cluster_sizes") or [], limit=show)
        if config.mode == CopyMode.NOP and plan is not None:
            ui.plan_preview(console, plan.destinations, str(config.out_dir_name))
        ui.render_diagnostics(console, verbose=verbose)
        console.print()

    raise typer.Exit(EXIT_OK)


@app.command()
def reconcile_cmd(  # noqa: C901
    source: Annotated[
        Path,
        typer.Option(
            "-s",
            "--source",
            help="Directory to reconcile (inbox or output dir with event folders).",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ],
    library: Annotated[
        Path,
        typer.Option(
            "-l",
            "--library",
            help="Main photo library root.",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ],
    duplicates_dir: Annotated[
        Path | None,
        typer.Option(
            "-d",
            "--duplicates-dir",
            help="Where to move confirmed duplicates. Defaults to <source>/../duplicates.",
            file_okay=False,
        ),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute",
            help="Apply the plan (move files). Without this flag nothing is written.",
        ),
    ] = False,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            help="Write a per-file CSV report to this path.",
            dir_okay=False,
            writable=True,
        ),
    ] = None,
    force_reindex: Annotated[
        bool,
        typer.Option(
            "-f",
            "--force-reindex",
            help=(
                "Rebuild the library index from scratch. Backs up the existing "
                "catalog before clearing it."
            ),
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print a machine-readable JSON summary."),
    ] = False,
    color: Annotated[
        bool | None,
        typer.Option("--color/--no-color", help="Force colour on or off."),
    ] = None,
    verbose: Annotated[
        int,
        typer.Option("-v", "--verbose", count=True, help="-v for info, -vv for debug."),
    ] = 0,
    quiet: Annotated[
        bool,
        typer.Option("-q", "--quiet", help="Only report errors."),
    ] = False,
) -> None:
    """Reconcile a source directory against the main photo library.

    Checks which source files already exist in the library (by content hash)
    and plans to move duplicates aside and new files into the library.
    Dry-run by default; pass --execute to apply.
    """
    from filecluster.reconcile import reconcile

    ui.configure_logging(verbosity=verbose, quiet=quiet)
    render = not as_json and not quiet
    console = ui.make_console(color=False if as_json else color)
    err_console = ui.make_console(stderr=True, color=color)
    reporter = (
        ui.RichReporter(console, verbose=verbose) if render else ui.NullReporter()
    )

    dup_dir = duplicates_dir or (source.parent / "duplicates")

    try:
        if render:
            ui.reconcile_banner(console, source, library, dup_dir, execute)

        with reporter.phase("Index library") as phase:
            # Phase is used for the spinner; the actual progress is inside reconcile
            pass

        with reporter.phase("Reconcile") as phase:
            plan = reconcile(
                source=source,
                library=library,
                duplicates_dir=dup_dir,
                execute=execute,
                force_reindex=force_reindex,
                progress=phase,
            )
            phase.detail = f"{plan.n_new} new, {plan.n_duplicates} dup"

    except KeyboardInterrupt:
        ui.error_panel(err_console, "Interrupted. No further files were touched.")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (FileNotFoundError, NotADirectoryError) as exc:
        _fail(err_console, exc, verbose, hint="Check the -s and -l paths.")
    except PermissionError as exc:
        _fail(err_console, exc, verbose, hint="Check file and folder permissions.")
    except ValueError as exc:
        _fail(err_console, exc, verbose)

    if report is not None:
        n_rows = plan.write_csv(report)
        if render:
            console.print(
                f"\n  [dim]Wrote {ui.fmt_count(n_rows)} rows to {report}[/]",
                highlight=False,
            )

    if as_json:
        typer.echo(json.dumps(plan.summary_dict(), indent=2))
        raise typer.Exit(EXIT_OK)

    if render:
        ui.reconcile_results(console, plan)
        if plan.folder_results:
            ui.reconcile_folder_table(console, plan)
        ui.reconcile_plan_preview(console, plan, execute)
        console.print()

    raise typer.Exit(EXIT_OK)


def _make_banner(console) -> Any:
    """Return a callback that shows the resolved configuration."""

    def _banner(config) -> None:
        ui.banner(console, config, get_version())

    return _banner


def _make_confirm(console, render: bool) -> Any:
    """Return the confirmation gate, or an auto-approve when not rendering."""
    if not render:
        return None

    def _confirm(plan, config) -> bool:
        return ui.confirm_plan(console, plan, config)

    return _confirm


def _fail(
    console,
    exc: Exception,
    verbose: int,
    message: str | None = None,
    hint: str = "",
) -> NoReturn:
    """Report a fatal error and exit, showing a traceback only at -vv."""
    ui.error_panel(console, message or str(exc) or exc.__class__.__name__, hint)
    if verbose >= 2:
        console.print_exception()
    raise typer.Exit(EXIT_USAGE) from exc


if __name__ == "__main__":  # pragma: no cover
    app()
