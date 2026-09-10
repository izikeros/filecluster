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

import typer
from typer.core import TyperGroup

from filecluster import ui
from filecluster.configuration import CopyMode
from filecluster.exceptions import DateStringNoneError, OverlappingPathsError
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

    # ``ctx`` is deliberately untyped: typer vendors its own copy of click, so
    # the base signature refers to ``typer._click.Context``, which is private
    # and not the same class as the public ``click.Context``.
    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
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


@app.command("reconcile")
def reconcile_cmd(  # noqa: C901 - a CLI entry point is a flat list of options by nature
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
        list[Path],
        typer.Option(
            "-l",
            "--library",
            help="Main photo library root. Repeatable; new files go to the first.",
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
            help=(
                "Where to move confirmed duplicates. "
                "Defaults to <source>/../duplicates."
            ),
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
    copy_mode: Annotated[
        bool,
        typer.Option(
            "-y",
            "--copy-mode",
            help="Copy files into the library instead of moving them.",
        ),
    ] = False,
    scan_only: Annotated[
        bool,
        typer.Option(
            "--scan-only",
            help="Classify and report only; plan no moves even with --execute.",
        ),
    ] = False,
    no_recursive: Annotated[
        bool,
        typer.Option(
            "--no-recursive",
            help="Only look at the top level of the source directory.",
        ),
    ] = False,
    no_sidecars: Annotated[
        bool,
        typer.Option(
            "--no-sidecars",
            help="Leave companion files (.xmp, .aae, …) behind instead of "
            "moving them with their media file.",
        ),
    ] = False,
    no_source_dupes: Annotated[
        bool,
        typer.Option(
            "--no-source-dupes",
            help="Skip detection of files duplicated inside the source itself.",
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
    from filecluster.reconcile import ReconcileAction, reconcile

    ui.configure_logging(verbosity=verbose, quiet=quiet)
    render = not as_json and not quiet
    console = ui.make_console(color=False if as_json else color)
    err_console = ui.make_console(stderr=True, color=color)
    reporter = (
        ui.RichReporter(console, verbose=verbose) if render else ui.NullReporter()
    )

    dup_dir = duplicates_dir or (source.parent / "duplicates")
    if scan_only:
        action = ReconcileAction.SCAN
    elif copy_mode:
        action = ReconcileAction.COPY
    else:
        action = ReconcileAction.MOVE

    try:
        if render:
            ui.reconcile_banner(
                console, source, library, dup_dir, execute, action.value
            )

        with reporter.phase("Reconcile") as phase:
            plan = reconcile(
                source=source,
                library=library,
                duplicates_dir=dup_dir,
                execute=execute,
                force_reindex=force_reindex,
                progress=phase,
                action=action,
                recursive=not no_recursive,
                include_sidecars=not no_sidecars,
                detect_source_duplicates=not no_source_dupes,
            )
            phase.detail = f"{plan.n_new} new, {plan.n_duplicates} dup"

    except KeyboardInterrupt:
        ui.error_panel(err_console, "Interrupted. No further files were touched.")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (FileNotFoundError, NotADirectoryError) as exc:
        _fail(err_console, exc, verbose, hint="Check the -s and -l paths.")
    except PermissionError as exc:
        _fail(err_console, exc, verbose, hint="Check file and folder permissions.")
    except OverlappingPathsError as exc:
        _fail(
            err_console,
            exc,
            verbose,
            hint=(
                "Give -s, -l and -d separate directories. Nothing was read or written."
            ),
        )
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
        ui.reconcile_plan_preview(console, plan, execute and not scan_only)
        ui.render_diagnostics(console, verbose=verbose)
        console.print()

    raise typer.Exit(EXIT_OK)


@app.command("dedup")
def dedup_cmd(
    directory: Annotated[
        Path,
        typer.Option(
            "-d",
            "--dir",
            help="Directory tree to scan for duplicate media files.",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ],
    quarantine_dir: Annotated[
        Path | None,
        typer.Option(
            "-q",
            "--quarantine-dir",
            help=(
                "Move redundant copies here instead of only reporting them. "
                "One copy of each group is always left in place."
            ),
            file_okay=False,
        ),
    ] = None,
    execute: Annotated[
        bool,
        typer.Option(
            "--execute",
            help="Apply the plan. Requires --quarantine-dir.",
        ),
    ] = False,
    min_size: Annotated[
        int,
        typer.Option(
            "--min-size",
            help="Ignore files smaller than this many bytes.",
            min=0,
        ),
    ] = 1,
    no_recursive: Annotated[
        bool,
        typer.Option("--no-recursive", help="Only look at the top level."),
    ] = False,
    show: Annotated[
        int,
        typer.Option(
            "--show",
            help="How many duplicate groups to list. 0 lists all of them.",
            min=0,
        ),
    ] = ui.MAX_TREE_FOLDERS,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            help="Write one CSV row per copy to this path.",
            dir_okay=False,
            writable=True,
        ),
    ] = None,
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
    quiet: Annotated[bool, typer.Option("--quiet", help="Only report errors.")] = False,
) -> None:
    """Find media files stored more than once inside one directory tree.

    Detects duplicates both within a single folder and across folders, picks
    one copy of each group to keep, and reports the rest. Dry-run by default.
    """
    from filecluster.dedup import DedupAction, dedup

    ui.configure_logging(verbosity=verbose, quiet=quiet)
    render = not as_json and not quiet
    console = ui.make_console(color=False if as_json else color)
    err_console = ui.make_console(stderr=True, color=color)
    reporter = (
        ui.RichReporter(console, verbose=verbose) if render else ui.NullReporter()
    )

    action = DedupAction.QUARANTINE if quarantine_dir else DedupAction.REPORT
    if execute and quarantine_dir is None:
        _fail(
            err_console,
            ValueError("--execute needs --quarantine-dir"),
            verbose,
            message="Nothing to execute: no quarantine directory was given.",
            hint="Add -q/--quarantine-dir to say where redundant copies go.",
        )

    try:
        if render:
            ui.dedup_banner(console, directory, quarantine_dir, action.value, execute)

        with reporter.phase("Scan for duplicates") as phase:
            plan = dedup(
                directory,
                quarantine_dir,
                action=action,
                execute=execute,
                min_size=min_size,
                recursive=not no_recursive,
                progress=phase,
            )
            phase.detail = f"{plan.n_groups} groups, {plan.n_duplicate_files} copies"
    except KeyboardInterrupt:
        ui.error_panel(err_console, "Interrupted. No further files were touched.")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (FileNotFoundError, NotADirectoryError) as exc:
        _fail(err_console, exc, verbose, hint="Check the -d path.")
    except PermissionError as exc:
        _fail(err_console, exc, verbose, hint="Check file and folder permissions.")

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
        ui.dedup_results(console, plan)
        ui.dedup_groups(console, plan, limit=show)
        console.print()

    raise typer.Exit(EXIT_OK)


catalog_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Inspect and maintain the per-library SQLite catalog.",
)
app.add_typer(catalog_app, name="catalog")

_LibraryOption = Annotated[
    Path,
    typer.Option(
        "-l",
        "--library",
        help="Library root holding the .filecluster.db catalog.",
        exists=True,
        file_okay=False,
        readable=True,
    ),
]
_JsonOption = Annotated[
    bool, typer.Option("--json", help="Print a machine-readable JSON summary.")
]


def _catalog_console(as_json: bool) -> Any:
    return ui.make_console(color=False if as_json else None)


@catalog_app.command("stats")
def catalog_stats_cmd(library: _LibraryOption, as_json: _JsonOption = False) -> None:
    """Show what the catalog currently holds."""
    from filecluster.catalog import LibraryCatalog

    with LibraryCatalog.open(library) as catalog:
        stats = catalog.stats()
    stats["backups"] = [str(p) for p in LibraryCatalog.list_backups(library)]

    if as_json:
        typer.echo(json.dumps(stats, indent=2))
        raise typer.Exit(EXIT_OK)
    ui.catalog_stats(_catalog_console(as_json), library, stats)
    raise typer.Exit(EXIT_OK)


@catalog_app.command("backup")
def catalog_backup_cmd(library: _LibraryOption, as_json: _JsonOption = False) -> None:
    """Copy the catalog to a timestamped .bak file."""
    from filecluster.catalog import LibraryCatalog

    path = LibraryCatalog.backup(library)
    if as_json:
        typer.echo(json.dumps({"backup": str(path) if path else None}, indent=2))
        raise typer.Exit(EXIT_OK)

    console = _catalog_console(as_json)
    if path is None:
        ui.catalog_message(console, "Nothing to back up", [("Library", str(library))])
    else:
        ui.catalog_message(console, "Catalog backed up", [("Backup", str(path))])
    raise typer.Exit(EXIT_OK)


@catalog_app.command("restore")
def catalog_restore_cmd(
    library: _LibraryOption,
    backup: Annotated[
        Path | None,
        typer.Option(
            "--from",
            help="Backup file to restore. Defaults to the newest one.",
            dir_okay=False,
            exists=True,
        ),
    ] = None,
    as_json: _JsonOption = False,
) -> None:
    """Restore the catalog from a backup, backing up the current one first."""
    from filecluster.catalog import LibraryCatalog

    err_console = ui.make_console(stderr=True)
    try:
        restored = LibraryCatalog.restore(library, backup)
    except FileNotFoundError as exc:
        _fail(err_console, exc, 0, hint="Run `filecluster catalog backup` first.")

    if as_json:
        typer.echo(json.dumps({"restored_from": str(restored)}, indent=2))
        raise typer.Exit(EXIT_OK)
    ui.catalog_message(
        _catalog_console(as_json),
        "Catalog restored",
        [("Restored from", str(restored)), ("Library", str(library))],
    )
    raise typer.Exit(EXIT_OK)


@catalog_app.command("verify")
def catalog_verify_cmd(
    library: _LibraryOption,
    prune: Annotated[
        bool,
        typer.Option(
            "--prune",
            help="Delete rows for files that are missing or have changed.",
        ),
    ] = False,
    as_json: _JsonOption = False,
) -> None:
    """Check cached file rows against the files on disk."""
    from filecluster.catalog import LibraryCatalog

    with LibraryCatalog.open(library) as catalog:
        result = catalog.verify(library)
        removed = 0
        if prune:
            removed = catalog.delete_file_rows(result["missing"] + result["stale"])
            catalog.vacuum()

    payload = {
        "library": str(library),
        "ok": len(result["ok"]),
        "stale": len(result["stale"]),
        "missing": len(result["missing"]),
        "pruned": removed,
    }
    if as_json:
        typer.echo(json.dumps(payload, indent=2))
        raise typer.Exit(EXIT_OK)

    rows = [
        ("Up to date", ui.fmt_count(payload["ok"])),
        ("Changed on disk", ui.fmt_count(payload["stale"])),
        ("Gone from disk", ui.fmt_count(payload["missing"])),
    ]
    if prune:
        rows.append(("Rows pruned", ui.fmt_count(removed)))
    else:
        rows.append(("Next step", "re-run with --prune to clean up"))
    ui.catalog_message(_catalog_console(as_json), "Catalog verification", rows)
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
