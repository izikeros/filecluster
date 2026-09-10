"""Command line interface for ``filecluster curate``.

Kept in the subpackage while curation matures. Wiring it into the main CLI is one
line in :mod:`filecluster.cli`::

    from filecluster.curation.cli import curate_cmd

    app.command("curate")(curate_cmd)

Until then the command is reachable as ``python -m filecluster.curation``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from filecluster import ui as base_ui
from filecluster.curation import ui as curation_ui
from filecluster.curation.catalog import CurationCatalog
from filecluster.curation.configuration import CurationSettings, load_settings
from filecluster.curation.exceptions import (
    CurationConfigError,
    MissingDependencyError,
)
from filecluster.curation.operations import (
    OperationMode,
    build_operation_plan,
    execute_plan,
)
from filecluster.curation.pipeline import CurationPipeline, CurationRun
from filecluster.curation.providers.base import Providers
from filecluster.curation.reporting import json_summary, write_report

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Sort inbox media into keep / review / reject before clustering.",
)


@app.command("curate")
def curate_cmd(  # noqa: C901 - a CLI entry point is a flat list of options
    inbox_dir: Annotated[
        Path,
        typer.Option(
            "-i",
            "--inbox-dir",
            help="Directory with the media files to curate.",
            exists=True,
            file_okay=False,
            readable=True,
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "-o",
            "--output-dir",
            help="Where the keep/review/reject folders are created.",
            file_okay=False,
        ),
    ],
    execute: Annotated[
        bool,
        typer.Option(
            "--execute",
            help="Apply the plan. Without this flag nothing is written.",
        ),
    ] = False,
    copy_files: Annotated[
        bool, typer.Option("--copy", help="Copy files (default with --execute).")
    ] = False,
    move_files: Annotated[
        bool, typer.Option("--move", help="Move files instead of copying them.")
    ] = False,
    report: Annotated[
        Path | None,
        typer.Option(
            "--report",
            help="Write the full per-file result to this CSV file.",
            dir_okay=False,
            writable=True,
        ),
    ] = None,
    as_json: Annotated[
        bool, typer.Option("--json", help="Print an aggregate JSON summary.")
    ] = False,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Cascade configuration file (JSON, or YAML with PyYAML).",
            dir_okay=False,
            exists=True,
        ),
    ] = None,
    cache: Annotated[
        Path | None,
        typer.Option("--cache", help="Explicit path for the verdict cache."),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            help="Analyse the first N files in path order. Deterministic.",
            min=1,
        ),
    ] = None,
    device: Annotated[
        str | None,
        typer.Option("--device", help="auto, cpu, mps or cuda."),
    ] = None,
    without_ocr: Annotated[
        bool, typer.Option("--without-ocr", help="Disable the OCR stage.")
    ] = False,
    with_ocr: Annotated[
        bool, typer.Option("--with-ocr", help="Enable the OCR stage.")
    ] = False,
    without_semantic: Annotated[
        bool, typer.Option("--without-semantic", help="Disable the semantic stage.")
    ] = False,
    with_semantic: Annotated[
        bool, typer.Option("--with-semantic", help="Enable the semantic stage.")
    ] = False,
    enable_vlm: Annotated[
        bool, typer.Option("--enable-vlm", help="Escalate uncertain files to a VLM.")
    ] = False,
    allow_remote_vlm: Annotated[
        bool,
        typer.Option(
            "--allow-remote-vlm",
            help="Permit sending images to a remote VLM service.",
        ),
    ] = False,
    force_recompute: Annotated[
        bool, typer.Option("--force-recompute", help="Ignore cached verdicts.")
    ] = False,
    no_cache: Annotated[
        bool, typer.Option("--no-cache", help="Neither read nor write the cache.")
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("-Y", "--yes", help="Do not ask for confirmation before writing."),
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
) -> None:
    """Classify inbox media as keep, review or reject.

    Dry-run by default: the plan is computed and shown, and nothing is written
    until --execute. Files are never deleted; reject only means "do not import
    automatically".
    """
    base_ui.configure_logging(verbosity=verbose, quiet=quiet)
    render = not as_json and not quiet
    console = base_ui.make_console(color=False if as_json else color)
    err_console = base_ui.make_console(stderr=True, color=color)

    if copy_files and move_files:
        _fail(err_console, "--copy and --move are mutually exclusive.")
    mode = OperationMode.SKIP
    if execute:
        mode = OperationMode.MOVE if move_files else OperationMode.COPY

    try:
        settings = load_settings(
            config,
            cache_path=cache,
            device=device,
            enable_ocr=_tri_state(with_ocr, without_ocr),
            enable_semantic=_tri_state(with_semantic, without_semantic),
            enable_vlm=enable_vlm or None,
            allow_remote_vlm=allow_remote_vlm or None,
        )
        providers = _build_providers(settings, err_console)
    except CurationConfigError as exc:
        _fail(err_console, str(exc), hint="Check --config and the threshold values.")

    reporter = (
        base_ui.RichReporter(console, verbose=verbose)
        if render
        else base_ui.NullReporter()
    )
    if render:
        curation_ui.banner(console, inbox_dir, output_dir, settings, mode, execute)

    catalog = None
    try:
        if not no_cache:
            catalog = CurationCatalog.open(settings.cache_path_for(inbox_dir))
        pipeline = CurationPipeline(settings, providers, catalog)
        with reporter.phase("Curate") as phase:
            run = pipeline.run(
                inbox_dir,
                limit=limit,
                progress=phase,
                force_recompute=force_recompute,
            )
            counts = run.decision_counts()
            phase.detail = (
                f"{counts['keep']} keep, {counts['review']} review, "
                f"{counts['reject']} reject"
            )
    except KeyboardInterrupt:
        base_ui.error_panel(err_console, "Interrupted. Nothing was written.")
        raise typer.Exit(EXIT_INTERRUPTED) from None
    except (NotADirectoryError, PermissionError) as exc:
        _fail(err_console, str(exc), hint="Check the -i and -o paths.")
    finally:
        if catalog is not None:
            catalog.close()

    plan = build_operation_plan(run.results, output_dir, mode)

    if execute:
        approved = yes or not render or curation_ui.confirm_plan(console, plan)
        if not approved:
            if render:
                console.print("\n  [yellow]Aborted.[/] [dim]Nothing was written.[/]\n")
            raise typer.Exit(EXIT_OK)
        try:
            with reporter.phase("Write files") as phase:
                execute_plan(plan, progress=phase)
                phase.detail = f"{plan.n_completed} done, {plan.n_failed} failed"
            run.executed = True
        except KeyboardInterrupt:
            base_ui.error_panel(
                err_console,
                "Interrupted. Files already written were left in place.",
            )
            raise typer.Exit(EXIT_INTERRUPTED) from None

    _emit(console, err_console, run, plan, report, as_json, render, verbose)


def _emit(
    console,
    err_console,
    run: CurationRun,
    plan,
    report: Path | None,
    as_json: bool,
    render: bool,
    verbose: int,
) -> NoReturn:
    """Write the report and the summary, then exit."""
    if report is not None:
        rows = write_report(report, run, plan)
        if render:
            console.print(
                f"\n  [dim]Wrote {base_ui.fmt_count(rows)} rows to {report}[/]",
                highlight=False,
            )

    if as_json:
        typer.echo(json.dumps(json_summary(run, plan), indent=2))
        raise typer.Exit(EXIT_OK)

    if render:
        curation_ui.results(console, run, plan)
        curation_ui.reasons(console, run)
        if not run.executed and plan.ops:
            base_ui.plan_preview(console, plan.preview(), str(plan.output_dir))
        console.print()

    raise typer.Exit(EXIT_FAILURE if plan.n_failed else EXIT_OK)


def _tri_state(enable: bool, disable: bool) -> bool | None:
    """Turn a pair of on/off flags into an override, or *None* for untouched."""
    if enable and not disable:
        return True
    if disable and not enable:
        return False
    return None


def _build_providers(settings: CurationSettings, err_console) -> Providers:
    """Construct the providers the settings ask for.

    A missing optional dependency is reported as a one-line install hint and the
    stage is skipped, so the run continues with fewer signals instead of failing
    with an ``ImportError``.
    """
    providers = Providers()
    if settings.enable_semantic:
        from filecluster.curation.providers.semantic import SigLipSemanticProvider

        try:
            providers.semantic = SigLipSemanticProvider(
                device=settings.device, batch_size=settings.batch_size
            )
        except MissingDependencyError as exc:
            base_ui.error_panel(err_console, str(exc), hint="Continuing without it.")
    if settings.enable_ocr:
        from filecluster.curation.providers.ocr import RapidOcrProvider

        try:
            providers.ocr = RapidOcrProvider()
        except MissingDependencyError as exc:
            base_ui.error_panel(err_console, str(exc), hint="Continuing without it.")
    if settings.enable_vlm:
        from filecluster.curation.providers.vlm import LocalVlmProvider

        try:
            providers.vlm = LocalVlmProvider()
        except MissingDependencyError as exc:
            base_ui.error_panel(err_console, str(exc), hint="Continuing without it.")
    return providers


def _fail(console, message: str, hint: str = "") -> NoReturn:
    base_ui.error_panel(console, message, hint)
    raise typer.Exit(EXIT_USAGE)


if __name__ == "__main__":  # pragma: no cover
    app()
