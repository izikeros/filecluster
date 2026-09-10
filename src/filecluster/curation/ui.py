"""Terminal rendering for the curation command.

Bounded on purpose, like :mod:`filecluster.ui`: an inbox of 50k files produces a
fixed handful of lines. Per-file detail belongs in ``--report``.
"""

from __future__ import annotations

import sys

from rich.console import Console, RenderableType
from rich.padding import Padding
from rich.prompt import Confirm
from rich.table import Table

from filecluster.curation.configuration import CurationSettings
from filecluster.curation.operations import CurationOperationPlan, OperationMode
from filecluster.curation.pipeline import CurationRun
from filecluster.curation.reporting import MAX_TOP_REASONS, top_reasons
from filecluster.ui import (
    fmt_count,
    fmt_duration,
    fmt_files,
    supports_animation,
)
from filecluster.version import get_version

_LABEL_WIDTH = 22


def _indent(renderable: RenderableType) -> Padding:
    return Padding(renderable, (0, 0, 0, 2), expand=False)


def _grid(rows: list[tuple[str, str]], *, right: bool = False) -> Table:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="dim", width=_LABEL_WIDTH)
    grid.add_column(justify="right" if right else "left", overflow="fold")
    for label, value in rows:
        grid.add_row(label, value)
    return grid


def banner(
    console: Console,
    inbox: object,
    output_dir: object,
    settings: CurationSettings,
    mode: OperationMode,
    execute: bool,
) -> None:
    """Show the resolved configuration before the cascade starts."""
    stages = " · ".join(
        [
            "rules",
            "features",
            f"ocr {'on' if settings.enable_ocr else 'off'}",
            f"semantic {'on' if settings.enable_semantic else 'off'}",
            f"vlm {'on' if settings.enable_vlm else 'off'}",
        ]
    )
    rows = [
        ("Inbox", str(inbox)),
        ("Output", str(output_dir)),
        ("Mode", f"[bold]{mode.value.upper() if execute else 'DRY RUN'}[/]"),
        ("Stages", stages),
        (
            "Thresholds",
            f"keep ≥ {settings.keep_threshold:.2f} · "
            f"reject ≤ {settings.reject_threshold:.2f} · "
            f"confidence ≥ {settings.minimum_confidence:.2f}",
        ),
    ]
    console.print()
    console.print(f"  [bold cyan]filecluster curate[/] [dim]{get_version()}[/]")
    console.print(_indent(_grid(rows)))
    console.print()


def results(
    console: Console,
    run: CurationRun,
    plan: CurationOperationPlan | None = None,
) -> None:
    """Render the aggregate outcome of a run."""
    counts = run.decision_counts()
    rows = [
        ("Files discovered", fmt_count(run.n_discovered)),
        ("Files analysed", fmt_count(len(run.results))),
        ("From cache", fmt_count(run.n_cache_hits)),
        ("Keep", f"[green]{fmt_count(counts['keep'])}[/]"),
        ("Review", f"[yellow]{fmt_count(counts['review'])}[/]"),
        ("Reject", f"[red]{fmt_count(counts['reject'])}[/]"),
    ]
    if run.n_errors:
        rows.append(("Files with errors", fmt_count(run.n_errors)))
    if run.n_skipped:
        rows.append(("Unreadable, skipped", fmt_count(run.n_skipped)))
    if plan is not None:
        rows.append(("Planned operations", fmt_count(plan.n_writes)))
        if plan.n_renamed:
            rows.append(("Renamed to be safe", fmt_count(plan.n_renamed)))
        if plan.n_completed:
            rows.append(("Completed", fmt_count(plan.n_completed)))
        if plan.n_failed:
            rows.append(("Failed", f"[red]{fmt_count(plan.n_failed)}[/]"))
    rows.append(("Elapsed", fmt_duration(run.elapsed_seconds)))

    console.print()
    console.print("  [bold]Results[/]")
    console.print(_indent(_grid(rows, right=True)))


def reasons(
    console: Console,
    run: CurationRun,
    limit: int = MAX_TOP_REASONS,
) -> None:
    """List the most frequent reason codes, capped."""
    items = top_reasons(run, limit)
    if not items:
        return

    table = Table(
        box=None, pad_edge=False, show_header=True, header_style="dim", padding=(0, 2)
    )
    table.add_column("Reason", overflow="ellipsis", no_wrap=True, max_width=48)
    table.add_column("Files", justify="right")
    for reason, count in items:
        table.add_row(reason, fmt_count(count))

    console.print()
    console.print("  [bold]Most common reasons[/]")
    console.print(_indent(table))


def confirm_plan(console: Console, plan: CurationOperationPlan) -> bool:
    """Ask once, before the first write. Non-interactive callers proceed."""
    by_decision = plan.counts_by_decision()
    console.print()
    console.print(
        f"  [bold]{plan.mode.value.capitalize()} {fmt_files(plan.n_writes)}"
        f" into {plan.output_dir}[/]"
    )
    console.print(
        f"  [dim]keep {by_decision['keep']} ·"
        f" review {by_decision['review']} ·"
        f" reject {by_decision['reject']}[/]"
    )
    if plan.n_renamed:
        console.print(
            f"  [yellow]{fmt_count(plan.n_renamed)} files will be renamed[/]"
            " [dim]to avoid overwriting existing files[/]"
        )
    if not supports_animation(console) or not sys.stdin.isatty():
        return True
    return Confirm.ask("  Proceed?", console=console, default=False)
