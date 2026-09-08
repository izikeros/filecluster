"""Terminal presentation layer.

All user-facing rendering lives here so orchestration code stays free of
formatting concerns.

Every renderer in this module is bounded on purpose: none of them emits one
line per media file. A single session may process tens of thousands of files,
so per-file volume belongs on a progress bar (which overwrites in place) or in
a report file, never in the terminal scrollback.
"""

from __future__ import annotations

import csv
import os
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Protocol

from rich.console import Console, RenderableType
from rich.padding import Padding
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
)
from rich.prompt import Confirm
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from filecluster import logger
from filecluster.configuration import CopyMode
from filecluster.version import get_version

# Rendering caps. Output stays this small no matter how large the inbox is.
MAX_CLUSTER_ROWS = 20
MAX_TREE_FOLDERS = 20
MAX_TREE_SAMPLES = 3
MAX_DIAGNOSTIC_SAMPLES = 20

# Progress advances are batched so a 50k-file loop does not issue 50k redraws.
MAX_PROGRESS_BATCH = 64

_LABEL_WIDTH = 20
_DETAIL_WIDTH = 34


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def fmt_count(value: object) -> str:
    """Format a count with thousands separators.

    Accepts anything countable, including numpy integers from pandas, and
    falls back to zero for a missing value so a summary never crashes.
    """
    try:
        return f"{int(value):,}"  # ty: ignore[invalid-argument-type]
    except (TypeError, ValueError):
        return "0"


def fmt_duration(seconds: float) -> str:
    """Format an elapsed time as h:mm:ss."""
    total = round(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def mode_label(mode: CopyMode) -> str:
    """Return a short human label for an operation mode."""
    return {
        CopyMode.NOP: "DRY RUN",
        CopyMode.COPY: "COPY",
        CopyMode.MOVE: "MOVE",
    }.get(mode, str(mode))


def fmt_files(count: int) -> str:
    """Format a file count with the right singular/plural noun."""
    return f"{fmt_count(count)} file{'' if count == 1 else 's'}"


def supports_animation(console: Console) -> bool:
    """Whether *console* can host a redrawing spinner or progress bar.

    ``Console.is_terminal`` is deliberately true when ``FORCE_COLOR`` is set,
    which is right for colour but wrong for animation: redrawing into a pipe
    leaves one line of debris per refresh. Animation therefore requires a real
    tty.
    """
    if not console.is_terminal:
        return False
    try:
        return bool(console.file.isatty())
    except (AttributeError, ValueError):
        return False


def _flag(name: str, enabled: bool) -> str:
    return f"{name} {'on' if enabled else 'off'}"


def _indent(renderable: RenderableType) -> Padding:
    """Indent a renderable to line up with the phase and heading lines.

    ``expand=False`` keeps lines from being padded out to the console width,
    which would otherwise add trailing whitespace to every row.
    """
    return Padding(renderable, (0, 0, 0, 2), expand=False)


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
@dataclass
class Diagnostic:
    """One class of problem, aggregated across all affected files."""

    kind: str
    count: int = 0
    samples: list[str] = field(default_factory=list)


class Diagnostics:
    """Collects per-file problems and reports them as one line per kind.

    Reading 50k files can produce thousands of individual complaints (missing
    EXIF, unreadable video header). Emitting those directly would bury the
    result, so they are counted here and rendered as a summary instead.
    """

    def __init__(self) -> None:
        self._items: dict[str, Diagnostic] = {}

    def add(self, kind: str, detail: str = "") -> None:
        """Record a single occurrence of *kind*, keeping a few examples."""
        item = self._items.setdefault(kind, Diagnostic(kind=kind))
        item.count += 1
        if detail and len(item.samples) < MAX_DIAGNOSTIC_SAMPLES:
            item.samples.append(detail)

    def add_count(self, kind: str, count: int) -> None:
        """Record *count* occurrences of *kind* at once."""
        if count <= 0:
            return
        item = self._items.setdefault(kind, Diagnostic(kind=kind))
        item.count += count

    def reset(self) -> None:
        """Forget everything collected so far."""
        self._items.clear()

    @property
    def items(self) -> list[Diagnostic]:
        """Diagnostics ordered by how many files they affected."""
        return sorted(self._items.values(), key=lambda d: d.count, reverse=True)

    def as_dict(self) -> dict[str, int]:
        """Return a serializable ``kind -> count`` mapping."""
        return {item.kind: item.count for item in self.items}

    def __len__(self) -> int:
        return len(self._items)


# Module-level collector. Utility functions deep in the call stack report here
# instead of taking a reporter argument through every layer.
diagnostics = Diagnostics()


# ---------------------------------------------------------------------------
# Progress plumbing
# ---------------------------------------------------------------------------
class ProgressSink(Protocol):
    """Minimal progress contract used by the processing modules."""

    #: One-line result of the phase, set by the caller when it finishes.
    detail: str

    def start(self, total: int, description: str = "") -> None:
        """Announce the total amount of work."""
        ...

    def advance(self, step: int = 1) -> None:
        """Report that *step* units of work completed."""
        ...

    def update_description(self, text: str) -> None:
        """Update the spinner/status text before the progress bar starts."""
        ...


class NullProgress:
    """Progress sink that discards everything."""

    def __init__(self) -> None:
        self.detail = ""

    def start(self, total: int, description: str = "") -> None:
        """Ignore the announced total."""

    def advance(self, step: int = 1) -> None:
        """Ignore the reported progress."""

    def update_description(self, text: str) -> None:
        """Ignore the description update."""


# ---------------------------------------------------------------------------
# Reporters
# ---------------------------------------------------------------------------
class NullReporter:
    """Reporter that renders nothing.

    Used when :func:`filecluster.file_cluster.main` runs as a library call, so
    importing and calling the pipeline never writes to the terminal.
    """

    @contextmanager
    def phase(self, name: str) -> Iterator[NullProgress]:
        """Run a phase without any output."""
        yield NullProgress()

    def note(self, message: str) -> None:
        """Ignore an informational note."""


class _Phase:
    """Live state of one pipeline phase, and its progress sink."""

    def __init__(self, reporter: RichReporter, name: str) -> None:
        self._reporter = reporter
        self._name = name
        self.detail = ""
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None
        self._pending = 0
        self._batch = 1

    # -- ProgressSink ------------------------------------------------------
    def update_description(self, text: str) -> None:
        """Update the spinner text shown before the progress bar starts.

        Useful for long discovery phases (e.g. scanning a network share) where
        the user needs feedback before the total item count is known.
        """
        if self._reporter._status is not None:
            self._reporter._status.update(f"[bold]{text}[/]…")

    def start(self, total: int, description: str = "") -> None:
        """Replace the spinner with a determinate progress bar.

        Called again within the same phase (for example once per watch folder)
        it grows the existing bar instead of stacking a second one.
        """
        if self._progress is not None and self._task_id is not None:
            known = self._progress.tasks[self._task_id].total or 0
            self._progress.update(self._task_id, total=known + total)
            return

        self._batch = max(1, min(MAX_PROGRESS_BATCH, (total // 100) or 1))

        # A live bar redrawing into a pipe or log file would emit one line per
        # refresh, so progress is shown on real terminals only.
        if not supports_animation(self._reporter.console):
            return

        self._reporter._stop_status()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=24),
            MofNCompleteColumn(),
            TextColumn("[dim]{task.fields[rate]}[/]"),
            TimeRemainingColumn(compact=True),
            console=self._reporter.console,
            transient=True,
            refresh_per_second=4,
        )
        self._progress.start()
        self._task_id = self._progress.add_task(
            description or self._name, total=total, rate=""
        )

    def advance(self, step: int = 1) -> None:
        """Advance the bar, flushing in batches to limit redraws."""
        self._pending += step
        if self._pending >= self._batch:
            self._flush()

    def _flush(self) -> None:
        if self._progress is None or self._task_id is None or not self._pending:
            self._pending = 0
            return
        self._progress.advance(self._task_id, self._pending)
        self._pending = 0
        task = self._progress.tasks[self._task_id]
        if task.speed:
            self._progress.update(
                self._task_id, rate=f"{task.speed:,.0f} files/s".replace(",", " ")
            )

    def close(self) -> None:
        """Tear down the progress bar, if one was started."""
        self._flush()
        if self._progress is not None:
            self._progress.stop()
            self._progress = None
            self._task_id = None


class RichReporter:
    """Renders pipeline progress and results to a terminal."""

    def __init__(self, console: Console, verbose: int = 0) -> None:
        self.console = console
        self.verbose = verbose
        self._status = None

    # -- phases ------------------------------------------------------------
    @contextmanager
    def phase(self, name: str) -> Iterator[_Phase]:
        """Show live progress for *name*, then one summary line.

        Transient progress bars keep the final screen at one line per phase
        rather than a screenful of completed bars.
        """
        phase = _Phase(self, name)
        started = perf_counter()
        self._start_status(name)
        try:
            yield phase
        except Exception:
            phase.close()
            self._stop_status()
            self.console.print(f"  [red]✗[/] {name}")
            raise
        else:
            phase.close()
            self._stop_status()
            elapsed = fmt_duration(perf_counter() - started)
            self.console.print(
                f"  [green]✔[/] [bold]{name:<{_LABEL_WIDTH}}[/]"
                f" {phase.detail:<{_DETAIL_WIDTH}}"
                f" [dim]{elapsed:>8}[/]"
            )

    def _start_status(self, name: str) -> None:
        if not supports_animation(self.console):
            return
        self._status = self.console.status(f"[bold]{name}[/]…", spinner="dots")
        self._status.start()

    def _stop_status(self) -> None:
        if self._status is not None:
            self._status.stop()
            self._status = None

    def note(self, message: str) -> None:
        """Print a dim informational line."""
        self.console.print(f"  [dim]{message}[/]")


Reporter = NullReporter | RichReporter


# ---------------------------------------------------------------------------
# Console setup
# ---------------------------------------------------------------------------
def make_console(*, stderr: bool = False, color: bool | None = None) -> Console:
    """Build a console honouring the usual colour conventions.

    Rich already respects ``NO_COLOR``, ``FORCE_COLOR`` and non-TTY output;
    *color* lets an explicit CLI flag override that.
    """
    return Console(
        stderr=stderr,
        no_color=color is False,
        force_terminal=True if color else None,
        soft_wrap=False,
    )


def configure_logging(verbosity: int = 0, quiet: bool = False) -> None:
    """Point loguru at stderr with a level derived from CLI verbosity.

    Logs are diagnostics, so they go to stderr and stdout stays clean for
    results and ``--json`` output.
    """
    if quiet:
        level = "ERROR"
    elif verbosity >= 2:
        level = "DEBUG"
    elif verbosity == 1:
        level = "INFO"
    else:
        level = "WARNING"

    logger.remove()
    logger.add(
        sys.stderr,
        format="<level>{level: <8}</level> {message}",
        colorize=_color_enabled(),
        level=level,
    )


def _color_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return sys.stderr.isatty()


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------
def banner(console: Console, config, version: str) -> None:
    """Show the resolved configuration before a run starts.

    A large run takes minutes, so the destination and mode are confirmed up
    front rather than discovered afterwards.
    """
    watch = config.watch_folders or []
    if not watch:
        watch_text = "[dim]none[/]"
    elif len(watch) == 1:
        watch_text = str(watch[0])
    else:
        watch_text = f"{watch[0]} [dim](+{len(watch) - 1} more)[/]"

    settings = " · ".join(
        [
            f"[bold]{mode_label(config.mode)}[/]",
            f"gap {int(config.time_granularity.total_seconds() // 60)} min",
            _flag("duplicates", config.skip_duplicated_existing_in_libs),
            _flag("existing-clusters", config.assign_to_clusters_existing_in_libs),
            _flag("restore-names", config.restore_original_names),
        ]
    )

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="dim", width=6)
    grid.add_column(overflow="fold")
    grid.add_row("Inbox", str(config.in_dir_name))
    grid.add_row("Output", str(config.out_dir_name))
    grid.add_row("Watch", watch_text)
    if config.inbox_limit is not None:
        # A truncated run gets its own row, stated before the work starts: the
        # results that follow describe a sample, not the whole inbox.
        grid.add_row(
            "Limit",
            f"[yellow]first {fmt_files(config.inbox_limit)}[/] [dim]in name order[/]",
        )
    grid.add_row("Mode", settings)

    console.print()
    console.print(f"  [bold cyan]filecluster[/] [dim]{version}[/]")
    console.print(_indent(grid))
    console.print()


def results_panel(console: Console, results: dict, config, elapsed: float) -> None:
    """Render the run summary as an aligned table of aggregate counts."""
    plan = results.get("file_operation_plan")
    dry_run = config.mode == CopyMode.NOP

    rows: list[tuple[str, str]] = [
        ("New clusters", fmt_count(len(results.get("new_folder_names") or []))),
    ]
    # Features that are switched off would only ever report zero, so they are
    # left out rather than padding the summary with noise.
    if config.assign_to_clusters_existing_in_libs:
        rows.append(
            (
                "Assigned to existing",
                fmt_count(len(results.get("files_existing_cl") or [])),
            )
        )
    if config.skip_duplicated_existing_in_libs:
        rows.append(("Duplicates", fmt_count(len(results.get("dup_files") or []))))
    if plan is not None:
        if plan.n_moves:
            rows.append(("Files moved", fmt_count(plan.n_moves)))
        if plan.n_copies:
            rows.append(("Files copied", fmt_count(plan.n_copies)))
        if plan.n_renamed:
            label = "Would be renamed" if dry_run else "Renamed on collision"
            rows.append((label, fmt_count(plan.n_renamed)))
        if dry_run and plan.n_skips:
            rows.append(("Files to process", fmt_count(plan.n_skips)))
    rows.append(("Elapsed", fmt_duration(elapsed)))

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="dim", width=_LABEL_WIDTH)
    grid.add_column(justify="right", style="bold")
    for label, value in rows:
        grid.add_row(label, value)

    console.print()
    console.print(f"  [bold]Results[/] [dim]({mode_label(config.mode)})[/]")
    console.print(_indent(grid))

    skipped = int(results.get("n_files_available") or 0) - int(
        results.get("n_files_read") or 0
    )
    if skipped > 0 and config.inbox_limit is not None:
        console.print()
        console.print(
            f"  [yellow]![/] {fmt_files(skipped)} not ingested"
            " [dim](--limit reached)[/]",
            highlight=False,
        )


def largest_clusters(
    console: Console,
    clusters: Sequence[tuple[str, int]],
    limit: int = MAX_CLUSTER_ROWS,
) -> None:
    """List the biggest clusters, collapsing the tail into one line.

    A 50k-file run can create hundreds of clusters; enumerating them all would
    flood the terminal, so only the largest are named.
    """
    if not clusters:
        return

    ordered = sorted(clusters, key=lambda item: item[1], reverse=True)
    shown = ordered if limit <= 0 else ordered[:limit]

    table = Table(
        box=None, pad_edge=False, show_header=True, header_style="dim", padding=(0, 2)
    )
    table.add_column("Cluster", overflow="ellipsis", no_wrap=True, max_width=52)
    table.add_column("Files", justify="right")
    for name, count in shown:
        table.add_row(name, fmt_count(count))

    console.print()
    console.print("  [bold]Largest clusters[/]")
    console.print(_indent(table))

    remaining = ordered[len(shown) :]
    if remaining:
        files = sum(count for _, count in remaining)
        console.print(
            f"    [dim]… and {fmt_count(len(remaining))} more clusters"
            f" ({fmt_files(files)})[/]"
        )


def plan_preview(
    console: Console,
    destinations: Sequence[tuple[str, str, str]],
    out_dir: str,
    limit: int = MAX_TREE_FOLDERS,
) -> None:
    """Show what a dry run would do, as a bounded tree.

    Args:
        console: Target console.
        destinations: ``(target_folder, source_name, destination_name)`` triples.
        out_dir: Output directory root, used as the tree label.
        limit: Maximum folders to show; ``0`` or less shows all of them.
    """
    if not destinations:
        return

    grouped: dict[str, list[tuple[str, str]]] = {}
    for target, src, dst in destinations:
        grouped.setdefault(target, []).append((src, dst))

    ordered = sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)
    shown = ordered if limit <= 0 else ordered[:limit]

    tree = Tree(f"[bold]{out_dir}[/]")
    for target, entries in shown:
        node = tree.add(
            f"[cyan]{_relative_to(target, out_dir)}[/]"
            f" [dim]{fmt_files(len(entries))}[/]"
        )
        for src, dst in entries[:MAX_TREE_SAMPLES]:
            label = src if src == dst else f"{src} [yellow]→ {dst}[/]"
            node.add(f"[dim]{label}[/]")
        hidden = len(entries) - MAX_TREE_SAMPLES
        if hidden > 0:
            node.add(f"[dim]… {fmt_count(hidden)} more[/]")

    console.print()
    console.print("  [bold]Planned layout[/] [dim](nothing written)[/]")
    console.print(_indent(tree))

    remaining = ordered[len(shown) :]
    if remaining:
        files = sum(len(entries) for _, entries in remaining)
        console.print(
            f"    [dim]… and {fmt_count(len(remaining))} more folders"
            f" ({fmt_files(files)})[/]"
        )


def _relative_to(path: str, root: str) -> str:
    """Shorten *path* for display by dropping the *root* prefix."""
    try:
        return str(Path(path).relative_to(root))
    except ValueError:
        return path


def render_diagnostics(
    console: Console, collector: Diagnostics | None = None, verbose: int = 0
) -> None:
    """Print one aggregated line per problem kind."""
    collector = collector if collector is not None else diagnostics
    items = collector.items
    if not items:
        return

    console.print()
    for item in items:
        console.print(
            f"  [yellow]![/] {fmt_files(item.count)}: {item.kind}",
            highlight=False,
        )
        if verbose and item.samples:
            for sample in item.samples:
                console.print(f"      [dim]{sample}[/]", highlight=False)
            hidden = item.count - len(item.samples)
            if hidden > 0:
                console.print(f"      [dim]… {fmt_count(hidden)} more[/]")
    if not verbose:
        console.print("  [dim]Re-run with -v to list affected files.[/]")


def error_panel(console: Console, message: str, hint: str = "") -> None:
    """Report a fatal problem without a traceback."""
    body = Text.from_markup(f"[bold red]Error[/] {message}")
    if hint:
        body.append("\n")
        body.append(hint, style="dim")
    console.print()
    console.print(_indent(body))
    console.print()


def json_summary(results: dict, config, elapsed: float) -> dict:
    """Build a machine-readable summary of a run.

    Aggregates only: the per-file detail belongs in ``--report``, so this stays
    the same size for 50 files and for 50,000.
    """
    plan = results.get("file_operation_plan")
    summary: dict[str, object] = {
        "version": get_version(),
        "mode": mode_label(config.mode),
        "inbox": str(config.in_dir_name),
        "output": str(config.out_dir_name),
        "watch": [str(w) for w in (config.watch_folders or [])],
        "aborted": bool(results.get("aborted")),
        "limit": config.inbox_limit,
        "files_read": int(results.get("n_files_read") or 0),
        "files_available": int(results.get("n_files_available") or 0),
        "new_clusters": len(results.get("new_folder_names") or []),
        "assigned_to_existing": len(results.get("files_existing_cl") or []),
        "duplicates": len(results.get("dup_files") or []),
        "elapsed_seconds": round(elapsed, 3),
        "diagnostics": diagnostics.as_dict(),
        "clusters": [
            {"name": name, "files": count}
            for name, count in (results.get("cluster_sizes") or [])
        ],
    }
    if plan is not None:
        summary.update(
            {
                "moved": plan.n_moves,
                "copied": plan.n_copies,
                "renamed": plan.n_renamed,
                "untouched": plan.n_skips,
                "folders": plan.n_mkdirs,
            }
        )
    return summary


def write_report(path: Path | str, plan, config) -> int:
    """Write the full per-file operation list to *path* as CSV.

    This is the escape hatch for per-file detail: tens of thousands of rows go
    to a file instead of the terminal.

    Returns:
        Number of data rows written.
    """
    rows = plan.destinations if plan is not None else []
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["operation", "source", "destination_folder", "destination"])
        action = mode_label(config.mode).lower().replace(" ", "-")
        for folder, src, dst in rows:
            writer.writerow([action, src, folder, dst])
    return len(rows)


def confirm_plan(console: Console, plan, config) -> bool:
    """Ask before touching files, and assume yes when not interactive.

    Non-interactive use (cron, pipelines, ``run_clustering.sh``) proceeds
    without prompting so existing automation keeps working.
    """
    pending = plan.n_moves + plan.n_copies
    verb = "move" if config.mode == CopyMode.MOVE else "copy"
    summary = (
        f"{verb.capitalize()} {fmt_count(pending)} files into"
        f" {fmt_count(plan.n_mkdirs)} folders under {config.out_dir_name}"
    )
    console.print()
    console.print(f"  [bold]{summary}[/]")
    if plan.n_renamed:
        console.print(
            f"  [yellow]{fmt_count(plan.n_renamed)} files will be renamed[/]"
            " [dim]to avoid overwriting existing files[/]"
        )
    if not supports_animation(console) or not sys.stdin.isatty():
        return True
    return Confirm.ask("  Proceed?", console=console, default=True)
