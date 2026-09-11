"""Cascade orchestration: discovery, fingerprinting, staging and fusion.

The pipeline owns everything the stages are not allowed to own: stage order,
early exit, the cache, per-file error containment and the aggregate counters.
Stages stay pure measurements, and this module decides what their measurements
mean for one file.

Nothing here touches the destination filesystem. Planning and executing file
operations happens in :mod:`filecluster.curation.operations`, after every verdict
is known.
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter

from filecluster import logger
from filecluster.curation import reasons
from filecluster.curation.catalog import CacheKey, CurationCatalog
from filecluster.curation.configuration import PIPELINE_VERSION, CurationSettings
from filecluster.curation.hashing import sha256_file
from filecluster.curation.image_features import ImageFeatureStage
from filecluster.curation.provider_stages import (
    OcrStage,
    QualityStage,
    SemanticStage,
    VlmStage,
)
from filecluster.curation.providers.base import Providers, model_fingerprint
from filecluster.curation.rules import MetadataRuleStage
from filecluster.curation.scoring import fuse, needs_vlm_escalation
from filecluster.curation.types import (
    CurationContext,
    CurationDecision,
    CurationResult,
    CurationStage,
    MediaItem,
    MediaKind,
    StageResult,
)
from filecluster.ui import NullProgress, ProgressSink
from filecluster.utils import SKIP_DIR_NAMES

#: Files the pipeline creates itself, never candidates for curation.
_OWN_FILES: tuple[str, ...] = (".filecluster-curation.db", ".filecluster.db")


@dataclass(frozen=True)
class DiscoveredFile:
    """A candidate file, before its content has been hashed."""

    path: Path
    relative_path: str
    size: int
    mtime: float
    extension: str
    media_type: MediaKind


@dataclass
class CurationRun:
    """Aggregate outcome of one curation pass over an inbox."""

    inbox: Path
    settings: CurationSettings
    cache_key: CacheKey
    results: list[CurationResult] = field(default_factory=list)
    n_discovered: int = 0
    n_skipped: int = 0
    elapsed_seconds: float = 0.0
    executed: bool = False
    unavailable_stages: tuple[str, ...] = ()

    @property
    def n_cache_hits(self) -> int:
        """How many verdicts came from the cache instead of the stages."""
        return sum(1 for r in self.results if r.cache_hit)

    @property
    def n_errors(self) -> int:
        """Files where at least one stage failed."""
        return sum(1 for r in self.results if any(s.failed for s in r.stage_trace))

    def decision_counts(self) -> dict[str, int]:
        """Return the number of files per decision, always all three keys."""
        counts = dict.fromkeys((d.value for d in CurationDecision), 0)
        for result in self.results:
            counts[result.decision.value] += 1
        return counts

    def reason_counts(self) -> Counter[str]:
        """Return how often each reason code fired across the run."""
        counter: Counter[str] = Counter()
        for result in self.results:
            counter.update(result.reasons)
        return counter

    def results_for(self, decision: CurationDecision) -> list[CurationResult]:
        """Return the results carrying *decision*."""
        return [r for r in self.results if r.decision is decision]


def discover_media(
    inbox: str | Path,
    settings: CurationSettings,
    limit: int | None = None,
) -> list[DiscoveredFile]:
    """Find candidate files under *inbox*, sorted by relative path.

    Sorting before applying *limit* is what makes a truncated run reproducible.
    Files that resolve outside the inbox (a symlink pointing away, for example)
    are dropped: nothing outside the inbox may be planned for a move.
    """
    root = Path(inbox)
    if not root.is_dir():
        raise NotADirectoryError(f"Inbox is not a directory: {root}")
    resolved_root = root.resolve()

    found: list[DiscoveredFile] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d for d in dirnames if d not in SKIP_DIR_NAMES and not d.startswith(".")
        ]
        here = Path(dirpath)
        for name in sorted(filenames):
            candidate = _describe(here / name, root, resolved_root, settings)
            if candidate is not None:
                found.append(candidate)

    found.sort(key=lambda f: f.relative_path)
    return found if limit is None else found[:limit]


def _describe(
    path: Path,
    root: Path,
    resolved_root: Path,
    settings: CurationSettings,
) -> DiscoveredFile | None:
    name = path.name
    if name.startswith(".") or name.startswith(_OWN_FILES):
        return None
    extension = path.suffix.lower()
    if settings.is_image_extension(extension):
        media_type = MediaKind.IMAGE
    elif settings.is_video_extension(extension):
        media_type = MediaKind.VIDEO
    else:
        return None
    try:
        if not path.resolve().is_relative_to(resolved_root):
            logger.warning(f"Ignoring {path}: resolves outside the inbox")
            return None
        stat = path.stat()
    except OSError as exc:
        logger.debug(f"Ignoring {path}: {exc}")
        return None

    return DiscoveredFile(
        path=path,
        relative_path=str(path.relative_to(root)),
        size=stat.st_size,
        mtime=stat.st_mtime,
        extension=extension,
        media_type=media_type,
    )


def build_stages(
    settings: CurationSettings,
    providers: Providers | None = None,
) -> list[CurationStage]:
    """Assemble the cascade in cost order, cheapest signal first.

    A stage that is enabled but has no provider is left out rather than faked,
    so a missing optional dependency degrades to fewer signals instead of wrong
    ones.
    """
    providers = providers or Providers()
    stages: list[CurationStage] = [
        MetadataRuleStage(),
        ImageFeatureStage(settings),
    ]
    if settings.enable_ocr and providers.ocr is not None:
        stages.append(OcrStage(providers.ocr))
    if settings.enable_semantic and providers.semantic is not None:
        stages.append(SemanticStage(providers.semantic, settings))
    if settings.enable_quality and providers.quality is not None:
        stages.append(QualityStage(providers.quality))
    return stages


def unavailable_stages(
    settings: CurationSettings, providers: Providers
) -> tuple[str, ...]:
    """Return enabled model-backed stages that cannot run in this pipeline."""
    requested = (
        ("ocr", settings.enable_ocr, providers.ocr),
        ("semantic", settings.enable_semantic, providers.semantic),
        ("quality", settings.enable_quality, providers.quality),
        ("vlm", settings.enable_vlm, providers.vlm),
    )
    return tuple(
        name for name, enabled, provider in requested if enabled and provider is None
    )


class CurationPipeline:
    """Runs the cascade over an inbox and returns one verdict per file."""

    def __init__(
        self,
        settings: CurationSettings,
        providers: Providers | None = None,
        catalog: CurationCatalog | None = None,
        stages: Sequence[CurationStage] | None = None,
    ) -> None:
        self.settings = settings
        self.providers = providers or Providers()
        self.catalog = catalog
        self.stages = (
            list(stages)
            if stages is not None
            else build_stages(settings, self.providers)
        )
        self._vlm_stage = (
            VlmStage(self.providers.vlm)
            if settings.enable_vlm and self.providers.vlm is not None
            else None
        )
        self.unavailable_stages = unavailable_stages(settings, self.providers)
        self.cache_key = CacheKey(
            pipeline_version=PIPELINE_VERSION,
            config_fingerprint=settings.fingerprint(),
            model_fingerprint=model_fingerprint(self.providers),
        )

    # -- public API --------------------------------------------------------
    def run(
        self,
        inbox: str | Path,
        limit: int | None = None,
        progress: ProgressSink | None = None,
        force_recompute: bool = False,
    ) -> CurationRun:
        """Analyse every candidate file under *inbox*."""
        started = perf_counter()
        progress = progress or NullProgress()
        discovered = discover_media(inbox, self.settings, limit)

        run = CurationRun(
            inbox=Path(inbox),
            settings=self.settings,
            cache_key=self.cache_key,
            n_discovered=len(discovered),
            unavailable_stages=self.unavailable_stages,
        )
        progress.start(len(discovered), "Curating")
        for candidate in discovered:
            item = self._fingerprint(candidate)
            if item is None:
                run.n_skipped += 1
                progress.advance()
                continue
            run.results.append(self.analyze(item, force_recompute=force_recompute))
            progress.advance()

        run.elapsed_seconds = perf_counter() - started
        return run

    def analyze(self, item: MediaItem, force_recompute: bool = False) -> CurationResult:
        """Return the verdict for one already-fingerprinted file."""
        if self.catalog is not None and not force_recompute:
            cached = self.catalog.get_analysis(item, self.cache_key)
            if cached is not None:
                return cached

        try:
            result = self._run_stages(item)
        except Exception as exc:  # one bad file may not end the run
            logger.warning(f"Curation failed for {item.relative_path}: {exc}")
            result = self._fallback(item, reasons.STAGE_ERROR)

        if self.catalog is not None:
            self.catalog.put_analysis(result, self.cache_key)
        return result

    # -- internals ---------------------------------------------------------
    def _fingerprint(self, candidate: DiscoveredFile) -> MediaItem | None:
        """Attach a content hash, reusing the cached one when nothing changed."""
        digest: str | None = None
        if self.catalog is not None:
            digest = self.catalog.lookup_sha256(
                candidate.relative_path, candidate.size, candidate.mtime
            )
        if digest is None:
            try:
                digest = sha256_file(candidate.path)
            except OSError as exc:
                logger.warning(f"Skipping {candidate.relative_path}: {exc}")
                return None

        item = MediaItem(
            path=candidate.path,
            relative_path=candidate.relative_path,
            size=candidate.size,
            mtime=candidate.mtime,
            sha256=digest,
            media_type=candidate.media_type,
            extension=candidate.extension,
        )
        if self.catalog is not None:
            self.catalog.put_file(item)
        return item

    def _run_stages(self, item: MediaItem) -> CurationResult:
        trace: list[StageResult] = []
        scores: dict[str, float] = {}
        labels: tuple[str, ...] = ()
        notes: list[str] = []

        with CurationContext(self.settings) as context:
            for stage in self.stages:
                outcome = stage.analyze(item, context)
                trace.append(outcome)
                scores.update(outcome.scores)
                labels = outcome.labels or labels
                notes.extend(outcome.reasons)

                if self._is_terminal(outcome):
                    return self._result(
                        item,
                        outcome.terminal_decision,  # ty: ignore[invalid-argument-type]
                        float(outcome.confidence or 1.0),
                        scores,
                        labels,
                        notes,
                        trace,
                    )

            failed = any(stage.failed for stage in trace)
            verdict = fuse(scores, labels, notes, self.settings, stage_failed=failed)

            if self._vlm_stage is not None and needs_vlm_escalation(
                verdict, self.settings
            ):
                outcome = self._vlm_stage.analyze(item, context)
                trace.append(outcome)
                scores.update(outcome.scores)
                labels = outcome.labels or labels
                notes.extend(outcome.reasons)
                decision = outcome.terminal_decision
                if (
                    not outcome.failed
                    and decision is not None
                    and self._is_terminal(outcome)
                ):
                    return self._result(
                        item,
                        decision,
                        float(outcome.confidence or 1.0),
                        scores,
                        labels,
                        notes,
                        trace,
                    )

        return self._result(
            item,
            verdict.decision,
            verdict.confidence,
            scores,
            labels,
            [*notes, *verdict.reasons],
            trace,
        )

    def _is_terminal(self, outcome: StageResult) -> bool:
        """Whether a stage may end the cascade with the decision it proposes.

        The confidence floor lives in configuration rather than inside a stage,
        so an early exit is something the user can see and turn off.
        """
        if outcome.terminal_decision is None:
            return False
        if outcome.terminal_decision is CurationDecision.REVIEW:
            return True
        return (outcome.confidence or 0.0) >= self.settings.minimum_confidence

    def _result(
        self,
        item: MediaItem,
        decision: CurationDecision,
        confidence: float,
        scores: dict[str, float],
        labels: tuple[str, ...],
        notes: Iterable[str],
        trace: list[StageResult],
    ) -> CurationResult:
        from filecluster.curation.scoring import resolve_signals

        signals = resolve_signals(scores)
        merged: dict[str, float | None] = dict(scores)
        merged.update(signals.as_scores())
        return CurationResult(
            item=item,
            decision=decision,
            confidence=round(float(confidence), 4),
            scores=merged,
            labels=labels,
            reasons=tuple(dict.fromkeys(notes)),
            stage_trace=tuple(trace),
            pipeline_version=PIPELINE_VERSION,
        )

    def _fallback(self, item: MediaItem, reason: str) -> CurationResult:
        """Verdict used when the cascade itself raised: always review."""
        return CurationResult(
            item=item,
            decision=CurationDecision.REVIEW,
            confidence=0.0,
            scores={},
            labels=(),
            reasons=(reason,),
            stage_trace=(
                StageResult(stage="pipeline", reasons=(reason,), failed=True),
            ),
            pipeline_version=PIPELINE_VERSION,
        )


def curate(
    inbox: str | Path,
    settings: CurationSettings | None = None,
    providers: Providers | None = None,
    limit: int | None = None,
    progress: ProgressSink | None = None,
    force_recompute: bool = False,
    use_cache: bool = True,
) -> CurationRun:
    """Analyse an inbox with a cache opened for the duration of the run.

    This is the library entry point: it renders nothing and writes no media
    files, so a caller gets verdicts and decides what to do with them.
    """
    settings = settings or CurationSettings()
    catalog = None
    try:
        if use_cache:
            catalog = CurationCatalog.open(settings.cache_path_for(Path(inbox)))
        pipeline = CurationPipeline(settings, providers, catalog)
        return pipeline.run(
            inbox, limit=limit, progress=progress, force_recompute=force_recompute
        )
    finally:
        if catalog is not None:
            catalog.close()


def iter_results(run: CurationRun) -> Iterator[CurationResult]:
    """Iterate the results of *run* in discovery order."""
    yield from run.results
