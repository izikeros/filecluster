"""Stage 1: metadata and filename rules.

The rules produce *signals*, not verdicts. Only an unambiguous screenshot can
end the cascade here, and only ever with a reject; every other combination of
signals is handed to the later stages, because "no EXIF" and "is a PNG" are
properties shared by plenty of photographs worth keeping.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from time import perf_counter

from filecluster import logger
from filecluster.curation import reasons
from filecluster.curation.types import (
    CurationContext,
    CurationDecision,
    MediaItem,
    MediaKind,
    StageResult,
)

STAGE_NAME = "metadata"

#: Prior probability that a file with photographic camera EXIF is a personal
#: photograph. Deliberately below the keep threshold on its own: a camera tag
#: plus decent technical quality keeps a file, a camera tag alone does not.
CAMERA_EXIF_PERSONAL_PRIOR = 0.78

#: Prior for a file whose metadata says nothing either way.
NEUTRAL_PERSONAL_PRIOR = 0.45

#: Prior once at least one strong utility signal fired.
UTILITY_PERSONAL_PRIOR = 0.20

#: Aspect ratio beyond which an image looks like a stitched panorama or a
#: scrolling screen capture rather than a single camera frame.
EXTREME_ASPECT_RATIO = 3.0

#: Tolerance in pixels when comparing against a known screen size. Status bars
#: and window chrome shift a capture by a few pixels.
RESOLUTION_TOLERANCE_PX = 4

_SCREENSHOT_NAME_RE = re.compile(
    r"(screen[\s_-]?shot|screenshot|zrzut[\s_-]?ekranu|bildschirmfoto"
    r"|captura[\s_-]?de[\s_-]?pantalla|capture[\s_-]?d.?ecran"
    r"|snimek[\s_-]?obrazovky|screen[\s_-]?capture)",
    re.IGNORECASE,
)

_SCREENSHOT_SOFTWARE_RE = re.compile(
    r"(screenshot|screen\s?capture|snipping\s?tool|greenshot|lightshot"
    r"|flameshot|sharex|snagit|screencapture)",
    re.IGNORECASE,
)

#: Container formats a camera does not write. On their own they mean nothing;
#: combined with the absence of camera EXIF they are one strong signal.
_APP_FORMATS: frozenset[str] = frozenset({".png", ".bmp", ".gif", ".webp"})

_DATA_PACKAGE = "filecluster.curation.data"
_RESOLUTIONS_FILE = "screen_resolutions.json"


@dataclass(frozen=True)
class ScreenResolutions:
    """Versioned list of known screen sizes, orientation-insensitive."""

    version: int
    sizes: frozenset[tuple[int, int]]

    def matches(self, width: int, height: int, tolerance: int = 0) -> bool:
        """Whether ``width x height`` looks like a full screen capture."""
        probe = (min(width, height), max(width, height))
        if probe in self.sizes:
            return True
        if tolerance <= 0:
            return False
        return any(
            abs(probe[0] - w) <= tolerance and abs(probe[1] - h) <= tolerance
            for w, h in self.sizes
        )


@lru_cache(maxsize=4)
def load_screen_resolutions(path: Path | None = None) -> ScreenResolutions:
    """Load the screen-size table from the packaged data file or *path*.

    A missing or unreadable table degrades to an empty one: losing this signal
    only makes the pipeline more cautious.
    """
    try:
        if path is None:
            text = (
                resources.files(_DATA_PACKAGE)
                .joinpath(_RESOLUTIONS_FILE)
                .read_text(encoding="utf-8")
            )
        else:
            text = Path(path).read_text(encoding="utf-8")
        data = json.loads(text)
        sizes = {
            (min(int(w), int(h)), max(int(w), int(h)))
            for w, h in data.get("resolutions", [])
        }
        return ScreenResolutions(
            version=int(data.get("version", 0)), sizes=frozenset(sizes)
        )
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f"Could not load screen resolution table: {exc}")
        return ScreenResolutions(version=0, sizes=frozenset())


@dataclass(frozen=True)
class MetadataFacts:
    """What could be read about a file without decoding all of its pixels."""

    width: int | None = None
    height: int | None = None
    has_camera_exif: bool = False
    has_any_exif: bool = False
    software: str | None = None
    image_format: str | None = None
    unreadable: bool = False

    @property
    def aspect_ratio(self) -> float | None:
        """Long side over short side, or *None* when the size is unknown."""
        if not self.width or not self.height:
            return None
        return max(self.width, self.height) / min(self.width, self.height)


def read_metadata_facts(path: Path) -> MetadataFacts:
    """Read size and EXIF headers of *path* without decoding the image.

    Pillow parses the header lazily, so this stays cheap even for a 100 MP
    file. An unreadable header is reported rather than raised: the pixel stage
    is the one that decides what an undecodable file means.
    """
    from PIL import Image

    try:
        with Image.open(path) as im:
            width, height = im.size
            image_format = im.format
            exif = im.getexif()
    except Exception as exc:  # Pillow raises a wide family of errors here
        logger.debug(f"No readable image header for {path}: {exc}")
        return MetadataFacts(unreadable=True)

    make = _exif_text(exif, 271)
    model = _exif_text(exif, 272)
    software = _exif_text(exif, 305)
    date_original = _exif_text(exif, 36867) or _exif_text(exif, 306)

    return MetadataFacts(
        width=width,
        height=height,
        has_camera_exif=bool((make or model) and date_original),
        has_any_exif=bool(len(exif)),
        software=software,
        image_format=image_format,
    )


def _exif_text(exif: object, tag: int) -> str | None:
    """Return an EXIF string tag, tolerating bytes and missing values."""
    try:
        value = exif.get(tag)  # ty: ignore[unresolved-attribute]
    except Exception:  # pragma: no cover - defensive, exif is a mapping
        return None
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    return text or None


class MetadataRuleStage:
    """Cheap, deterministic first pass over names, sizes and EXIF."""

    name = STAGE_NAME

    def __init__(self, resolutions: ScreenResolutions | None = None) -> None:
        self._resolutions = resolutions or load_screen_resolutions()

    def analyze(self, item: MediaItem, context: CurationContext) -> StageResult:
        """Collect metadata signals for *item*."""
        started = perf_counter()

        if item.media_type is not MediaKind.IMAGE:
            return StageResult(
                stage=self.name,
                reasons=(reasons.UNSUPPORTED_MEDIA_TYPE,),
                terminal_decision=CurationDecision.REVIEW,
                confidence=1.0,
                duration_ms=(perf_counter() - started) * 1000,
            )

        facts = read_metadata_facts(item.path)
        context.values["metadata_facts"] = facts

        signals = self._collect_signals(item, facts)
        strong = [s for s in signals if s in reasons.STRONG_UTILITY_SIGNALS]
        decisive = [s for s in signals if s in reasons.DECISIVE_UTILITY_SIGNALS]

        if facts.has_camera_exif:
            signals.append(reasons.CAMERA_EXIF_PRESENT)
        elif not facts.unreadable:
            signals.append(reasons.NO_CAMERA_EXIF)

        personal_prior, utility_evidence = self._priors(facts, strong)
        scores = {
            "rules.personal_prior": personal_prior,
            "rules.utility_evidence": utility_evidence,
            "rules.strong_signals": float(len(strong)),
        }

        # An operating-system screenshot marker, or two independent strong
        # signals agreeing, is the only reject this stage may hand down.
        terminal = None
        confidence = None
        if decisive or len(strong) >= 2:
            terminal = CurationDecision.REJECT
            confidence = 0.95 if decisive else 0.85

        return StageResult(
            stage=self.name,
            scores=scores,
            labels=("screenshot",) if terminal else (),
            reasons=tuple(signals),
            terminal_decision=terminal,
            confidence=confidence,
            model_id=f"rules-v1+screens-{self._resolutions.version}",
            duration_ms=(perf_counter() - started) * 1000,
        )

    def _collect_signals(self, item: MediaItem, facts: MetadataFacts) -> list[str]:
        signals: list[str] = []
        if _SCREENSHOT_NAME_RE.search(Path(item.relative_path).name):
            signals.append(reasons.SCREENSHOT_FILENAME)
        if facts.software and _SCREENSHOT_SOFTWARE_RE.search(facts.software):
            signals.append(reasons.SCREENSHOT_SOFTWARE)
        if (
            facts.width
            and facts.height
            and self._resolutions.matches(
                facts.width, facts.height, RESOLUTION_TOLERANCE_PX
            )
            and not facts.has_camera_exif
        ):
            signals.append(reasons.SCREEN_RESOLUTION_MATCH)
        if item.extension in _APP_FORMATS:
            signals.append(reasons.APP_GENERATED_FORMAT)
            if not facts.has_camera_exif:
                signals.append(reasons.PNG_WITHOUT_CAMERA_EXIF)
        ratio = facts.aspect_ratio
        if ratio is not None and ratio >= EXTREME_ASPECT_RATIO:
            signals.append(reasons.EXTREME_ASPECT_RATIO)
        return signals

    @staticmethod
    def _priors(facts: MetadataFacts, strong: list[str]) -> tuple[float, float]:
        """Turn the collected signals into two coarse priors.

        These are explicit heuristics, not calibrated probabilities. They exist
        so the pipeline is useful before a semantic model is installed, and the
        semantic stage overwrites them with real signals once it runs.
        """
        if strong:
            utility = min(0.55 + 0.15 * len(strong), 0.95)
            return UTILITY_PERSONAL_PRIOR, utility
        if facts.has_camera_exif:
            return CAMERA_EXIF_PERSONAL_PRIOR, 0.10
        return NEUTRAL_PERSONAL_PRIOR, 0.25
