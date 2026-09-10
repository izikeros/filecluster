"""Settings for the curation cascade.

Everything that can change a verdict lives here, because the cache key is
derived from these values: a threshold tweak has to invalidate stored results
just as reliably as a model swap does.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from filecluster.curation.exceptions import CurationConfigError

#: Bumped whenever a stage changes how it computes a signal. Cached verdicts
#: from an older pipeline are ignored rather than migrated.
PIPELINE_VERSION = "curation-1"

#: Default database file, created inside the inbox next to the media.
CACHE_FILENAME = ".filecluster-curation.db"

#: Extensions the pixel stages can open. RAW files are decoded by Pillow only
#: for a few vendors, so they are treated as images and allowed to fail into
#: review rather than being excluded from discovery.
IMAGE_EXTENSIONS: tuple[str, ...] = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".tif",
    ".tiff",
    ".heic",
    ".heif",
    ".dng",
    ".cr2",
    ".nef",
    ".arw",
)

VIDEO_EXTENSIONS: tuple[str, ...] = (
    ".mp4",
    ".mov",
    ".3gp",
    ".avi",
    ".mkv",
    ".m4v",
)


class Weights(BaseModel):
    """Linear weights of the fusion step.

    These are a starting point for calibration on a private evaluation set,
    not tuned constants.
    """

    personal: float = Field(default=0.50, ge=0.0, le=1.0)
    technical_quality: float = Field(default=0.15, ge=0.0, le=1.0)
    aesthetic: float = Field(default=0.10, ge=0.0, le=1.0)
    preference: float = Field(default=0.25, ge=0.0, le=1.0)
    utility_penalty: float = Field(default=0.65, ge=0.0, le=1.0)


class Thresholds(BaseModel):
    """Decision boundaries applied to the fused score."""

    keep: float = Field(default=0.70, ge=0.0, le=1.0)
    reject: float = Field(default=0.30, ge=0.0, le=1.0)
    minimum_confidence: float = Field(default=0.75, ge=0.0, le=1.0)
    conflict_margin: float = Field(default=0.12, ge=0.0, le=1.0)
    #: Confidence band that qualifies a file for VLM escalation.
    vlm_band: tuple[float, float] = (0.35, 0.75)

    @model_validator(mode="after")
    def _ordered(self) -> Thresholds:
        if self.reject >= self.keep:
            raise CurationConfigError(
                f"reject threshold ({self.reject}) must be below "
                f"keep threshold ({self.keep})"
            )
        low, high = self.vlm_band
        if not 0.0 <= low < high <= 1.0:
            raise CurationConfigError(
                f"vlm_band must be an ordered pair, got {(low, high)}"
            )
        return self


class CurationSettings(BaseSettings):
    """Configuration of one curation run."""

    model_config = SettingsConfigDict(
        env_prefix="FILECLUSTER_CURATION_",
        env_file=".env",
        extra="ignore",
    )

    cache_path: Path | None = None
    config_path: Path | None = None
    prompt_bank_path: Path | None = None

    max_image_side: int = Field(default=1024, ge=64, le=8192)
    #: Refuse to decode above this pixel count; a decompression bomb must not
    #: be able to exhaust memory on a batch run.
    max_pixels: int = Field(default=80_000_000, ge=1_000_000)
    batch_size: int = Field(default=8, ge=1)
    device: Literal["auto", "cpu", "mps", "cuda"] = "auto"

    enable_ocr: bool = False
    enable_semantic: bool = False
    enable_quality: bool = False
    enable_preference: bool = False
    enable_vlm: bool = False
    allow_remote_vlm: bool = False
    #: Set only for a hosted VLM. Sending pixels off the machine additionally
    #: requires ``allow_remote_vlm``, which the CLI exposes as its own flag.
    vlm_endpoint: str | None = None

    weights: Weights = Field(default_factory=Weights)
    thresholds: Thresholds = Field(default_factory=Thresholds)

    image_extensions: tuple[str, ...] = IMAGE_EXTENSIONS
    video_extensions: tuple[str, ...] = VIDEO_EXTENSIONS

    @model_validator(mode="after")
    def _check_combinations(self) -> CurationSettings:
        if self.vlm_endpoint and not self.allow_remote_vlm:
            raise CurationConfigError(
                "A remote VLM endpoint is configured but allow_remote_vlm is off; "
                "sending images to a remote service has to be opted into"
            )
        return self

    # -- convenience -------------------------------------------------------
    @property
    def keep_threshold(self) -> float:
        """Score at or above which a file is kept."""
        return self.thresholds.keep

    @property
    def reject_threshold(self) -> float:
        """Score below which a file may be rejected."""
        return self.thresholds.reject

    @property
    def minimum_confidence(self) -> float:
        """Confidence a terminal decision has to clear."""
        return self.thresholds.minimum_confidence

    def cache_path_for(self, inbox: Path) -> Path:
        """Resolve the cache database path for *inbox*."""
        return self.cache_path or Path(inbox) / CACHE_FILENAME

    def is_image_extension(self, extension: str) -> bool:
        """Whether *extension* (with dot, any case) is a known image type."""
        return extension.lower() in self.image_extensions

    def is_video_extension(self, extension: str) -> bool:
        """Whether *extension* (with dot, any case) is a known video type."""
        return extension.lower() in self.video_extensions

    def fingerprint(self) -> str:
        """Hash of every setting that can change a verdict.

        Paths are excluded on purpose: moving the cache file or the inbox does
        not change what a stage would decide, but a threshold does.
        """
        payload = {
            "pipeline_version": PIPELINE_VERSION,
            "max_image_side": self.max_image_side,
            "max_pixels": self.max_pixels,
            "enable_ocr": self.enable_ocr,
            "enable_semantic": self.enable_semantic,
            "enable_quality": self.enable_quality,
            "enable_preference": self.enable_preference,
            "enable_vlm": self.enable_vlm,
            "vlm_endpoint": self.vlm_endpoint,
            "weights": self.weights.model_dump(),
            "thresholds": {
                **self.thresholds.model_dump(),
                "vlm_band": list(self.thresholds.vlm_band),
            },
            "image_extensions": sorted(self.image_extensions),
            "video_extensions": sorted(self.video_extensions),
            "config_file": _file_digest(self.config_path),
            "prompt_bank": _file_digest(self.prompt_bank_path),
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _file_digest(path: Path | None) -> str | None:
    """Return a short content digest of *path*, or *None* when unavailable."""
    if path is None:
        return None
    try:
        data = Path(path).read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()[:32]


def load_settings(
    config_path: Path | None = None,
    **overrides: Any,
) -> CurationSettings:
    """Build settings from an optional config file plus explicit overrides.

    JSON is always accepted; YAML only when PyYAML happens to be installed, so
    the base install stays dependency-free.

    Raises:
        CurationConfigError: when the file cannot be parsed or the resulting
            settings are invalid.
    """
    data: dict[str, Any] = {}
    if config_path is not None:
        data = _read_config_file(Path(config_path))
        data["config_path"] = Path(config_path)

    clean = {k: v for k, v in overrides.items() if v is not None}
    data.update(clean)
    try:
        return CurationSettings(**data)
    except CurationConfigError:
        raise
    except Exception as exc:  # pydantic ValidationError and friends
        raise CurationConfigError(str(exc)) from exc


def _read_config_file(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CurationConfigError(f"Cannot read config file {path}: {exc}") from exc

    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:
            raise CurationConfigError(
                f"{path} is YAML but PyYAML is not installed; use JSON instead"
            ) from exc
        loaded = yaml.safe_load(text) or {}
    else:
        try:
            loaded = json.loads(text or "{}")
        except json.JSONDecodeError as exc:
            raise CurationConfigError(f"Invalid JSON in {path}: {exc}") from exc

    if not isinstance(loaded, dict):
        raise CurationConfigError(f"{path} must contain a mapping at the top level")
    return dict(loaded)
