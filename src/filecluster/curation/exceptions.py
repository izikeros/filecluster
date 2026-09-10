"""Exceptions raised by the curation subpackage."""

from __future__ import annotations


class CurationError(Exception):
    """Base class for every curation failure."""


class CurationConfigError(CurationError):
    """Settings are self-contradictory or out of range."""


class UnsafeRelativePathError(CurationError):
    """A discovered file resolved outside the inbox it was discovered in."""


class MissingDependencyError(CurationError):
    """An optional extra is required for the requested stage.

    Carries the install command so the CLI can print a one-line remedy instead
    of an ``ImportError`` traceback.
    """

    def __init__(self, feature: str, extra: str) -> None:
        self.feature = feature
        self.extra = extra
        super().__init__(
            f"{feature} needs extra dependencies. "
            f'Install them with: pip install "filecluster[{extra}]"'
        )


class ProviderUnavailableError(CurationError):
    """A model provider could not be loaded or answered with an error.

    The pipeline turns this into a ``review`` verdict for the affected file:
    a broken model must never be able to reject a photograph.
    """
