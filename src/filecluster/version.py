"""Version information about package."""

from importlib.metadata import PackageNotFoundError, version

# bump-my-version updates this fallback; importlib.metadata reads the
# installed package metadata when available.
_FALLBACK_VERSION = "0.5.2"

try:
    __version__ = version("filecluster")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = _FALLBACK_VERSION


def get_version() -> str:
    """Return the installed package version."""
    return __version__
