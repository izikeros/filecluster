"""Version information about package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("filecluster")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0.dev0"


def get_version() -> str:
    """Return the installed package version."""
    return __version__
