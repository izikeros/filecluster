"""Module with custom exceptions to be used in the filecluster."""

from collections.abc import Mapping


class DateStringNoneError(Exception):
    """Exception for a case when the date string is none."""

    def __init__(self):
        self.message = "date_string is None"


class MissingDfClusterColumnError(Exception):
    """Exception for the case when there is a missing cluster column."""

    def __init__(self, column_name):
        self.message = f"Column {column_name} is missing in data frame."


class HashPolicyConflictError(ValueError):
    """A catalog build requested a hashing/CRC policy the library already fixed.

    The hashing algorithm and CRC32 choice are recorded per library the first
    time it is built, so every file shares one comparable set of hashes.
    Appending with a different policy would silently mix incompatible hashes,
    so it is refused; changing the policy requires a full re-hash via
    ``--rebuild``.
    """

    def __init__(self, stored: Mapping[str, object], requested: Mapping[str, object]):
        self.stored = stored
        self.requested = requested
        self.message = (
            "This library was built with a different hashing policy "
            f"(stored: {self._fmt(stored)}; requested: {self._fmt(requested)}). "
            "Re-run with --rebuild to re-hash the whole library under the new "
            "policy, or drop the conflicting options to keep the current one."
        )
        super().__init__(self.message)

    @staticmethod
    def _fmt(policy: Mapping[str, object]) -> str:
        algo = policy.get("hash_algo") or "sha1"
        crc = "crc32=on" if policy.get("crc32") else "crc32=off"
        return f"hash_algo={algo}, {crc}"


class OverlappingPathsError(ValueError):
    """Two directories that must stay separate are the same or nested.

    Reconciling a source that overlaps a library would have every file match
    itself, so a move would empty the library into the duplicates folder.
    """

    def __init__(
        self,
        first_label: str,
        first: object,
        second_label: str,
        second: object,
    ):
        self.message = (
            f"{first_label} ({first}) and {second_label} ({second}) overlap. "
            "They must be separate directories."
        )
        super().__init__(self.message)
