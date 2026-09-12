"""In-memory content index for exact duplicate matching.

The index applies the project-wide size -> partial hash -> full hash cascade.
It is intentionally storage-agnostic: callers may populate it from a library,
an inbox, or another cache-backed source.
"""

from __future__ import annotations

from pathlib import Path

from filecluster.utils import get_partial_hash, hash_file


class ContentIndex:
    """Content-addressed index with lazy partial and full hash computation."""

    def __init__(self) -> None:
        self._by_size: dict[int, list[Path]] = {}
        self._partial: dict[str, str | None] = {}
        self._full: dict[str, str | None] = {}

    def add(self, path: Path, size: int) -> None:
        """Index *path* as a candidate having *size* bytes."""
        self._by_size.setdefault(size, []).append(path)

    def candidates_by_size(self, size: int) -> list[Path]:
        """Return indexed files whose size can match a candidate."""
        return self._by_size.get(size, [])

    def partial_hash(self, path: Path) -> str | None:
        """Return the cached first-megabyte MD5 hash for *path*."""
        key = str(path)
        if key not in self._partial:
            self._partial[key] = get_partial_hash(key)
        return self._partial[key]

    def full_hash(self, path: Path) -> str | None:
        """Return the cached full SHA-1 hash for *path*."""
        key = str(path)
        if key not in self._full:
            try:
                self._full[key] = hash_file(key)
            except OSError:
                self._full[key] = None
        return self._full[key]

    def find_matches(self, path: Path, size: int) -> list[Path]:
        """Return indexed files with byte-identical content to *path*."""
        candidates = self.candidates_by_size(size)
        if not candidates:
            return []
        source_partial = get_partial_hash(str(path))
        if source_partial is None:
            return []

        source_full: str | None = None
        matches: list[Path] = []
        for candidate in candidates:
            if self.partial_hash(candidate) != source_partial:
                continue
            if source_full is None:
                try:
                    source_full = hash_file(str(path))
                except OSError:
                    return []
            if self.full_hash(candidate) == source_full:
                matches.append(candidate)
        return matches
