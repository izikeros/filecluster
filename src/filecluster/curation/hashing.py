"""Content hashing for the curation cache.

The cache key has to survive a rename, so it is the file content and not the
path. SHA-256 is used rather than the SHA-1 default of
:func:`filecluster.utlis.hash_file`, because these digests are stored and
compared for years across configuration and model changes.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

#: Read size chosen to keep the syscall count low without holding much memory.
CHUNK_SIZE = 1024 * 1024


def sha256_file(path: str | Path, chunk_size: int = CHUNK_SIZE) -> str:
    """Return the SHA-256 digest of *path*, read in one streaming pass."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()
