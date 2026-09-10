# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`filecluster.curation` subpackage** — cascaded photo curation that sorts an
  inbox into `keep` / `review` / `reject` before clustering, so screenshots,
  receipts, documents and product shots never enter the photo library.
  - Cheap stages first: filename and EXIF rules with a versioned screen-size
    table, then a single-decode pixel pass (sharpness, exposure, contrast,
    entropy, colourfulness, edge density, uniform background).
  - Model-backed stages (semantic labelling, OCR density, aesthetics, VLM
    escalation) sit behind injected providers with lazy imports; install them
    with `pip install "filecluster[curation]"`.
  - Safety asymmetry: nothing is deleted, unknowns and failures go to `review`,
    and automatic `reject` needs semantic evidence or a decisive screenshot
    marker. Protected subjects (people, pets, home, events) block reject.
  - SQLite verdict cache keyed by content hash plus pipeline, config and model
    fingerprints; CSV report and aggregate-only JSON summary.
  - Runnable standalone today (`python -m filecluster.curation -i inbox -o out`)
    and wired into the main CLI with one line when desired.
  - **`docs/curation.md`** — reference for the subpackage: decision model, the
    six cascade stages, signal and reason-code vocabulary, configuration, cache
    invalidation, extension points, current limitations, and the pending work
    needed to reach the design's full potential.
- **`filecluster reconcile` command** — checks whether files in a source
  directory (inbox or filecluster output dirs with event folders) already exist
  in the main library, then moves duplicates aside and integrates new files.
  - Auto-detects event-folder mode (`[YYYY_MM_DD]…` subdirs), flat inbox mode,
    and sources that mix the two.
  - Uses the same 3-level matching cascade as `mark_inbox_duplicates`:
    size → partial hash (1 MB MD5) → full hash (SHA1).
  - Classifies each file as DUPLICATE, NEW, or SOURCE_DUPLICATE (duplicated
    inside the source itself); in event-folder mode each folder also gets an
    aggregate status (ALL_DUPLICATE, ALL_NEW, PARTIAL).
  - Walks the source recursively, and keeps companion files (`.xmp`, `.aae`,
    `.thm`, `.lrv`, …) and `.cluster.ini` with the media they belong to.
  - Matches against several libraries (`-l` is repeatable) and reports every
    library copy of a duplicate, not just the first.
  - Dry-run by default; `--execute` applies. `-y`/`--copy-mode` copies instead
    of moving, `--scan-only` reports without planning any move.
  - `--report` exports a per-file CSV; `--json` prints a machine-readable summary.
  - `-f`/`--force-reindex` backs up the existing `.filecluster.db` catalog with
    a timestamp, clears it, and eagerly recomputes all partial hashes.
- **`filecluster dedup` command** — finds media stored more than once inside a
  single directory tree, in the same folder or across folders. Picks one
  canonical copy per group (prefers a file in an event folder, without a
  `-Kopiuj`/` - Copy` suffix, highest up the tree), reports how much space is
  reclaimable, and with `-q`/`--quarantine-dir` moves the redundant copies out
  while preserving their path relative to the scanned root.
- **`filecluster catalog` command** — `stats`, `backup`, `restore` and
  `verify [--prune]` subcommands for the per-library SQLite index.
- `LibraryCatalog`: `backup()`, `restore()`, `list_backups()`,
  `clear_file_hashes()`, `delete_file_rows()`, `stats()`, `verify()` and
  `vacuum()`. `get_file_records()` returns `FileRecord`s carrying `mtime`.
- `file_operations.DestinationAllocator` and `unique_name()`: reusable
  collision-free destination allocation, now shared by the clustering plan,
  `reconcile` and `dedup`.
- `utlis`: `get_partial_hash()`, `PARTIAL_HASH_SIZE`, `walk_media_files()`,
  `find_sidecar_files()`, `is_sidecar_file()`, `is_event_folder_name()`,
  `extract_year_from_folder()`, `extract_date_from_folder()`.
- **`docs/user-guide.md`** — task-oriented guide ("I want to …") covering every
  command, with real terminal output, the destination rules for `reconcile`,
  the quarantine workflow for `dedup`, catalog maintenance, configuration,
  scripting with `--json`, troubleshooting, and a full option reference.
- `filecluster catalog stats` now reports how many catalog backups exist and
  names the newest one, which is the one `restore` picks by default.
- `exceptions.OverlappingPathsError`, raised when two directories that must stay
  separate are the same or nested.
- `LibraryCatalog.open(read_only=True)` for previews: reuses an existing catalog
  without creating one, and refuses every write.
- `file_operations.reserve_exclusive()` and `numbered_name()`, the write-time
  half of the overwrite guarantee.

### Changed
- README trimmed to overview, installation, quick start and how clustering
  works; the per-command walkthroughs and option tables moved to
  `docs/user-guide.md`.
- `requires-python` raised from `>=3.10` to `>=3.12`, matching the Python
  version the code needs (`enum.StrEnum`, `datetime.UTC`) and the one ruff and
  `ty` already target. The README states the same version.
- CLI restructured as a multi-command app (`run`, `reconcile`, `dedup`,
  `catalog`). Bare invocation (`filecluster -i … -o …`) still defaults to `run`
  for full backwards compatibility.
- `get_partial_hash()` and `PARTIAL_HASH_SIZE` moved from `image_grouper` to
  `utlis` and are re-exported, so existing imports keep working.
- `ty` now type-checks against Python 3.12 instead of inferring 3.10 from
  `requires-python`, which had it reporting `enum.StrEnum` and `datetime.UTC`
  as missing.

### Fixed
- `filecluster reconcile` was registered as `reconcile-cmd`, so the documented
  command name did not exist.
- Reconcile could overwrite library files: a collision was detected but the
  destination was left unchanged, and `shutil.move` replaces silently on POSIX.
  Every destination now goes through `DestinationAllocator`.
- Cached file hashes were trusted without checking `mtime`, so a file edited
  in place (same size, new content) could be reported as a duplicate of its
  former self. A cache entry is now used only while size *and* mtime match.
- A source holding one event folder plus loose files silently skipped the loose
  files.
- Moving an event folder was not recursive, leaving sidecars, `.cluster.ini`
  and nested media behind.
- **Data loss: reconciling overlapping folders emptied the library.** Passing
  the same directory (or a symlinked alias, or a nested subdirectory) as both
  source and library made every file match itself, and `--execute` moved the
  whole library into the duplicates folder. `reconcile` now rejects overlapping
  source, library and duplicates roots with `OverlappingPathsError` before
  opening a catalog or reading a file. A duplicates folder inside the source is
  refused for the same reason.
- **Data loss: collision protection ignored directories and symlinks.** The
  allocator only claimed names of regular files, so an existing directory made
  a move nest the file inside it, and a destination symlink redirected the write
  to its target, outside the library. Every directory entry is claimed now.
- **Data loss: a file created after planning was silently overwritten.** Names
  were claimed against a directory listing, leaving a time-of-check gap that
  `shutil.move` and `copy2` would happily overwrite. Each destination is now
  reserved with `O_CREAT | O_EXCL` at write time and gets a numeric suffix if it
  was taken meanwhile; moves use `os.replace` onto that reservation, and a
  failed write removes only the placeholder it created.
- **Catalog backups could omit committed data.** `backup()` checkpointed the WAL
  and copied the database file, but a checkpoint fails silently while another
  connection holds a read snapshot, leaving those rows out of the `.bak`. It now
  uses SQLite's own backup API, and a partial backup is deleted rather than left
  for `catalog restore` to offer.
- `catalog restore` validated nothing, so a truncated or corrupt backup could
  replace a working catalog. The backup is opened and checked first.
- `--no-sidecars` still moved sidecars that sat inside an event folder: they were
  skipped as sidecars and then swept up again as generic extra files.
- **Dry runs wrote to disk.** Previewing `reconcile` or `dedup` created a
  `.filecluster.db` in the scanned tree, and `reconcile -f` cleared the cached
  hashes even without `--execute`. Both now open the catalog read-only, so a
  preview leaves the tree byte-identical; `LibraryCatalog` refuses every write
  on a read-only handle.
- The same library passed twice (`-l LIB -l LIB`, or via a symlink) was indexed
  twice, reporting every file as its own duplicate.

## [0.6.2] - 2026-09-09

## [0.6.1] - 2026-09-09

### Fixed
- Auto-refresh stale `.cluster.ini` when files are added to a watched folder.
  `get_this_ini` now compares folder mtime vs ini mtime and rescans
  automatically, so the typical workflow (run → copy output to library → run
  again) no longer requires `--force-recalc`.
- Catalog path forces a deep rescan for all folders with a mtime mismatch
  instead of reading the outdated ini.

## [0.6.0] - 2026-09-09

### Added
- **Per-library SQLite catalog** (`.filecluster.db`) caches cluster metadata
  and file hashes across runs. Unchanged event folders (same mtime) are served
  from the catalog instead of re-reading `.cluster.ini` files.
- File-hash caching for duplicate detection: library file hashes are persisted
  in the catalog so repeated runs skip re-hashing entirely.
- Stale-row pruning removes catalog entries for folders/files deleted from disk.

### Changed
- Library scan pipeline checks the catalog before dispatching to the worker
  pool; results are written back as write-through (both SQLite and `.cluster.ini`).
- `mark_inbox_duplicates` pre-seeds hash caches from catalogs and persists
  newly computed hashes after the check.

## [0.5.2] - 2026-09-09

### Fixed
- Handle nanosecond-precision timestamps in `.cluster.ini` files. Pandas
  `median()` can produce Timestamps with 9 fractional digits, but
  `datetime.strptime` `%f` only handles 6, causing "unconverted data remains"
  errors. Added `_parse_datetime()` helper that truncates excess digits.
- `start_date`/`end_date` parsing in `read_cluster_ini_as_dict` now tries the
  microsecond format instead of silently dropping to `None`.

## [0.5.1] - 2026-09-08

### Added
- Folder-discovery progress feedback during library scanning for slow network
  mounts (`update_clusters`, `fast_scandir`).
- `update_description()` method on `ProgressSink` protocol for pre-bar spinner
  updates.

### Fixed
- Timestamp parsing coerces unparseable values to `NaT` instead of crashing the
  entire run on a single malformed EXIF date.
- `get_date_from_file` returns `datetime` objects instead of `ctime` strings,
  fixing downstream type mismatches with pandas.

## [0.5.0] - 2026-09-08

### Added
- **Typer/Rich CLI** — new `cli.py` entry point with `--version`, `-v`/`-q`,
  `--yes`, `--show`, `--json`, `--report`, `--color`; exit codes 0/1/2/130.
- **`ui.py`** presentation layer (723 lines) — banner, phase progress, results
  panel, largest-clusters table, planned-layout preview, capped diagnostics,
  JSON summary, and CSV report writer.
- **`-l`/`--limit`** flag caps ingestion at the first N files for fast dry runs.
- **Overwrite protection** — destination filenames are collision-aware; existing
  or already-claimed names get a numeric suffix.
- **Bounded output** — nothing user-facing is per-file; detail goes to a progress
  bar, a capped sample, or `--report`.
- **bump-my-version** for version management with changelog integration.

### Changed
- `file_cluster.py` stripped to orchestration only; argparse removed.
- Swapped `tqdm` for `rich`/`typer`; declared the `console_scripts` entry point.
- Memoized partial and full hashes for duplicate checks; existing event folders
  scanned through `pool.imap` for stable progress.

### Fixed
- Duplicate event bridging — gap calculation now selects `Status.UNKNOWN` rows
  instead of rows without a cluster id.
- Order-independent cluster assignment — existing-cluster assignment iterates
  chronologically.
- Empty inbox handling — returns an empty dataframe with the expected schema
  instead of raising `KeyError: 'm_date'`.
- Silent no-op in `update_clusters` replaced with an explicit cast.

## [0.4.0] - 2026-06-29

### Added
- `--restore-original-names` to revert copy-suffixed filenames.
- `FileOperationPlan` — separates I/O from business logic for testability.

### Changed
- Critical performance fixes to `ImageGrouper`.
- All 45 `ty` type checker errors resolved.

### Fixed
- Duplicate target path bug in CLI output.

## [0.3.0] - 2026-03-08

### Added
- Test suite rewritten with 187 tests, full business-logic coverage, and minimal
  mocking.
- Retro specification document.

### Changed
- Migrated tooling to **uv**, **ruff** (format + lint), and **ty** (type check);
  dropped black, isort, flake8, and pdm.

### Fixed
- Clustering edge cases and duplicate handling hardened.

## [0.2.0] - 2025-04-24

### Added
- GitHub Actions CI workflow (UV, isort, black, ruff).
- `uv.lock` checked into the repository.
- Support for fractional EXIF dates.

### Changed
- Code-quality pass: docstrings, grammar, imports, formatting consistency.
- Renamed `DateStringNoneException` to `DateStringNoneError`.
- Fixed typo `is_continous` → `is_continuous` in `image_grouper.py`.
- Makefile updated to specify source directories for isort and ruff.

## [0.1.0] - 2024-01-06

### Added
- First packaged release using **pdm** as the build system.
- `pytest` configuration and initial test suite.
- Argparse CLI entry point with `--inbox-dir`, `--watch-dirs`, `--db-driver`.
- Pydantic-based configuration (`configuration.py`).
- EXIF metadata extraction with `ExifRead` and `Pillow`.
- Time-gap-based clustering into event folders.
- Watch-folder support for incremental organization.
- Simple GUI implementations (PySimpleGUI and Tkinter).
- Pre-commit hooks (black, isort, flake8).

[Unreleased]: https://github.com/izikeros/filecluster/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/izikeros/filecluster/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/izikeros/filecluster/compare/v0.5.2...v0.6.0
[0.5.2]: https://github.com/izikeros/filecluster/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/izikeros/filecluster/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/izikeros/filecluster/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/izikeros/filecluster/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/izikeros/filecluster/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/izikeros/filecluster/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/izikeros/filecluster/releases/tag/v0.1.0
