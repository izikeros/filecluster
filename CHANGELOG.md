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
  - Auto-detects event-folder mode (`[YYYY_MM_DD]…` subdirs) vs flat inbox mode.
  - Uses the same 3-level matching cascade as `mark_inbox_duplicates`:
    size → partial hash (1 MB MD5) → full hash (SHA1).
  - Classifies each file as DUPLICATE, NEW, or NAME_COLLISION; in event-folder
    mode, each folder gets an aggregate status (ALL_DUPLICATE, ALL_NEW, PARTIAL).
  - Dry-run by default; `--execute` applies moves.
  - `--report` exports a per-file CSV; `--json` prints a machine-readable summary.
  - `-f`/`--force-reindex` backs up the existing `.filecluster.db` catalog with
    a timestamp, clears it, and eagerly recomputes all partial hashes.
- `LibraryCatalog.backup()` creates a timestamped `.bak` copy of the SQLite catalog.
- `LibraryCatalog.clear_file_hashes()` deletes all file-hash rows for a clean reindex.

### Changed
- CLI restructured as a multi-command app (`run` + `reconcile`). Bare invocation
  (`filecluster -i … -o …`) still defaults to `run` for full backwards compatibility.

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
