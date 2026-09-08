# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/izikeros/filecluster/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/izikeros/filecluster/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/izikeros/filecluster/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/izikeros/filecluster/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/izikeros/filecluster/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/izikeros/filecluster/releases/tag/v0.1.0
