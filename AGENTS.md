# Filecluster

Image and video clustering by date. Groups photos/videos into event-based folders using EXIF timestamps and configurable time gaps.

## Build & Test

- Install (editable): `pip install -e .`
- Run tests: `pytest tests` or `make test`
- Run single test: `pytest tests -k <test_name>`
- Format code: `make format` (runs isort, black, ruff --fix-only)
- Lint: `make lint` (ruff)
- Type check: `make type` (pyright)
- All CI checks: `make run-ci`

## Project Layout

```
src/filecluster/         → main package
  configuration.py       → Pydantic settings and config
  file_cluster.py        → core clustering orchestration
  image_reader.py        → EXIF/metadata extraction from images/videos
  image_grouper.py       → grouping logic based on time gaps
  update_clusters.py     → cluster assignment and update logic
  dbase.py               → database/dataframe operations
  utlis.py               → utility functions (note: intentional typo in filename)
  gui.py                 → GUI (PySimpleGUI)
  gui_tkinter.py         → GUI (Tkinter alternative)
  filecluster_types.py   → type definitions
  exceptions.py          → custom exceptions
tests/                   → pytest test suite
```

## Conventions & Patterns

- Python 3.12, line length 88 (black-compatible)
- Formatting: black + isort (profile=black) + ruff fix
- Linting: ruff (rules: C4, C9, N, E, W, F, UP, B, SIM, I, RUF, PIE)
- Type checking: pyright (strict) and mypy (for pandas/numpy)
- Configuration via Pydantic Settings (`configuration.py`)
- Logging via loguru
- Tests use pytest with INFO-level log output

## Key Dependencies

- `ExifRead` for EXIF metadata extraction
- `Pillow` for image processing
- `pandas` / `numpy` for data manipulation
- `pydantic` / `pydantic-settings` for configuration
- `loguru` for logging
- `tqdm` for progress bars

## Gotchas

- The utils module is named `utlis.py` (typo is intentional, do not rename)
- Both `setup.py` and `pyproject.toml` exist; `pyproject.toml` is the primary config
- Two GUI implementations exist: `gui.py` (PySimpleGUI) and `gui_tkinter.py` (Tkinter)
