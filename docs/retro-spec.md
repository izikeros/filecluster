# Filecluster — Retro-created Product & Technical Specification

## 1. Context and Product Intent

`filecluster` is a personal/family photo and video organization tool.
Its core purpose is to turn a flat, mixed-media **inbox** into a structured, event-based library with minimal manual work.

This specification is reconstructed from current code, tests, and existing project notes.

## 2. Product Goals

### Primary goal
- Support **regular incremental imports** of photos/videos into a long-lived personal archive.

### Secondary goals
- Avoid re-importing duplicates already present in the main library.
- Reuse/extend existing event clusters when new files fit them.
- Keep import behavior transparent via dry-run mode and deterministic outputs.

### Non-goals (current scope)
- Full digital asset management (DAM) with tagging/search UX.
- Cloud sync, multi-user collaboration, or real-time ingestion pipelines.
- Automated semantic grouping (face/person/object-based clustering).

## 3. Core User Workflow (Incremental Import)

1. User places new media files in an inbox directory.
2. Tool scans inbox metadata (EXIF + filesystem times + file hashes/sizes).
3. Tool optionally scans watch/library folders for existing cluster metadata and duplicates.
4. Tool assigns each file to one of:
   - existing cluster,
   - new cluster,
   - duplicate bucket.
5. Tool creates target directories and moves/copies files.
6. Tool emits diagnostics useful for verification (counts, cluster names, duplicate lists).

## 4. Functional Requirements

### FR-1: Inbox metadata extraction
- The system must read media from configured inbox path.
- The system must build a media dataframe including name, timestamps, size, hash, media type, and status.

### FR-2: Timestamp normalization
- The system must derive a single clustering timestamp per file using configured rules (EXIF preferred, fallbacks).

### FR-3: Time-gap clustering
- The system must create clusters by time granularity threshold.
- The system must support representative cluster date strategies (median/random).

### FR-4: Existing-library awareness
- The system should detect and load existing cluster metadata from watch folders.
- The system should assign inbox files to existing clusters when timestamp continuity criteria pass.

### FR-5: Duplicate handling
- The system should identify probable duplicates using filename preselection + file property confirmation.
- The system must support sending duplicates to separate output structure.

### FR-6: File operations
- The system must support:
  - `MOVE` mode (default production behavior),
  - `COPY` mode (safe/dev behavior),
  - `NOP` mode (dry-run, no file movement).

### FR-7: Operational interfaces
- CLI must remain the primary supported interface.
- Experimental GUI interfaces may exist but are not primary acceptance paths.

## 5. Non-functional Requirements

### NFR-1: Safety and recoverability
- Dry-run mode must allow verifying behavior before changing filesystem state.
- Copy mode must preserve inbox during test runs.

### NFR-2: Determinism and readability
- Given equal input and config, clustering and target assignment should be reproducible.
- Output folder naming should stay human-readable and date-centric.

### NFR-3: Performance
- Must handle regular home-library incremental imports within practical desktop runtimes.
- Scanning library metadata should leverage parallelism where safe.

### NFR-4: Maintainability
- The codebase should remain modular by responsibility (config, reader, grouping, cluster update, db ops).
- Tooling should enforce consistent style and checks.

## 6. Architecture Overview

### Runtime flow
- **Entrypoint:** `src/filecluster/file_cluster.py`
- **Configuration:** `configuration.py`
- **Library cluster scan:** `update_clusters.py` + `dbase.py`
- **Inbox read/metadata:** `image_reader.py`
- **Grouping/assignment/operations:** `image_grouper.py`
- **Utilities and low-level helpers:** `utlis.py`

### Data model style
- Pandas DataFrames are the central in-memory representation:
  - media dataframe (per-file state),
  - clusters dataframe (per-cluster state).

### Persistence model
- Filesystem remains the source of truth for media placement.
- Existing cluster metadata is represented via `.cluster.ini` files in library folders.

## 7. Architecture Decisions (Retro ADRs)

### ADR-001: Filesystem-first organization
**Decision:** Store clustering outcome as directory structure, not DB records.
**Why:** Fits personal archive workflows, portable, inspectable without app runtime.

### ADR-002: DataFrame-centered processing
**Decision:** Use pandas as orchestration data model for media and clusters.
**Why:** Fast iteration for filtering/sorting/time computations and incremental feature growth.

### ADR-003: Timestamp gap as clustering heuristic
**Decision:** Event boundaries are based primarily on temporal distance.
**Why:** Robust default for family/event photography where chronological bursts dominate.

### ADR-004: Existing cluster reuse before creating new clusters
**Decision:** Attempt assignment to existing clusters when configured.
**Why:** Preserves archive continuity and reduces duplicate/fragmented event folders.

### ADR-005: Multi-mode file operations (NOP/COPY/MOVE)
**Decision:** Keep explicit operation modes in config and CLI.
**Why:** Balances safety during experimentation with practical production imports.

## 8. Technology Selection (Current)

### Language/runtime
- Python 3.12

### Core libraries
- `pandas`, `numpy` for tabular processing and clustering logic.
- `ExifRead`, `Pillow` for media metadata/image handling.
- `pydantic` + `pydantic-settings` for typed config.
- `loguru` for runtime logs.

### Toolchain (migrated)
- Dependency + execution: **uv** (`pyproject.toml` + `uv.lock`).
- Formatting/linting/fixing: **Ruff-only** (`ruff format`, `ruff check`, `--fix`, `--unsafe-fixes`).
- Type checking: **Astral Ty** (`ty check`), currently non-blocking in CI while legacy issues are reduced.
- Tests: `pytest`.

## 9. Known Gaps and Follow-up Work

- Improve type coverage to make Ty blocking in CI.
- Improve duplicate confidence (beyond filename+size heuristics).
- Improve MOV/video timestamp robustness across devices/platforms.
- Define stronger guarantees for cluster naming stability and collision handling.

## 10. Acceptance Signals for Current Product Stage

- Incremental import run completes without crashes.
- New media is either clustered, attached to existing clusters, or isolated as duplicates.
- Dry-run behavior matches real run plan.
- Test suite remains green for core flows.
