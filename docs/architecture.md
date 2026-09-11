# Architecture

`filecluster` organizes personal media without deleting or overwriting source
files. It has five workflows:

| Workflow | Entry point | Purpose |
| --- | --- | --- |
| Cluster | `filecluster run` | Group an inbox into timestamp-based event folders. |
| Reconcile | `filecluster reconcile` | Compare a source tree with one or more libraries. |
| Deduplicate | `filecluster dedup` | Find byte-identical files inside one tree. |
| Catalog | `filecluster catalog` | Build, inspect, and verify a library cache. |
| Curate | `python -m filecluster.curation` | Classify inbox media as keep, review, or reject before clustering. |

## Execution model

Every workflow follows the same sequence:

```text
discover -> inspect / classify -> build a plan -> preview -> execute
```

The plan is the boundary between decisions and filesystem writes. It permits
dry runs, reports, confirmation prompts, and deterministic tests. Writes use
exclusive destination reservation, so files created after planning cannot be
overwritten.

## Module responsibilities

| Module | Owns |
| --- | --- |
| `cli.py` | Command parsing, console setup, and exception-to-exit-code mapping. |
| `file_cluster.py` | Legacy clustering workflow orchestration. |
| `image_reader.py` | Inbox discovery and timestamp extraction. |
| `image_grouper.py` | Time-gap grouping and target-folder assignment. |
| `file_operations.py` | Shared file operation types, collision-free planning, and safe writes. |
| `catalog.py` | Persistent SQLite cache for library metadata and hashes. |
| `reconcile.py` / `dedup.py` | Content-based matching workflows. |
| `curation/` | Independent pre-clustering classification pipeline. |
| `ui.py` | Bounded Rich rendering, reporting, progress, and diagnostics. |
| `utils.py` | Shared media walking, hashing, EXIF, and sidecar helpers. |

`utlis.py` is the historical spelling retained only for compatibility. New
code must import from `utils.py`.

## Curation pipeline

The curation package is intentionally layered:

```text
CLI -> pipeline -> stages -> provider protocols
                  -> scoring and safety policy
                  -> catalog / operations / reporting
```

Stages measure evidence and return a `StageResult`. The pipeline owns stage
ordering, cache use, context lifetime, early exits, and fallback behavior.
`scoring.py` owns thresholding and safety policy. Providers are injected
through `Providers`, so model implementations remain optional and testable.

## Invariants

1. A dry run does not write media or create a cache.
2. No workflow deletes a source file unless a requested move succeeds.
3. A destination is never overwritten, including when it appears after
   planning.
4. Curation uncertainty and failed stages resolve to `review`.
5. UI output is bounded; per-file details belong in reports, not terminal
   scrollback.
6. Cache entries are reused only when their content and relevant configuration
   still match.

## Contributor guidance

- Add a workflow-specific decision to its planner, never directly to an
  executor.
- Reuse `file_operations.execute_file_operation()` for individual writes.
- Keep command functions thin; application code must be usable without Rich or
  Typer.
- Add a domain type before adding another dictionary key or implicit DataFrame
  column.
- Prefer extending the shared hashing and catalog paths over creating a new
  size/hash cache.
- Keep optional dependencies behind provider construction or explicit entry
  points; importing `filecluster` must remain inexpensive.
