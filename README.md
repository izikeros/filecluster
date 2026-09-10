[![uv, ruff, ty](https://github.com/izikeros/filecluster/actions/workflows/isort_black_ruff.yml/badge.svg)](https://github.com/izikeros/filecluster/actions/workflows/isort_black_ruff.yml)


## filecluster
Python library for creating image and video catalog. Catalog is organized by the dates and events. The main purpose is to handle a task when you have a large number of pictures in a flat directory and want to automatically group them into separate directories corresponding to events e.g., directory for your daughter's birthday, separate directory for the excursion you make the next day after the birthday, etc.

### Features
- clustering media (images, video) by event, using EXIF timestamps with a
  filesystem-timestamp fallback
- detecting duplicate files and storing them in a separate output dir
- detecting media belonging to events that are already in the library
- assigning imported media to an event already present in the library
- marking folders of events with a large amount of media (folder suffix: `_rich`)
- never overwrites: a destination name already taken gets a numeric suffix,
  claimed again at write time so a file created after planning is safe too
- dry run (`-n`) that shows the full plan without writing anything at all, not
  even the hash catalog
- output stays compact whether you import 8 files or 50,000
- maintenance commands: `reconcile` merges a folder into the library, `dedup`
  finds files stored twice inside one tree, `catalog` inspects and repairs the
  per-library SQLite index

### Installation
Requires Python 3.12 or newer. The recommended way to install is with `uv`:

```bash
uv sync            # dependencies + the `filecluster` command in .venv/bin
```

Then either activate the environment, call `uv run filecluster ...`, or install
the command onto your PATH:

```bash
uv tool install .
```

On Windows to have numpy working, one might need to install:

[vc_redist.x64.exe](https://aka.ms/vs/15/release/vc_redist.x64.exe)

### Quick start
Installing the package provides the `filecluster` command with four
subcommands. All of them change nothing until you say so.

```bash
# 1. sort a flat folder into event folders — preview first
filecluster -i inbox -o clustered --no-operation

# 2. run it for real, matching against a library you already have
filecluster -i inbox -o clustered -w zdjecia --drop-duplicates --use-existing-clusters

# 3. merge an already-organised folder into that library
filecluster reconcile -s to_sort -l zdjecia          # add --execute to apply

# 4. find photos stored twice inside one tree
filecluster dedup -d zdjecia                         # add -q DIR --execute to quarantine

# 5. inspect the per-library index
filecluster catalog stats -l zdjecia
```

A dry run over a small inbox looks like this:

```
  filecluster 0.6.2
  Inbox   inbox
  Output  clustered
  Watch   none
  Mode    DRY RUN · gap 60 min · duplicates off · existing-clusters off ·
          restore-names off

  ✔ Read inbox           8 files                             0:00:00
  ✔ Clustered            4 new clusters                      0:00:00

  Results (DRY RUN)
  New clusters                4
  Files to process            8
  Elapsed               0:00:00

  Largest clusters
  Cluster                               Files
  new/[2021_09_21]_151138_IC_5_VC_0_        5
  new/[2016_11_04]_175047_IC_0_VC_1_        1
  new/[2018_11_23]_084250_IC_1_VC_0_        1
  new/[2018_11_24]_113201_IC_1_VC_0_        1

  Planned layout (nothing written)
  clustered
  ├── new/[2021_09_21]_151138_IC_5_VC_0_ 5 files
  │   ├── IMG_4128.jpg
  │   ├── IMG_3784.jpg
  │   ├── IMG_4124.jpg
  │   └── … 2 more
  ├── new/[2016_11_04]_175047_IC_0_VC_1_ 1 file
  │   └── IMG_2250.MOV
  ├── new/[2018_11_23]_084250_IC_1_VC_0_ 1 file
  │   └── IMG_4026.JPG
  └── new/[2018_11_24]_113201_IC_1_VC_0_ 1 file
      └── IMG_4029.JPG

  ! 6 files: no EXIF date (used file timestamp)
  Re-run with -v to list affected files.
```

Before writing anything it asks for confirmation. Pass `--yes` to skip the
prompt; a non-interactive session (cron, pipelines) proceeds on its own.

Output stays this compact no matter how many files are processed: per-file
detail goes to a progress bar while a phase runs, or to `--report FILE`, never
to the scrollback. Logs go to stderr, so `stdout` can be piped safely.

### Documentation
**[User guide](docs/user-guide.md)** — organised by task, with the full option
reference for every command:

- [Sort a flat folder into event folders](docs/user-guide.md#sort-a-flat-folder-into-event-folders)
- [Import into an existing library](docs/user-guide.md#import-into-an-existing-library)
- [Preview a large import](docs/user-guide.md#preview-a-large-import)
- [Merge an organised folder into the library](docs/user-guide.md#merge-an-organised-folder-into-the-library) (`reconcile`)
- [Find files stored twice in one tree](docs/user-guide.md#find-files-stored-twice-in-one-tree) (`dedup`)
- [Look after the catalog](docs/user-guide.md#look-after-the-catalog) (`catalog`)
- [Change the defaults](docs/user-guide.md#change-the-defaults) (environment variables)
- [Automation and scripting](docs/user-guide.md#automation-and-scripting) (`--json`, exit codes)
- [Troubleshooting](docs/user-guide.md#troubleshooting)
- [Command reference](docs/user-guide.md#command-reference)

**[Curation subpackage](docs/curation.md)** — the experimental
`filecluster.curation` cascade that sorts an inbox into keep / review / reject
before clustering: how to run it, how the decision is made, its current
limitations and what is still pending.

### How the clustering works
Files are sorted by timestamp and split wherever the gap between two
consecutive files exceeds the time granularity (60 minutes by default). Each
resulting group becomes one event folder under the output directory:

```
clustered/
├── new/          ← newly detected events
├── existing/     ← media assigned to events already in the watch folders (-c)
└── duplicated/   ← files already present in the library (-d)
```

Event folder names carry the event date and time (the median timestamp of the
group, by default) plus the image and video counts:
`[2018_11_23]_084250_IC_1_VC_0_`. Folders holding more than ten images or ten
videos get a `_rich` suffix.

Timestamps come from EXIF where available, from the QuickTime atom for `.mov`
files, and from the filesystem otherwise. The summary reports how many files
fell back to a filesystem timestamp, since those dates are the least reliable.

Duplicate detection is exact-content, never perceptual, and uses a three-level
cascade — file size, then MD5 of the first 1 MB, then full SHA1 — so a library
where most files are unique is barely read. Hashes are cached in each library's
`.filecluster.db` and re-used while the file's size and mtime still match.

### Configuration
Defaults live in `src/filecluster/configuration.py` and can be overridden with
`FILECLUSTER_`-prefixed environment variables or a `.env` file, without
touching the code:

```bash
$ FILECLUSTER_TIME_GRANULARITY_MINUTES=180 filecluster -i inbox -o clustered -n
```

See [Change the defaults](docs/user-guide.md#change-the-defaults) for the
settings that matter most.

Exit codes: `0` success or a declined confirmation, `2` bad usage or
environment, `130` interrupted.

### Development
```bash
uv sync --group dev   # install dev dependencies
make test             # pytest
make run-ci           # format check, lint, type check, tests
make help             # all targets
```

Formatting and linting are Ruff-only, type checking uses `ty`. Release versions
are managed with `make bump-patch`, `bump-minor` or `bump-major`, which update
`CHANGELOG.md` and tag the commit.

`tests/assets/` holds a small end-to-end fixture set: an inbox (`set_1`) plus
two library folders (`zdjecia`, `clusters`). See `tests/assets/README.md` for
what each file exercises, and
[Trying it on the bundled fixtures](docs/user-guide.md#trying-it-on-the-bundled-fixtures)
for a safe way to drive the CLI by hand.

## Graphical Interface
There is available experimental graphical interface: (`src/filecluster/gui.py`).
![img](screenshot.png)

## Changelog
See [CHANGELOG.md](CHANGELOG.md).
