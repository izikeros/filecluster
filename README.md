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
- never overwrites: a destination name already taken gets a numeric suffix
- dry run (`-n`) that shows the full plan without touching a file
- output stays compact whether you import 8 files or 50,000

### Installation
Requires Python 3.10 or newer. The recommended way to install is with `uv`:

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

### Usage
Installing the package provides the `filecluster` command.

Preview what would happen, without touching a single file:
```bash
$ filecluster -i inbox -o clustered --no-operation
```

On a huge inbox, cap the ingestion to get a preview in seconds. `--limit` takes
the first N files in name order, so repeated previews stay comparable:
```bash
$ filecluster -i inbox -o clustered --no-operation --limit 500
```

Then run it for real, matching against an existing library:
```bash
$ filecluster -i inbox -o clustered -w zdjecia --drop-duplicates --use-existing-clusters
```

A dry run over a small inbox looks like this:

```
  filecluster 0.5.0
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
prompt; when the output is not a terminal (cron, pipelines) it proceeds on its
own.

Output stays this compact no matter how many files are processed: per-file
detail goes to a progress bar while a phase runs, or to `--report FILE`, never
to the scrollback. Logs go to stderr, so `stdout` can be piped safely.

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

Run options:
```
Usage: filecluster [OPTIONS]

  Group photos and videos into event folders based on their timestamps.

  -i, --inbox-dir DIRECTORY       Directory with input media files to process
  -o, --output-dir DIRECTORY      Directory where clustered media will be placed
  -w, --watch-dir DIRECTORY       Existing media library to match against. Repeatable
  -t, --development-mode          Use the development test directories
  -n, --no-operation              Dry run: show the plan, change nothing
  -y, --copy-mode                 Copy files instead of moving
  -f, --force-deep-scan           Recompute cluster info for every existing cluster
  -d, --drop-duplicates           Put duplicates in a separate folder instead of clustering
  -c, --use-existing-clusters     Assign media to clusters already in the watch folders
  -r, --restore-original-names    Strip copy suffixes such as '-Kopiuj(1)' or ' - Copy'
  -l, --limit INTEGER             Ingest at most this many inbox files, in name order
  -Y, --yes                       Do not ask for confirmation before writing
      --show INTEGER              How many of the largest clusters to list (0 = all)  [20]
      --report FILE               Write the full per-file operation list to this CSV
      --json                      Print a machine-readable summary instead
      --color / --no-color        Force colour on or off
  -v, --verbose                   -v for info, -vv for debug
  -q, --quiet                     Only report errors
  -V, --version                   Show the version and exit
  -h, --help                      Show this message and exit
```

`-d` and `-c` compare the inbox against the library, so both require at least
one `-w` watch folder.

### Scripting
`--json` replaces the rendered summary with a single JSON document on stdout
(counts, per-cluster sizes, diagnostics, elapsed time), so a run can be checked
from a script:

```bash
$ filecluster -i inbox -o clustered -n --json | jq '.new_clusters, .duplicates'
```

`--report FILE` writes every planned or performed operation to CSV
(`operation,source,destination_folder,destination`). That is where per-file
detail belongs on a large import, instead of the terminal.

### Configuration
Defaults live in `src/filecluster/configuration.py` and can be overridden with
`FILECLUSTER_`-prefixed environment variables or a `.env` file, without
touching the code:

```bash
$ FILECLUSTER_TIME_GRANULARITY_MINUTES=180 filecluster -i inbox -o clustered -n
```

Useful settings: `TIME_GRANULARITY_MINUTES` (event gap), `IMAGE_EXTENSIONS`,
`VIDEO_EXTENSIONS`, `INBOX_DIR`, `OUTBOX_DIR`. List settings take a JSON array,
for example `FILECLUSTER_VIDEO_EXTENSIONS='[".mp4", ".mov"]'`.

Exit codes: `0` success or a declined confirmation, `2` bad usage or
environment, `130` interrupted.

### Development
```bash
uv sync --group dev   # install dev dependencies
make test             # pytest (325 tests)
make run-ci           # format check, lint, type check, tests
make help             # all targets
```

Formatting and linting are Ruff-only, type checking uses `ty`. Release versions
are managed with `make bump-patch`, `bump-minor` or `bump-major`, which update
`CHANGELOG.md` and tag the commit.

`tests/assets/` holds a small end-to-end fixture set: an inbox (`set_1`) plus
two library folders (`zdjecia`, `clusters`). See `tests/assets/README.md` for
what each file exercises. To try the CLI by hand, copy the fixtures somewhere
scratch first, keeping their timestamps, so a MOVE run cannot damage them:

```bash
mkdir -p /tmp/fc-demo
cp -Rp tests/assets/set_1 /tmp/fc-demo/inbox
filecluster -i /tmp/fc-demo/inbox -o /tmp/fc-demo/out -n
```

`cp -Rp` matters: five of the fixture files carry no EXIF date, so without
preserved timestamps they all fall into a single present-day event.

## Graphical Interface
There is available experimental graphical interface: (`src/filecluster/gui.py`).
![img](screenshot.png)

## Changelog
See [CHANGELOG.md](CHANGELOG.md).
