# filecluster user guide

This guide is organised by task. Find the row that matches what you want to do
and jump to that section.

| I want to … | Command | Section |
| --- | --- | --- |
| Sort a folder of loose photos into event folders | `filecluster` | [Sort a flat folder](#sort-a-flat-folder-into-event-folders) |
| Import a card or download folder into a library I already have | `filecluster -w` | [Import into an existing library](#import-into-an-existing-library) |
| See what would happen to 50,000 files without waiting | `-n --limit` | [Preview a large import](#preview-a-large-import) |
| Merge a folder that is *already* organised into my library | `filecluster reconcile` | [Merge an organised folder](#merge-an-organised-folder-into-the-library) |
| Find photos that are stored twice inside one tree | `filecluster dedup` | [Find files stored twice](#find-files-stored-twice-in-one-tree) |
| Reclaim the space those copies waste | `dedup -q --execute` | [Quarantine and delete](#quarantine-then-delete) |
| Know what the library index holds, or repair it | `filecluster catalog` | [Look after the catalog](#look-after-the-catalog) |
| Change what counts as one event | `FILECLUSTER_*` | [Change the defaults](#change-the-defaults) |
| Drive all this from a script or cron job | `--json` | [Automation](#automation-and-scripting) |
| Work out why the result looks wrong | — | [Troubleshooting](#troubleshooting) |

Every option of every command is listed in the
[command reference](#command-reference) at the end.

---

## Concepts

| Term | Meaning |
| --- | --- |
| **inbox** | The folder you are importing *from*: a memory card, a download folder, a flat dump of pictures. |
| **output dir** | Where `filecluster` writes the event folders it creates. |
| **library** / **watch folder** | A folder tree you already curated. `run` calls it a watch folder (`-w`), `reconcile` and `catalog` call it a library (`-l`). |
| **event folder** | One event, named `[2018_11_23]_084250_IC_1_VC_0_`: date, time, image count, video count. A `_rich` suffix means more than ten images or ten videos. |
| **catalog** | A `.filecluster.db` SQLite file at the root of each library, caching cluster metadata and file hashes so repeat runs stay fast. |
| **sidecar** | A companion file that belongs to a photo: `.xmp`, `.aae`, `.thm`, and so on. It travels with its media file. |

The four commands:

| Command | Compares | Writes to |
| --- | --- | --- |
| `filecluster` (`run`) | inbox files against each other, optionally against a library | a fresh output dir |
| `filecluster reconcile` | a source folder against one or more libraries | into the library, and duplicates aside |
| `filecluster dedup` | a tree against itself | a quarantine folder, or nothing |
| `filecluster catalog` | the catalog against the files on disk | the catalog only |

## Safety rules

These hold for every command, so you can experiment without fear:

- **Nothing is written until you say so.** `run` prints the plan and asks for
  confirmation; `reconcile` and `dedup` need an explicit `--execute`. A preview
  does not even create the hash catalog, so it leaves the tree byte-identical.
- **No file is ever overwritten.** If a destination name is taken, the incoming
  file gets a ` (1)` suffix. This applies to names already on disk *and* to two
  incoming files that want the same name. The name is claimed again at write
  time, so a file that appeared after the plan was printed is protected too.
- **Overlapping folders are refused.** `reconcile` will not run when the source,
  a library or the duplicates folder is the same directory as another, or nested
  inside it, even through a symlink. It stops before reading anything.
- **Moving is the default, copying is opt-in** (`-y/--copy-mode`). Use copy when
  the source is a camera card or a share you do not want to empty.
- **Results go to stdout, logs go to stderr.** Piping stdout is always safe.
- **Exit codes:** `0` success (a declined confirmation counts as success), `2`
  bad usage or a missing path, `130` interrupted.

Two flags mean different things depending on the command. Check before you
paste:

| Flag | `run` | `reconcile` | `dedup` | `catalog` |
| --- | --- | --- | --- | --- |
| `-d` | `--drop-duplicates` | `--duplicates-dir` | `--dir` (tree to scan) | — |
| `-l` | `--limit` | `--library` | — | `--library` |
| `-q` | `--quiet` | `--quiet` | `--quarantine-dir` | — |

---

## Sort a flat folder into event folders

The base case: a directory full of pictures. By default, `filecluster` scans the inbox recursively across subdirectories; pass `--flat` (or `--no-recursive`) to restrict scanning to top-level files only.

```bash
filecluster -i inbox -o clustered -n     # -n = dry run, change nothing
```

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

  Planned layout (nothing written)
  clustered
  ├── new/[2021_09_21]_151138_IC_5_VC_0_ 5 files
  │   ├── IMG_4128.jpg
  │   └── … 4 more
  └── new/[2016_11_04]_175047_IC_0_VC_1_ 1 file
      └── IMG_2250.MOV

  ! 6 files: no EXIF date (used file timestamp)
  Re-run with -v to list affected files.
```

Happy with it? Drop the `-n`:

```bash
filecluster -i inbox -o clustered
```

It shows the same plan and asks for confirmation before the first file moves.
`--yes` skips the question; so does a non-interactive session (no tty on stdin,
or output redirected), so cron jobs and pipelines are never left waiting.

**How the split is decided.** Files are sorted by timestamp and cut wherever
the gap between two consecutive files is longer than the time granularity, 60
minutes by default. Each resulting group becomes one event folder. Timestamps
come from EXIF where present, from the QuickTime atom for `.mov`, and from the
filesystem otherwise. The summary always tells you how many files fell back to
a filesystem timestamp, because those dates are the least trustworthy.

**Do not lose timestamps while experimenting.** Copy with `cp -Rp` (or
`rsync -a`), never plain `cp`. Without the original mtimes, files that have no
EXIF date all collapse into a single present-day event.

## Import into an existing library

Point `-w` at the library you already have, and two extra checks become
possible:

```bash
filecluster -i inbox -o clustered -w zdjecia --drop-duplicates --use-existing-clusters
```

- `-d/--drop-duplicates` puts files that already exist in the library into
  `clustered/duplicated/` instead of clustering them again.
- `-c/--use-existing-clusters` assigns files that belong to an event already in
  the library into `clustered/existing/`, so a second card from the same
  wedding does not create a second folder.

Both compare against the library, so both need at least one `-w`.

The output dir then holds up to three subtrees:

```
clustered/
├── new/          ← newly detected events
├── existing/     ← belongs to an event already in a watch folder (-c)
└── duplicated/   ← content already in the library (-d)
```

Two more flags matter on real-world imports:

- `-r/--restore-original-names` strips copy suffixes such as `-Kopiuj(1)` or
  ` - Copy` while moving, so `IMG_4026 - Copy.JPG` lands as `IMG_4026.JPG`
  (with a ` (1)` suffix if that name is taken).
- `-f/--force-deep-scan` recomputes cluster metadata for every existing
  cluster instead of trusting the cached `.cluster.ini` and catalog. Use it
  after you reorganised the library by hand.

## Preview a large import

A session can involve tens of thousands of files. Per-file detail never goes to
the terminal; it goes to a progress bar while a phase runs, or to a CSV.

```bash
# first 500 files in name order: a preview in seconds, repeatable
filecluster -i inbox -o clustered -n --limit 500

# full per-file plan as CSV: operation,source,destination_folder,destination
filecluster -i inbox -o clustered -n --report plan.csv

# list every cluster, not just the 20 largest
filecluster -i inbox -o clustered -n --show 0
```

## Merge an organised folder into the library

`reconcile` is for the folder you sorted six months ago and never filed: it
compares the source against the library **by content**, moves what is already
there aside, and integrates the rest. It is dry-run by default.

```bash
filecluster reconcile -s to_sort -l zdjecia              # preview
filecluster reconcile -s to_sort -l zdjecia --execute    # apply
```

When everything is already filed you get a clean answer:

```
  filecluster reconcile 0.6.2
  Source        /tmp/fc-guide/inbox
  Library       /tmp/fc-guide/zdjecia
  Duplicates    /tmp/fc-guide/duplicates
  Action        move
  Mode          DRY RUN

  ✔ Reconcile            0 new, 8 dup                        0:00:00

  Results
  Source mode           flat
  Total files              8
  In library already       8
  New files                0
  Planned moves            8

  Planned moves (nothing written)
  Moves
  └── /tmp/fc-guide/duplicates 8 files
      ├── IMG_2250.MOV
      └── … 7 more
```

and when nothing is, you see exactly where each file would land:

```
  ✔ Reconcile            8 new, 0 dup                        0:00:00

  Planned moves (nothing written)
  Moves
  ├── /tmp/fc-guide/lib/2021/[2021_09_21]_unsorted 6 files
  ├── /tmp/fc-guide/lib/2018/[2018_11_23]_unsorted 1 file
  └── /tmp/fc-guide/lib/2018/[2018_11_24]_unsorted 1 file
```

### Where each file goes

| In the source | Destination |
| --- | --- |
| Media inside an event folder, new | `library/YYYY/<event folder name>/` |
| Media inside an event folder, already in the library | `<duplicates>/<event folder name>/` |
| Loose media, new | `library/YYYY/[YYYY_MM_DD]_unsorted/` (EXIF date, else mtime) |
| Loose media, already in the library | `<duplicates>/<same relative path>` |
| `.cluster.ini`, unrecognised extensions, nested non-media | follows its folder |
| Sidecars (`.xmp`, `.aae`, …) | follow their media file |

`<duplicates>` defaults to `<source>/../duplicates`; override it with
`-d/--duplicates-dir`. Sub-directory structure inside the source is preserved
on both sides, so a nested inbox never collapses into one flat folder.

### Sources that mix shapes

The source is walked recursively and may mix event folders with loose files.
The summary reports which shape it found: `event-folders`, `flat` or `mixed`.
Each event folder also gets a status of its own, visible in the folder table:

| Folder status | Meaning |
| --- | --- |
| `ALL_NEW` | Nothing in it is in the library yet; the whole folder moves in. |
| `ALL_DUPLICATE` | Every file is already filed; the folder moves to duplicates. |
| `PARTIAL` | Mixed: duplicates go aside, the rest goes into the library. |

`--no-recursive` limits the scan to the top level of the source.

### Duplicates inside the source itself

By default `reconcile` also notices when the source holds the same content
twice. The first copy is treated as new and filed; the later ones are reported
as `SOURCE_DUPLICATE` and moved aside, so one pass never files the same photo
twice. `--no-source-dupes` turns that check off.

### Several libraries

`-l` is repeatable. Matches are looked for in **all** the libraries you pass,
and every match is listed in the CSV report, so you can see when a photo is
filed in more than one place. New files are placed in the **first** library.

```bash
filecluster reconcile -s to_sort -l /photos/main -l /archive/2019 --report r.csv
```

### Copy instead of move, or only look

```bash
filecluster reconcile -s /Volumes/CARD -l zdjecia -y --execute  # copy in, leave the card intact
filecluster reconcile -s to_sort -l zdjecia --scan-only         # classify and report only
```

`--scan-only` never writes, even with `--execute`, but still resolves every
destination name, so the preview shows the exact folders and renames a real run
would produce. It is the right flag for "tell me what I have" questions.

## Find files stored twice in one tree

Where `reconcile` compares two trees, `dedup` compares one tree against itself:
the same photo twice in one event folder, or scattered over folders after years
of ad-hoc copying. It reports by default and writes nothing.

```bash
filecluster dedup -d zdjecia
```

```
  filecluster dedup 0.6.2
  Scan          /tmp/fc-guide/zdjecia
  Action        report
  Mode          DRY RUN

  ✔ Scan for duplicates  2 groups, 2 copies                  0:00:00

  Results
  Files scanned              10
  Files hashed                2
  Duplicate groups            2
  Redundant copies            2
  Same folder                 1
  Across folders              1
  Reclaimable           4.0 MiB

  Duplicate groups
  ├── 2 copies 2.2 MiB · across folders
  │   ├── keep
  │   │   /tmp/fc-guide/zdjecia/2018/[2018_11_24]_oklejanie_drzwi/IMG_4029.JPG
  │   └── dup  /tmp/fc-guide/zdjecia/2018/backup_2019/IMG_4029.JPG
  └── 2 copies 1.8 MiB · same folder
      ├── keep /tmp/fc-guide/zdjecia/2018/[2018_11_23]_mgla/IMG_4026.JPG
      └── dup  /tmp/fc-guide/zdjecia/2018/[2018_11_23]_mgla/IMG_4026 - Copy.JPG
```

`Files hashed 2` out of ten scanned is the point of the cascade: files are
grouped by size first, then only same-size files get a 1 MB MD5, and only
matching partial hashes get a full SHA1. Hashes are cached in the library
catalog, so a second run over an unchanged library hashes nothing at all.

**Which copy is kept.** The one that looks most like the original, in this
order: inside an event folder, without a `-Kopiuj`/` - Copy` suffix, higher up
the tree, shorter name, then alphabetical so the choice is stable between runs.
In the example above that picks the copy in `[2018_11_24]_oklejanie_drzwi` over
the one in `backup_2019`, and the un-suffixed name over ` - Copy`.

Useful knobs:

```bash
filecluster dedup -d zdjecia --show 0             # list every group, not just 20
filecluster dedup -d zdjecia --min-size 102400    # ignore files under 100 KB
filecluster dedup -d zdjecia --no-recursive       # one folder only
filecluster dedup -d zdjecia --report dupes.csv   # one CSV row per copy
```

The CSV has a `role` column (`keep` or `duplicate`) and a `scope` column
(`same-folder` or `cross-folder`), so you can review the decisions in a
spreadsheet before touching anything.

### Quarantine, then delete

`dedup` never deletes. It moves redundant copies to a quarantine folder,
keeping each file's path relative to the scanned root, so the original layout
stays recoverable.

```bash
filecluster dedup -d zdjecia -q /tmp/dupes             # preview the moves
filecluster dedup -d zdjecia -q /tmp/dupes --execute   # do it
```

A quarantine folder placed *inside* the scanned tree is excluded from the scan,
so re-running does not rediscover what you already quarantined.

Recommended sequence:

1. `filecluster dedup -d zdjecia --report dupes.csv` and read the report.
2. `filecluster dedup -d zdjecia -q ../dupes --execute`.
3. Browse the library. Everything still there? Nothing missing?
4. Delete `../dupes` yourself, or keep it until the next backup cycle.

To undo step 2, move the files back: each one sits at
`<quarantine>/<its old path relative to the library>`.

## Look after the catalog

Each library keeps a `.filecluster.db` at its root. These subcommands inspect
and repair it. They only ever touch the database, never your media.

```bash
filecluster catalog stats   -l zdjecia
```

```
  Catalog
  Library               /tmp/fc-guide/zdjecia
  Database              /tmp/fc-guide/zdjecia/.filecluster.db
  Database size         24.0 KiB
  Schema version        3
  Cluster rows          4
  File rows             10
  Hash algo             not set
  CRC32 policy          off
  Partial hashes        10
  Full hashes           10
  CRC32 checksums       0
  Indexed bytes         68.0 MiB
  Backups               none
```

`catalog build` chooses the digest with `--hash-algo`. The default `sha1`
keeps the historical fast-prefilter split (MD5 partial + SHA-1 full) and stays
compatible with `reconcile`/`dedup`. `--hash-algo blake3` uses BLAKE3 — a fast,
modern, cryptographically strong hash — for both the partial and full hashes;
the algorithm is recorded per file, so old and new catalogs coexist and dedup
stays correct (it recomputes rather than trusting an incompatible hash).
Independently, `--crc32` stores a whole-file CRC32 checksum per file, a cheap
extra baseline for the bit-rot detection in `verify --deep` (off by default).

```bash
filecluster catalog build -l zdjecia --hash-algo blake3
filecluster catalog build -l zdjecia --crc32
```

The algorithm and CRC32 choice are **pinned to the library** on its first
build and stored in the catalog, so you never end up with a mix of
incomparable hashes across runs. A plain re-run (`filecluster catalog build -l
zdjecia`) keeps whatever policy is stored. If you *explicitly* ask for a
different one on a plain build, it is refused:

```bash
filecluster catalog build -l zdjecia --hash-algo sha1
# error: this library is pinned to blake3; re-run with --rebuild to change it
```

Changing the policy means re-hashing every file, so it only happens under
`--rebuild`, which backs up the current catalog first. Interactively the CLI
asks you to confirm; in `--json` or non-interactive use the change is refused
rather than silently re-hashing a large library.

```bash
filecluster catalog build -l zdjecia --hash-algo sha1 --rebuild
```

```bash
filecluster catalog verify  -l zdjecia          # rows vs files on disk
filecluster catalog verify  -l zdjecia --prune  # drop changed/missing rows, then VACUUM
filecluster catalog verify  -l zdjecia --deep    # also decode images / probe videos
filecluster catalog backup  -l zdjecia          # timestamped .bak next to the db
filecluster catalog restore -l zdjecia          # newest backup, or --from PATH
```

`verify` splits the cached rows into three buckets: up to date, changed on
disk (size or mtime no longer match), and gone from disk. A cached hash is only
trusted while size *and* mtime still match, so an edited file is never mistaken
for its former self even without pruning. Pruning just keeps the database
small and honest.

`--deep` adds content-level corruption detection on top of the size/mtime
check, using two independent signals. First, a **structural decode**: decodable
images (JPEG, PNG, TIFF, BMP, GIF, WebP) are fully decoded with Pillow, and
videos are validated with `ffprobe`. Second, a **baseline re-hash**: for files
that still match their cached size/mtime, the stored full hash and CRC32 are
recomputed and compared, which catches silent bit rot even in RAW/HEIC files
that cannot be decoded here (as long as they were hashed or given a `--crc32`
checksum at build time). The extra buckets are content OK, corrupt (a truncated
JPEG, a broken container, or a hash/CRC32 mismatch), unreadable (a permission or
I/O error), and not checked (nothing to decode and no baseline to compare — for
example a RAW file with no stored hash, or a video when `ffprobe` is absent).
Damaged files are listed by path. A file whose size/mtime changed is only
decoded, never compared against its now-outdated hash, so an edit is never
mistaken for corruption. A deep verify only reads files; it never overwrites the
stored baseline, so it compares current content against the known-good
fingerprint captured when the catalog was built.

`restore` backs up the database currently in place before overwriting it, and
clears stale `-wal`/`-shm` files, so a restore cannot leave a half-written
state behind.

To throw the index away and rebuild it from the files on disk:

```bash
filecluster reconcile -s to_sort -l zdjecia -f
```

`-f/--force-reindex` backs up the existing catalog, clears the cached hashes,
and re-reads the library. Use it after moving files around outside filecluster,
or if you ever suspect the cache. It costs one full hashing pass.

## Change the defaults

Defaults live in `src/filecluster/configuration.py` and can be overridden with
`FILECLUSTER_`-prefixed environment variables or a `.env` file in the working
directory. No code change needed.

```bash
# treat gaps of up to 3 hours as the same event
FILECLUSTER_TIME_GRANULARITY_MINUTES=180 filecluster -i inbox -o clustered -n

# recognise a different set of video files (JSON array, note the quoting)
FILECLUSTER_VIDEO_EXTENSIONS='[".mp4", ".mov", ".avi"]' filecluster -i inbox -o out -n
```

| Setting | Default | Effect |
| --- | --- | --- |
| `TIME_GRANULARITY_MINUTES` | `60` | Gap that separates two events. |
| `IMAGE_EXTENSIONS` | `.jpg .jpeg .dng .cr2 .tif .tiff .heic` | What counts as an image. |
| `VIDEO_EXTENSIONS` | `.mp4 .3gp .mov` | What counts as a video. |
| `INBOX_DIR` / `OUTBOX_DIR` | `inbox` / `outbox_clust` | Defaults for `-i` / `-o`. |
| `INI_FILENAME` | `.cluster.ini` | Per-folder metadata file name. |

The extension lists are shared by all four commands: widening them makes
`reconcile` and `dedup` consider more files too.

## Automation and scripting

`--json` replaces the rendered summary with one JSON document on stdout. All
four commands support it.

```bash
# how many duplicates would this import produce?
filecluster -i inbox -o clustered -n --json | jq '.new_clusters, .duplicates'

# is the library free of internal duplicates?
test "$(filecluster dedup -d zdjecia --json | jq .duplicate_groups)" -eq 0

# how much space would deduplication free?
filecluster dedup -d zdjecia --json | jq .wasted_bytes

# did the reconcile find anything new?
filecluster reconcile -s to_sort -l zdjecia --json | jq '.new, .duplicates'
```

Selected keys:

| Command | Keys |
| --- | --- |
| `run` | `version`, `mode`, `files_read`, `new_clusters`, `assigned_to_existing`, `duplicates`, `clusters[]`, `diagnostics`, `elapsed_seconds` |
| `reconcile` | `source_mode`, `action`, `libraries[]`, `total_files`, `new`, `duplicates`, `source_duplicates`, `renamed`, `sidecars`, `extra_files`, `moves`, `copies`, `skips` |
| `dedup` | `root`, `action`, `files_scanned`, `files_hashed`, `duplicate_groups`, `duplicate_files`, `intra_folder_groups`, `cross_folder_groups`, `wasted_bytes`, `moves` |
| `catalog` | `stats`: db path and row counts; `verify`: `ok`, `stale`, `missing`, `pruned` |

For unattended runs: pass `--yes` to `run` so it never waits for input (not
needed when the session is already non-interactive), use `--quiet` to silence
everything but errors, and check the exit code (`0` / `2` / `130`).

```bash
# nightly: file new arrivals, then report on library health
filecluster reconcile -s ~/Pictures/incoming -l ~/Pictures/library --execute --quiet
filecluster dedup -d ~/Pictures/library --json > ~/logs/dupes-$(date +%F).json
```

## Troubleshooting

**Everything landed in one event folder.**
The files carry no EXIF date and their mtimes are all identical, usually
because they were copied without `-p`. Re-copy with `cp -Rp` or `rsync -a`. The
summary line `! N files: no EXIF date (used file timestamp)` tells you how many
files were affected; `-v` lists them.

**Some files got a ` (1)` suffix.**
That is the overwrite guard: the destination name was already taken, either by
a file on disk or by another incoming file with the same name. Nothing was
lost. `--report` shows the exact source-to-destination mapping. A warning like
`… appeared after planning; writing to … instead` means the name was taken
between printing the plan and writing the file, which is also handled.

**`reconcile` refuses to start: "… overlap. They must be separate directories."**
The source, a library or the duplicates folder point at the same directory, or
one sits inside another. Reconciling a library against itself would match every
file with itself and move the whole library into the duplicates folder, so the
command stops before reading anything. Move the inbox out of the library (or
point `-d` somewhere outside the source) and re-run.

**`reconcile` says everything is a duplicate, but I cannot find the copies.**
Matching is by content, not by name, so a renamed copy still counts. Run with
`--report r.csv`: the `library_match` and `all_library_matches` columns give
the paths of every copy found.

**`reconcile` says 0 new, but I expected new files.**
Check the source mode line. If it reads `flat` and you expected
`event-folders`, your folder names do not start with `[YYYY_MM_DD]`, so their
contents were treated as loose files. That changes destinations, not duplicate
detection.

**`dedup` reports nothing on a library I know has copies.**
Only extensions in `IMAGE_EXTENSIONS`/`VIDEO_EXTENSIONS` are scanned, and
`--min-size` (default 1 byte) skips smaller files. Duplicates that differ by a
single byte, for example re-encoded or metadata-edited copies, are *not*
duplicates here: detection is exact-content, never perceptual.

**A run was interrupted.**
Files already moved stay moved; nothing is left half-written, because each file
is moved individually. Re-run the same command: content matching means files
already filed are recognised as duplicates rather than filed twice.

**The catalog looks wrong after I reorganised by hand.**
`filecluster catalog verify -l LIB --prune`, and if that is not enough,
`filecluster reconcile -s SRC -l LIB -f` to rebuild it. Both back up the
database first.

**I need the previous state of the database.**
`filecluster catalog stats -l LIB` lists the available backups;
`filecluster catalog restore -l LIB --from PATH` puts one back.

---

## Command reference

### `filecluster` / `filecluster run`

Group photos and videos into event folders based on their timestamps. The
command name `run` is optional: `filecluster -i inbox -o out` works.

```
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
      --flat / --no-recursive     Scan top-level inbox files only (default: recursive)
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

`-d` and `-c` compare the inbox against the library, so both need at least one
`-w`.

### `filecluster reconcile`

Merge a source directory into one or more libraries. Dry run unless
`--execute`.

```
  -s, --source DIRECTORY          Directory to reconcile                    [required]
  -l, --library DIRECTORY         Library root. Repeatable; new files go to the first
                                                                            [required]
  -d, --duplicates-dir DIRECTORY  Where duplicates go  [<source>/../duplicates]
      --execute                   Apply the plan
  -y, --copy-mode                 Copy into the library instead of moving
      --scan-only                 Classify and report only; never writes
      --no-recursive              Only look at the top level of the source
      --no-sidecars               Leave companion files behind
      --no-source-dupes           Skip duplicate detection inside the source
  -f, --force-reindex             Rebuild the library index (backs up the catalog first)
      --report FILE               Per-file CSV report
      --json                      Machine-readable summary
      --color / --no-color        Force colour on or off
  -v, --verbose                   -v for info, -vv for debug
  -q, --quiet                     Only report errors
  -h, --help                      Show this message and exit
```

### `filecluster dedup`

Find media stored more than once inside one tree. Reports unless
`-q` *and* `--execute` are given.

```
  -d, --dir DIRECTORY             Tree to scan for duplicates               [required]
  -q, --quarantine-dir DIRECTORY  Move redundant copies here instead of only reporting
      --execute                   Apply the plan. Requires --quarantine-dir
      --min-size INTEGER          Ignore files smaller than this many bytes  [1]
      --no-recursive              Only look at the top level
      --show INTEGER              How many duplicate groups to list (0 = all)  [20]
      --report FILE               One CSV row per copy
      --json                      Machine-readable summary
      --color / --no-color        Force colour on or off
  -v, --verbose                   -v for info, -vv for debug
      --quiet                     Only report errors (no short form: -q is quarantine)
  -h, --help                      Show this message and exit
```

### `filecluster catalog`

Inspect and maintain the per-library SQLite catalog. Every subcommand takes
`-l/--library` and `--json`.

```
  filecluster catalog build   -l LIB [-f/--rebuild] [--image-hash MODE]
                              [--video-hash MODE] [--full-hash]
                              [--hash-algo sha1|blake3] [--crc32] [--no-exif]
                                                Build/update the catalog from disk
  filecluster catalog stats   -l LIB            Rows, hash coverage, backups
  filecluster catalog verify  -l LIB [--prune] [--deep]
                                                Cached rows vs files on disk
                                                (--deep decodes images/probes video
                                                 and re-hashes against the baseline)
  filecluster catalog backup  -l LIB            Timestamped .bak copy
  filecluster catalog restore -l LIB [--from PATH]
                                                Newest backup unless --from
```

---

## Trying it on the bundled fixtures

`tests/assets/` holds a small end-to-end fixture set: an inbox (`set_1`) plus
two library folders (`zdjecia`, `clusters`). `tests/assets/README.md` explains
what each file exercises. Copy them somewhere scratch first, preserving
timestamps, so a real run cannot damage them:

```bash
mkdir -p /tmp/fc-demo
cp -Rp tests/assets/set_1 /tmp/fc-demo/inbox
cp -Rp tests/assets/zdjecia /tmp/fc-demo/zdjecia

filecluster -i /tmp/fc-demo/inbox -o /tmp/fc-demo/out -n
filecluster reconcile -s /tmp/fc-demo/inbox -l /tmp/fc-demo/zdjecia
filecluster dedup -d /tmp/fc-demo/zdjecia
filecluster catalog stats -l /tmp/fc-demo/zdjecia
```

The output in this guide was produced exactly this way.
