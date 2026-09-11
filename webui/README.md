# Catalog browser (web UI)

A single self-contained HTML page for browsing and searching a filecluster
SQLite catalog (`.filecluster.db`). It runs entirely in the browser using
[sql.js](https://sql.js.org/) (SQLite compiled to WebAssembly) loaded from a
CDN — there is no server and the database file is never uploaded anywhere.

## Use it

1. Build a catalog if you don't have one yet:

   ```bash
   filecluster catalog build -l /path/to/library
   ```

   Images receive both a short (first 1 MiB) and full hash by default; videos
   receive a short hash. Use `--image-hash full|short` and
   `--video-hash full|short` to choose each policy independently, `--hash-algo
   blake3` to hash with BLAKE3, and `--crc32` to also store a per-file CRC32
   checksum.

2. Open `catalog-browser.html` in a browser. Because sql.js is fetched from a
   CDN, you need a network connection the first time. Opening the file directly
   (`file://`) works; if your browser blocks the CDN over `file://`, serve the
   folder instead:

   ```bash
   python3 -m http.server 8777        # then visit http://localhost:8777/catalog-browser.html
   ```

3. Drop your `.filecluster.db` onto the page (or use the **Open** button) and
   browse.

## What you get

- **Summary**: file count, indexed size, cluster count, how many rows carry an
  EXIF date, a full hash and a CRC32 checksum.
- **Files tab**: sortable, paginated table of every indexed file — path, size,
  EXIF date, modified time, partial hash, full hash and CRC32. Search matches
  the path or either hash as a substring; a checkbox limits the view to rows
  that have an EXIF date. Click a hash to copy it. (Catalogs built before the
  CRC32 column show an empty CRC32 column.)
- **Clusters tab**: the cached cluster rows (event folder, start/end/median
  dates, file count, continuity), searchable by path.

## Notes

- The page reads only the main `.filecluster.db` file. If the catalog was left
  with uncommitted WAL data by another process, run `filecluster catalog backup`
  and open the resulting `.bak` (it is a fully consolidated database), or simply
  reopen after the writing process has closed.
- It is read-only: nothing you do here changes the catalog on disk.
