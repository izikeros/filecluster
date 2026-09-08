#!/usr/bin/env bash
#  -i, --inbox-dir DIRECTORY     directory with input media files to process
#  -o, --output-dir DIRECTORY    output directory for clustered images
#  -w, --watch-dir DIRECTORY     directory with structured media (official media repository)
#  -t, --development-mode        Run with development configuration - work on tests directories
#  -n, --no-operation            Do not introduce any changes on the disk. Dry run.
#  -y, --copy-mode               Copy instead of default move
#  -f, --force-deep-scan         Force recalculate cluster info for each existing cluster.
#  -d, --drop-duplicates         Do not cluster duplicates, store them in separate folder.
#  -c, --use-existing-clusters   Assign media to clusters already in the watch folders.
#  -Y, --yes                     Do not ask for confirmation before writing.
#  --report FILE                 Write the full per-file operation list to a CSV file.
#  -V, --version                 show program's version number and exit

filecluster -i h:\incomming\inbox -o h:\incomming\inbox_clust \
--force-deep-scan \
--drop-duplicates \
--use-existing-clusters \
--yes \
-w h:\zdjecia\2022
