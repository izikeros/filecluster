#!/usr/bin/env bash
#  -o OUTPUT_DIR, --output-dir OUTPUT_DIR     output directory for clustered images
#  -w WATCH_DIR, --watch-dir WATCH_DIR        directory with structured media (official media repository)
#  -t, --development-mode Run script with development configuration - work on tests directories
#  -n, --no-operation     Do not introduce any changes on the disk. Dry run.
#  -y, --copy-mode        Copy instead of default move
#  -f, --force-deep-scan  Force recalculate cluster info for each existing cluster.
#  -d, --drop-duplicates  Do not cluster duplicates, store them in separate folder.
#  -c, --use-existing-clusters
#                        If possible, check watch folders if the inbox media can be assigned to already existing cluster.
#  --version             show program's version number and exit


python filecluster/file_cluster.py -i h:\incomming\inbox -o h:\incomming\inbox_clust --force-deep-scan --drop-duplicates --use-existing-clusters -w h:\zdjecia\2022
