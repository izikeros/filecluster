[![Build Status](https://semaphoreci.com/api/v1/izikeros/filecluster/branches/master/shields_badge.svg)](https://semaphoreci.com/izikeros/filecluster)
# filecluster
Python library for creating image and video catalog. Media catalog is stored in sqlite database.

## Features
- clustering media (images, video) by event
- detecting duplicate files
- detect if imported photo belongs to event already in database/filesystem

## Installation:
clone the repo, install required packages (see `filecluster/requirements.txt`)

On Windows to have numpy working one might need to install:

https://aka.ms/vs/15/release/vc_redist.x64.exe

## Usage
Typical usage:
```bash
$ file_cluster.py --inbox-dir inbox --watch-dirs zdjecia --db-driver dataframe
```

or in abbreviated form:

```bash
$ file_cluster.py -i inbox -w zdjecia -d dataframe
```
Other run options:
```
usage: file_cluster.py [-h] [-i INBOX_DIR] [-o OUTPUT_DIR] [-w WATCH_DIRS] [-d DB_DRIVER] [-t] [-n]

Purpose of the script

optional arguments:
  -h, --help            show this help message and exit
  -i INBOX_DIR, --inbox-dir INBOX_DIR
                        directory with input images
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        output directory for clustered images
  -w WATCH_DIRS, --watch-dirs WATCH_DIRS
                        directories with structured media (official media repository)
  -d DB_DRIVER, --db-driver DB_DRIVER
                        technology to use to store cluster and media databases. sqlite|dataframe
  -t, --development-mode
                        Run script with development configuration - work on tests directories
  -n, --no-operation    Do not introduce any changes on the disk. Dry run.

```
