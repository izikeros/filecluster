"""Image and video clustering by the event time.

The goal of this package is to help with organization of the multimedia
 (photos and videos) by the event and date.

It help coping with the problem when the content is coming from various devices
 and in different time.

## Organizational scheme
### inbox folder

Inbox - incoming media lands here.

### outbox folder

Generated clusters are created here. If matching cluster already exist in watch
 folder

### watch folder

Folder with main, structured collection of media. It is watched and compared
with potential newly created clusters - if corresponding cluster already exists
in watch folder, the cluster folder in outbox should have the same name as
existing luster including parent directory (year).

## Quick start
```bash
$ file_cluster.py --inbox-dir inbox --watch-dirs zdjecia --db-driver dataframe
```

or in abbreviated form:

```bash
$ file_cluster.py -i inbox -w zdjecia -d dataframe
```

You can add `-n` switch to have dry run.

## Development mode
As name indicates can be helpful during the development phase

- "copy" operation instead of "move" to protect source files.
- "delete db" database is usually deleted to ensure "fresh" start
"""

import sys

from loguru import logger

# remove existing handlers (or handler?)
logger.remove()

# ----- add console handler
logger.add(
    sys.stdout,
    # format="<green>{time:HH:mm:ss}</green> <level>{level}</level> {message}",
    format="<level>{level}</level> {message}",
    colorize=True,
    level="INFO",
)
logger.info("Logger initialized")
