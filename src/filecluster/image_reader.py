"""Module for reading media data on files from given folder."""

import os
import struct
from datetime import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel

import filecluster.utils as ut
from filecluster import logger
from filecluster.configuration import (
    Config,
    CopyMode,
    Status,
    default_settings,
    get_default_config,
)
from filecluster.filecluster_types import MediaDataFrame
from filecluster.ui import NullProgress, ProgressSink, diagnostics

# for extracting timestamp from MOV files
ATOM_HEADER_SIZE = 8
# difference between Unix epoch and QuickTime epoch, in seconds
EPOCH_ADJUSTER = 2082844800

#: How many folders the inbox walk covers between two status updates. Keeps a
#: deep tree from issuing one update per directory.
WALK_REPORT_EVERY = 25


def _walk_reporter(progress: ProgressSink, root: Path) -> Any:
    """Build an ``on_progress`` callback that retitles the running phase."""

    def _report(n_folders: int, n_files: int) -> None:
        if n_folders % WALK_REPORT_EVERY:
            return
        progress.update_description(
            f"Scanning {root} — {n_folders:,} folders, {n_files:,} media files"
        )

    return _report


class Metadata(BaseModel):
    """Class defining media metadata."""

    file_name: str = ""
    path_name: str = ""
    m_time: str = ""
    c_time: str = ""
    exif_date: str = ""
    date: str | None = None
    file_size: int = 0
    hash_value: int = 0
    image: int = 0
    is_image: bool = True
    cluster_id: int | None = 0
    status: Status = Status.UNKNOWN
    duplicated_to: list[str] = []
    duplicated_cluster: list[str] = []


def multiple_timestamps_to_one(
    image_df: MediaDataFrame, rule="m_date", drop_columns: bool = True
) -> MediaDataFrame:
    """Get a timestamp from exif (primary) or m_date. Drop didn't need date cols.

    Prepare a single timestamp out of cdate, mdate and exif (disambiguation)

    Args:
      rule: rule used for disambiguation
      drop_columns:                 whether to drop columns with timestamps or not
      image_df: MediaDataFrame: input dataframe with media data

    Returns:
      media dataframe with a selected single date out of cdate, mdate and exif

    """
    # logger.trace("Cleaning-up timestamps in imported media.")

    # normalize date format - coerce unparseable values to NaT rather than
    # crashing the entire run because of one file with an odd timestamp.
    image_df["m_date"] = pd.to_datetime(image_df["m_date"], errors="coerce").astype(
        "datetime64[ns]"
    )
    image_df["c_date"] = pd.to_datetime(image_df["c_date"], errors="coerce").astype(
        "datetime64[ns]"
    )
    image_df["exif_date"] = pd.to_datetime(
        image_df["exif_date"], errors="coerce"
    ).astype("datetime64[ns]")

    # TODO: Ensure that any date is assigned to file
    # use exif date as base

    # unless it is missing - then use the modification date:
    if rule == "m_date":
        # use exif date if available
        image_df["date"] = image_df["exif_date"]
        # fill missing (no exif date) with m_date
        image_df["date"] = image_df["date"].fillna(image_df["m_date"])
    elif rule == "earliest":
        image_df["date"] = image_df[["m_date", "c_date", "exif_date"]].min(axis=1)

    if drop_columns:
        image_df.drop(["m_date", "c_date", "exif_date"], axis=1, inplace=True)
    return image_df


def initialize_row_dict(meta: Metadata) -> dict[str, Any]:
    """Generate a single row based on values defined in the outer method.

    Returns:
        Dictionary filled-in data from the input Metadata object.
    """
    return {
        "file_name": meta.file_name,
        "m_date": meta.m_time,
        "c_date": meta.c_time,
        "exif_date": meta.exif_date,
        "date": meta.date,
        "size": meta.file_size,
        "hash_value": meta.hash_value,
        "is_image": meta.is_image,
        "cluster_id": meta.cluster_id,
        "status": meta.status,
        "duplicated_to": meta.duplicated_to,
        "duplicated_cluster": meta.duplicated_cluster,
    }


def prepare_new_row_with_meta(
    media_file_name: str,
    accepted_media_file_extensions: list[str],
    in_dir_name: Path,
    meta: Metadata,
) -> dict[str, Any]:
    """Prepare dictionary with metadata for input media file.

    Args:
      media_file_name:                  name of the media file
      accepted_media_file_extensions:   list of accepted media file extensions
      in_dir_name:                      input directory name
      meta:                             Metadata object

    Returns:
        Dictionary with metadata.


    TODO: Better reading metadata from MOV video files - see:
    # https://stackoverflow.com/questions/21355316/getting-metadata-for-mov-video
    # data: [2021_10_11]_Hania_...
    """
    meta.file_name = media_file_name
    # full path + file name
    path_name = os.path.join(in_dir_name, media_file_name)
    meta.path_name = path_name
    # get modification, creation and exif dates
    meta.m_time, meta.c_time, meta.exif_date = ut.get_date_from_file(path_name)
    # determine if a media file is image or other type
    is_image = ut.is_image(path_name, accepted_media_file_extensions)
    meta.is_image = is_image
    if media_file_name.lower().endswith("mov"):
        try:
            meta.c_time, meta.m_time = get_mov_timestamps(path_name)
        except Exception:
            diagnostics.add("unreadable video timestamp", path_name)
            logger.debug(f"Cannot get dates from MOV file: {path_name}")

    # file size
    meta.file_size = os.path.getsize(path_name)
    # file hash
    meta.hash_value = ut.hash_file(path_name)
    # placeholder for date representative for a file
    meta.date = None  # to be filled in later in: multiple_timestamps_to_one()
    # placeholder for assignment to cluster
    meta.cluster_id = None
    # status
    meta.status = Status.UNKNOWN
    # duplication info
    meta.duplicated_to = []
    meta.duplicated_cluster = []
    return initialize_row_dict(meta)


def get_mov_timestamps(filename):
    """Get the creation and modification date-time from .mov metadata.

    Returns None if a value is not available.

    from: https://stackoverflow.com/a/54683292
    """
    creation_time = modification_time = None

    # search for moov item
    with open(filename, "rb") as f:
        while True:
            atom_header = f.read(ATOM_HEADER_SIZE)
            if len(atom_header) < ATOM_HEADER_SIZE:
                raise RuntimeError('expected to find "moov" header.')
            # ~ print('atom header:', atom_header)  # debug purposes
            if atom_header[4:8] == b"moov":
                break  # found
            else:
                atom_size = struct.unpack(">I", atom_header[0:4])[0]
                if atom_size < ATOM_HEADER_SIZE:
                    raise RuntimeError("invalid atom size")
                f.seek(atom_size - 8, 1)

        # found 'moov', look for 'mvhd' and timestamps
        atom_header = f.read(ATOM_HEADER_SIZE)
        if atom_header[4:8] == b"cmov":
            raise RuntimeError("moov atom is compressed")
        elif atom_header[4:8] != b"mvhd":
            raise RuntimeError('expected to find "mvhd" header.')
        else:
            f.seek(4, 1)
            creation_time = get_creation_time(struct, f)
            modification_time = creation_time
    return creation_time, modification_time


# TODO Rename this here and in `get_mov_timestamps`
def get_creation_time(struct, f):
    result = struct.unpack(">I", f.read(4))[0] - EPOCH_ADJUSTER
    result = dt.fromtimestamp(result)
    if result.year < 1990:  # invalid or censored data
        result = None

    return result


class InboxReader:
    """Initialize a media database with existing media dataframe or create empty one."""

    def __init__(
        self,
        in_dir_name,
        media_df: MediaDataFrame | None = None,
        limit: int | None = None,
        recursive: bool = True,
    ) -> None:
        """Initialize the reader.

        Args:
            in_dir_name: Directory to read media from.
            media_df: Pre-built media frame, if the caller already has one.
            limit: Ingest at most this many files. Useful for trying a run on a
                large inbox without reading all of it.
            recursive: Whether to scan subdirectories recursively.
        """
        self.in_dir_name = in_dir_name
        self.image_extensions = default_settings.image_extensions
        self.video_extensions = default_settings.video_extensions
        self.limit = limit
        self.recursive = recursive
        #: Supported files present in the inbox, before *limit* is applied.
        self.n_available = 0

        if media_df is None:
            logger.debug(
                f"Initializing empty media dataframe in InboxReader ({in_dir_name})"
            )
            self.media_df = MediaDataFrame(DataFrame())
        else:
            msg = "Initializing media dataframe in InboxReader with provided df."
            logger.debug(f"{msg}Num records: {len(media_df)}")
            self.media_df = media_df

    def get_data_from_files_as_list_of_rows(
        self, progress: ProgressSink | None = None
    ) -> list[dict]:
        """Recursively read exif data from files given in a path provided in config.

        Args:
          progress: Optional sink notified of the file count and each file read.

        Returns:
          List of rows: list of rows with all information
        """
        progress = progress or NullProgress()
        list_of_rows = []
        in_dir_name = Path(self.in_dir_name)
        ext = self.image_extensions + self.video_extensions

        logger.debug(f"Reading data from: {in_dir_name} (recursive={self.recursive})")
        image_extensions = self.image_extensions
        meta = Metadata()

        # Walking a 50k-file tree can take a while on its own, and it happens
        # before the file total (and so the progress bar) is known, so the walk
        # reports its own headway.
        progress.update_description(f"Scanning {in_dir_name}")
        full_paths = ut.walk_media_files(
            in_dir_name,
            ext,
            recursive=self.recursive,
            on_progress=_walk_reporter(progress, in_dir_name),
        )
        file_list = [str(p.relative_to(in_dir_name)) for p in full_paths]
        self.n_available = len(file_list)

        if self.limit is not None and self.limit < len(file_list):
            # Sorted only when truncating, so a limited run picks the same
            # files every time instead of whatever order the filesystem
            # happened to return. Unlimited runs keep their original order.
            file_list = sorted(file_list)[: self.limit]
            logger.debug(
                f"Ingesting {len(file_list)} of {self.n_available} files (--limit)"
            )

        progress.start(len(file_list), "Reading media")
        for file_name in file_list:
            new_row = prepare_new_row_with_meta(
                file_name, image_extensions, Path(in_dir_name), meta
            )
            list_of_rows.append(new_row)
            progress.advance()
        return list_of_rows

    def get_media_files_info(self, progress: ProgressSink | None = None) -> None:
        """Read data from files, return media info in a dataframe.

        Args:
          progress: Optional sink notified of reading progress.
        """
        row_list = self.get_data_from_files_as_list_of_rows(progress=progress)
        logger.debug(f"Read info from {len(row_list)} files.")
        if not row_list:
            empty_df = MediaDataFrame(
                DataFrame(columns=list(initialize_row_dict(Metadata())))
            )
            self.media_df = multiple_timestamps_to_one(empty_df)
            return

        # Count files lacking EXIF before the raw date columns are dropped.
        # Counted rather than listed: a large inbox can hold thousands of them.
        n_no_exif = sum(1 for row in row_list if row["exif_date"] is None)
        diagnostics.add_count("no EXIF date (used file timestamp)", n_no_exif)
        logger.debug(f"{n_no_exif} of {len(row_list)} files lack an EXIF date")

        # convert a list of rows to a data frame
        inbox_media_df = MediaDataFrame(DataFrame(row_list))
        inbox_media_df = multiple_timestamps_to_one(inbox_media_df)
        self.media_df = inbox_media_df


def configure_inbox_reader(in_dir_name: str | Path) -> Config:
    """Customize configuration for the purpose of scanning the media library.

    Args:
        in_dir_name: input directory name to be scanned for the media contents.

    Returns:
        Configuration object for the im_reader.
    """
    conf = get_default_config()
    # modify config
    conf.in_dir_name = Path(in_dir_name)
    conf.out_dir_name = Path("")
    conf.mode = CopyMode.NOP
    return conf


def get_media_df(in_dir_name: Path, recursive: bool = True) -> MediaDataFrame | None:
    """Get a data frame with metadata description of media indicated in Config.

    Returns:
        Dataframe with metadata of the contents of the directory.
    """
    if os.path.exists(in_dir_name) and os.listdir(in_dir_name):
        inbox_reader = InboxReader(in_dir_name, recursive=recursive)
        if row_list := inbox_reader.get_data_from_files_as_list_of_rows():
            df = MediaDataFrame(DataFrame(row_list))
            return multiple_timestamps_to_one(df)
        else:
            logger.debug(f" - directory {in_dir_name} is empty?.")
            return None
    else:
        logger.debug(f" - directory {in_dir_name} is empty.")
        return None


def get_media_stats(df: DataFrame, time_granularity: int) -> dict:
    """Get statistics of media data represented in a data frame.

        Get statistics of the media folder.

    Returns:
        Dictionary describing media in a dataframe.
    """
    date_min = df.date.min()
    date_max = df.date.max()
    date_median = df["date"].median()

    df = df[["file_name", "date"]].copy()
    df["date_int"] = df["date"].apply(lambda x: x.value / 10**9)
    df = df.sort_values("date_int")
    df["delta"] = df.date_int.diff()

    # check all media in the folder are compliant with the assumption that
    # all the media from the same event are not more than 1 hour apart
    # from each other
    is_time_consistent = not (any(df.delta.values > time_granularity))

    file_count = len(df)
    return {
        "date_min": date_min,
        "date_max": date_max,
        "date_median": date_median,
        "is_time_consistent": is_time_consistent,
        "file_count": file_count,
    }
