#!/usr/bin/env python3
"""Scan recursively directory and get information on clusters.

Module with a function to scan the directories and get existing cluster info
by reading or creating .cluster.ini files.

Usage:
./update_clusters.py -f -l tests/zdjecia

    -f force recalculation of cluster info ini files
    -l path to a library
"""
# TODO: KS: 2020-12-28: Consider changing data format from ini to yaml

import argparse
import multiprocessing
import os
import re
from collections.abc import Callable
from configparser import ConfigParser
from datetime import datetime
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Any, cast

import pandas as pd

from filecluster import logger

# from filecluster.configuration import ini_filename
from filecluster.configuration import FileClusterSettings
from filecluster.image_reader import (
    configure_inbox_reader,
    get_media_df,
    get_media_stats,
)
from filecluster.ui import NullProgress, ProgressSink


def _parse_datetime(s: str) -> datetime | None:
    """Parse a datetime string that may have microsecond or nanosecond precision.

    Pandas Timestamps serialised with ``str()`` can carry up to 9 fractional
    digits (nanoseconds), but ``datetime.strptime`` with ``%f`` only handles 6.
    This helper truncates any excess digits before parsing.
    """
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        # Truncate fractional part to 6 digits so %f can handle it.
        # A dot followed by more than 6 digits means nanosecond precision.
        truncated = re.sub(r"(\.\d{6})\d+$", r"\1", s)
        try:
            return datetime.strptime(truncated, fmt)
        except ValueError:
            continue
    return None


def str_to_bool(s: str) -> bool:
    """Convert 'True' or 'False' provided as string to the corresponding bool value."""
    if s == "True":
        return True
    elif s == "False":
        return False
    else:
        raise ValueError


def _scan_event_dir(args: tuple[Any, bool, str]) -> dict | Path | None:
    """Unpack pool arguments for :func:`get_this_ini`.

    ``imap`` passes a single argument per item, so the tuple is unpacked here
    rather than using ``starmap``, which cannot report progress incrementally.
    """
    event_dir, force_deep_scan, library_path = args
    return get_this_ini(event_dir, force_deep_scan, library_path)


def _catalog_row_to_dict(row: dict, library_path: str) -> dict:
    """Convert a catalog DB row into the same dict shape that ``get_this_ini`` returns."""
    pth = Path(library_path) / row["path"]
    return {
        "start_date": _parse_datetime(row["start_date"]) if row["start_date"] else None,
        "end_date": _parse_datetime(row["end_date"]) if row["end_date"] else None,
        "median": _parse_datetime(row["median"]) if row["median"] else None,
        "is_continuous": bool(row["is_continuous"]),
        "file_count": row["file_count"],
        "path": pth,
    }


def _dict_to_catalog_row(d: dict) -> dict:
    """Extract the fields needed for :meth:`LibraryCatalog.put_cluster`."""
    return {
        "start_date": str(d.get("start_date", "")),
        "end_date": str(d.get("end_date", "")),
        "median": str(d.get("median", "")),
        "is_continuous": bool(d.get("is_continuous", True)),
        "file_count": int(d.get("file_count", 0)),
    }


def get_or_create_library_cluster_ini_as_dataframe(
    library_path: str | Path,
    pool: Pool,
    force_deep_scan: bool = False,
    progress: ProgressSink | None = None,
) -> tuple[pd.DataFrame, list[Path]]:
    """Scan the folder for cluster info and return the dataframe with clusters.

    Uses a per-library SQLite catalog (``.filecluster.db``) to skip
    unchanged event folders on repeated runs.  When the catalog is not
    available (permissions, first run on old library, etc.) the function
    falls back to reading ``.cluster.ini`` files directly.

    Args:
        library_path:
        force_deep_scan:
        pool:
        progress: Optional sink notified of each event folder scanned.

    Returns:
        Tuple of:
            - dataframe with cluster info
            - list of empty directories
    """
    from filecluster.catalog import LibraryCatalog

    progress = progress or NullProgress()
    # strip trailing '/' and '\' if any
    library_path = str(library_path).rstrip("/").rstrip("\\")
    lib_name = Path(library_path).name
    logger.info(f"Scanning ini files in {library_path}")

    # --- open catalog (best-effort) ---
    catalog: LibraryCatalog | None = None
    try:
        catalog = LibraryCatalog.open(library_path)
    except Exception as exc:
        logger.debug(f"Could not open catalog for {library_path}: {exc}")

    _folder_count = 0

    def _on_folder_found(count: int) -> None:
        nonlocal _folder_count
        _folder_count += count
        progress.update_description(f"Discovering {lib_name} ({_folder_count} folders)")

    progress.update_description(f"Discovering folders in {lib_name}")
    subfolders = fast_scandir(library_path, _on_found=_on_folder_found)

    # remove the library path part from the library subfolders paths
    subfolders_root = [s.replace(f"{library_path}/", "") for s in subfolders]

    subs_labeled = identify_folder_types(subfolders_root)

    # TODO: support more types of events dirs
    # is_event or is_year_folder
    event_dirs = list(filter(is_event, subs_labeled))

    # --- split into cached (mtime match) and stale (needs scanning) ---
    cached_results: list[dict] = []
    stale_event_dirs: list[tuple[str, str]] = []

    for event_dir in event_dirs:
        event_dir_name = event_dir[0]
        pth = Path(library_path) / event_dir_name

        if force_deep_scan or catalog is None:
            stale_event_dirs.append(event_dir)
            continue

        try:
            disk_mtime = os.stat(pth).st_mtime
        except OSError:
            stale_event_dirs.append(event_dir)
            continue

        cached = catalog.get_cluster(event_dir_name)
        if cached is not None and cached["folder_mtime"] == disk_mtime:
            cached_results.append(_catalog_row_to_dict(cached, library_path))
        else:
            stale_event_dirs.append(event_dir)

    n_cached = len(cached_results)
    n_stale = len(stale_event_dirs)
    logger.debug(
        f"Catalog: {n_cached} cached, {n_stale} to scan"
        f" ({len(event_dirs)} total event dirs)"
    )

    # --- scan stale folders in parallel (same as before) ---
    progress.update_description(
        f"Scanning {n_stale} event folders in {lib_name}"
        + (f" ({n_cached} cached)" if n_cached else "")
    )
    progress.start(len(event_dirs), f"Scanning {lib_name}")

    # Advance progress for cached hits immediately
    for _ in range(n_cached):
        progress.advance()

    # Stale folders always get a deep scan: either the user asked for it
    # globally (-f) or the catalog detected a mtime change.  Without this,
    # get_this_ini would read a stale .cluster.ini instead of rescanning.
    pool_args = [(event_dir, True, library_path) for event_dir in stale_event_dirs]
    scanned_results: list[dict | Path | None] = []
    for result in pool.imap(_scan_event_dir, pool_args):
        scanned_results.append(result)
        progress.advance()

    # --- write freshly scanned results back to catalog ---
    if catalog is not None:
        for event_dir, result in zip(stale_event_dirs, scanned_results, strict=True):
            if isinstance(result, dict):
                event_dir_name = event_dir[0]
                pth = Path(library_path) / event_dir_name
                try:
                    disk_mtime = os.stat(pth).st_mtime
                except OSError:
                    disk_mtime = 0.0
                cat_row = _dict_to_catalog_row(result)
                catalog.put_cluster(event_dir_name, folder_mtime=disk_mtime, **cat_row)

        # Prune folders that no longer exist on disk
        existing_rel_paths = {ed[0] for ed in event_dirs}
        catalog.prune_clusters(existing_rel_paths)
        catalog.close()

    # --- combine cached + scanned into result lists ---
    res_dict_list = list(cached_results)
    res_dict_list.extend(d for d in scanned_results if isinstance(d, dict))
    res_empty_dir_list = [d for d in scanned_results if isinstance(d, Path)]

    df = pd.DataFrame(res_dict_list)

    df["target_path"] = None
    df["new_file_count"] = None
    n_clusters = len(df)
    try:
        n_files = (
            df.file_count.sum()
        )  # FIXME: KS: 2021-02-28: Error here - no file_count
        logger.debug(f"== Found {n_clusters} clusters. Total file count: {n_files}")
    except AttributeError as e:
        logger.error(f"No 'file_count' column in dataframe {e}")

    return df, res_empty_dir_list


def get_this_ini(
    event_dir: str, force_deep_scan: bool, library_path
) -> dict | Path | None:
    """Get stats of event_dir that are subdir of a library.

    Returns
        a single object that can be:
        Dictionary with characterization of the cluster - if the directory is
            not empty and as media files.
        Path object of the cluster - if the directory is empty
        None - if is not empty but no media files directly in that path.
    """
    event_dir_name = event_dir[0]
    pth = Path(library_path) / event_dir_name
    settings = FileClusterSettings()

    ini_path = Path(pth) / settings.ini_filename
    is_ini = os.path.isfile(ini_path)

    # Detect stale ini: if the folder was modified after the ini was last
    # written, the ini is outdated (e.g. the user added files to the folder).
    needs_rescan = force_deep_scan or not is_ini
    if is_ini and not force_deep_scan:
        try:
            folder_mtime = os.stat(pth).st_mtime
            ini_mtime = os.stat(ini_path).st_mtime
            if folder_mtime > ini_mtime:
                needs_rescan = True
                logger.debug(f"Stale ini detected for {event_dir_name} (folder newer)")
        except OSError:
            needs_rescan = True

    is_empty = False
    if needs_rescan:
        # calculate ini
        conf = configure_inbox_reader(in_dir_name=pth)

        f_name = conf.in_dir_name
        if os.listdir(f_name):
            media_df = get_media_df(conf.in_dir_name)
        else:
            logger.debug(f" - directory {f_name} is empty.")
            media_df = None
            is_empty = True

        if media_df is not None:
            time_granularity = int(conf.time_granularity.total_seconds())
            media_stats = get_media_stats(media_df, time_granularity)
            cluster_ini = initialize_cluster_info_dict(
                start=media_stats["date_min"],
                stop=media_stats["date_max"],
                is_continuous=media_stats["is_time_consistent"],
                median=media_stats["date_median"],
                file_count=media_stats["file_count"],
            )
            save_cluster_ini(cluster_ini, pth)

    if cluster_ini_r := read_cluster_ini_as_dict(pth):
        # return dict with cluster characterization
        ret = dict_from_ini_range_section(cluster_ini_r, pth)
    elif is_empty:
        ret = pth
    else:
        return None
    return ret


def dict_from_ini_range_section(cluster_ini_r, pth):
    """Read data from an ini section and adjust data types."""
    d = cluster_ini_r["Range"]
    # convert types
    d["is_continuous"] = str_to_bool(d["is_continuous"])
    d["median"] = _parse_datetime(d["median"])
    d["file_count"] = int(d["file_count"])
    d["path"] = pth
    return d


def initialize_cluster_info_dict(
    start: str,
    stop: str,
    is_continuous: bool,
    median: int | None = None,
    file_count: int | None = None,
) -> ConfigParser:
    """Return a dictionary that stores information on a cluster existing on the disk.

    Args:
      median:
      file_count:
      start: Cluster start datetime
      stop:         Cluster end datetime
      is_continuous: Indicate if there are no gaps (larger than allowed) in the cluster

    Returns:
        configparser object with predefined structure of information
    """
    cluster_ini = ConfigParser()
    cluster_ini["Range"] = {}
    cluster_ini["Range"]["start_date"] = str(start)
    cluster_ini["Range"]["end_date"] = str(stop)
    cluster_ini["Range"]["is_continuous"] = str(is_continuous)
    cluster_ini["Range"]["median"] = str(median)
    cluster_ini["Range"]["file_count"] = str(file_count)
    return cluster_ini


def save_cluster_ini(
    cluster_ini: ConfigParser,
    path: str | Path,
) -> None:
    """Save cluster information dictionary.

    Args:
      cluster_ini:      cluster info object to be saved on disk
      path:             path, where an object has to be saved

    Returns:
        None
    """
    settings = (
        FileClusterSettings()
    )  # FIXME: KS: 2025-04-24: are these proper settings?
    with open(Path(path) / settings.ini_filename, "w") as cluster_ini_file:
        cluster_ini.write(cluster_ini_file)


def read_cluster_ini_as_dict(
    path: Path,
) -> dict[str, dict[str, datetime | str | None]] | None:
    """Read cluster info from the path and return as dictionary.

    Args:
      path: full path to the ini file to be read

    Returns:
        dictionary with information from the cluster ini file.
    """
    settings = FileClusterSettings()
    cluster_ini = ConfigParser()
    cluster_ini.read(Path(path) / settings.ini_filename)

    raw_dict = {
        section: dict(cluster_ini.items(section)) for section in cluster_ini.sections()
    }
    if not raw_dict:
        return None

    cluster_dict = cast(dict[str, dict[str, datetime | str | None]], raw_dict)

    # correct timestamps
    dt_start = str(cluster_dict["Range"]["start_date"])
    dt_end = str(cluster_dict["Range"]["end_date"])

    cluster_dict["Range"]["start_date"] = _parse_datetime(dt_start)
    cluster_dict["Range"]["end_date"] = _parse_datetime(dt_end)
    return cluster_dict


def fast_scandir(
    dirname: str,
    _on_found: Callable[[int], None] | None = None,
) -> list[str]:
    """Get a list of folders of a given directory.

    Args:
        dirname: directory names that have to be scanned for folders
        _on_found: optional callback invoked with the running total of
            discovered folders after each directory level. Used by the
            progress system to show discovery feedback on slow mounts.

    Returns:
        list of folders
    """
    if dirname:
        subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
        if _on_found:
            _on_found(len(subfolders))
        for dirname in list(subfolders):
            subfolders.extend(fast_scandir(dirname, _on_found))
    else:
        subfolders = []
    return subfolders


def identify_folder_types(subfolders_list: list[str]) -> list[tuple[str, str]]:
    """Assign a folder-type label.

    Args:
      subfolders_list: list of subfolders to be labelled.

    Returns:
        list of tuples (subfolder_name, folder_type)
    """
    subs_labeled = []
    for s in subfolders_list:
        if is_year_folder(s):
            subs_labeled.append((s, "year"))
        elif is_sel_folder(s):
            subs_labeled.append((s, "sel"))
        elif is_event_folder(s):
            subs_labeled.append((s, "event"))
        elif is_event_subcategory_folder(s):
            subs_labeled.append((s, "sub_event"))
        else:
            subs_labeled.append((s, "unknown"))
    return subs_labeled


def is_year_folder(folder: str) -> bool:
    """Check if a given folder is a folder that stores all media from a given year.

    Valid year-folder starts with 19 or 20 followed by two digits

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if the folder is year-folder
    """
    last_part = Path(folder).parts[-1]
    return bool(re.match(r"^(19|20)\d{2}$", last_part))


def is_event_folder(folder: str) -> bool:
    """Check if a given folder is a top-level event folder.

    Check if a given folder is directly under year folder

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if the folder is event-folder
    """
    parts = Path(folder).parts
    if len(parts) < 2:
        return False
    # is under year-folder
    parent_part = parts[-2]
    return is_year_folder(parent_part)


def is_sel_folder(folder: str) -> bool:
    """Check if a given folder is a sel-type folder.

    Sel-type folder is a subfolder of the event folder dedicated to keeping
    best, selected images or videos.

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if the folder is sel-folder
    """
    return os.path.basename(folder) == "sel"


def is_event_subcategory_folder(folder: str) -> bool:
    """Check if a given folder is an event subfolder folder.

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if the folder is an event subfolder folder.
    """
    # TODO: KS: 2020-12-23: add separator (for given system - / or \) after year
    # is_year_in_the_path = bool(re.match(r"(19|20)\d{2}", folder))
    # FIXME: Implement
    return False


def validate_library_structure(library_dir):
    """Check if a library has a structure following the assumed convention.

    Folder types:
    - year
    - event
    - subevent
    - sel
    - out (?)

    Args:
        library_dir:
    Returns:
        True if there is no unknown folder-type in the library.
    """
    subfolders = fast_scandir(str(library_dir))
    labels = identify_folder_types(subfolders)
    return all(folder_type != "unknown" for _, folder_type in labels)


def is_event(item: tuple[str, str]) -> bool:
    """Check an item from a labelled list of folders if it is an event folder.

    Args:
      item: item from a labeled list of folders

    Returns:
        True if the folder in tuple is event-type
    """
    i_type = item[1]
    return i_type == "event"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan media library locations and get information on existing clusters."
    )

    # named parameters
    parser.add_argument(
        "-l",
        "--library",
        help="top-level directory of the media library.",
        type=str,
        action="append",
    )

    parser.add_argument(
        "-f",
        "--force-recalc",
        help="recalculate cluster info even if .cluster.ini files exists",
        action="store_true",
        default=False,
    )

    args = parser.parse_args()
    libs = args.library

    n_cpu = multiprocessing.cpu_count()
    logger.debug(f"Setting-up multiprocessing pool with {n_cpu} processes")
    with multiprocessing.Pool(processes=n_cpu) as pool:
        logger.debug("Pool ready to use")
        for lib in libs:
            _ = get_or_create_library_cluster_ini_as_dataframe(
                library_path=lib, pool=pool, force_deep_scan=args.force_recalc
            )
