#!/usr/bin/env python3
"""Scan recursively directory and get information on clusters.

Module with function to scan the directories and obtain existing cluster info,
by reading or creating .cluster.ini files.

Usage:
./update_clusters.py -f -l tests/zdjecia

    -f force recalculation of inin file
    -l path to library
"""
# TODO: KS: 2020-12-28: Consider changing data format from ini to yaml

import argparse
import multiprocessing
import os
import re
from configparser import ConfigParser
from datetime import datetime
from multiprocessing.pool import Pool
from pathlib import Path

import pandas as pd

from filecluster import logger

# from filecluster.configuration import ini_filename
from filecluster.configuration import FileClusterSettings
from filecluster.image_reader import configure_im_reader, get_media_df, get_media_stats


def str_to_bool(s: str) -> bool:
    """Convert 'True' or 'False' provided as string to the corresponding bool value."""
    if s == "True":
        return True
    elif s == "False":
        return False
    else:
        raise ValueError


def get_or_create_library_cluster_ini_as_dataframe(
    library_path: str | Path, pool: Pool, force_deep_scan: bool = False
) -> tuple[pd.DataFrame, list[Path]]:
    """Scan folder for cluster info and return dataframe with clusters.

    Args:
        library_path:
        force_deep_scan:
        pool:

    Returns:
        Tuple of:
            - dataframe with cluster info
            - list of empty directories
    """
    # strip trailing '/' and '\' if any
    library_path = str(library_path).rstrip("/").rstrip("\\")
    logger.info(f"Scanning ini files in {library_path}")

    subfolders = fast_scandir(library_path)

    # remove library path part from the library subfolders paths
    subfolders_root = [s.replace(f"{library_path}/", "") for s in subfolders]

    subs_labeled = identify_folder_types(subfolders_root)
    # TODO: support more types of events dirs
    # is_event or is_year_folder
    event_dirs = list(filter(is_event, subs_labeled))
    n_event_dirs = len(event_dirs)

    # parallel version
    force_list = n_event_dirs * [force_deep_scan]
    library_path_list = n_event_dirs * [library_path]

    res_list = pool.starmap(
        get_this_ini, zip(event_dirs, force_list, library_path_list, strict=False)
    )
    res_dict_list = [d for d in res_list if isinstance(d, dict)]
    res_empty_dir_list = [d for d in res_list if isinstance(d, Path)]

    df = pd.DataFrame(res_dict_list)

    df["target_path"] = None
    df["new_file_count"] = None
    n_clusters = len(df)
    try:
        n_files = (
            df.file_count.sum()
        )  # FIXME: KS: 2021-02-28: Error here - no file_count
        logger.debug(f"== Found {n_clusters} clusters. Total file count: {n_files}")
    except Exception:
        logger.error("No 'file_count' column in dataframe")

    return df, res_empty_dir_list


def get_this_ini(
    event_dir: str, force_deep_scan: bool, library_path
) -> dict | Path | None:
    """Get stats of event_dir that is subdir of library.

    Returns
        single object that can be:
        Dictionary with characterization of the cluster - if directory is
            not empty and as media files.
        Path object of the cluster - if directory is empty
        None - if is not empty but no media files directly in that path.
    """
    event_dir_name = event_dir[0]
    pth = Path(library_path) / event_dir_name
    settings = (
        FileClusterSettings()
    )  # FIXME: KS: 2025-04-24: are these proper settings?
    is_ini = os.path.isfile(Path(pth) / settings.ini_filename)
    is_empty = False
    if force_deep_scan or not is_ini:
        # calculate ini
        conf = configure_im_reader(in_dir_name=pth)

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
    """Read data from ini section and adjust data types."""
    d = cluster_ini_r["Range"]
    # convert types
    d["is_continuous"] = str_to_bool(d["is_continuous"])
    try:
        d["median"] = datetime.strptime(d["median"], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        d["median"] = datetime.strptime(d["median"], "%Y-%m-%d %H:%M:%S.%f")
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
    """Return dictionary that store information on cluster existing on the disk.

    Args:
      median:
      file_count:
      start:        Cluster start datetime
      stop:         Cluster end datetime
      is_continuous: Indicate if there are not gaps (larger than allowed) in the cluster

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
    cluster_ini = ConfigParser()
    cluster_ini.read(Path(path) / ".cluster.ini")

    if not (
        cluster_dict := {
            section: dict(cluster_ini.items(section))
            for section in cluster_ini.sections()
        }
    ):
        return None
    # correct timestamps
    dt_start = cluster_dict["Range"]["start_date"]
    dt_end = cluster_dict["Range"]["end_date"]

    try:
        cluster_dict["Range"]["start_date"] = datetime.strptime(
            dt_start, "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        cluster_dict["Range"]["start_date"] = None

    try:
        cluster_dict["Range"]["end_date"] = datetime.strptime(
            dt_end, "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        cluster_dict["Range"]["end_date"] = None
    return cluster_dict


def fast_scandir(dirname: str) -> list[str]:
    """Get a list of folders of a given directory.

    Args:
        dirname: directory names that have to be scanned for folders

    Returns:
        list of folders
    """
    if dirname:
        subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
        for dirname in list(subfolders):
            subfolders.extend(fast_scandir(dirname))
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
    # is under year-folder
    parrent_part = Path(folder).parts[-2]
    return is_year_folder(parrent_part)


def is_sel_folder(folder: str) -> bool:
    """Check if given folder is a sel-type folder.

    Sel-type folder is a subfolder of the event folder dedicated to keeping
    best, selected images or videos.

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if folder is sel-folder
    """
    return os.path.basename(folder) == "sel"


def is_event_subcategory_folder(folder: str) -> bool:
    """Check if given folder is an event subfolder folder.

    Args:
      folder: path to the folder that has to be examined.

    Returns:
        True if folder is an event subfolder folder.
    """
    # TODO: KS: 2020-12-23: add separator (for given system - / or \) after year
    # is_year_in_the_path = bool(re.match(r"(19|20)\d{2}", folder))
    # FIXME: Implement
    return False


def validate_library_structure(library_dir):
    """Check if a library has structure following the assumed convention.

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
    # check if there are no 'unknown-type' folders in the structure.


def is_event(item: tuple[str, str]) -> bool:
    """Check item from labelled list of folders if it is event folder.

    Args:
      item: item from a labelled list of folders

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
    pool = multiprocessing.Pool(processes=n_cpu)
    logger.debug("Pool ready to use")

    for lib in libs:
        _ = get_or_create_library_cluster_ini_as_dataframe(
            library_path=lib, pool=pool, force_deep_scan=args.force_recalc
        )
