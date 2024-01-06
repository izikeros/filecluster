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
import logging
import multiprocessing
import os
import re
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from pathlib import PosixPath
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd
from filecluster.configuration import INI_FILENAME
from filecluster.image_reader import configure_im_reader
from filecluster.image_reader import get_media_df
from filecluster.image_reader import get_media_stats

from filecluster import logger

# from: https://stackoverflow.com/a/21732183
def str_to_bool(s: str) -> bool:
    """Convert 'True' or 'False' provided as string to corresponding bool value."""
    if s == "True":
        return True
    elif s == "False":
        return False
    else:
        raise ValueError


def get_or_create_library_cluster_ini_as_dataframe(
    library_path: str, pool, force_deep_scan: bool = False
) -> Tuple[pd.DataFrame, List[Path]]:
    """Scan folder for cluster info and return dataframe with clusters.

    Args:
        library_path:
        force_deep_scan:
        pool:

    Returns:
        None
    """
    # strip trailing '/' and '\' if any
    library_path = library_path.rstrip("/").rstrip("\\")
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
        get_this_ini, zip(event_dirs, force_list, library_path_list)
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
) -> Union[dict, Optional[Path]]:
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
    is_ini = os.path.isfile(Path(pth) / INI_FILENAME)
    is_empty = False
    if force_deep_scan or not is_ini:
        # calculate ini
        conf = configure_im_reader(in_dir_name=pth)

        f_name = conf.in_dir_name
        if os.listdir(f_name):
            media_df = get_media_df(conf)
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
                is_continous=media_stats["is_normal"],
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
    d["is_continous"] = str_to_bool(d["is_continous"])
    d["median"] = datetime.strptime(d["median"], "%Y-%m-%d %H:%M:%S")
    d["file_count"] = int(d["file_count"])
    d["path"] = pth
    return d


def initialize_cluster_info_dict(
    start: str,
    stop: str,
    is_continous: bool,
    median: Optional[int] = None,
    file_count: Optional[int] = None,
) -> ConfigParser:
    """Return dictionary that store information on cluster existing on the disk.

    Args:
      median:
      file_count:
      start:        Cluster start datetime
      stop:         Cluster end datetime
      is_continous: Indicate if there are not gaps (larger than allowed) in the cluster

    Returns:
        configparser object with predefined structure of information
    """
    cluster_ini = ConfigParser()
    cluster_ini["Range"] = {}
    cluster_ini["Range"]["start_date"] = str(start)
    cluster_ini["Range"]["end_date"] = str(stop)
    cluster_ini["Range"]["is_continous"] = str(is_continous)
    cluster_ini["Range"]["median"] = str(median)
    cluster_ini["Range"]["file_count"] = str(file_count)
    return cluster_ini


def save_cluster_ini(
    cluster_ini: ConfigParser,
    path: str,
) -> None:
    """Save cluster information dictionary.

    Args:
      cluster_ini:      cluster info object to be saved on disk
      path:             path, where object has to be saved

    Returns:
        None
    """
    with open(Path(path) / INI_FILENAME, "w") as cluster_ini_file:
        cluster_ini.write(cluster_ini_file)


def read_cluster_ini_as_dict(
    path: PosixPath,
) -> Optional[Dict[str, Dict[str, Union[datetime, str]]]]:
    """Read cluster info from the path and return as dictionary.

    Args:
      path: full path to the ini file to be read

    Returns:
        dictionary with information from the cluster ini file.
    """
    cluster_ini = ConfigParser()
    cluster_ini.read(Path(path) / ".cluster.ini")

    # convert parser object to dict
    cluster_dict = {
        section: dict(cluster_ini.items(section)) for section in cluster_ini.sections()
    }

    if cluster_dict:
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
    else:
        return None


def fast_scandir(dirname: str) -> List[str]:
    """Get list of subfolders of given directory.

    Args:
        dirname: directory nam that has to be scanned for subfolders

    Returns:
        list of subfolders
    """
    if dirname:
        subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
        for dirname in list(subfolders):
            subfolders.extend(fast_scandir(dirname))
    else:
        subfolders = []
    return subfolders


def identify_folder_types(subfolders_list: List[str]) -> List[Tuple[str, str]]:
    """Assign folder-type label.

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
    """Check if given folder is a folder that store all media from given year.

    Valid year-folder starts with 19 or 20 followed by two digits

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is year-folder
    """
    last_part = Path(folder).parts[-1]
    is_four_digits = bool(re.match(r"^(19|20)\d{2}$", last_part))
    return is_four_digits


def is_event_folder(folder: str) -> bool:
    """Check if given folder is a top-level event folder.

    Check if given folder is directly under year folder

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is event-folder
    """
    # is under year-folder
    parrent_part = Path(folder).parts[-2]
    is_parrent_year = is_year_folder(parrent_part)
    # last_part = Path(folder).parts[-1]
    # starts_with_date_timestamp = bool(re.match(r"^\[\d\d\d\d/", last_part))
    return is_parrent_year  # and starts_with_date_timestamp


def is_sel_folder(folder: str) -> bool:
    """Check if given folder is a sel-type folder.

    Sel-type folder is a sub-folder of event folder dedicated for keeping
    best, selected images or videos.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is sel-folder
    """
    return os.path.basename(folder) == "sel"


def is_event_subcategory_folder(folder: str) -> bool:
    """Check if given folder is a event sub-folder folder.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is a event sub-folder folder.
    """
    # TODO: KS: 2020-12-23: add separator (for given system - / or \) after year
    # is_year_in_the_path = bool(re.match(r"(19|20)\d{2}", folder))
    # FIXME: Implement
    return False


def is_video_folder(folder: str) -> bool:
    """Check if given folder is a video folder.

    Sometimes, videos can be kept in separate video folder.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is event-folder
    """
    # check if name is sel
    pass


def validate_library_structure(library_dir):
    """Check if library has structure following assumed convention.

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


def is_event(item: Tuple[str, str]) -> bool:
    """Check item from labelled list of folders if it is event folder.

    Args:
      item: item from labelled list of folders

    Returns:
        True if folder in tuple is event-type
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
