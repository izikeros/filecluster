"""Module with function to scan the directories and obtain existing cluster info."""
import os
import re
from configparser import ConfigParser
from datetime import datetime
from pathlib import PosixPath, Path
from typing import Dict, Union, List, Tuple, Optional

from filecluster.configuration import INI_FILENAME


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
    is_year_in_the_path = bool(re.match(r"(19|20)\d{2}", folder))
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
