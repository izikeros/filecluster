"""Module with function to scan the directories and obtain existing cluster info."""
import os
import re
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# standard filename for cluster info file to be placed in cluster directory
INI_FILENAME: str = ".cluster.ini"


def initialize_cluster_info_dict(
    start: str, stop: str, is_continous: bool
) -> ConfigParser:
    """Return dictionary that store information on cluster existing on the disk.

    Args:
      start:        Cluster start datetime
      stop:         Cluster end datetime
      is_continous: Indicate if there are not gaps (larger than allowed) in the cluster

    Returns:
        configparser object with predefined structure of information
    """
    cluster_ini = ConfigParser()
    cluster_ini["Range"] = {}
    cluster_ini["Range"]["start"] = str(start)
    cluster_ini["Range"]["stop"] = str(stop)
    cluster_ini["Range"]["is_continous"] = str(is_continous)
    return cluster_ini


def save_cluster_ini(
    cluster_ini: ConfigParser,
    path: str,
):
    """Save cluster information dictionary.

    Args:
      cluster_ini:      cluster info object to be saved on disk
      path:             path, where object has to be saved

    Returns:
        None
    """
    with open(Path(path) / INI_FILENAME, "w") as cluster_ini_file:
        cluster_ini.write(cluster_ini_file)


def read_cluster_ini_as_dict(path):
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

    # correct timestamps
    cluster_dict["Range"]["start"] = datetime.strptime(
        cluster_dict["Range"]["start"], "%Y-%m-%d %H:%M:%S"
    )
    cluster_dict["Range"]["stop"] = datetime.strptime(
        cluster_dict["Range"]["stop"], "%Y-%m-%d %H:%M:%S"
    )

    return cluster_dict


def fast_scandir(dirname: str) -> List[str]:
    """Get list of subfolders of given directory.

    Args:
        dirname: directory nam that has to be scanned for subfolders

    Returns:
        list of subfolders
    """
    subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
    for dirname in list(subfolders):
        subfolders.extend(fast_scandir(dirname))
    return subfolders


def identify_folder_types(subfolders_list) -> List[Tuple[str, str]]:
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
        else:
            subs_labeled.append((s, "unknown"))
    return subs_labeled


def is_year_folder(folder: str) -> bool:
    """Check if given folder is a folder that store all media from given year.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is year-folder
    """
    is_four_digits = re.match(r"^\d\d\d\d$", folder)
    return is_four_digits


def is_event_folder(folder: str) -> bool:
    """Check if given folder is a top-level event folder.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is event-folder
    """

    # is under year-folder
    starts_with_4_digits = re.match(r"^\d\d\d\d/", folder)
    is_first_level_event = folder.count("/") == 1
    return is_first_level_event and starts_with_4_digits


def is_sel_folder(folder: str) -> bool:
    """Check if given folder is a sel-type folder.

    Sel-type folder is a sub-folder of event folder dedicated for keeping
    best, selected images or videos.

    Args:
      folder: path to folder that has to be examined.

    Returns:
        True if folder is sel-folder
    """
    starts_with_4_digits = re.match(r"^\d\d\d\d/", folder)
    return starts_with_4_digits and os.path.basename(folder) == "sel"


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