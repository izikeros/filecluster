import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Optional

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Databases paths and filenames
CLUSTER_DB_FILENAME = ".clusters.csv"

# standard filename for cluster info file to be placed in cluster directory
INI_FILENAME = ".cluster.ini"

# Filename extensions in scope of clustering
IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".dng", ".cr2"]
VIDEO_EXTENSIONS = [".mp4", ".3gp", "mov"]

# Locations for media: Inbox/Outbox/Library
INBOX_PATH_WINDOWS = "h:\\incomming\\"
INBOX_PATH_WINDOWS_DEV = "h:\\incomming"

INBOX_PATH_LINUX = "/media/root/Foto/incomming/"
INBOX_PATH_LINUX_DEV = "./"

INBOX_DIR = "inbox"
INBOX_DIR_DEV = "inbox_test_a"

OUTBOX_DIR = "inbox_clust"
OUTBOX_DIR_DEV = "inbox_clust_test"

LIBRARY_WINDOWS = ["h:\\zdjecia\\"]
LIBRARY_WINDOWS_DEV = [""]
LIBRARY_LINUX = ["/media/root/Foto/zdjecia/"]
LIBRARY_LINUX_DEV = ["zdjecia", "clusters"]

FORCE_DEEP_SCAN = False

ASSIGN_TO_CLUSTERS_EXISTING_IN_LIBS = False
SKIP_DUPLICATED_EXISTING_IN_LIBS = False

# keep this columns in sync with: initialize_cluster_info_dict() and
CLUSTER_DF_COLUMNS = [
    "cluster_id",
    "start_date",
    "end_date",
    "median",
    "is_continous",
    "path",
    "target_path",
    "file_count",
    "new_file_count",
]

MEDIA_DF_COLUMNS = [
    "file_name",
    "m_date",
    "c_date",
    "exif_date",
    "date",
    "size",
    "hash_value",
    "image",
    "is_image",
    "status",
    "duplicated_to",
    "duplicated_cluster",
]


class Status(Enum):
    UNKNOWN = 0
    NEW_CLUSTER = 1
    EXISTING_CLUSTER = 2
    DUPLICATE = 3


class Driver(Enum):
    """ """

    SQLITE = 1
    DATAFRAME = 2


class AssignDateToClusterMethod(Enum):
    """Method for selecting representative date for the cluster.

    RANDOM - comming from random file in the cluster
    MEDIAN - median datetime from the cluster

    Args:

    Returns:

    """

    RANDOM = 1
    MEDIAN = 2


class ClusteringMethod(Enum):
    """ """

    # method that is used to group images, default: assume different events
    # are separated by significant time gape (max_gap config parameter)
    # TODO: KS: 2020-12-15: Any alternative ideas? If no - delete this class
    TIME_GAP = 1


class CopyMode(Enum):
    """ """

    COPY = 1
    MOVE = 2
    NOP = 3


@dataclass
class Config:
    """Dataclass for keeping configuration parameters.

    - `in_dir_name` - Name of the input directory. Inbox where new media to be
    discovered

    - 'out_dir_name' - Name of output directory where cluster directories are located/

    Args:

    Returns:

    """

    in_dir_name: Path
    out_dir_name: Path
    watch_folders: List[str]
    file_cluster_dbs: List[Path]
    image_extensions: List[str]
    video_extensions: List[str]
    time_granularity: timedelta
    assign_date_to_clusters_method: AssignDateToClusterMethod
    clustering_method: ClusteringMethod
    mode: CopyMode
    force_deep_scan: bool
    assign_to_clusters_existing_in_libs: bool
    skip_duplicated_existing_in_libs: bool

    def __repr__(self):
        rep = []
        for p in self.__dataclass_fields__.keys():
            rep.append(f"{p}:\t{self.__getattribute__(p)}")
        return "\n".join(rep)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)


def configure_paths_for_this_os() -> Tuple[str, str, List[str]]:
    """Configure production paths depending on detected operating system.

    Paths to configure:
    - inbox,
    - outbox
    - library.

    Args:

    Returns:

    """
    if os.name == "nt":
        pth = INBOX_PATH_WINDOWS
        library_paths = LIBRARY_WINDOWS
    else:
        pth = INBOX_PATH_LINUX
        library_paths = LIBRARY_LINUX
    inbox_path = os.path.join(pth, INBOX_DIR)
    outbox_path = os.path.join(pth, OUTBOX_DIR)
    return inbox_path, outbox_path, library_paths


def configure_watch_folder_paths() -> List:
    """Configure watch folder path depending on os.

    Watch folder is a location of official folder with media.
     This is your media library.

    Args:

    Returns:

    """
    if os.name == "nt":
        pth = LIBRARY_WINDOWS
    else:
        pth = LIBRARY_LINUX
    return pth


def get_proper_mode_config(is_development_mode: bool) -> Config:
    """Get config for development or production mode.

    Args:
      is_development_mode: bool:

    Returns:

    """
    # get proper config
    if is_development_mode:
        config = get_development_config()
    else:
        config = get_default_config()
    return config


def get_default_config() -> Config:
    """Provide default configuration that can be further modified."""

    # path to files to be clustered
    inbox_path, outbox_path, library_paths = configure_paths_for_this_os()

    # one cluster db per each library
    file_cluster_dbs = [os.path.join(lib, CLUSTER_DB_FILENAME) for lib in library_paths]

    # ensure extensions are lowercase
    image_extensions = [ext.lower() for ext in IMAGE_EXTENSIONS]
    video_extensions = [ext.lower() for ext in VIDEO_EXTENSIONS]

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    assign_date_to_clusters_method = AssignDateToClusterMethod.MEDIAN
    conf_dict = {
        "in_dir_name": inbox_path,
        "out_dir_name": outbox_path,
        "file_cluster_dbs": file_cluster_dbs,
        "image_extensions": image_extensions,
        "video_extensions": video_extensions,
        "time_granularity": max_gap,
        "assign_date_to_clusters_method": assign_date_to_clusters_method,
        "clustering_method": ClusteringMethod.TIME_GAP,
        "mode": CopyMode.MOVE,
        "watch_folders": library_paths,
        "force_deep_scan": FORCE_DEEP_SCAN,
        "assign_to_clusters_existing_in_libs": ASSIGN_TO_CLUSTERS_EXISTING_IN_LIBS,
        "skip_duplicated_existing_in_libs": SKIP_DUPLICATED_EXISTING_IN_LIBS,
    }
    config = Config(**conf_dict)
    return config


def get_development_config(os_name=os.name) -> Config:
    """Configuration for development phase.
    Key features of development mode:

    - use copy operation instead of move (source files remain in their original locations)
    - use different paths than in the production (for DBs and in/out media folders)

    Args:
      os_name:  (Default value = os.name)

    Returns:

    """
    logger.warning("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    # move/copy/nop
    config.mode = CopyMode.COPY

    # overwrite defaults with development specific params
    if os_name == "nt":
        pth = INBOX_PATH_WINDOWS_DEV
    else:
        pth = INBOX_PATH_LINUX_DEV

    config.in_dir_name = os.path.join(pth, INBOX_DIR_DEV)
    config.out_dir_name = os.path.join(pth, OUTBOX_DIR_DEV)

    if os_name == "nt":
        config.watch_folders = LIBRARY_WINDOWS_DEV
    else:
        config.watch_folders = LIBRARY_LINUX_DEV

    watch_folders = config.watch_folders
    if watch_folders:
        file_cluster_dbs = [
            os.path.join(lib, CLUSTER_DB_FILENAME) for lib in watch_folders
        ]
        config.file_cluster_dbs = file_cluster_dbs

    return config


def override_config_with_cli_params(
    config: Config,
    inbox_dir: str,
    no_operation: bool,
    copy_mode: bool,
    output_dir: str,
    watch_dir_list: List[str],
    force_deep_scan: Optional[bool] = None,
    drop_duplicates: Optional[bool] = None,
    use_existing_clusters: Optional[bool] = None,
) -> Config:
    """Use CLI arguments to override default configuration.

    Args:
      use_existing_clusters:
      drop_duplicates:
      config: Config:
      inbox_dir: str:
      no_operation: bool:
      output_dir: str:
      watch_dir_list: List[str]:
      force_deep_scan: Optional[bool]:  (Default value = None)

    Returns:
        Config object updated with cli params
    """
    if inbox_dir is not None:
        config.in_dir_name = inbox_dir
    if output_dir is not None:
        config.out_dir_name = output_dir

    # TODO: better handle these two mutually exclusive options
    if no_operation is not None:
        config.mode = CopyMode.NOP
    if copy_mode is not None:
        config.mode = CopyMode.COPY

    if watch_dir_list is not None:
        config.watch_folders = watch_dir_list
        file_cluster_dbs = [
            os.path.join(lib, CLUSTER_DB_FILENAME) for lib in watch_dir_list
        ]
        config.file_cluster_dbs = file_cluster_dbs
    if force_deep_scan is not None:
        config.force_deep_scan = force_deep_scan
    if drop_duplicates is not None:
        config.skip_duplicated_existing_in_libs = drop_duplicates
    if use_existing_clusters is not None:
        config.assign_to_clusters_existing_in_libs = use_existing_clusters

    # Sanity check: if using 'drop_duplicates' or 'use_existing_clusters' - need to provide watch_dirs
    if (
        config.skip_duplicated_existing_in_libs
        or config.assign_to_clusters_existing_in_libs
    ):
        assert (
            len(watch_dir_list) > 0
        ), "Need to provide watch folders if using 'drop_duplicates' or 'use_existing_clusters'"
    return config
