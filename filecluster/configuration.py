import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Optional

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Databases paths and filenames
# DB_FILE_SQLITE3 = 'filecluster_db.sqlite3'
DB_FILE_CLUSTERS_PICKLE = 'clusters.p'
DB_FILE_MEDIA_PICKLE = 'media.p'

DB_PATH_WINDOWS = 'h:\\zdjecia\\'
DB_PATH_LINUX = '/tmp/'
CLUSTER_COLUMN_IN_CLUSTER_DB = 'cluster_id'
DELETE_DB = True  # delete database during the start - provide clean start for development mode

# Filename extensions in scope of clustering
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.dng', '.cr2']
VIDEO_EXTENSIONS = ['.mp4', '.3gp', 'mov']

# Locations for media: Inbox/Outbox/Library
INBOX_PATH_WINDOWS = 'h:\\incomming\\'
INBOX_PATH_WINDOWS_DEV = 'h:\\incomming'

INBOX_PATH_LINUX = '/media/root/Foto/incomming/'
INBOX_PATH_LINUX_DEV = '/home/safjan/Pictures'

INBOX_DIR = 'inbox'
INBOX_DIR_DEV = 'inbox_test_a'

OUTBOX_DIR = 'inbox_clust'
OUTBOX_DIR_DEV = 'inbox_clust_test'

LIBRARY_WINDOWS = ['h:\\zdjecia\\']
LIBRARY_WINDOWS_DEV = ['']
LIBRARY_LINUX = ['/media/root/Foto/zdjecia/']
LIBRARY_LINUX_DEV = ['zdjecia']

GENERATE_THUMBNAILS = False  # generate thumbnail to be stored in pandas dataframe during the processing. Might be used in notebook.


class Driver(Enum):
    SQLITE = 1
    DATAFRAME = 2


class AssignDateToClusterMethod(Enum):
    RANDOM = 1
    MEDIAN = 2


class ClusteringMethod(Enum):
    TIME_GAP = 1


class CopyMode(Enum):
    COPY = 1
    MOVE = 2
    NOP = 3


@dataclass
class Config:
    """Dataclass for keeping configuration parameters.

    - `in_dir_name` - Name of the input directory. Inbox where new media to be
    discovered

    - 'out_dir_name' - Name of output directory where cluster directories are located/
    """
    in_dir_name: Path
    out_dir_name: Path
    watch_folders: List[str]
    db_file_clusters: Path
    db_file_media: Path
    # db_file: Path
    image_extensions: List[str]
    video_extensions: List[str]
    time_granularity: timedelta
    cluster_col: str
    assign_date_to_clusters_method: AssignDateToClusterMethod
    clustering_method: ClusteringMethod
    mode: CopyMode
    db_driver: Driver
    generate_thumbnails: bool
    delete_db: bool

    def __repr__(self):
        rep = []
        for p in self.__dataclass_fields__.keys():
            rep.append(f'{p}:\t{self.__getattribute__(p)}')
        return '\n'.join(rep)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)


def configure_db_path() -> str:
    """Configure database path depending on detected operating system."""
    if os.name == 'nt':
        db_pth = DB_PATH_WINDOWS
    else:
        db_pth = DB_PATH_LINUX
    return db_pth


def configure_paths_for_this_os() -> Tuple[str, str, List[str]]:
    """Configure production paths depending on detected operating system.

    Paths to configure:
    - inbox,
    - outbox
    - library."""
    if os.name == 'nt':
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
     This is your media library."""
    if os.name == 'nt':
        pth = LIBRARY_WINDOWS
    else:
        pth = LIBRARY_LINUX
    return pth


def setup_directory_for_database(config: Config, db_dir: Optional[str]):
    """Setup common directory for storing databases."""
    if not db_dir:
        # if db dir not provided - pus db files in output directory
        db_dir = config.out_dir_name

    # TODO: KS: 2020-05-23: do not use picke (use csv for accessibility? )
    #   pickle do not have problems with escaping
    config.db_file_clusters = str(Path(db_dir) / DB_FILE_CLUSTERS_PICKLE)
    config.db_file_media = str(Path(db_dir) / DB_FILE_MEDIA_PICKLE)
    return config


def get_proper_mode_config(is_development_mode: bool) -> Config:
    """Get config for development or production mode."""
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
    db_pth = configure_db_path()
    # db_file = os.path.join(db_pth, DB_FILE_SQLITE3)
    db_file_clusters = os.path.join(db_pth, DB_FILE_CLUSTERS_PICKLE)
    db_file_media = os.path.join(db_pth, DB_FILE_MEDIA_PICKLE)

    # ensure extensions are lowercase
    image_extensions = [ext.lower() for ext in IMAGE_EXTENSIONS]
    video_extensions = [ext.lower() for ext in VIDEO_EXTENSIONS]

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    assign_date_to_clusters_method = AssignDateToClusterMethod.RANDOM
    conf_dict = {
        'in_dir_name': inbox_path,
        'out_dir_name': outbox_path,
        'db_file_clusters': db_file_clusters,
        'db_file_media': db_file_media,
        # 'db_file': db_file,
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'time_granularity': max_gap,
        'cluster_col': CLUSTER_COLUMN_IN_CLUSTER_DB,
        'assign_date_to_clusters_method': assign_date_to_clusters_method,
        # method that is used to group images, default: assume different events
        # are separated by significant time gape (max_gap config parameter)
        'clustering_method': ClusteringMethod.TIME_GAP,
        'mode': CopyMode.MOVE,
        'db_driver':
            Driver.DATAFRAME,  # driver for db_file, can be: dataframe | sqlite
        'generate_thumbnails': GENERATE_THUMBNAILS,
        'delete_db': DELETE_DB,
        'watch_folders': library_paths
    }
    config = Config(**conf_dict)
    return config


def get_development_config(os_name=os.name) -> Config:
    """Configuration for development phase.
    Key features of development mode:

    - use copy operation instead of move (source files remain in their original locations)
    - use different paths than in the production (for DBs and in/out media folders)
    """
    logger.warning("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    # move/copy/nop
    config.mode = CopyMode.COPY

    # overwrite defaults with development specific params
    if os_name == 'nt':
        pth = INBOX_PATH_WINDOWS_DEV
    else:
        pth = INBOX_PATH_LINUX_DEV

    config.in_dir_name = os.path.join(pth, INBOX_DIR_DEV)
    config.out_dir_name = os.path.join(pth, OUTBOX_DIR_DEV)

    # config.db_file = os.path.join(pth, DB_FILE_SQLITE3)
    config.db_file_media = os.path.join(pth, DB_FILE_MEDIA_PICKLE)
    config.db_file_clusters = os.path.join(pth, DB_FILE_CLUSTERS_PICKLE)

    if os_name == 'nt':
        config.watch_folders = LIBRARY_WINDOWS_DEV
    else:
        config.watch_folders = LIBRARY_LINUX_DEV

    return config


def override_config_with_cli_params(config: Config, inbox_dir: str,
                                    no_operation: bool, output_dir: str,
                                    db_driver: Driver,
                                    watch_dir_list: List[str]) -> Config:
    """Use CLI arguments to override default configuration.
    :param watch_dir_list:
    :param db_driver:
    :param config:
    :param inbox_dir:
    :param no_operation:
    :param output_dir:
    :return:
    """
    if inbox_dir:
        config.in_dir_name = inbox_dir
    if output_dir:
        config.out_dir_name = output_dir
    if no_operation:
        config.mode = CopyMode.NOP
    if db_driver:
        config.db_driver = db_driver
    if watch_dir_list:
        config.watch_folders = watch_dir_list
    return config
