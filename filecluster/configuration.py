import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# === Configuration
# generate thumbnail to be stored in pandas dataframe during the processing.
# Might be used in notebook.
# GENERATE_THUMBNAIL = False

# in dev mode path are set to development datasets

# delete database during the start - provide clean start for development mode
# DELETE_DB = True

# for more configuration options see: utils.get_development_config() and get_default_config()


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
    in_dir_name: Path
    out_dir_name: Path
    db_file_clusters: Path
    db_file_media: Path
    db_file: Path
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
        repr = []
        for p in self.__dataclass_fields__.keys():
            repr.append(f'{p}:\t{self.__getattribute__(p)}')
        return '\n'.join(repr)


def get_default_config():
    # path to files to be clustered

    inbox_path, outbox_path = configure_inbox_outbox_paths()
    db_pth = configure_db_path()

    db_file = os.path.join(db_pth, 'filecluster_db.sqlite3')
    db_file_clusters = os.path.join(db_pth, 'clusters.p')
    db_file_media = os.path.join(db_pth, 'media.p')

    # Filename extensions in scope of clustering
    image_extensions = ['.jpg', '.jpeg', '.dng', '.cr2']
    video_extensions = ['.mp4', '.3gp', 'mov']

    # ensure extensions are lowercase
    image_extensions = [ext.lower() for ext in image_extensions]
    video_extensions = [ext.lower() for ext in video_extensions]

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    assign_date_to_clusters_method = AssignDateToClusterMethod.RANDOM

    conf_dict = {
        'in_dir_name': inbox_path,
        'out_dir_name': outbox_path,
        'db_file_clusters': db_file_clusters,
        'db_file_media': db_file_media,
        'db_file': db_file,
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'time_granularity': max_gap,
        'cluster_col': 'cluster_id',
        'assign_date_to_clusters_method': assign_date_to_clusters_method,
        # method that is used to group images, default: assume different events
        # are separated by significant time gape (max_gap config parameter)
        'clustering_method': ClusteringMethod.TIME_GAP,
        'mode': CopyMode.MOVE,
        'db_driver': Driver.SQLITE,  # dataframe | sqlite
        'generate_thumbnails': False,
        'delete_db': True,
    }
    config = Config(**conf_dict)
    return config


def configure_db_path():
    # Configure database path
    if os.name == 'nt':
        db_pth = 'h:\\zdjecia\\'
    else:
        db_pth = '/media/root/Foto/zdjecia/'
    return db_pth


def configure_inbox_outbox_paths():
    # Configure inbox and outbox paths
    if os.name == 'nt':
        pth = 'h:\\incomming\\'
    else:
        pth = '/media/root/Foto/incomming/'
    inbox_dir = 'inbox'
    outbox_dir = 'inbox_clust'
    inbox_path = os.path.join(pth, inbox_dir)
    outbox_path = os.path.join(pth, outbox_dir)
    return inbox_path, outbox_path


def get_development_config():
    """Configuration for development phase."""
    logger.warning("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    # move/copy/nop
    config.mode = CopyMode.COPY

    # overwrite defaults with development specific params
    if os.name == 'nt':
        pth = 'h:\\incomming'
    else:
        pth = '/home/safjan/Pictures'

    inbox_dir = 'inbox_test_a'
    outbox_dir = 'inbox_clust_test'

    config.in_dir_name = os.path.join(pth, inbox_dir)
    config.out_dir_name = os.path.join(pth, outbox_dir)

    config.db_file = os.path.join(pth, 'filecluster_db.sqlite3')
    config.db_file_media = os.path.join(pth, 'media.p')
    config.db_file_clusters = os.path.join(pth, 'clusters.p')
    return config
