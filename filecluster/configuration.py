import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List

# === Configuration
# generate thumbnail to be stored in pandas dataframe during the processing.
# Might be used in notebook.
# GENERATE_THUMBNAIL = False
# in dev mode path are set to development datasets
# delete database during the start - provide clean start for development mode
# DELETE_DB = True
# for more configuration options see: utils.get_development_config() and get_default_config()

@dataclass
class Config:
    in_dir_name: Path
    out_dir_name: Path
    db_file_clusters: Path
    db_file_media: Path
    db_file: Path
    image_extensions: List[str]
    video_extensions: List[str]
    granularity_minutes: timedelta
    cluster_col: str
    assign_date_to_clusters_method: str  # TODO: KS: 2020-05-22: enum
    clustering_method: str  # TODO: KS: 2020-05-22: enum
    mode: str  # TODO: KS: 2020-05-22: enum
    db_driver: str  # TODO: KS: 2020-05-22: enum
    generate_thumbnails: bool
    delete_db: bool


def get_default_config():
    # path to files to be clustered

    # Configure inbox
    if os.name == 'nt':
        pth = 'h:\\incomming\\'
    else:
        pth = '/media/root/Foto/incomming/'

    inbox_dir = 'inbox'
    outbox_dir = 'inbox_clust'

    inbox_path = os.path.join(pth, inbox_dir)
    outbox_path = os.path.join(pth, outbox_dir)

    # Configure database
    if os.name == 'nt':
        pth = 'h:\\zdjecia\\'
    else:
        pth = '/media/root/Foto/zdjecia/'

    db_file = os.path.join(pth, 'filecluster_db.sqlite3')
    db_file_clusters = os.path.join(pth, 'clusters.p')
    db_file_media = os.path.join(pth, 'media.p')

    # Filename extensions in scope of clustering
    image_extensions = ['.jpg', '.jpeg', '.dng', '.cr2']
    video_extensions = ['.mp4', '.3gp', 'mov']

    # ensure extensions are lowercase
    image_extensions = [ext.lower() for ext in image_extensions]
    video_extensions = [ext.lower() for ext in video_extensions]

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    # method that is used to group images, default: assume different events
    # are separated by significant time gape (max_gap config parameter)
    clustering_method = 'time_gap'

    assign_date_to_clusters_method = 'random'

    conf_dict = {
        'in_dir_name': inbox_path,
        'out_dir_name': outbox_path,
        'db_file_clusters': db_file_clusters,
        'db_file_media': db_file_media,
        'db_file': db_file,
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'granularity_minutes': max_gap,
        'cluster_col': 'cluster_id',
        'assign_date_to_clusters_method': assign_date_to_clusters_method,
        'clustering_method': clustering_method,
        'mode': 'move',  # move | copy | nop
        'db_driver': 'sqlite',  # dataframe | sqlite
        'generate_thumbnails': False,
        'delete_db': True,
    }
    config = Config(**conf_dict)
    return config


def get_development_config():
    """ Configuration for development"""
    print("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    # move/copy/nop
    config.mode = 'copy'

    # overwrite defaults with development specific params
    if os.name == 'nt':
        pth = 'h:\\incomming'
    else:
        pth = '/home/izik/bulk/fc_data'
        pth = '/home/safjan/Pictures'

    inbox_dir = 'inbox_test_a'
    outbox_dir = 'inbox_clust_test'

    config.in_dir_name = os.path.join(pth, inbox_dir)
    config.out_dir_name = os.path.join(pth, outbox_dir)

    config.db_file = os.path.join(pth, 'filecluster_db.sqlite3')
    config.db_file_media = os.path.join(pth, 'media.p')
    config.db_file_clusters = os.path.join(pth, 'clusters.p')
    return config
