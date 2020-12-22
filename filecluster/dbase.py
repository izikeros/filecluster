"""Module for handling operations on both databases: media and clusters."""
import logging

import pandas as pd

from filecluster.configuration import Config, CLUSTER_DF_COLUMNS
from filecluster.update_cluster_db_deep import scan_library_dir

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_existing_clusters_info(config: Config):
    watch_folders = config.watch_folders
    use_watch_folders = (
        config.skip_duplicated_existing_in_libs
        or config.assign_to_clusters_existing_in_libs
    )
    if use_watch_folders and len(watch_folders):
        dfs = [
            scan_library_dir(lib, force_deep_scan=config.force_deep_scan)
            for lib in watch_folders
        ]
        df = pd.concat(dfs, axis=0)
        df.index = range(len(df))
        df = df.reset_index()
        df = df.rename(columns={"index": "cluster_id"})
    else:
        df = pd.DataFrame(columns=CLUSTER_DF_COLUMNS)
    return df


def get_new_cluster_id_from_dataframe(df_clusters):
    cluster_ids = df_clusters.cluster_id.dropna().values
    if len(cluster_ids) > 0:
        last_cluster = max(cluster_ids)
        new_cluster_id = last_cluster + 1
    else:
        new_cluster_id = 0
    return new_cluster_id
