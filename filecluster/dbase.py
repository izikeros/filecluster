"""Module for handling operations on both databases: media and clusters."""
import logging

import pandas as pd

from filecluster.configuration import Config, CLUSTER_DF_COLUMNS
from filecluster.update_cluster_db_deep import scan_library_dir
from numpy import int64
from pandas.core.frame import DataFrame
from typing import Union

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_existing_clusters_info(config: Config) -> DataFrame:
    # TODO: Any non-empty subfolder of year folder should contain .cluster.ini file (see: Runmageddon example)
    #   non-empty means - contains media files
    watch_folders = config.watch_folders

    # is there a reason for using watch folders (library folders)?
    #   do we have enabled duplicates or existing cluster functionalities
    use_watch_folders = (
        config.skip_duplicated_existing_in_libs
        or config.assign_to_clusters_existing_in_libs
    )

    # Start scanning watch folders to get cluster information
    if use_watch_folders and len(watch_folders):
        dfs = [scan_library_dir(lib, config.force_deep_scan) for lib in watch_folders]
        df = pd.concat(dfs, axis=0)
        df.index = range(len(df))
        df = df.reset_index()
        df = df.rename(columns={"index": "cluster_id"})
    else:
        df = pd.DataFrame(columns=CLUSTER_DF_COLUMNS)
    return df


def get_new_cluster_id_from_dataframe(df_clusters: DataFrame) -> Union[int64, int]:
    cluster_ids = df_clusters.cluster_id.dropna().values
    if len(cluster_ids) > 0:
        last_cluster = max(cluster_ids)
        new_cluster_id = last_cluster + 1
    else:
        new_cluster_id = 1
    return new_cluster_id
