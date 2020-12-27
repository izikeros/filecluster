"""Module for handling operations on both databases: media and clusters."""
import logging

import pandas as pd

from filecluster.configuration import Config, CLUSTER_DF_COLUMNS
from filecluster.filecluster_types import ClustersDataFrame
from filecluster.update_cluster_db_deep import get_or_create_library_cluster_ini_as_dataframe
from numpy import int64
from pandas.core.frame import DataFrame
from typing import Union, List, Tuple

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_existing_clusters_info(
    config: Config,
) -> Tuple[ClustersDataFrame, List[str], List[str]]:
    """Scan library, find existing clusters and empty or non-compliant folders."""
    # TODO: Any non-empty subfolder of year folder should contain .cluster.ini
    #  file (see: Runmageddon example). Non-empty means - contains media files
    watch_folders = config.watch_folders

    # NOTE: these requires refactoring in scan_library_dir()
    empty_folders = []  # TODO: KS: 2020-12-26: totally empty folders
    non_compliant_folders = (
        []
    )  # TODO: KS: 2020-12-26: folders than contains no media files but subfolders

    # is there a reason for using watch folders (library folders)?
    #   do we have enabled duplicates or existing cluster functionalities
    use_watch_folders = (
        config.skip_duplicated_existing_in_libs or config.assign_to_clusters_existing_in_libs
    )

    # Start scanning watch folders to get cluster information
    if use_watch_folders and len(watch_folders):
        dfs = [
            get_or_create_library_cluster_ini_as_dataframe(lib, config.force_deep_scan)
            for lib in watch_folders
        ]
        df = pd.concat(dfs, axis=0)
        df.index = range(len(df))
        df = df.reset_index()
        df = df.rename(columns={"index": "cluster_id"})
    else:
        df = pd.DataFrame(columns=CLUSTER_DF_COLUMNS)
    return ClustersDataFrame(df), empty_folders, non_compliant_folders


def get_new_cluster_id_from_dataframe(df_clusters: DataFrame) -> Union[int64, int]:
    """Return cluster id value that is greater than all already used cluster ids.

    If there are gaps, there will be no first not-used returned.
    """
    cluster_ids = df_clusters.cluster_id.dropna().values
    if len(cluster_ids) > 0:
        last_cluster = max(cluster_ids)
        new_cluster_id = last_cluster + 1
    else:
        new_cluster_id = 1
    return new_cluster_id
