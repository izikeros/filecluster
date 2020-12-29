"""Module for handling operations on both databases: media and clusters."""
import itertools
import logging
import multiprocessing
from pathlib import Path

import pandas as pd

from filecluster.configuration import Config, CLUSTER_DF_COLUMNS
from filecluster.filecluster_types import ClustersDataFrame
from filecluster.update_clusters import get_or_create_library_cluster_ini_as_dataframe
from numpy import int64
from pandas.core.frame import DataFrame
from typing import Union, List, Tuple

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_existing_clusters_info(
    config: Config,
) -> Tuple[ClustersDataFrame, List[Path], List[str]]:
    """Scan library, find existing clusters and empty or non-compliant folders."""
    # TODO: Any non-empty subfolder of year folder should contain .cluster.ini
    #  file (see: Runmageddon example). Non-empty means - contains media files

    USE_PARALLEL = True
    # setting-up pool is time consuming
    # list_is_short = len(event_dirs) < 50

    # if USE_PARALLEL:
    n_cpu = multiprocessing.cpu_count()
    logger.debug(f"Setting-up multiprocessing pool with {n_cpu} processes")
    pool = multiprocessing.Pool(processes=n_cpu)
    logger.debug(f"Pool ready to use")

    watch_folders = config.watch_folders

    # NOTE: these requires refactoring in scan_library_dir()
    # TODO: KS: 2020-12-26: folders than contains no media files but subfolders
    non_compliant_folders = []
    # totally empty folders (no files, no dirs)
    empty_folder_list = []
    # is there a reason for using watch folders (library folders)?
    #   do we have enabled duplicates or existing cluster functionalities
    use_watch_folders = (
        config.skip_duplicated_existing_in_libs or config.assign_to_clusters_existing_in_libs
    )

    # Start scanning watch folders to get cluster information
    if use_watch_folders and len(watch_folders):
        tuples = [
            get_or_create_library_cluster_ini_as_dataframe(lib, pool, config.force_deep_scan)
            for lib in watch_folders
        ]
        dfs, empty_folder_list = map(list, zip(*tuples))
        df = pd.concat(dfs, axis=0)
        df.index = range(len(df))
        df = df.reset_index()
        df = df.rename(columns={"index": "cluster_id"})

        # Flatten the list of empty directories:
        empty_folder_list = list(itertools.chain(*empty_folder_list))
    else:
        df = pd.DataFrame(columns=CLUSTER_DF_COLUMNS)
    return ClustersDataFrame(df), empty_folder_list, non_compliant_folders


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
