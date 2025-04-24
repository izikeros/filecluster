"""Module for handling operations on both databases: media and clusters."""

import itertools
import multiprocessing
from pathlib import Path

import pandas as pd
from configuration import FileClusterSettings
from numpy import int64
from pandas.core.frame import DataFrame

from filecluster import logger
from filecluster.filecluster_types import ClustersDataFrame
from filecluster.update_clusters import get_or_create_library_cluster_ini_as_dataframe


def get_existing_clusters_info(
    watch_folders: list[Path],
    skip_duplicated_existing_in_libs: bool,
    assign_to_clusters_existing_in_libs: bool,
    force_deep_scan: bool,
) -> tuple[ClustersDataFrame, list[Path], list[str]]:
    """Scan the library, find existing clusters and empty or non-compliant folders.

    Returns:
        Tuple of:
            - ClustersDataFrame object with columns: ['cluster_id', 'start_date', 'end_date', 'median', 'is_continuous',
                  'path', 'target_path', 'file_count', 'new_file_count'], where
                  path - path to the folder with media files
                  target_path - path to the folder where media files should be moved

            - list of empty folders
            - list of non-compliant folders (folders that contain only subfolders
                instead of media files) - not implemented yet
    """
    # TODO: Any non-empty subfolder of year folder should contain .cluster.ini
    #  file (see: Runmageddon example). Non-empty means - contains media files

    n_cpu = multiprocessing.cpu_count()
    logger.debug(f"Setting-up multiprocessing pool with {n_cpu} processes")
    pool = multiprocessing.Pool(processes=n_cpu)
    logger.debug("Pool ready to use")

    # NOTE: this requires refactoring in scan_library_dir()

    # TODO: KS: 2020-12-26: non-compliant - folders than contains no media files but subfolders
    non_compliant_folders = []

    # totally empty folders (no files, no dirs)
    empty_folder_list = []

    # is there a reason for using watch folders (library folders)?
    #   do we have enabled duplicates or existing cluster functionalities
    use_watch_folders = (
        skip_duplicated_existing_in_libs or assign_to_clusters_existing_in_libs
    )

    # Start scanning watch folders to get cluster information
    if use_watch_folders and len(watch_folders):

        # tuples of
        tuples = [
            get_or_create_library_cluster_ini_as_dataframe(lib, pool, force_deep_scan)
            for lib in watch_folders
        ]
        dfs, empty_folder_list = map(list, zip(*tuples, strict=False))
        df = pd.concat(dfs, axis=0)
        df.index = range(len(df))
        df = df.reset_index()
        df = df.rename(columns={"index": "cluster_id"})

        # Flatten the list of empty directories:
        empty_folder_list = list(itertools.chain(*empty_folder_list))
    else:
        settings = (
            FileClusterSettings()
        )  # FIXME: KS: 2025-04-24: are these proper settings?
        df = pd.DataFrame(columns=settings.CLUSTER_DF_COLUMNS)
    return ClustersDataFrame(df), empty_folder_list, non_compliant_folders


def get_new_cluster_id_from_dataframe(df_clusters: DataFrame) -> int64 | int:
    """Return cluster id value that is greater than all already used cluster ids.

    If there are gaps, there will be no first not-used returned.
    """
    cluster_ids = df_clusters.cluster_id.dropna().values
    if len(cluster_ids) <= 0:
        return 1
    last_cluster = max(cluster_ids)
    return last_cluster + 1
