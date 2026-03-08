"""Module for file clustering and supporting operations."""

from __future__ import annotations

import hashlib
import os
import random
from collections.abc import Iterator
from pathlib import Path, PosixPath
from typing import Any

import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp
from tqdm import tqdm

from filecluster import logger
from filecluster.configuration import (
    AssignDateToClusterMethod,
    Config,
    Status,
    default_settings,
)
from filecluster.dbase import get_new_cluster_id_from_dataframe
from filecluster.exceptions import MissingDfClusterColumnError
from filecluster.file_operations import FileOperationPlan, build_file_operation_plan
from filecluster.filecluster_types import ClustersDataFrame, MediaDataFrame
from filecluster.utlis import hash_file


class TargetPathCreator:
    """Target path creator."""

    def __init__(self, out_dir_name):
        self.out_dir = Path(out_dir_name)

    def for_new_cluster(self, date_string):
        """Path creator for new clusters' folder."""
        return str(Path("new") / date_string)

    def for_existing_cluster(self, dir_string):
        """Path creator for existing clusters' folder."""
        cluster_name = Path(dir_string).name
        return str(Path("existing") / cluster_name)

    def for_duplicates(self, dir_string):
        """Path creator for duplicated clusters folder."""
        cluster_name = Path(dir_string).name
        return str(Path("duplicated") / cluster_name)


class ImageGrouper:
    """Class for clustering media objects by date."""

    def __init__(
        self,
        configuration: Config,
        df_clusters: ClustersDataFrame | None = None,
        inbox_media_df: MediaDataFrame | None = None,
    ):
        """Init for the class.

        Args:
            configuration: instance of Configuration
            df_clusters: clusters database (loaded from a file or empty)
            inbox_media_df: dataframe with inbox media
        """
        # read the config
        self.config = configuration

        # initialize cluster data frame (if provided)
        if df_clusters is not None:
            self.df_clusters = df_clusters

        # initialize imported media df (inbox)
        if inbox_media_df is not None:
            self.inbox_media_df = inbox_media_df

    def calculate_gaps(self, date_col="date", delta_col="date_delta"):
        """Calculate gaps between consecutive shots, save delta to dataframe.

        Use 'creation date' from a given column and save results to
         the selected 'delta' column
        """
        # sort by creation date
        self.inbox_media_df.sort_values(by=date_col, ascending=True, inplace=True)

        # select not clustered items
        sel = self.inbox_media_df.cluster_id.isna()

        # calculate breaks between the non-clustered images
        self.inbox_media_df[delta_col] = None
        self.inbox_media_df[date_col] = pd.to_datetime(self.inbox_media_df[date_col])
        self.inbox_media_df.loc[sel, delta_col] = self.inbox_media_df.loc[
            sel, date_col
        ].diff()

    def run_clustering(self) -> ClustersDataFrame:
        """Identify clusters in media not clustered so far.

        Responsibilities:
            - update media_df - assign cluster, and cluster status (NEW_CLUSTER)
            - add new clusters to clusters_df
        """
        max_gap = self.config.time_granularity
        starting_cluster_idx = get_new_cluster_id_from_dataframe(self.df_clusters)

        sel_not_clustered = self.inbox_media_df["status"] == Status.UNKNOWN

        if not sel_not_clustered.any():
            new_cluster_df = ClustersDataFrame(
                pd.DataFrame(columns=pd.Index(default_settings.cluster_df_columns))
            )
            return new_cluster_df

        # Filter to only the rows we are analyzing, but keeping original indices to update back
        df_unclustered = self.inbox_media_df[sel_not_clustered].copy()

        # Calculate boundaries
        is_new_cluster = df_unclustered["date_delta"] > max_gap
        # The first item in the unclustered dataframe is ALWAYS a new cluster
        is_new_cluster.iloc[0] = True

        # Calculate cluster IDs
        # cumsum() increments by 1 every time is_new_cluster is True
        # We subtract 1 from starting_cluster_idx because cumsum starts at 1 for the first True
        cluster_ids = is_new_cluster.cumsum() + (starting_cluster_idx - 1)

        # Update back the main dataframe
        self.inbox_media_df.loc[sel_not_clustered, "cluster_id"] = cluster_ids
        self.inbox_media_df.loc[sel_not_clustered, "status"] = Status.NEW_CLUSTER

        # Create the new clusters dataframe by grouping by cluster_id
        grouped = self.inbox_media_df[sel_not_clustered].groupby("cluster_id")

        # Aggregation
        new_clusters_data = {
            "cluster_id": list(grouped.groups.keys()),
            "start_date": grouped["date"].min(),
            "end_date": grouped["date"].max(),
            "is_continuous": True,
        }

        # Create new dataframe
        new_cluster_df = pd.DataFrame(new_clusters_data).reset_index(drop=True)

        # Ensure all required columns are present
        for col in default_settings.cluster_df_columns:
            if col not in new_cluster_df.columns:
                new_cluster_df[col] = None

        # Reorder columns to match
        new_cluster_df = ClustersDataFrame(
            new_cluster_df[default_settings.cluster_df_columns]
        )

        # Concatenate with existing
        if not new_cluster_df.empty:
            if self.df_clusters is None or self.df_clusters.empty:
                self.df_clusters = new_cluster_df
            else:
                # Drop empty columns from df_clusters to avoid FutureWarning
                non_empty_cols = self.df_clusters.columns[
                    self.df_clusters.notna().any()
                ].tolist()
                if not non_empty_cols:
                    self.df_clusters = new_cluster_df
                else:
                    self.df_clusters = ClustersDataFrame(
                        pd.concat([self.df_clusters, new_cluster_df], ignore_index=True)
                    )

        return new_cluster_df

    def add_new_cluster_data_to_data_frame(self, new_cluster_df):
        """Concatenate df with existing clusters with new clusters df."""
        self.df_clusters = pd.concat([self.df_clusters, new_cluster_df])

    def get_new_cluster_ids(self):
        """Get ids of the new clusters."""
        sel_new = self.inbox_media_df["status"] == Status.NEW_CLUSTER
        return self.inbox_media_df[sel_new]["cluster_id"].unique()

    def get_existing_to_be_expanded_cluster_ids(self):
        """Get id of existing clusters that will be expanded by adding new media."""
        sel_existing = ~(self.df_clusters["path"].isna())
        sel_with_files_to_appened = self.df_clusters["new_file_count"] > 0
        sel = sel_existing & sel_with_files_to_appened
        return self.df_clusters[sel]["cluster_id"].unique()

    def assign_target_folder_name_and_file_count_to_new_clusters(
        self, method=AssignDateToClusterMethod.MEDIAN
    ) -> list[str]:
        """Set target_path and new_file_count in clusters_df.

        Returns:
            target_folder names for debug and testing purposes
        """
        new_folder_names = []

        # initialize the "path" column if not exists
        if "target_path" not in self.df_clusters.columns:
            self.df_clusters["target_path"] = None

        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        new_clusters = self.get_new_cluster_ids()
        for new_cluster in new_clusters:
            # Set the cluster folder name for new clusters
            mask = self.inbox_media_df["cluster_id"] == new_cluster
            df = self.inbox_media_df.loc[mask]
            if method == AssignDateToClusterMethod.RANDOM:
                # get a random date
                exif_date = df.sample(n=1)["date"].values[0]
            elif method == AssignDateToClusterMethod.MEDIAN:
                df = df.sort_values("date")
                exif_date = df.iloc[int(len(df) / 2)]["date"]

            try:
                date_str = exif_date.strftime("[%Y_%m_%d]")
            except ValueError:
                date_str = f"[NaT_]_{random.randint(100000, 999999)}"

            try:
                time_str = exif_date.strftime("%H%M%S")
            except ValueError:
                time_str = f"{random.randint(100000, 999999)}"

            image_count = df.loc[df["is_image"]].shape[0]
            video_count = df.loc[~df["is_image"]].shape[0]

            rich_str = ""
            if (image_count > 10) or (video_count > 10):
                rich_str = "_rich"
            date_string = "_".join(
                [
                    date_str,
                    time_str,
                    f"IC_{image_count}",
                    f"VC_{video_count}",
                    rich_str,
                ]
            )

            # save 'target_path' and 'new_file_count' info to cluster db
            pth = path_creator.for_new_cluster(date_string=date_string)
            sel_cluster = self.df_clusters.cluster_id == new_cluster
            self.df_clusters.loc[sel_cluster, "target_path"] = pth
            self.df_clusters.loc[sel_cluster, "new_file_count"] = (
                image_count + video_count
            )
            new_folder_names.append(pth)
        return new_folder_names

    def build_file_operation_plan(self) -> FileOperationPlan:
        """Build a plan of file operations without executing them.

        Returns:
            A FileOperationPlan that can be inspected or executed separately.
        """
        return build_file_operation_plan(
            inbox_media_df=self.inbox_media_df,
            in_dir=Path(self.config.in_dir_name),
            out_dir=Path(self.config.out_dir_name),
            mode=self.config.mode,
        )

    def move_files_to_cluster_folder(self):
        """Physical move of the file to the cluster folder.

        Delegates to the file_operations module: builds a plan, then executes it.
        """
        from filecluster.file_operations import execute_plan

        plan = self.build_file_operation_plan()
        execute_plan(plan)

    def add_target_dir_for_duplicates(self):
        """Add a target directory for the duplicated media files."""
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)
        # add target dir for the duplicates
        sel_dups = self.inbox_media_df.status == Status.DUPLICATE
        # TODO: use pandas apply or even parallel apply instead of iterrows
        for idx, _row in self.inbox_media_df[sel_dups].iterrows():
            dup_cluster = self.inbox_media_df.duplicated_cluster[idx]
            try:
                self.inbox_media_df.loc[idx, "target_path"] = (
                    path_creator.for_duplicates(dup_cluster[0])
                )
            except Exception as e:
                logger.error(f"{e}")

    def add_cluster_info_from_clusters_to_media(self):
        """Add clusters info to media dataframe."""

        # Ensure both columns have the same data type
        self.inbox_media_df["cluster_id"] = self.inbox_media_df["cluster_id"].astype(
            str
        )
        self.df_clusters["cluster_id"] = self.df_clusters["cluster_id"].astype(str)

        self.inbox_media_df = self.inbox_media_df.merge(
            self.df_clusters[["cluster_id", "target_path"]], on="cluster_id", how="left"
        )
        return None

    def assign_to_existing_clusters(self) -> tuple[list[str], list[str]]:
        """Assign media to an existing cluster if possible."""
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        check_df_has_all_expected_columns(
            df=self.df_clusters, expected_cols=default_settings.cluster_df_columns
        )

        margin = self.config.time_granularity

        # Filter for items that aren't duplicates
        sel_no_duplicated = ~(self.inbox_media_df.status == Status.DUPLICATE)

        if not sel_no_duplicated.any() or self.df_clusters.empty:
            # Avoid running logic if there's nothing to do
            return [], []

        continuous_clusters = self.df_clusters[self.df_clusters.is_continuous].copy()
        if continuous_clusters.empty:
            return [], []

        continuous_clusters["margin_start"] = continuous_clusters["start_date"] - margin
        continuous_clusters["margin_end"] = continuous_clusters["end_date"] + margin

        # For each unassigned file, find matching clusters
        for index, row in self.inbox_media_df[sel_no_duplicated].iterrows():
            img_time: Timestamp = row["date"]

            # Check against all pre-calculated boundaries
            matches = continuous_clusters[
                (continuous_clusters["margin_start"] <= img_time)
                & (continuous_clusters["margin_end"] >= img_time)
            ]

            n_sel = len(matches)
            if n_sel > 0:
                if n_sel > 1:
                    logger.warning("Ambiguity")

                cluster_id = matches["cluster_id"].values[0]
                self.inbox_media_df.loc[index, "cluster_id"] = cluster_id
                self.inbox_media_df.loc[index, "status"] = Status.EXISTING_CLUSTER

                # Update the original df_clusters dataframe
                cluster_idx = self.df_clusters[
                    self.df_clusters.cluster_id == cluster_id
                ].index[0]

                current_new_file_count = self.df_clusters.loc[
                    cluster_idx, "new_file_count"
                ]
                if pd.isna(current_new_file_count) or not current_new_file_count:
                    self.df_clusters.loc[cluster_idx, "new_file_count"] = 1
                else:
                    self.df_clusters.loc[cluster_idx, "new_file_count"] += 1

                old_start = self.df_clusters.loc[cluster_idx, "start_date"]
                self.df_clusters.loc[cluster_idx, "start_date"] = min(
                    old_start, img_time
                )

                old_end = self.df_clusters.loc[cluster_idx, "end_date"]
                self.df_clusters.loc[cluster_idx, "end_date"] = max(old_end, img_time)

                pth = self.df_clusters.loc[cluster_idx, "path"]
                if pd.notna(pth):
                    target_pth = path_creator.for_existing_cluster(dir_string=pth)
                    self.df_clusters.loc[cluster_idx, "target_path"] = target_pth

                # Update the continuous_clusters copy as well so subsequent files in this run
                # can match against the expanded boundaries!
                continuous_clusters.loc[
                    continuous_clusters.cluster_id == cluster_id, "margin_start"
                ] = min(old_start, img_time) - margin

                continuous_clusters.loc[
                    continuous_clusters.cluster_id == cluster_id, "margin_end"
                ] = max(old_end, img_time) + margin

        # Return results
        has_cluster_id = self.inbox_media_df.cluster_id.notna()
        has_status_existing_cluster = (
            self.inbox_media_df.status == Status.EXISTING_CLUSTER
        )

        mask = has_cluster_id & has_status_existing_cluster
        files_assigned_to_existing_cl = self.inbox_media_df[
            mask
        ].file_name.values.tolist()

        existing_cluster_ids = self.inbox_media_df[mask].cluster_id.unique()

        existing_cluster_names = []
        for cl_id in existing_cluster_ids:
            cl_rows = self.df_clusters[self.df_clusters.cluster_id == cl_id]
            if not cl_rows.empty:
                pth = cl_rows.iloc[0].path
                if pd.notna(pth):
                    existing_cluster_names.append(Path(str(pth)).parts[-1])

        return files_assigned_to_existing_cl, existing_cluster_names

    def assign_target_folder_name_to_existing_clusters(self):
        """Set a cluster string in the dataframe and return the string."""
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        existing_clusters_extended_by_inbox = (
            self.get_existing_to_be_expanded_cluster_ids()
        )

        for cluster in existing_clusters_extended_by_inbox:
            # save to cluster db
            sel_cluster = self.df_clusters.cluster_id == cluster
            target_path_serie = self.df_clusters.loc[sel_cluster, "target_path"]
            cl = Path(target_path_serie.values[0]).parts[-1]
            pth = path_creator.for_existing_cluster(dir_string=cl)
            self.df_clusters.loc[sel_cluster, "target_path"] = pth

    def mark_inbox_duplicates(self) -> tuple[list[str], list[str]]:
        """Check if imported files are not in the library already, if so - skip them.

        Uses a lazy evaluation strategy:
        1. Size match
        2. Partial hash match (first 1MB)
        3. Full hash match

        Returns:
            List of inbox filenames that have duplicates in a library
        """
        clusters_with_dups = []
        confirmed_inbox_dups = []
        confirmed_library_dups = []

        if not self.config.skip_duplicated_existing_in_libs:
            return [], []

        if not any(self.config.watch_folders):
            logger.debug("No library folder defined. Skipping duplicate search.")
            return [], []

        # Get files in watch folders
        watch_file_names, watch_full_paths = get_watch_folders_files_path(
            self.config.watch_folders
        )

        # 1. First pass: Map library files by size to avoid unnecessary hashing
        logger.info("Building library size index")
        library_by_size = {}
        for path in watch_full_paths:
            try:
                size = os.path.getsize(path)
                if size not in library_by_size:
                    library_by_size[size] = []
                library_by_size[size].append(path)
            except OSError:
                pass

        logger.info("Checking for duplicates using size -> partial hash -> full hash")

        # Process unassigned files in inbox
        sel_unknown = self.inbox_media_df.status == Status.UNKNOWN

        for idx, row in tqdm(
            self.inbox_media_df[sel_unknown].iterrows(), total=sum(sel_unknown)
        ):
            inbox_size = row["size"]
            inbox_file_name = row["file_name"]

            # 1. Size match
            if inbox_size not in library_by_size:
                continue

            potential_matches = library_by_size[inbox_size]

            def get_partial_hash(filepath, size=1024 * 1024):
                try:
                    with open(filepath, "rb") as f:
                        return hashlib.md5(f.read(size)).hexdigest()
                except OSError:
                    return None

            # Helper for full hashing
            # (inbox already has full hash computed in hash_value column if image_reader did it)
            inbox_hash = row.get("hash_value")
            if pd.isna(inbox_hash) or not inbox_hash:
                # We need to compute it if not present
                try:
                    inbox_path = os.path.join(self.config.in_dir_name, inbox_file_name)
                    inbox_hash = hash_file(inbox_path)
                except OSError:
                    continue

            # Check against potential matches
            inbox_path = os.path.join(self.config.in_dir_name, inbox_file_name)
            inbox_partial = get_partial_hash(inbox_path)

            for lib_path in potential_matches:
                # 2. Partial hash match
                lib_partial = get_partial_hash(lib_path)

                if inbox_partial and lib_partial and inbox_partial == lib_partial:
                    # 3. Full hash match
                    try:
                        lib_hash = hash_file(lib_path)

                        if inbox_hash == lib_hash:
                            confirmed_inbox_dups.append(inbox_file_name)
                            confirmed_library_dups.append(lib_path)

                            # Mark duplicate in dataframe
                            self.inbox_media_df.loc[idx, "status"] = Status.DUPLICATE

                            # Add tracking information
                            dup_cluster = Path(lib_path).parts[-2]
                            clusters_with_dups.append(dup_cluster)

                            # Set destination fields
                            self.inbox_media_df.loc[idx, "duplicated_to"] = str(
                                lib_path
                            )
                            self.inbox_media_df.loc[idx, "duplicated_cluster"] = (
                                dup_cluster
                            )

                            break  # Found a match, no need to check other files of same size
                    except OSError:
                        continue

        return list(set(confirmed_inbox_dups)), list(set(clusters_with_dups))


def get_files_from_folder(folder: str | Path) -> Iterator[Any]:
    """Get iterator over recursive listing of files in the folder.

    Args:
      folder: Folder to get files from

    Returns:
        Iterator over recursive directory contents (items matching *.* a pattern)
    """
    return Path(folder).rglob("*.*")


def get_watch_folders_files_path(
    watch_folders: list[str] | list[Path],
) -> tuple[list[str], list[PosixPath]]:
    """Get a list of files and files with full paths from the folder (recursively).

    Args:
      watch_folders: folder to be recursively listed

    Returns:
        List of filenames, list of a full path of filenames
    """
    watch_full_paths = []
    for w in watch_folders:
        file_list_watch: Iterator = get_files_from_folder(w)
        path_list = list(file_list_watch)
        watch_full_paths.extend(path_list)
    watch_file_names = [path.name for path in watch_full_paths]
    return watch_file_names, watch_full_paths


def check_df_has_all_expected_columns(df: pd.DataFrame, expected_cols: list[str]):
    """Check if the data frame has all expected columns."""
    missing_columns = [col for col in expected_cols if col not in df.columns]
    if missing_columns:
        raise MissingDfClusterColumnError(missing_columns[0])


def check_df_has_all_expected_columns_and_types(
    df: pd.DataFrame, col_expectations: dict
):
    """Check if the data frame has all expected columns."""
    for c in df.columns:
        if c not in list(col_expectations.keys()):
            raise MissingDfClusterColumnError(c)
        # TODO: KS: 2020-12-26: check dtype
