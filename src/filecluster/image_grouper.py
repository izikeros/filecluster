"""Module for file clustering and supporting operations."""

import os
import random
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path, PosixPath
from shutil import copy2, move
from typing import Any

import pandas as pd
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from tqdm import tqdm

from filecluster import logger
from filecluster import utlis as ut
from filecluster.configuration import (
    CLUSTER_DF_COLUMNS,
    AssignDateToClusterMethod,
    Config,
    CopyMode,
    Status,
)
from filecluster.dbase import get_new_cluster_id_from_dataframe
from filecluster.exceptions import DateStringNoneError, MissingDfClusterColumnError
from filecluster.filecluster_types import ClustersDataFrame, MediaDataFrame


def expand_cluster_or_init_new(
    delta_from_previous: Timedelta,
    max_time_delta: timedelta,
    index: int,
    new_cluster_idx: int,
    list_new_clusters: list[dict],
    media_date: Timestamp,
    cluster: dict,
):
    """Add new item to existing cluster and update cluster info or init new cluster.

    If this image is too far (in time) from previous image - it means that
     cluster can be created from the images in the "buffer"/"backlog".

    Args:
        delta_from_previous:    distance of this image to previous (on image list sorted by time)
        max_time_delta:         max allowed time delta to for images from the same event
        index:                  image index from "inbox" media dataframe. Used only to
                                    check if we are starting clustering.
        new_cluster_idx:        new cluster id in case we need to create new cluster
        list_new_clusters:      list of new cluster dictionaries
        start_date:             current "buffer" start date
        end_date:               current "buffer" end date
        cluster:                cluster dictionary with description of current "buffer" cluster
    :return:
    """
    # check if new cluster encountered
    is_first_image_analysed = index == 0
    is_this_image_too_far_from_other_in_the_cluster = (
        delta_from_previous > max_time_delta
    )
    is_new_cluster = (
        is_this_image_too_far_from_other_in_the_cluster or is_first_image_analysed
    )
    if is_new_cluster:
        new_cluster_idx += 1
        # == We are starting new cluster here ==
        # append previous cluster date to the list
        if not is_first_image_analysed:
            # previous cluster is completed, add previous cluster info
            #  to the list of clusters
            list_new_clusters.append(cluster)

        # initialize record for new cluster
        cluster = dict.fromkeys(CLUSTER_DF_COLUMNS)
        cluster.update(
            {
                "cluster_id": new_cluster_idx,
                "start_date": media_date,
                "end_date": media_date,
            }
        )
    else:
        cluster["start_date"] = media_date

    # update cluster stop date
    cluster["end_date"] = media_date
    return cluster, new_cluster_idx, list_new_clusters


class TargetPathCreator:
    """Target path creator."""

    def __init__(self, out_dir_name):
        self.out_dir = Path(out_dir_name)

    def for_new_cluster(self, date_string):
        """Path creator for new clusters folder."""
        return str(self.out_dir / "new" / date_string)

    def for_existing_cluster(self, dir_string):
        """Path creator for existing clusters folder."""
        return str(self.out_dir / "existing" / dir_string)

    def for_duplicates(self, dir_string):
        """Path creator for duplicated clusters folder."""
        return str(self.out_dir / "duplicated" / dir_string)


def filter_by_substring_list(string_list: list[str], substr_list: list[str]):
    """Return strings that contains any of substrings from another list."""
    return [str for str in string_list if any(sub in str for sub in substr_list)]


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
            df_clusters: clusters database (loaded from file or empty)
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

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.inbox_media_df.sort_values(by=date_col, ascending=True, inplace=True)

        # select not clustered items
        sel = self.inbox_media_df.cluster_id.isna()

        # calculate breaks between the non-clustered images
        self.inbox_media_df[delta_col] = None
        self.inbox_media_df[date_col] = pd.to_datetime(self.inbox_media_df[date_col])
        self.inbox_media_df[delta_col][sel] = self.inbox_media_df[date_col][
            sel
        ].diff()  # FIXME: SettingWithCopyWarning

    def run_clustering(self) -> ClustersDataFrame:
        """Identify clusters in media not clustered so far.

        Responsibilities:
            - update media_df - assign cluster, and cluster status (NEW_CLUSTER)
            - add new clusters to clusters_df

        """
        list_new_cluster_dictionaries = []
        max_gap = self.config.time_granularity

        cluster_idx = get_new_cluster_id_from_dataframe(self.df_clusters)

        # prepare placeholder for first cluster row (as dict).
        #   First cluster for the media that are not clustered yet.
        current_cluster_dict = dict.fromkeys(CLUSTER_DF_COLUMNS)
        current_cluster_dict.update(
            {
                "cluster_id": cluster_idx,
                "start_date": None,
                "end_date": None,
                "is_continous": True,  # FIXME: rename `is_continous` to `is_continuous`
            }
        )

        n_files = len(self.inbox_media_df)

        # set new index in inbox_media_df (as range)
        self.inbox_media_df.index = list(range(n_files))

        # Iterate over all inbox media and create new clusters for each item
        # or assign to one just created

        # create mask for selecting not clustered media items
        sel_not_clustered = self.inbox_media_df["status"] == Status.UNKNOWN
        n_not_clustered = sum(sel_not_clustered)
        is_first_image_analysed = True
        for media_index, _row in tqdm(
            self.inbox_media_df[sel_not_clustered].iterrows(), total=n_not_clustered
        ):
            # gap to previous
            delta_from_previous = self.inbox_media_df.loc[media_index]["date_delta"]
            # date of this media object
            media_date = self.inbox_media_df.loc[media_index]["date"]

            # check if new cluster encountered
            is_this_image_too_far_from_other_in_the_cluster = (
                delta_from_previous > max_gap
            )
            is_new_cluster = (
                is_this_image_too_far_from_other_in_the_cluster
                or is_first_image_analysed
            )
            if is_new_cluster:
                # == We are starting new cluster here ==
                if not is_first_image_analysed:
                    cluster_idx += 1
                    # append previous cluster date to the list
                    #   previous cluster is completed, add previous cluster info
                    #   to the list of clusters
                    list_new_cluster_dictionaries.append(current_cluster_dict)

                # initialize record for new cluster
                current_cluster_dict = dict.fromkeys(CLUSTER_DF_COLUMNS)
                current_cluster_dict.update(
                    {
                        "cluster_id": cluster_idx,
                        "start_date": media_date,
                        "end_date": media_date,
                        "is_continous": True,
                    }
                )
            else:
                # update cluster start & stop date
                current_cluster_dict["start_date"] = min(
                    media_date, current_cluster_dict["start_date"]
                )
                current_cluster_dict["end_date"] = max(
                    media_date, current_cluster_dict["end_date"]
                )

            # assign cluster id to image
            self.inbox_media_df.loc[media_index, "cluster_id"] = cluster_idx
            self.inbox_media_df.loc[media_index, "status"] = Status.NEW_CLUSTER
            # ----
            is_first_image_analysed = False

        # save last cluster (TODO: check border cases: one file, one cluster, no-files,...)
        list_new_cluster_dictionaries.append(current_cluster_dict)

        new_cluster_df = ClustersDataFrame(pd.DataFrame(list_new_cluster_dictionaries))
        # Concatenate df with existing clusters with new clusters df."""
        self.df_clusters = pd.concat([self.df_clusters, new_cluster_df])
        return new_cluster_df

    def add_new_cluster_data_to_data_frame(self, new_cluster_df):
        """Concatenate df with existing clusters with new clusters df."""
        self.df_clusters = pd.concat([self.df_clusters, new_cluster_df])

    def get_new_cluster_ids(self):
        """Get ids of the new clusters."""
        sel_new = self.inbox_media_df["status"] == Status.NEW_CLUSTER
        return self.inbox_media_df[sel_new]["cluster_id"].unique()

    def get_existing_to_be_expanded_cluster_ids(self):
        """Get Id of existing clusters that will be expanded by adding new media."""
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

        # initialize "path" column if not exists
        if "target_path" not in self.df_clusters.columns:
            self.df_clusters["target_path"] = None

        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        new_clusters = self.get_new_cluster_ids()
        for new_cluster in new_clusters:
            # Set cluster folder name for new clusters
            mask = self.inbox_media_df["cluster_id"] == new_cluster
            df = self.inbox_media_df.loc[mask]
            if method == AssignDateToClusterMethod.RANDOM:
                # get random date
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

    def move_files_to_cluster_folder(self):
        """Physical move of the file to the cluster folder."""
        dirs = self.inbox_media_df["target_path"].unique()
        mode = self.config.mode

        # prepare directories in advance
        for dir_name in dirs:
            if dir_name is None:
                raise DateStringNoneError()
            ut.create_folder_for_cluster(
                config=self.config, date_string=dir_name, mode=mode
            )

        # Move or copy items to dedicated folder."""
        pth_out = self.config.out_dir_name
        pth_in = self.config.in_dir_name
        n_files = len(self.inbox_media_df)
        for _, row in tqdm(self.inbox_media_df.iterrows(), total=n_files):
            date_string = row["target_path"]
            file_name = row["file_name"]
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == CopyMode.COPY:
                copy2(src, dst)
            elif mode == CopyMode.MOVE:
                move(src, dst)

    def add_target_dir_for_duplicates(self):
        """Add target directory for the duplicated media files."""
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
        # add path info from cluster dir,
        self.inbox_media_df = self.inbox_media_df.merge(
            self.df_clusters[["cluster_id", "target_path"]], on="cluster_id", how="left"
        )
        return None

    def assign_to_existing_clusters(self) -> tuple[list[str], list[str]]:
        """Assign media to existing cluster if possible."""
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        check_df_has_all_expected_columns(
            df=self.df_clusters, expected_cols=CLUSTER_DF_COLUMNS
        )

        margin = self.config.time_granularity
        sel_no_duplicated = ~(self.inbox_media_df.status == Status.DUPLICATE)
        for index, row in tqdm(self.inbox_media_df[sel_no_duplicated].iterrows()):
            img_time: Timestamp = row["date"]  # read item_date

            # conditions for the range
            not_too_old_clusters = self.df_clusters.start_date - margin <= img_time
            not_too_new_clusters = self.df_clusters.end_date + margin >= img_time
            range_ok = not_too_old_clusters & not_too_new_clusters

            # condition for the continuity
            continuous = self.df_clusters.is_continous

            sel = range_ok & continuous
            n_sel = sum(sel.values)
            if n_sel > 0:
                candidate_clusters = self.df_clusters[sel]
                if n_sel > 1:
                    logger.warning("Ambiguity")
                    # TODO: KS: 2020-12-17: Solve ambiguity other way?
                    # assign to first anyway
                cluster_id = candidate_clusters["cluster_id"].values[0]
                self.inbox_media_df["cluster_id"].loc[index] = cluster_id
                self.inbox_media_df["status"].loc[index] = Status.EXISTING_CLUSTER

                # == Update cluster info
                # update counter
                cluster_idx = self.df_clusters[
                    self.df_clusters.cluster_id == cluster_id
                ].index
                if self.df_clusters.new_file_count.loc[cluster_idx].values[0]:
                    self.df_clusters.new_file_count.loc[cluster_idx] += 1
                else:
                    self.df_clusters.new_file_count.loc[cluster_idx] = 1
                # update boundaries
                old_start = self.df_clusters.start_date.loc[cluster_idx].values[0]
                new_start = min(old_start, img_time)
                self.df_clusters.start_date.loc[cluster_idx] = new_start

                old_end = self.df_clusters.end_date.loc[cluster_idx].values[0]
                new_end = max(old_end, img_time)
                self.df_clusters.end_date.loc[cluster_idx] = new_end
                # add target patch
                pth = self.df_clusters.path.loc[cluster_idx].values[0]
                target_pth = path_creator.for_existing_cluster(dir_string=pth)
                self.df_clusters.target_path.loc[cluster_idx] = target_pth

        has_cluster_id = self.inbox_media_df.cluster_id >= 0
        has_status_existing_cluster = (
            self.inbox_media_df.status == Status.EXISTING_CLUSTER
        )
        files_assigned_to_existing_cl = self.inbox_media_df[
            has_cluster_id & has_status_existing_cluster
        ].file_name.values.tolist()
        existing_cluster_ids = self.inbox_media_df[
            has_cluster_id & has_status_existing_cluster
        ].cluster_id.unique()
        existing_cluster_names = [
            Path(cl_row.path).parts[-1]
            for _, cl_row in self.df_clusters.iterrows()
            if cl_row.cluster_id in existing_cluster_ids
        ]
        return files_assigned_to_existing_cl, existing_cluster_names

    def assign_target_folder_name_to_existing_clusters(self):
        """Set cluster string in the dataframe and return the string."""
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

        Returns:
            List of inbox filenames that has duplicates in library
        """
        clusters_with_dups = []
        confirmed_inbox_dups = []
        confirmed_library_dups = []
        if not self.config.skip_duplicated_existing_in_libs:
            return [], []
        else:
            logger.debug("Checking import for duplicates in watch folders")

        if not any(self.config.watch_folders):
            logger.debug("No library folder defined. Skipping duplicate search.")
            return [], []

        (
            potential_inbox_dups,
            potential_library_dups,
            watch_full_paths,
        ) = self.file_name_based_duplicates()

        # verify potential dups using size comparison
        file_already_in_library = []

        logger.info("Confirm potential duplicates")
        for potential_duplicate in tqdm(potential_inbox_dups):
            # get inbox item info
            inbox_item = self.inbox_media_df[
                self.inbox_media_df.file_name == potential_duplicate
            ]
            inbox_item_size = inbox_item["size"].values[0]

            for lib_item in potential_library_dups:
                lib_item_size = os.path.getsize(lib_item)
                in_file_name = inbox_item.file_name.values[0]
                if inbox_item_size == lib_item_size:
                    file_already_in_library.append(lib_item)

                    confirmed_library_dups.append(lib_item)
                    confirmed_inbox_dups.append(in_file_name)
                    # logger.debug(f"Inbox {in_file_name} is duplicate to library: {lib_item}")

        # mark confirmed duplicates in import batch
        logger.info("mark confirmed duplicates in import batch")
        # FIXME: KS: 2020-12-26: Very slow stage 1sec/it
        # deduplicate list
        confirmed_inbox_dups = list(set(confirmed_inbox_dups))
        sel_dups = self.inbox_media_df.file_name.isin(confirmed_inbox_dups)
        for idx, _row in tqdm(
            self.inbox_media_df[sel_dups].iterrows(), total=sum(sel_dups)
        ):
            self.inbox_media_df.loc[idx, "status"] = (
                Status.DUPLICATE
            )  # Fixme: copy of a slice
            if dups_lib_patch := list(
                filter(lambda x: _row.file_name in str(x), confirmed_library_dups)
            ):
                dups_lib_str_list = [str(x) for x in dups_lib_patch]
                dups_lib_clust_list = [Path(x).parts[-2] for x in dups_lib_patch]

                self.inbox_media_df["duplicated_to"][
                    idx
                ] = dups_lib_str_list  # FIXME: SettingWithCopyWarning
                self.inbox_media_df["duplicated_cluster"][
                    idx
                ] = dups_lib_clust_list  # FIXME: SettingWithCopyWarning

                # return first cluster with this duplicated media file (for debug and testing)
                clusters_with_dups.append(
                    dups_lib_clust_list[0]
                )  # FIXME: KS: 2020-12-27: Not entirely correct

                if len(dups_lib_clust_list) > 1:
                    # TODO: KS: 2020-12-27: Properly handle duplicate to multiple
                    #  destignations in library
                    pass
            else:
                pass  # TODO: investigate such case, should not happen

        return confirmed_inbox_dups, list(
            set(clusters_with_dups)
        )  # ,confirmed_lib_dups

    def file_name_based_duplicates(self):
        """Find duplicates that have the same filename."""
        # get files in library
        watch_file_names, watch_full_paths = get_watch_folders_files_path(
            self.config.watch_folders
        )
        # get files in inbox
        new_names = self.inbox_media_df.file_name.values.tolist()
        # commons - list of new names that appear in watch folders
        #   potential dups - inbox files with the same name as files in library (no other criterion)
        potential_inbox_dups = [f for f in new_names if f in watch_file_names]
        watch_full_paths_str = [str(x) for x in watch_full_paths]
        potential_library_dups = filter_by_substring_list(
            watch_full_paths_str, potential_inbox_dups
        )
        return potential_inbox_dups, potential_library_dups, watch_full_paths


def get_files_from_folder(folder: str) -> Iterator[Any]:
    """Get iterator over recursive listing of files in folder.

    Args:
      folder: str: Folder to get files from

    Returns:
        Iterator over recursive directory contents (items matching *.* pattern)
    """
    return Path(folder).rglob("*.*")


def get_watch_folders_files_path(
    watch_folders: list[str],
) -> tuple[list[str], list[PosixPath]]:
    """Get list of files and files with full paths from the folder (recursively).

    Args:
      watch_folders: folder to be recursively listed

    Returns:
        List of filenames, list of full path of filenames
    """
    watch_full_paths = []
    for w in watch_folders:
        file_list_watch: Iterator = get_files_from_folder(w)
        path_list = list(file_list_watch)
        watch_full_paths.extend(path_list)
    watch_file_names = [path.name for path in watch_full_paths]
    return watch_file_names, watch_full_paths


def check_df_has_all_expected_columns(df: pd.DataFrame, expected_cols: list[str]):
    """Check if data frame has all expected columns."""
    for c in df.columns:
        if c not in expected_cols:
            raise MissingDfClusterColumnError(c)


def check_df_has_all_expected_columns_and_types(
    df: pd.DataFrame, col_expectations: dict
):
    """Check if data frame has all expected columns."""
    for c in df.columns:
        if c not in list(col_expectations.keys()):
            raise MissingDfClusterColumnError(c)
        # TODO: KS: 2020-12-26: check dtype
