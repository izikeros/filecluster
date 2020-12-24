import logging
import math
import os
import random
from datetime import timedelta
from pathlib import Path
from shutil import copy2, move
from typing import List, Optional

import pandas as pd
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from tqdm import tqdm

from filecluster import utlis as ut
from filecluster.configuration import (
    CopyMode,
    AssignDateToClusterMethod,
    CLUSTER_DF_COLUMNS,
    Status,
    Config,
)
from filecluster.dbase import get_new_cluster_id_from_dataframe
from filecluster.exceptions import DateStringNoneException, MissingDfClusterColumn
from filecluster.filecluster_types import MediaDataFrame, ClustersDataFrame

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def expand_cluster_or_init_new(
    delta_from_previous: Timedelta,
    max_time_delta: timedelta,
    index: int,
    new_cluster_idx: int,
    list_new_clusters: List[dict],
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
    def __init__(self, out_dir_name):
        self.out_dir = Path(out_dir_name)

    def for_new_cluster(self, date_string):
        return str(self.out_dir / "new" / date_string)

    def for_existing_cluster(self, dir_string):
        return str(self.out_dir / "existing" / dir_string)

    def for_duplicates(self, dir_string):
        return str(self.out_dir / "duplicated" / dir_string)


class ImageGrouper(object):
    def __init__(
        self,
        configuration: Config,
        df_clusters: Optional[ClustersDataFrame] = None,
        inbox_media_df: Optional[MediaDataFrame] = None,
    ):
        """

        configuration: instance of Configuration
        media_df: media database (loaded from file or empty)
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
        """Calculate gaps between consecutive shots, save delta to dataframe

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
        self.inbox_media_df[delta_col][sel] = self.inbox_media_df[date_col][sel].diff()

    def run_clustering(self):
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
                "is_continous": True,
            }
        )

        n_files = len(self.inbox_media_df)
        i_file = 0

        # set new index (as range)
        self.inbox_media_df.index = list(range(n_files))

        # Iterate over all inbox media and create new clusters for each item
        # or assign to one just created

        # create mask for selecting not clustered media items
        # FIXME: Dataframe store status as string - make it uniform either keep string of object
        sel_not_clustered = self.inbox_media_df["status"] == Status.UNKNOWN

        is_first_image_analysed = True
        for media_index, _row in self.inbox_media_df[sel_not_clustered].iterrows():
            if _row.cluster_id:
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

                i_file += 1
                ut.print_progress(i_file, n_files, "clustering: ")
            is_first_image_analysed = False

        # save last cluster (TODO: check border cases: one file, one cluster, no-files,...)
        list_new_cluster_dictionaries.append(current_cluster_dict)

        print("")
        print("{num_clusters} clusters identified".format(num_clusters=cluster_idx))

        return list_new_cluster_dictionaries

    def add_new_cluster_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        new_df = pd.DataFrame(row_list)
        self.df_clusters = pd.concat([self.df_clusters, new_df])

    def get_num_of_clusters_in_df(self):
        return self.inbox_media_df["cluster_id"].value_counts()

    def get_new_cluster_ids(self):
        sel_new = self.inbox_media_df["status"] == Status.NEW_CLUSTER
        return self.inbox_media_df[sel_new]["cluster_id"].unique()

    def assign_target_folder_name_to_clusters(
        self, method=AssignDateToClusterMethod.MEDIAN
    ) -> None:
        """Set cluster string in the dataframe and return the string."""
        date_string = ""

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

            ts = pd.to_datetime(str(exif_date))
            try:
                date_str = ts.strftime("[%Y_%m_%d]")
            except ValueError:
                date_str = f"[NaT_]_{str(random.randint(100000, 999999))}"

            try:
                time_str = ts.strftime("%H%M%S")
            except ValueError:
                time_str = f"{str(random.randint(100000, 999999))}"

            image_count = df.loc[df["is_image"]].shape[0]
            video_count = df.loc[~df["is_image"]].shape[0]

            date_string = "_".join(
                [
                    date_str,
                    time_str,
                    "IC_{ic}".format(ic=image_count),
                    "VC_{vc}".format(vc=video_count),
                ]
            )

            # save to cluster db
            pth = path_creator.for_new_cluster(date_string=date_string)
            sel_cluster = self.df_clusters.cluster_id == new_cluster
            self.df_clusters.target_path[sel_cluster] = pth
            self.df_clusters.new_file_count[sel_cluster] = image_count + video_count
        return None

    def move_files_to_cluster_folder(self):
        dirs = self.inbox_media_df["target_path"].unique()
        mode = self.config.mode

        # prepare directories in advance
        for dir_name in dirs:
            if dir_name is None:
                raise DateStringNoneException()
            ut.create_folder_for_cluster(
                config=self.config, date_string=dir_name, mode=mode
            )

        # Move or copy items to dedicated folder."""
        pth_out = self.config.out_dir_name
        pth_in = self.config.in_dir_name
        n_files = len(self.inbox_media_df)
        i_file = 0
        for idx, row in self.inbox_media_df.iterrows():
            date_string = row["target_path"]
            file_name = row["file_name"]
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == CopyMode.COPY:
                copy2(src, dst)
            elif mode == CopyMode.MOVE:
                move(src, dst)
            elif mode == CopyMode.NOP:
                pass
            i_file += 1
            ut.print_progress(i_file, n_files, f"{mode}: ")
        print("")

    def add_target_dir_for_duplicates(self):
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)
        # add target dir for the duplicates
        sel_dups = self.inbox_media_df.status == Status.DUPLICATE
        for idx, row in self.inbox_media_df[sel_dups].iterrows():
            dup_cluster = self.inbox_media_df.duplicated_cluster[idx][0]
            self.inbox_media_df.target_path[idx] = path_creator.for_duplicates(
                dup_cluster
            )
            pass

    def add_cluster_info_from_clusters_to_media(self):
        # add path info from cluster dir,
        self.inbox_media_df = self.inbox_media_df.merge(
            self.df_clusters[["cluster_id", "target_path"]], on="cluster_id", how="left"
        )
        return None

    def assign_to_existing_clusters(self) -> List[str]:
        path_creator = TargetPathCreator(out_dir_name=self.config.out_dir_name)

        check_df_has_all_expected_columns(
            df=self.df_clusters, expected_cols=CLUSTER_DF_COLUMNS
        )

        margin = self.config.time_granularity
        sel_no_duplicated = ~(self.inbox_media_df.status == Status.DUPLICATE)
        for index, row in tqdm(self.inbox_media_df[sel_no_duplicated].iterrows()):
            # index: str = row[0]
            img_time: Timestamp = row["date"]  # read item_date

            not_too_old_clusters = self.df_clusters.start_date - margin <= img_time
            not_too_new_clusters = self.df_clusters.end_date + margin >= img_time

            range_ok = not_too_old_clusters & not_too_new_clusters
            continous = self.df_clusters.is_continous
            candidate_clusters = self.df_clusters[range_ok & continous]
            if len(candidate_clusters) > 0:
                if len(candidate_clusters) > 1:
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
                self.df_clusters.start_date.loc[cluster_idx] = min(
                    self.df_clusters.start_date.loc[cluster_idx].values[0], img_time
                )
                self.df_clusters.end_date.loc[cluster_idx] = max(
                    self.df_clusters.end_date.loc[cluster_idx].values[0], img_time
                )
                # add target patch
                pth = self.df_clusters.path.loc[cluster_idx].values[0]
                target_pth = path_creator.for_existing_cluster(dir_string=pth)
                self.df_clusters.target_path.loc[cluster_idx] = target_pth

        has_cluster_id = self.inbox_media_df.cluster_id >= 0
        has_status_existing_cluster = (
            self.inbox_media_df.status == Status.EXISTING_CLUSTER
        )
        assigned = self.inbox_media_df[
            has_cluster_id & has_status_existing_cluster
        ].file_name.values.tolist()
        return assigned


def check_df_has_all_expected_columns(df, expected_cols):
    for c in df.columns:
        if c not in expected_cols:
            raise MissingDfClusterColumn(c)
