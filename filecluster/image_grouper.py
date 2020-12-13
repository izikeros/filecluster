import logging
import os
from shutil import copy2, move

import pandas as pd

from filecluster import utlis as ut
from filecluster.configuration import CopyMode, AssignDateToClusterMethod, Driver
from filecluster.dbase import get_new_cluster_id_from_dataframe

log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def update_cluster_info(
        delta_from_previous,
        max_time_delta,
        index,
        new_cluster_idx,
        list_new_clusters,
        media_date,
        cluster,
):
    """Update cluster info in cluster database.

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
    # TODO: KS: 2020-12-09: introduce custom types to verify input types
    # TODO: KS: 2020-12-09: consider using TypedDict as cluster
    #  (won't work on older versions of python)

    # check if new cluster encountered

    is_first_image_analysed = index == 0
    is_this_image_too_far_from_other_in_the_cluster = (
            delta_from_previous > max_time_delta
    )
    if is_this_image_too_far_from_other_in_the_cluster or is_first_image_analysed:
        new_cluster_idx += 1  # FIXME: KS: 2020-12-09: should'n this be incremented only in case
        #                           of first image?

        # append previous cluster date to the list
        if index > 0:
            # add previous cluster info to the list of clusters
            list_new_clusters.append(cluster)

        # create record for new cluster
        cluster = {"id": new_cluster_idx, "start_date": media_date, "end_date": None}
    else:
        cluster["start_date"] = media_date

    # update cluster stop date
    cluster["end_date"] = media_date
    return cluster, new_cluster_idx, list_new_clusters


class ImageGrouper(object):
    def __init__(
            self, configuration, media_df=None, df_clusters=None, inbox_media_df=None
    ):
        """

        configuration: instance of Configuration
        media_df: media database (loaded from file or empty)
        df_clusters: clusters database (loaded from file or empty)
        inbox_media_df: dataframe with inbox media
        """

        # read the config
        self.config = configuration

        # initialize image data frame (if provided)
        if media_df is not None:
            self.media_df = media_df

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
        # calculate breaks between the shoots
        self.inbox_media_df[delta_col] = self.inbox_media_df[date_col].diff()

    # TODO: KS: 2020-12-09: Needs implementation
    def assign_images_to_existing_clusters(self, assign_to_existing_is_active=False):
        """Check if image can be assigned to any of existing clusters.

            assign_to_existing_is_active: enable assigning to existing clusters
        :return:
        """
        has_media_to_cluster = True  # initialize
        date_start = None
        date_end = None
        margin = self.config.time_granularity

        # TODO: KS: 2020-05-25: consider quick assign first
        #  (find closest cluster for each media and assign + update cluster info)
        #  then run precise, multiple-run approach implemented below

        # Loop over not clustered media, try to assign cluster,
        # update cluster info and try again with remaining media.
        # Note that after adding new media to cluster boundaries might change and
        # new media might fit now
        while has_media_to_cluster and assign_to_existing_is_active:
            has_media_to_cluster = False
            # find images <existing_clusters_start, existing_clusters_end>
            # see pandas Query: https://stackoverflow.com/questions/11869910/
            not_clustered = self.media_df["cluster_id"].isnull()
            not_too_old = self.media_df["date"] > date_start - margin
            not_too_new = self.media_df["date"] < date_end + margin

            sel_clusters_of_interest = not_clustered & not_too_old & not_too_new
            for index, _row in self.media_df[sel_clusters_of_interest].iterrows():
                # TODO: add query to the cluster
                fit = None
                # is in cluster range with margins:
                # where
                # date > (date_start - margin) and
                # date < (date_stop + margin)
                if fit:
                    has_media_to_cluster = True
                    # add cluster info to image
                    # update cluster range (start/end date)

    def add_cluster_id_to_files_in_data_frame(self):
        try:
            if self.config.db_driver == Driver.DATAFRAME:
                new_cluster_idx = get_new_cluster_id_from_dataframe()
            else:
                raise TypeError("Other drivers than Dataframe not supported")
                # new_cluster_idx = get_new_cluster_id_from_dataframe(
                #     db_connect(self.config.db_file))
        except:
            new_cluster_idx = 0

        cluster_dict = {"id": new_cluster_idx, "start_date": None, "end_date": None}
        list_new_cluster_dictionaries = []
        max_time_delta = self.config.time_granularity
        n_files = len(self.inbox_media_df)
        i_file = 0

        # iterate over all inbox media and create new clusters for each item
        # or assign to one just created
        sel_not_clustered = self.inbox_media_df["cluster_id"].isnull()
        for media_index, _row in self.inbox_media_df[sel_not_clustered].iterrows():
            delta_from_previous = self.inbox_media_df.loc[media_index]["date_delta"]
            media_date = self.inbox_media_df.loc[media_index]["date"]

            (
                cluster_dict,
                new_cluster_idx,
                list_new_cluster_dictionaries,
            ) = update_cluster_info(
                delta_from_previous,
                max_time_delta,
                media_index,
                new_cluster_idx,
                list_new_cluster_dictionaries,
                media_date,
                cluster_dict,
            )

            # assign cluster id to image
            self.inbox_media_df.loc[media_index, "cluster_id"] = new_cluster_idx

            i_file += 1
            ut.print_progress(i_file, n_files, "clustering: ")

        # save last cluster (TODO: check border cases: one file, one cluster, no-files,...)
        list_new_cluster_dictionaries.append(cluster_dict)

        print("")
        print("{num_clusters} clusters identified".format(num_clusters=new_cluster_idx))

        return list_new_cluster_dictionaries

    def save_cluster_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        self.cluster_df = pd.DataFrame(row_list)

    def get_num_of_clusters_in_df(self):
        return self.inbox_media_df["cluster_id"].value_counts()

    def get_cluster_ids(self):
        return self.inbox_media_df["cluster_id"].unique()

    def assign_representative_date_to_clusters(
            self, method=AssignDateToClusterMethod.RANDOM
    ):
        """return date representing cluster"""
        date_string = ""
        if method == AssignDateToClusterMethod.RANDOM:
            clusters = self.get_cluster_ids()
            for cluster in clusters:
                mask = self.inbox_media_df["cluster_id"] == cluster
                df = self.inbox_media_df.loc[mask]

                exif_date = df.sample(n=1)["date"]
                exif_date = exif_date.values[0]
                ts = pd.to_datetime(str(exif_date))
                date_str = ts.strftime("[%Y_%m_%d]")
                time_str = ts.strftime("%H%M%S")

                image_count = df.loc[df["is_image"] == True].shape[0]
                video_count = df.loc[df["is_image"] == False].shape[0]

                date_string = "_".join(
                    [
                        date_str,
                        time_str,
                        "IC_{ic}".format(ic=image_count),
                        "VC_{vc}".format(vc=video_count),
                    ]
                )

                self.inbox_media_df.loc[mask, "date_string"] = date_string
        return date_string

    def move_files_to_cluster_folder(self):
        dirs = self.media_df["date_string"].unique()
        mode = self.config.mode

        # prepare directories in advance
        for dir_name in dirs:
            ut.create_folder_for_cluster(self.config, dir_name, mode=mode)

        # Move or copy items to dedicated folder."""
        pth_out = self.config.out_dir_name
        pth_in = self.config.in_dir_name
        n_files = len(self.media_df)
        i_file = 0
        for idx, row in self.media_df.iterrows():
            date_string = row["date_string"]
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
