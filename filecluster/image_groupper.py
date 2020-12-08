import logging
import os
from shutil import copy2, move

import pandas as pd

from filecluster import utlis as ut
from filecluster.configuration import CopyMode, AssignDateToClusterMethod, Driver
from filecluster.dbase import get_new_cluster_id_from_dataframe

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def update_cluster_info(delta_from_previous, max_time_delta, index,
                        new_cluster_idx, list_new_clusters, start_date,
                        end_date, cluster):
    # check if new cluster encountered
    if delta_from_previous > max_time_delta or index == 0:
        new_cluster_idx += 1

        # append previous cluster date to the list
        if index > 0:
            # add previous cluster info to the list of clusters
            list_new_clusters.append(cluster)

        # create record for new cluster
        cluster = {
            'id': new_cluster_idx,
            'start_date': start_date,
            'end_date': None
        }
    else:
        cluster['start_date'] = start_date,

    # update cluster stop date
    cluster['end_date'] = end_date
    return cluster, new_cluster_idx, list_new_clusters


class ImageGroupper(object):
    def __init__(self,
                 configuration,
                 image_df=None,
                 df_clusters=None,
                 new_media_df=None):
        # read the config
        self.config = configuration

        # initialize image data frame (if provided)
        if image_df is not None:
            self.image_df = image_df

        # initialize cluster data frame (if provided)
        if df_clusters is not None:
            self.df_clusters = df_clusters

        # initialize imported media df
        if new_media_df is not None:
            self.new_media_df = new_media_df

    def run_clustering(self):
        """Perform clustering."""

        # Try to assign media to existing clusters
        self.assign_images_to_existing_clusters(active=False)

        logger.info("Calculating gaps for creating new clusters")
        self.calculate_gaps(date_col='date', delta_col='date_delta')

        # create new clusters, assign media
        cluster_list = self.add_cluster_id_to_files_in_data_frame()

        self.save_cluster_data_to_data_frame(cluster_list)
        self.assign_representative_date_to_clusters(
            method=self.config.assign_date_to_clusters_method)

    def calculate_gaps(self, date_col, delta_col):
        """Calculate gaps between consecutive shots, save delta to dataframe

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.new_media_df.sort_values(by=date_col,
                                      ascending=True,
                                      inplace=True)
        # calculate breaks between the shoots
        self.new_media_df[delta_col] = self.new_media_df[date_col].diff()

    def assign_images_to_existing_clusters(self, active):
        """Check if image can be assigned to any of existing clusters."""
        media_to_cluster = True

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
        while media_to_cluster and active:
            media_to_cluster = False
            # find images <existing_clusters_start, existing_clusters_end>
            # see pandas Query: https://stackoverflow.com/questions/11869910/
            not_clustered = self.image_df['cluster_id'].isnull()
            not_too_old = self.image_df['date'] > date_start - margin
            not_too_new = self.image_df['date'] < date_end + margin

            for index, _row in self.image_df[not_clustered & not_too_old
                                             & not_too_new].iterrows():
                # TODO: add query to the cluster
                fit = None
                # is in cluster range with margins:
                # where
                # date > (date_start - margin) and
                # date < (date_stop + margin)
                if fit:
                    media_to_cluster = True
                    # add cluster info to image
                    # update cluster range (start/end date)

    def add_cluster_id_to_files_in_data_frame(self):
        try:
            if self.config.db_driver == Driver.DATAFRAME:
                new_cluster_idx = get_new_cluster_id_from_dataframe(self.config)
            else:
                raise TypeError('Other drivers than Dataframe not supported')
                # new_cluster_idx = get_new_cluster_id_from_dataframe(
                #     db_connect(self.config.db_file))
        except:
            new_cluster_idx = 0

        cluster = {'id': new_cluster_idx, 'start_date': None, 'end_date': None}

        list_new_clusters = []

        max_time_delta = self.config.time_granularity

        n_files = len(self.new_media_df)
        i_file = 0

        # create new clusters for remaining media
        # new_images_df
        # df.loc[df['column_name'] == some_value]
        not_clusterd_map = self.new_media_df['cluster_id'].isnull()
        for index, _row in self.new_media_df[not_clusterd_map].iterrows():
            delta_from_previous = self.new_media_df.loc[index]['date_delta']
            start_date = end_date = self.new_media_df.loc[index]['date']

            cluster, new_cluster_idx, list_new_clusters = update_cluster_info(
                delta_from_previous, max_time_delta, index, new_cluster_idx,
                list_new_clusters, start_date, end_date, cluster)

            # assign cluster id to image
            self.new_media_df.loc[index, 'cluster_id'] = new_cluster_idx

            i_file += 1
            ut.print_progress(i_file, n_files, 'clustering: ')

        # save last cluster (TODO: check border cases: one file, one cluster, no-files,...)
        list_new_clusters.append(cluster)

        print("")
        print("{num_clusters} clusters identified".format(
            num_clusters=new_cluster_idx))

        return list_new_clusters

    def save_cluster_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        self.cluster_df = pd.DataFrame(row_list)

    def get_num_of_clusters_in_df(self):
        return self.new_media_df['cluster_id'].value_counts()

    def get_cluster_ids(self):
        return self.new_media_df['cluster_id'].unique()

    def assign_representative_date_to_clusters(
            self, method=AssignDateToClusterMethod.RANDOM):
        """ return date representing cluster
        """
        date_string = ''
        if method == AssignDateToClusterMethod.RANDOM:
            clusters = self.get_cluster_ids()
            for cluster in clusters:
                mask = self.new_media_df['cluster_id'] == cluster
                df = self.new_media_df.loc[mask]

                exif_date = df.sample(n=1)['date']
                exif_date = exif_date.values[0]
                ts = pd.to_datetime(str(exif_date))
                date_str = ts.strftime('[%Y_%m_%d]')
                time_str = ts.strftime('%H%M%S')

                image_count = df.loc[df['is_image'] == True].shape[0]
                video_count = df.loc[df['is_image'] == False].shape[0]

                date_string = "_".join([
                    date_str, time_str, 'IC_{ic}'.format(ic=image_count),
                    'VC_{vc}'.format(vc=video_count)
                ])

                self.new_media_df.loc[mask, 'date_string'] = date_string
        return date_string

    def move_files_to_cluster_folder(self):
        dirs = self.image_df['date_string'].unique()
        mode = self.config.mode

        # prepare directories in advance
        for dir_name in dirs:
            ut.create_folder_for_cluster(self.config, dir_name, mode=mode)

        # Move or copy items to dedicated folder."""
        pth_out = self.config.out_dir_name
        pth_in = self.config.in_dir_name
        n_files = len(self.image_df)
        i_file = 0
        for idx, row in self.image_df.iterrows():
            date_string = row['date_string']
            file_name = row['file_name']
            src = os.path.join(pth_in, file_name)
            dst = os.path.join(pth_out, date_string, file_name)
            if mode == CopyMode.COPY:
                copy2(src, dst)
            elif mode == CopyMode.MOVE:
                move(src, dst)
            elif mode == CopyMode.NOP:
                pass
            i_file += 1
            ut.print_progress(i_file, n_files, f'{mode}: ')
        print("")
