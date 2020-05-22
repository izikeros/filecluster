import logging
import os
from shutil import copy2, move

import pandas as pd

from filecluster import utlis as ut
from filecluster.dbase import get_new_cluster_id, db_connect

logger = logging.getLogger(__name__)


class ImageGroupper(object):
    def __init__(self, configuration, image_df=None, cluster_df=None):
        # read the config
        self.config = configuration

        # initialize image data frame (if provided)
        if image_df is not None:
            self.image_df = image_df

        # initialize cluster data frame (if provided)
        if cluster_df is not None:
            self.cluster_df = cluster_df

    def calculate_gaps(self, date_col, delta_col):
        """Calculate gaps between consecutive shots, save delta to dataframe

        Use 'creation date' from given column and save results to
        selected 'delta' column
        """
        # sort by creation date
        self.image_df.sort_values(by=date_col, ascending=True, inplace=True)
        # calculate breaks between the shoots
        self.image_df[delta_col] = self.image_df[date_col].diff()

    def assign_images_to_existing_clusters(self, date_start, date_end,
                                           margin, conn):
        # TODO: finalize implemantation
        # --- check if image can be assigned to any of existing clusters
        run_again = True
        while run_again:
            # iterate over and over since new cluster members might drag
            # cluster boundaries that new images will fit now
            run_again = False
            # find images <existing_clusters_start, existing_clusters_end>
            # see pandas Query:
            # https://stackoverflow.com/questions/11869910/
            for index, _row in self.image_df[
                (self.image_df['cluster_id'].isnull() &
                 self.image_df['date'] > date_start - margin &
                 self.image_df['date'] < date_end + margin)].iterrows():

                # TODO: add query to the cluster
                fit = None
                # is in cluster range with margins:
                # where
                # date > (date_start - margin) and
                # date < (date_stop + margin)
                if fit:
                    run_again = True
                    # add cluster info to image
                    # update cluster range (start/end date)

    def add_tmp_cluster_id_to_files_in_data_frame(self):
        new_cluster_idx = get_new_cluster_id(db_connect(self.config.db_file))

        cluster = {'id': new_cluster_idx,
                   'start_date': None,
                   'stop_date': None}

        list_new_clusters = []

        max_time_delta = self.config.granularity_minutes

        n_files = len(self.image_df)
        i_file = 0

        # # TODO: uncomment when implemented
        # if 0 == 1:
        #     self.assign_images_to_existing_clusters(
        #         date_start=existing_clusters_start,
        #         date_end=existing_clusters_end,
        #         margin=self.config['granularity_minutes'],
        #         conn=conn)

        # new_images_df
        # df.loc[df['column_name'] == some_value]
        for index, _row in self.image_df[
            self.image_df['cluster_id'].isnull()].iterrows():
            delta_from_previous = self.image_df.loc[index]['date_delta']

            # check if new cluster encountered
            if delta_from_previous > max_time_delta or index == 0:
                new_cluster_idx += 1

                # append previous cluster date to the list
                if index > 0:
                    # add previous cluster info to the list of clusters
                    list_new_clusters.append(cluster)

                # create record for new cluster
                cluster = {'id': new_cluster_idx,
                           'start_date': self.image_df.loc[index]['date'],
                           'end_date': None}

            # assign cluster id to image
            self.image_df.loc[index, 'cluster_id'] = new_cluster_idx

            # update cluster stop date
            cluster['end_date'] = self.image_df.loc[index]['date']

            i_file += 1
            ut.print_progress(i_file, n_files, 'clustering: ')

        # save last cluster (TODO: check border cases: one file,
        # one cluster, no-files,...)
        list_new_clusters.append(cluster)

        print("")
        print("{num_clusters} clusters identified".format(
            num_clusters=new_cluster_idx))

        return list_new_clusters

    def save_cluster_data_to_data_frame(self, row_list):
        """convert list of rows to pandas dataframe"""
        self.cluster_df = pd.DataFrame(row_list)

    def get_num_of_clusters_in_df(self):
        return self.image_df['cluster_id'].value_counts()

    def get_cluster_ids(self):
        return self.image_df['cluster_id'].unique()

    def assign_representative_date_to_clusters(self, method='random'):
        """ return date representing cluster
        """
        if method == 'random':
            clusters = self.get_cluster_ids()
            for cluster in clusters:
                mask = self.image_df['cluster_id'] == cluster
                df = self.image_df.loc[mask]

                exif_date = df.sample(n=1)['date']
                exif_date = exif_date.values[0]
                ts = pd.to_datetime(str(exif_date))
                date_str = ts.strftime('[%Y_%m_%d]')
                time_str = ts.strftime('%H%M%S')

                image_count = df.loc[df['is_image'] == True].shape[0]
                video_count = df.loc[df['is_image'] == False].shape[0]

                date_string = "_".join([
                    date_str,
                    time_str,
                    'IC_{ic}'.format(ic=image_count),
                    'VC_{vc}'.format(vc=video_count)])

                self.image_df.loc[mask, 'date_string'] = date_string
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
            if mode == 'copy':
                copy2(src, dst)
            elif mode == 'move':
                move(src, dst)
            elif mode == 'nop':
                pass
            i_file += 1
            ut.print_progress(i_file, n_files, f'{mode}: ')
        print("")
