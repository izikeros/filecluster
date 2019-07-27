import logging
import os
import sqlite3
from shutil import copy2, move

import pandas as pd

from filecluster import utlis as ut
from filecluster.dbase import get_new_cluster_id

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
        connection = self.db_connect()
        new_cluster_idx = get_new_cluster_id(connection)

        cluster = {'id': new_cluster_idx,
                   'start_date': None,
                   'stop_date': None}

        list_new_clusters = []

        max_time_delta = self.config['granularity_minutes']

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

    def move_or_copy_pictures(self, mode):
        """ move or copy items to dedicated folder"""
        pth_out = self.config['outDirName']
        pth_in = self.config['inDirName']
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

    def move_files_to_cluster_folder(self):
        dirs = self.image_df['date_string'].unique()

        for dir_name in dirs:
            ut.create_folder_for_cluster(self.config, dir_name, mode=self.config['move_or_copy'])

        self.move_or_copy_pictures(mode=self.config['move_or_copy'])

    def db_connect(self):
        connection = sqlite3.connect(self.config['db_file'])
        return connection

    def db_get_table_rowcount(self, table, connection=None):
        if not connection:
            connection = self.db_connect()
        cursor = connection.execute(f"SELECT * FROM {table};")
        num_records = len(cursor.fetchall())
        return num_records

    def db_save_images(self):
        """Export data frame with media information into database. Existing
        records will be replaced by new."""
        connection = self.db_connect()

        # TODO: consider insert or ignore
        query = '''INSERT OR REPLACE INTO media (file_name, date, size, 
        hash_value, full_path, image, is_image) 
        VALUES (?,?,?,?,?,?,?);'''

        # get number of rows before importing new media
        num_before = self.db_get_table_rowcount('media')

        # see: # https://stackoverflow.com/questions/23574614/appending
        # -pandas-dataframe-to
        # # -sqlite-table-by-primary-key
        connection.executemany(query, self.image_df[
            ['file_name', 'date',
             'size', 'hash_value', 'full_path', 'image',
             'is_image']].to_records(
            index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount('media')
        print(f"{num_after - num_before} image rows added, before: "
              f"{num_before}, "
              f"after: {num_after}")

    def db_save_clusters(self):
        """Export data frame with media information into database. Existing
        records will be replaced by new."""
        connection = self.db_connect()

        cluster_table_name = 'clusters'
        # TODO: consider insert or ignore
        query = f'''INSERT OR REPLACE INTO {cluster_table_name} (id, 
        start_date, end_date) VALUES (?,?,?);'''

        # get number of rows before importing new media
        num_before = self.db_get_table_rowcount(cluster_table_name)

        # see: # https://stackoverflow.com/questions/23574614/appending
        # -pandas-dataframe-to-sqlite-table-by-primary-key
        new_df = self.cluster_df[['id', 'start_date', 'end_date']].copy()
        new_df.id = new_df.id.astype(float)
        # temporal workaround
        connection.executemany(query, new_df.to_records(index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount(cluster_table_name)
        print(f"{num_after - num_before} cluster rows added, before: "
              f"{num_before}, "
              f"after: {num_after}")
