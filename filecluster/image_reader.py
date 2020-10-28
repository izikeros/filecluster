import logging
import os

import pandas as pd

import filecluster.utlis as ut

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def cleanup_data_frame_timestamps(image_df):
    """Get timestamp from exif (primary) or m_date. Drop not needed date cols."""
    logger.debug("Cleaning-up timestamps in imported media.")
    # use exif date as base
    image_df['date'] = image_df['exif_date']
    # unless is missing - then use modification date:
    image_df['date'] = image_df['date'].fillna(image_df['m_date'])

    # infer date format  from strings
    image_df['date'] = pd.to_datetime(image_df['date'], infer_datetime_format=True)

    image_df.drop(['m_date', 'c_date', 'exif_date'], axis=1, inplace=True)
    # image_df['m_date'] = pd.to_datetime(image_df['m_date'], infer_datetime_format=True)
    # image_df['c_date'] = pd.to_datetime(image_df['c_date'], infer_datetime_format=True)
    # image_df['exif_date'] = pd.to_datetime(image_df['exif_date'], infer_datetime_format=True)
    return image_df


class ImageReader(object):
    def __init__(self, config, image_df=None):
        # read the config
        self.config = config

        # initialize media database with existing media dataframe or cereate empty dataframe
        if image_df is None:
            logger.debug("Initializing empty media dataframe in ImageReader")
            self.image_df = pd.DataFrame
        else:
            logger.debug(
                f"Initializing media dataframe in ImageReader with provided df. Num records: {len(image_df)}")
            self.image_df = image_df

    def get_data_from_files(self):
        """return files data as list of rows (each row represented by dict)

        :param pth: path to inbox directory with files (pictures, video)
        :type pth: basestring
        :param ext: list of filename extensions taken into account
        :type ext: list
        :return: dataframe with all information
        :rtype: pandas dataframe
        """

        def _add_new_row():
            """generate single row based on values defined in outer method"""
            thumbnail = None
            if self.config.generate_thumbnails:
                thumbnail = ut.get_thumbnail(path_name)

            # define structure of images dataframe and fill with data
            row = {'file_name': fn,
                   'm_date': m_time,
                   'c_date': c_time,
                   'exif_date': exif_date,
                   'date': date,
                   'size': file_size,
                   'hash_value': hash_value,
                   'full_path': path_name,
                   'image': thumbnail,
                   'is_image': is_img,
                   'cluster_id': cluster_id,
                   'duplicate_to_ids': duplicate_to_ids
                   }
            return row

        list_of_rows = []
        pth = self.config.in_dir_name
        ext = self.config.image_extensions + self.config.video_extensions

        print(f"Reading data from: {pth}")
        list_dir = os.listdir(pth)
        n_files = len(list_dir)
        for i_file, fn in enumerate(os.listdir(pth)):
            if ut.is_supported_filetype(fn, ext):
                # full path + file name
                path_name = os.path.join(pth, fn)

                # get modification, creation and exif dates
                m_time, c_time, exif_date = ut.get_date_from_file(path_name)

                # determine if media file is image or other type
                is_img = ut.is_image(path_name, self.config.image_extensions)

                # file size
                file_size = os.path.getsize(path_name)

                # file hash
                hash_value = ut.hash_file(path_name)

                # placeholder for date representative for file
                date = None  # to be filled in later

                # placeholder for assignment to cluster
                cluster_id = None

                # placeholder for storing info on this file duplicates
                duplicate_to_ids = []

                # generate new row using data obtained above
                new_row = _add_new_row()

                list_of_rows.append(new_row)
            ut.print_progress(i_file, n_files - 1, 'reading files: ')
        print("")
        return list_of_rows

    # def save_image_data_to_data_frame(self, list_of_rows):
    #     """Convert list of rows to pandas dataframe."""
    #     # save image data: name, path, date, hash to data frame

    def check_import_for_duplicates_in_existing_clusters(self, new_media_df):
        if self.image_df.empty:
            logger.debug('Media db empty. Skipping duplicate analysis.')
            # TODO: KS: 2020-05-24: Consider checking for duplicates within import
            return None
        else:
            logger.debug("Checking newly imported files against database")
            # TODO: 1. check for duplicates: in newly imported files
            # TODO: 2. check for duplicates: newly imported files against database
            # TODO: mark duplicates if found any
            logger.warning("Duplicates check not implemented")
            return None


def check_on_updates_in_watch_folders(config):
    # TODO: KS: 2020-10-28: implement
    logger.info(f'Running media scan in {config.watch_folders} (not implemented yet)')


def check_if_media_files_from_db_exists():
    # TODO: KS: 2020-10-28: implement
    logger.info('Running media scan (not implemented yet)')
