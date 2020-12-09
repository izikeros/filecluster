"""Module for handling operations on both databases: media and clusters."""
import logging
import os

import pandas as pd

from filecluster.configuration import Driver, CopyMode, Config

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

MEDIA_DF_COLUMNS = [
    'file_name', 'm_date', 'c_date', 'exif_date', 'date', 'size', 'hash_value',
    'full_path', 'image', 'is_image'
]


def delete_dbs_if_needed(config: Config):
    if config.delete_db:
        if config.db_driver == Driver.DATAFRAME:
            try:
                os.remove(config.db_file_clusters)
                logger.info(
                    f"Database: {config.db_file_clusters} has been deleted")
            except FileNotFoundError:
                pass

            try:
                os.remove(config.db_file_media)
                logger.info(
                    f"Database: {config.db_file_media} has been deleted")
            except FileNotFoundError:
                pass
        else:
            if config.db_driver == Driver.SQLITE:
                # delete_sql_db(config)
                logger.error(f"Unknown driver: {config.db_driver}")


def read_or_create_db_clusters(config: Config):
    if config.db_driver == Driver.DATAFRAME:
        db_create_clusters_df(config)
    elif config.db_driver == Driver.SQLITE:
        raise TypeError("SQLITE not supported anymore")
        # db_create_clusters_sqlite_table(config)


def db_create_clusters_df(config: Config):
    logger.debug('Check if need to create empty df for clusters')
    if not os.path.isfile(config.db_file_clusters):
        df = pd.DataFrame(columns=['id', 'start_date', 'end_date', 'median'])
        df.to_pickle(str(config.db_file_clusters))
        logger.info(
            f'Empty dataframe for cluster data created in {config.db_file_clusters}'
        )
    else:
        logger.debug(
            f'Dataframe with cluster data {config.db_file_clusters} already exists'
        )


def db_create_media_df(config: Config):
    """Create media database file in location from the config.

    NOTE: default config.db_file_media is created automatically in
          configure_db_path() function.
    """
    logger.debug('Check if need to create empty df for media')
    if not os.path.isfile(config.db_file_media):
        df = pd.DataFrame(columns=MEDIA_DF_COLUMNS)
        df.to_pickle(str(config.db_file_media))
        logger.info(
            f'Empty dataframe for media data created in: {config.db_file_media}'
        )
        return True
    else:
        logger.debug(
            f'Dataframe with media data {config.db_file_media} already exists')
        return False


def read_or_create_clusters_database(config=None):
    logger.info('Trying to read clusters database')
    db_file_clusters = config.db_file_clusters
    try:
        pkl_file = open(db_file_clusters, 'rb')
        df_clusters = pd.read_pickle(pkl_file)
    except FileNotFoundError:
        logger.info(
            f'File {db_file_clusters} not found. New one will be created.')
        db_create_clusters_df(
            config)  # TODO: KS: 2020-05-24: add support for sqlite
        df_clusters = None
    return df_clusters


def read_or_create_media_database(config: Config):
    logger.info('Trying to read media database')
    try:
        pkl_file = open(config.db_file_media, 'rb')
        media_df = pd.read_pickle(pkl_file)
    except FileNotFoundError:
        logger.info(
            f'File {config.db_file_media} not found. New one will be created.')
        db_create_media_df(config)
        media_df = None
    return media_df


def save_media_and_cluster_info_to_database(image_groupper):
    if image_groupper.config.db_driver == Driver.DATAFRAME:
        mode = image_groupper.config.mode
        if mode != CopyMode.NOP:
            pd.to_pickle(image_groupper.cluster_df,
                         image_groupper.config.db_file_clusters)
            pd.to_pickle(image_groupper.image_df,
                         image_groupper.config.db_file_media)
        else:
            logger.debug(
                "No update to media and cluster databases since 'nop' option is selected."
            )
    else:
        # save_sql_dbs(image_groupper)
        raise TypeError("Unspupported driver.")


def get_new_cluster_id_from_dataframe():
    new_cluster_id = 0
    logger.warning('Implement reading new cluster id based on existing cluster db')
    return new_cluster_id
