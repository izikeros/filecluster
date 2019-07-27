import logging
import os
import sqlite3

import pandas as pd

from filecluster.configuration import DELETE_DB

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def delete_db_if_needed(config):
    if DELETE_DB:
        try:
            if config['db_driver'] == 'dataframe':
                os.remove(config['db_file_clusters'])
                print(f"Database: {config['db_file_clusters']} has been deleted")
                os.remove(config['db_file_media'])
                print(f"Database: {config['db_file_media']} has been deleted")
            else:
                os.remove(config['db_file'])
                print(f"Database: {config['db_file']} has been deleted")

        except Exception as ex:
            print(ex)


def db_create_clusters(config):
    if config['db_driver'] == 'dataframe':
        db_create_clusters_df(config)
    elif config['db_driver'] == 'sqlite':
        db_create_clusters_sqlite_table(config)


def db_create_media(config):
    if config['db_driver'] == 'dataframe':
        db_create_media_df(config)
    elif config['db_driver'] == 'sqlite':
        db_create_media_sqlite_table(config)


def db_create_clusters_df(config):
    logger.info('Check if need to create empty df for clusters')
    if not os.path.isfile(config['db_file_media']):
        df = pd.DataFrame(columns=['id', 'start_date', 'end_date', 'median'])
        df.to_pickle(config['db_file_media'])
        logger.info('Empty dataframe for cluster data created')
    else:
        logger.info('Dataframe with cluster data already exists')


def db_create_media_df(config):
    logger.info('Check if need to create empty df for media')
    if not os.path.isfile(config['db_file_clusters']):
        df = pd.DataFrame(
            columns=['file_name', 'm_date', 'c_date', 'exif_date', 'date', 'size', 'hash_value',
                     'full_path', 'image', 'is_image'])
        df.to_pickle(config['db_file'])
        logger.info('Empty dataframe for media data created')
    else:
        logger.info('Dataframe with media data already exists')


def db_create_media_sqlite_table(configuration):
    # TODO: remove when development will be done
    conn = None

    try:
        # Creates or opens a file called mydb with a SQLite3 DB
        conn = sqlite3.connect(configuration['db_file'])
        # Get a cursor object
        cursor = conn.cursor()
        # Check if table users does not exist and create it

        # table for individual media files
        cursor.execute('''CREATE TABLE IF NOT EXISTS media(
                             file_name TEXT(256) PRIMARY KEY,
                             m_date DATETIME, 
                             c_date DATETIME, 
                             exif_date DATETIME, 
                             date DATETIME, 
                             size INTEGER,
                             hash_value TEXT,
                             full_path TEXT,
                             image BLOB,
                             is_image INTEGER)''')

        # Commit the change
        conn.commit()
        print("Database created/opened")
    # Catch the exception
    except Exception as e:
        # Roll back any change if something goes wrong
        if conn is not None:
            conn.rollback()
        raise e
    finally:
        # Close the db connection
        if conn is not None:
            conn.close()


def db_create_clusters_sqlite_table(configuration):
    conn = None

    try:
        # Creates or opens a file called mydb with a SQLite3 DB
        conn = sqlite3.connect(configuration['db_file'])
        # Get a cursor object
        cursor = conn.cursor()
        # Check if table users does not exist and create it

        # table with cluster information
        cursor.execute('''CREATE TABLE IF NOT EXISTS clusters(
                          id INTEGER PRIMARY KEY, 
                          start_date DATETIME, 
                          end_date DATETIME, 
                          median DATETIME)''')
        # Commit the change
        conn.commit()
        print("Database created/opened")
    # Catch the exception
    except Exception as e:
        # Roll back any change if something goes wrong
        if conn is not None:
            conn.rollback()
        raise e
    finally:
        # Close the db connection
        if conn is not None:
            conn.close()


def read_clusters_database(config=None):
    logger.info('Reading clusters database (not implemented yet)')
    clusters = None
    return clusters


def read_images_database():
    logger.info('Reading media database (not implemented yet)')


def get_new_cluster_id(conn):
    """Add tmp cluster id information to each file
    """

    # Hint on reading date: http://numericalexpert.com/blog/sqlite_blob_time/
    # set cursor to start?
    cursor = conn.execute(f"SELECT MIN(start_date) FROM clusters;")
    existing_clusters_start = cursor.fetchone()  # FIXME

    # set cursor to end?
    cursor = conn.execute(f"SELECT MAX(end_date) FROM clusters;")
    existing_clusters_end = cursor.fetchone()  # FIXME

    cursor = conn.execute(f"SELECT MAX(id) FROM clusters;")
    existing_clusters_last_id = cursor.fetchone()[0]

    if not existing_clusters_last_id:
        existing_clusters_last_id = 0  # FIXME: check if this is ok

    # entry for new potential record
    new_cluster_idx = existing_clusters_last_id + 1

    return new_cluster_idx
