"""Sqlite3 handler - dropped for now"""
import logging
import os
import sqlite3

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_new_cluster_id_from_sqlite(conn):
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


def db_connect(db_file):
    connection = sqlite3.connect(db_file)
    return connection


class SqliteHandler:
    """Handler for sqlite database

    see: https://stackabuse.com/a-sqlite-tutorial-with-python/
    """
    def __init__(self):
        self.config = None
        self.image_df = None
        self.cluster_df = None
        self.connection = None

    def init_with_image_handler(self, image_handler):
        self.config = image_handler.config
        self.image_df = image_handler.image_df
        self.cluster_df = image_handler.cluster_df

    def db_connect(self):
        connection = sqlite3.connect(self.config.db_file)
        return connection

    def db_get_table_rowcount(self, table, connection=None):
        if not connection:
            connection = self.db_connect()
        cursor = connection.execute(f"SELECT * FROM {table};")
        num_records = len(cursor.fetchall())
        return num_records

    def db_save_images(self):
        """Save media information into database.

         Existing records will be replaced by new."""
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
        connection.executemany(
            query, self.image_df[[
                'file_name', 'date', 'size', 'hash_value', 'full_path',
                'image', 'is_image'
            ]].to_records(index=False))
        connection.commit()

        # get number of rows after importing new media
        num_after = self.db_get_table_rowcount('media')
        print(f"DB save:\t{num_after - num_before} image rows added, before: "
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
        print(
            f"DB save:\t{num_after - num_before} cluster rows added, before: "
            f"{num_before}, "
            f"after: {num_after}")

def db_create_media_sqlite_table(configuration):
    # TODO: remove when development will be done
    conn = None

    try:
        # Ensure that path for DBs exists, create if needed
        parent = configuration.db_file.parent
        if not os.path.isdir(parent):
            os.makedirs(name=parent, exist_ok=False)
            logger.info(f"Created all dirs needed for {parent}")

        # Creates or opens a file called mydb with a SQLite3 DB
        conn = sqlite3.connect(configuration.db_file)
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
        logger.info("MediaDataframe database created/opened")
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
        conn = sqlite3.connect(configuration.db_file)
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
        logger.info("Cluster database created/opened")
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


def save_sql_dbs(image_groupper):
    db_handler = SqliteHandler()
    db_handler.init_with_image_handler(image_groupper)
    db_handler.db_save_images()
    db_handler.db_save_clusters()

def delete_sql_db(config):
    if os.path.isfile(config.db_file):
        try:
            os.remove(config.db_file)
            logger.info(f"Database: {config.db_file} has been deleted")
        except Exception as ex:
            print(ex)
