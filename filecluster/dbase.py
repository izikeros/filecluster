import logging
import os
import sqlite3

DELETE_DB = True

logger = logging.getLogger(__name__)


def db_create_tables_if_not_exists(configuration):
    # TODO: remove when development will be done
    if DELETE_DB:
        try:
            os.remove(configuration['db_file'])
            print(f"Database: {configuration['db_file']} has been deteted")
        except Exception as ex:
            print(ex)

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
        conn.rollback()
        raise e
    finally:
        # Close the db connection
        conn.close()


def read_clusters_database(config=None):
    clusters = None
    return clusters


def read_images_database():
    pass
