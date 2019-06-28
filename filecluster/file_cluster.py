# -*- coding: utf-8 -*-
# Copyright (c) 2017 Krystian Safjan
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# pylint: disable=C0103

import logging

from filecluster import utlis as ut

# In debug mode thumbnails are generated
from filecluster.dbase import db_create_tables_if_not_exists, read_images_database, \
    read_clusters_database
from filecluster.image_groupper import ImageGroupper
from filecluster.image_reader import ImageReader

# === Configuration
# generate thumbnail to be stored in pandas dataframe during the processing.
# Might be used in notebook.
GENERATE_THUMBNAIL = False
# in dev mode path are set to development datasets
DEV_MODE = True
# delete database during the start - provide clean start for development mode
DELETE_DB = True
# for more configuration options see: utils.get_development_config() and get_default_config()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Using info log-level")

if __name__ == '__main__':
    """Main routine to perform groupping process"""

    config = ut.get_default_config()
    if DEV_MODE:
        config = ut.get_development_config()

    # --- create database with schema if not exist (image and cluster tables)
    db_create_tables_if_not_exists(config)
    read_images_database()

    # --- TODO: run media scan if needed

    # --- Read date when pictures/recordings in inbox were taken
    image_reader = ImageReader(config)
    row_list = image_reader.get_data_from_files()
    # save image data to data frame (name, path, date, hash)
    image_reader.save_image_data_to_data_frame(row_list)
    # handle cases with missing exif data
    image_reader.cleanup_data_frame_timestamps()

    # TODO: mark duplicates
    image_reader.compare_data_frame_to_image_database()

    # --- initialize media grouper
    image_groupper = ImageGroupper(configuration=config,
                                   image_df=image_reader.image_df)

    # --- Read clusters from database
    # TODO: read clusters
    read_clusters_database()

    # --- Perform clustering
    print("calculating gaps")
    image_groupper.calculate_gaps(date_col='date', delta_col='date_delta')
    # actual clustering takes place here:
    cluster_list = image_groupper.add_tmp_cluster_id_to_files_in_data_frame()
    image_groupper.save_cluster_data_to_data_frame(cluster_list)

    image_groupper.assign_representative_date_to_clusters(
        method=config['assign_date_to_clusters_method'])

    # Physically move or copy files to folders
    image_groupper.move_files_to_cluster_folder()

    # -- Save info to database
    image_groupper.db_save_images()
    image_groupper.db_save_clusters()

# TODO: read config from yaml
