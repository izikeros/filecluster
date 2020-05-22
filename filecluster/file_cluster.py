#!/usr/bin/env python3
# Copyright (c) 2017 Krystian Safjan
# This software is released under the MIT License.
import argparse
import logging
from pathlib import Path

from filecluster.configuration import get_development_config, get_default_config
from filecluster.dbase import delete_db_if_needed, db_create_clusters, db_create_media, DbHandler
from filecluster.image_groupper import ImageGroupper
from filecluster.image_reader import ImageReader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.info("Using info log-level")


def main(inbox_dir, output_dir, db_dir, development_mode, no_operation=False):
    if development_mode:
        config = get_development_config()
    else:
        config = get_default_config()

    # CLI arguments overrides default configuration options
    if inbox_dir:
        config.in_dir_name = inbox_dir

    if output_dir:
        config.out_dir_name = output_dir

    if no_operation:
        config.mode = 'nop'

    if not db_dir:
        db_dir = config.out_dir_name

    config.db_file = Path(db_dir) / 'filecluster_db'
    config.db_file_clusters = Path(db_dir) / 'clusters.p'  # TODO: KS: 2020-05-23: do not use picke
    config.db_file_media = Path(db_dir) / 'media.p'  # TODO: KS: 2020-05-23: do not use picke

    # --- create database with schema if not exist: image(media) and cluster tables
    delete_db_if_needed(config)
    db_create_media(config)
    db_create_clusters(config)
    # read_images_database()  # To be implemented
    # run_media_scan()  # To be implemented

    # --- Read date when pictures/recordings in inbox were taken
    image_reader = ImageReader(config)
    row_list = image_reader.get_data_from_files()
    image_reader.save_image_data_to_data_frame(row_list)
    image_reader.cleanup_data_frame_timestamps()
    # image_reader.check_import_for_duplicates_in_existing_clusters()  # To be implemented
    # --- initialize media grouper
    image_groupper = ImageGroupper(configuration=config,
                                   image_df=image_reader.image_df)

    # --- Read clusters from database
    # read_clusters_database()  # To be implemented
    image_groupper = run_clustering(config, image_groupper)

    mode = image_groupper.config.mode
    if mode != 'nop':
        # -- Physically move or copy files to folders
        image_groupper.move_files_to_cluster_folder()

    # -- Save info to database
    db_handler = DbHandler()
    db_handler.init_with_image_handler(image_groupper)
    db_handler.db_save_images()
    db_handler.db_save_clusters()


def run_clustering(config, image_groupper):
    """Perform clustering."""
    print("calculating gaps")
    image_groupper.calculate_gaps(date_col='date', delta_col='date_delta')
    # actual clustering takes place here:
    cluster_list = image_groupper.add_tmp_cluster_id_to_files_in_data_frame()
    image_groupper.save_cluster_data_to_data_frame(cluster_list)
    image_groupper.assign_representative_date_to_clusters(
        method=config.assign_date_to_clusters_method)
    return image_groupper


if __name__ == '__main__':
    """Main routine to perform grouping process"""
    parser = argparse.ArgumentParser(description="Purpose of the script")
    parser.add_argument(
        '-i', '--inbox-dir',
        help="directory with input images")
    parser.add_argument(
        '-o', '--output-dir',
        help="output directory for clustered images")
    parser.add_argument(
        '-d', "--development-mode",
        help="Run script with development configuration - work on tests directories",
        action='store_true',
        default=False)
    parser.add_argument(
        '-n', "--no-operation",
        help="Do not introduce any changes on the disk. Dry run.",
        action="store_true",
        default=False)
    args = parser.parse_args()

    main(inbox_dir=args.inbox_dir,
         output_dir=args.output_dir,
         development_mode=args.development_mode,
         no_operation=args.no_operation)
