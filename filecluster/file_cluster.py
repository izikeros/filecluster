#!/usr/bin/env python3
# Copyright (c) 2017 Krystian Safjan
# This software is released under the MIT License.
import argparse
import logging

from filecluster.clustering import override_config_with_cli_params, set_db_paths_in_config, \
    read_timestamps_form_media_files, run_clustering_no_prior
from filecluster.configuration import get_development_config, get_default_config, Driver
from filecluster.dbase import delete_db_if_needed, db_create_clusters, db_create_media, \
    save_media_and_cluster_info_to_database, read_images_database, read_clusters_database
from filecluster.image_reader import run_media_scan_on_watch_folders, ImageReader

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main(inbox_dir, output_dir, db_dir, db_driver, development_mode, no_operation=False):
    # get proper config
    if development_mode:
        config = get_development_config()
    else:
        config = get_default_config()
    config = override_config_with_cli_params(config=config, inbox_dir=inbox_dir,
                                             no_operation=no_operation, output_dir=output_dir,
                                             db_driver=db_driver)
    # setup directory for storing databases
    if not db_dir:
        db_dir = config.out_dir_name
    config = set_db_paths_in_config(config, db_dir)

    logger.debug(config)
    # create databases with schema
    delete_db_if_needed(config)
    db_create_media(config)
    db_create_clusters(config)

    df_media = read_images_database(config)
    run_media_scan_on_watch_folders()

    # initialize image reader
    image_reader = ImageReader(config, df_media)

    # Read timestamps from imported pictures/recordings
    image_groupper = read_timestamps_form_media_files(config, image_reader)

    # Read clusters from database
    df_clusters = read_clusters_database()  # To be implemented

    # Run clustering
    if df_clusters is None:
        image_groupper = run_clustering_no_prior(config, image_groupper)
    else:
        image_groupper = run_clustering_with_prior(config, image_groupper)

    # Physically move or copy files to folders
    mode = image_groupper.config.mode
    if mode != 'nop':
        image_groupper.move_files_to_cluster_folder()

    # Save media and cluster info to database
    save_media_and_cluster_info_to_database(image_groupper)


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
        '-d', '--db-driver',
        help="technology to use to store cluster and media databases. sqlite|dataframe",
        required=False
    )
    parser.add_argument(
        '-t', "--development-mode",
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
         db_dir=None,
         db_driver=Driver[args.db_driver.upper()],
         development_mode=args.development_mode,
         no_operation=args.no_operation)
