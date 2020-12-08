#!/usr/bin/env python3
"""Main module for image clustering."""
import argparse
import logging
from typing import List, Optional

from filecluster.configuration import Driver, CopyMode, \
    setup_directory_for_database, override_config_with_cli_params, get_proper_mode_config
from filecluster.dbase import delete_dbs_if_needed, read_or_create_db_clusters, \
    save_media_and_cluster_info_to_database, read_or_create_media_database
from filecluster.image_groupper import ImageGroupper
from filecluster.image_reader import check_on_updates_in_watch_folders, ImageReader, \
    check_if_media_files_from_db_exists, get_media_info_from_inbox_files

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main(inbox_dir: str,
         output_dir: str,
         watch_dir_list: List[str],
         db_dir_str: Optional[str],
         db_driver: Driver,
         development_mode: bool,
         no_operation: bool = False):
    # get proper config
    config = get_proper_mode_config(development_mode)

    # override config with CLI params
    config = override_config_with_cli_params(config=config,
                                             inbox_dir=inbox_dir,
                                             no_operation=no_operation,
                                             output_dir=output_dir,
                                             db_driver=db_driver,
                                             watch_dir_list=watch_dir_list)

    config = setup_directory_for_database(config, db_dir_str)
    logger.debug(config)

    # delete DBs (option for development)
    delete_dbs_if_needed(config)

    # read or create media database (to store exif data)
    df_media = read_or_create_media_database(config)

    # read or create cluster database (to store cluster descriptions)
    df_clusters = read_or_create_db_clusters(config)

    #  check if watch folders contains files that are not in library
    check_on_updates_in_watch_folders(config)

    #  Not implemented: check if media captured by media db still exists on the disk
    check_if_media_files_from_db_exists()

    # Configure image reader, initialize media database (if needed)
    image_reader = ImageReader(config, df_media)

    # read timestamps from imported pictures/recordings
    new_media_df = get_media_info_from_inbox_files(image_reader)

    # check if not duplicated with media in output clusters dir
    # Not implemented yet
    duplicates = image_reader.check_import_for_duplicates_in_existing_clusters(
        new_media_df)

    # check if not duplicated with watch folders (structured repository)
    new_media_df = image_reader.check_import_for_duplicates_in_watch_folders(
        new_media_df)

    # configure media grouper, initialize internal dataframes
    image_groupper = ImageGroupper(
        configuration=config,
        image_df=image_reader.image_df,
        df_clusters=df_clusters,
        new_media_df=new_media_df,
    )

    # Run clustering
    image_groupper.run_clustering()

    # FIXME: KS: 2020-05-25: Merge new media and images_df
    image_groupper.image_df = image_groupper.new_media_df

    # Physically move or copy files to folders
    mode = image_groupper.config.mode
    if mode != CopyMode.NOP:
        image_groupper.move_files_to_cluster_folder()
    else:
        logger.debug(
            "No copy/move operation performed since 'nop' option selected.")

    # Save media and cluster info to database
    save_media_and_cluster_info_to_database(image_groupper)


if __name__ == '__main__':
    """Main routine to perform grouping process."""
    parser = argparse.ArgumentParser(description="Purpose of the script")
    parser.add_argument('-i',
                        '--inbox-dir',
                        help="directory with input images")
    parser.add_argument('-o',
                        '--output-dir',
                        help="output directory for clustered images")
    parser.add_argument(
        '-w',
        '--watch-dirs',
        help="directories with structured media (official media repository)")
    parser.add_argument(
        '-d',
        '--db-driver',
        help=
        "technology to use to store cluster and media databases. sqlite|dataframe",
        required=False)
    parser.add_argument(
        '-t',
        "--development-mode",
        help=
        "Run script with development configuration - work on tests directories",
        action='store_true',
        default=False)
    parser.add_argument(
        '-n',
        "--no-operation",
        help="Do not introduce any changes on the disk. Dry run.",
        action="store_true",
        default=False)
    # TODO: KS: 2020-10-28: add watch folder(s)
    args = parser.parse_args()

    if isinstance(args.watch_dirs, str):
        watch_dirs = [args.watch_dirs]
    elif isinstance(args.watch_dirs, str):
        watch_dirs = args.watch_dirs
    else:
        raise TypeError("watch_dirs should be a list")

    main(inbox_dir=args.inbox_dir, output_dir=args.output_dir, watch_dir_list=watch_dirs,
         db_dir_str=None, db_driver=Driver[args.db_driver.upper()],
         development_mode=args.development_mode, no_operation=args.no_operation)
