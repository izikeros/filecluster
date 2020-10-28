#!/usr/bin/env python3
"""Main module for image clustering.

Development mode
================
- "copy" operation instead of "move" to protect source files.
- "delete db" database is usually deleted to ensure "fresh" start

inbox folder
============
Inbox - incoming media lands here.

outbox folder
============
Generated clusters are created here. If matching cluster already exist in watch
 folder

watch folder
============
Folder with main, structured collection of media. It is watched and compared
with potential newly created clusters - if corresponding cluster already exists
in watch folder, the cluster folder in outbox should have the same name as
existing luster including parent directory (year).

"""
import argparse
import logging
from typing import List

from filecluster.clustering import override_config_with_cli_params, set_db_paths_in_config, \
    get_media_info_from_imported_files
from filecluster.configuration import get_development_config, get_default_config, Driver, CopyMode
from filecluster.dbase import delete_dbs_if_needed, read_or_create_db_clusters, \
    save_media_and_cluster_info_to_database, read_or_create_media_database
from filecluster.image_groupper import ImageGroupper
from filecluster.image_reader import check_on_updates_in_watch_folders, ImageReader, \
    check_if_media_files_from_db_exists

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main(inbox_dir: str,
         output_dir: str,
         watch_dirs: List[str],
         db_dir: str,
         db_driver: str,
         development_mode: str,
         no_operation: bool = False):
    config = get_config(db_driver, development_mode, inbox_dir, no_operation,
                        output_dir, watch_dirs)
    config = setup_directory_for_database(config, db_dir)
    logger.debug(config)

    # delete DBs (option for development)
    delete_dbs_if_needed(config)

    # read or create media database (to store exif data)
    df_media = read_or_create_media_database(config)

    # read or create cluster database (to store cluster descriptions)
    df_clusters = read_or_create_db_clusters(config)

    #  check if watch folders contains files that are not in media database
    check_on_updates_in_watch_folders(config)

    #  check if media captured by media db still exists on the disk
    check_if_media_files_from_db_exists()

    # initialize image reader
    image_reader = ImageReader(config, df_media)

    # read timestamps from imported pictures/recordings
    new_media_df = get_media_info_from_imported_files(image_reader)

    duplicates = image_reader.check_import_for_duplicates_in_existing_clusters(
        new_media_df)

    # initialize media grouper
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


def setup_directory_for_database(config, db_dir):
    # setup directory for storing databases
    if not db_dir:
        db_dir = config.out_dir_name
    config = set_db_paths_in_config(config, db_dir)
    return config


def get_config(db_driver, development_mode, inbox_dir, no_operation,
               output_dir, watch_dirs):
    """Get proper config, override with CLI params."""
    # get proper config
    if development_mode:
        config = get_development_config()
    else:
        config = get_default_config()
    config = override_config_with_cli_params(config=config,
                                             inbox_dir=inbox_dir,
                                             no_operation=no_operation,
                                             output_dir=output_dir,
                                             db_driver=db_driver,
                                             watch_dirs=watch_dirs)
    return config


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

    main(inbox_dir=args.inbox_dir,
         output_dir=args.output_dir,
         watch_dirs=args.watch_dirs,
         db_dir=None,
         db_driver=Driver[args.db_driver.upper()],
         development_mode=args.development_mode,
         no_operation=args.no_operation)
