#!/usr/bin/env python3
"""Main module for image grouping by the event.

force deep scan
use existing clusters
"""
import argparse
import os
from pathlib import Path
from typing import List
from typing import Optional

from filecluster import version
from filecluster.configuration import CopyMode
from filecluster.configuration import get_proper_mode_config
from filecluster.configuration import override_config_with_cli_params
from filecluster.dbase import get_existing_clusters_info
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import ImageReader
from filecluster import logger

# TODO: KS: 2020-12-17: There are copies of config in the classes.
#  In extreme case various configs can be modified in different way.



def main(
    inbox_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    watch_dir_list: Optional[List[str]] = None,
    development_mode: Optional[bool] = None,
    no_operation: Optional[bool] = None,
    copy_mode: Optional[bool] = None,
    force_deep_scan: Optional[bool] = None,
    drop_duplicates: Optional[bool] = None,
    use_existing_clusters: Optional[bool] = None,
) -> dict:
    """Run clustering on the media files provided as inbox.

    Input args are default parameters to override and all are optional.

    Args:
        copy_mode:      Copy files instead of move
        inbox_dir:
        output_dir:
        watch_dir_list:
        development_mode:
        no_operation:
        force_deep_scan:
        drop_duplicates:
        use_existing_clusters:

    Returns:
        dictionary with diagnostics data
    """
    # get development or production config
    config = get_proper_mode_config(development_mode)

    # override config with CLI params
    logger.info("Override config with CLI params")
    config = override_config_with_cli_params(
        config=config,
        inbox_dir=inbox_dir,
        no_operation=no_operation,
        copy_mode=copy_mode,
        output_dir=output_dir,
        watch_dir_list=watch_dir_list,
        force_deep_scan=force_deep_scan,
        drop_duplicates=drop_duplicates,
        use_existing_clusters=use_existing_clusters,
    )
    # read cluster info from clusters in libraries (or empty dataframe)
    logger.info("Read cluster info from clusters in libraries")
    df_clusters, empty, non_compliant = get_existing_clusters_info(config)
    results = {
        "df_clusters": df_clusters,
        "empty": empty,
        "non_compliant": non_compliant,
    }

    # Configure image reader, initialize media database
    image_reader = ImageReader(config)

    # read timestamps from imported pictures/recordings
    USE_CSV = False
    INBOX_CSV_FILE_NAME = "h:\\incomming\\inbox.csv"
    if USE_CSV and os.path.isfile(INBOX_CSV_FILE_NAME):
        read_inbox_info_from_csv(INBOX_CSV_FILE_NAME, image_reader)
    else:
        logger.info("Read inbox info from files")
        image_reader.get_media_info_from_inbox_files()
        if USE_CSV:
            image_reader.media_df.to_csv(INBOX_CSV_FILE_NAME, index=False)

    # configure media grouper, initialize internal dataframes
    image_grouper = ImageGrouper(
        configuration=config,
        df_clusters=df_clusters,  # existing clusters
        inbox_media_df=image_reader.media_df.copy(),  # inbox media
    )

    # Mark inbox files duplicated with watch folders (if feature enabled)
    dup_files, dup_clusters = image_grouper.mark_inbox_duplicates()
    results |= {"dup_files": dup_files, "dup_clusters": dup_clusters}

    # == Assign to existing ==
    results |= {"files_existing_cl": None, "existing_cluster_names": None}
    if config.assign_to_clusters_existing_in_libs:
        # try assign media items to clusters already existing in the library
        (
            files_assigned_to_existing_cl,
            existing_cluster_names,
        ) = (
            image_grouper.assign_to_existing_clusters()
        )  # TODO: KS: 2020-12-26: should not have assigned target path yet
        results |= {
            "files_existing_cl": files_assigned_to_existing_cl,
            "existing_cluster_names": existing_cluster_names,
        }

    # == Handle not-clustered items ==
    # Calculate gaps for non-clustered items
    logger.info("Calculating gaps for creating new clusters")
    image_grouper.calculate_gaps()

    # create new clusters, assign media
    logger.info("run_clustering")
    new_cluster_df = image_grouper.run_clustering()
    results["new_cluster_df"] = new_cluster_df

    # assign target folder for new clusters (update media_df)
    logger.info("assign_target_folder_name_to_new_clusters")
    new_folder_names = (
        image_grouper.assign_target_folder_name_and_file_count_to_new_clusters(
            method=config.assign_date_to_clusters_method
        )
    )
    results["new_folder_names"] = new_folder_names

    # assign target folder for existing clusters
    logger.info("assign_target_folder_name_to_existing_clusters")
    image_grouper.assign_target_folder_name_to_existing_clusters()

    # left-merge clusters to media_df (to add "cluster_id" and "target_path")
    # from clusters_df to media_df
    logger.info("add_cluster_info_from_clusters_to_media")
    image_grouper.add_cluster_info_from_clusters_to_media()

    # assign target folder for duplicates
    if config.skip_duplicated_existing_in_libs:
        logger.info("assign target folder for duplicates")
        image_grouper.add_target_dir_for_duplicates()

    # Physically move or copy files to folders
    mode = image_grouper.config.mode
    if mode != CopyMode.NOP:
        image_grouper.move_files_to_cluster_folder()
    else:
        logger.debug("No copy/move operation performed since 'nop' option selected.")
    return results


# TODO Rename this here and in `main`
def read_inbox_info_from_csv(INBOX_CSV_FILE_NAME, image_reader):
    import pandas as pd
    from filecluster.configuration import Status

    logger.info("Read inbox info from CSV")
    image_reader.media_df = pd.read_csv(INBOX_CSV_FILE_NAME)
    # Revert data types after reading from CSV
    image_reader.media_df.date = pd.to_datetime(image_reader.media_df.date)
    image_reader.media_df.status = image_reader.media_df.status.apply(
        lambda x: Status[x.replace("Status.", "")]
    )


def add_args_to_parser(parser):
    """Add arguments to the parser."""
    parser.add_argument("-i", "--inbox-dir", help="directory with input images")
    parser.add_argument(
        "-o", "--output-dir", help="output directory for clustered images"
    )
    parser.add_argument(
        "-w",
        "--watch-dir",
        help="directory with structured media (official media repository)",
        action="append",
    )
    parser.add_argument(
        "-t",
        "--development-mode",
        help="Run script with development configuration - work on tests directories",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-n",
        "--no-operation",
        help="Do not introduce any changes on the disk. Dry run.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-y",
        "--copy-mode",
        help="Copy instead of default move",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-f",
        "--force-deep-scan",
        help="Force recalculate cluster info for each existing cluster.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-d",
        "--drop-duplicates",
        help="Do not cluster duplicates, store them in separate folder.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-c",
        "--use-existing-clusters",
        help=(
            "If possible, check watch folders if the inbox media can be "
            "assigned to already existing cluster."
        ),
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-v",
        "--version",
        help="Display program version",
        action="version",
        version=f"%(prog)s {version.__version__}",
    )
    return parser


def ensure_watch_dir_is_list(arguments):
    if isinstance(arguments.watch_dir, str):
        watch_dirs = [arguments.watch_dir]
    elif isinstance(arguments.watch_dir, List):
        watch_dirs = arguments.watch_dir
    elif arguments.watch_dir is None:
        watch_dirs = []
    else:
        raise TypeError("watch_dirs should be a list")
    return watch_dirs


if __name__ == "__main__":
    """Main routine to perform grouping process."""

    parser = argparse.ArgumentParser(description="Group media files by event")
    parser = add_args_to_parser(parser)
    args = parser.parse_args()

    watch_dirs = ensure_watch_dir_is_list(args)

    main(
        inbox_dir=str(Path(args.inbox_dir)),
        output_dir=str(Path(args.output_dir)),
        watch_dir_list=watch_dirs,
        development_mode=args.development_mode,
        no_operation=args.no_operation,
        copy_mode=args.copy_mode,
        force_deep_scan=args.force_deep_scan,
        drop_duplicates=args.drop_duplicates,
        use_existing_clusters=args.use_existing_clusters,
    )
