#!/usr/bin/env python3
"""Main module for image clustering."""
import argparse
import logging
from pathlib import Path
from typing import List, Optional
import pandas as pd
from filecluster import version
from filecluster.configuration import (
    CopyMode,
    override_config_with_cli_params,
    get_proper_mode_config,
)
from filecluster.dbase import (
    get_existing_clusters_info,
)
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import (
    ImageReader,
    mark_inbox_duplicates_vs_watch_folders,
)

# TODO: KS: 2020-12-17: There are copies of config in the classes.
#  In extreme case various configs can be modified in different way.
log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
):
    """Main function to run clustering.

    Input args are default parameters to override and all are optional.

    Args:
        inbox_dir:
        output_dir:
        watch_dir_list:
        development_mode:
        no_operation:
        force_deep_scan:
        drop_duplicates:
        use_existing_clusters:

    Returns:

    """
    # get development or production config
    config = get_proper_mode_config(development_mode)

    # override config with CLI params
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
    # TODO: Add listing of dirs without media
    df_clusters = get_existing_clusters_info(config)

    # Configure image reader, initialize media database
    image_reader = ImageReader(config)

    # read timestamps from imported pictures/recordings
    try:
        image_reader.media_df = pd.read_csv('h:\\incomming\\inbox.csv')
    except:
        image_reader.get_media_info_from_inbox_files()
        image_reader.media_df.to_csv('h:\\incomming\\inbox.csv', index=False)

    # skip inbox files duplicated with watch folders (if feature enabled)
    inbox_media_df, dups = mark_inbox_duplicates_vs_watch_folders(
        config.watch_folders,
        image_reader.media_df.copy(),
        config.skip_duplicated_existing_in_libs,
    )

    # configure media grouper, initialize internal dataframes
    image_grouper = ImageGrouper(
        configuration=config,
        df_clusters=df_clusters,
        inbox_media_df=inbox_media_df,
    )

    if config.assign_to_clusters_existing_in_libs:
        # try assign media items to clusters already existing in the library
        assigned = image_grouper.assign_to_existing_clusters()

    # Calculate gaps for non-clustered items
    logger.info("Calculating gaps for creating new clusters")
    image_grouper.calculate_gaps()

    # create new clusters, assign media
    cluster_list = image_grouper.run_clustering()

    image_grouper.add_new_cluster_data_to_data_frame(cluster_list)
    image_grouper.assign_target_folder_name_to_clusters(
        method=config.assign_date_to_clusters_method
    )
    # left-merge clusters to media_df
    image_grouper.add_cluster_info_from_clusters_to_media()

    # add target dir for duplicates
    image_grouper.add_target_dir_for_duplicates()

    # Physically move or copy files to folders
    mode = image_grouper.config.mode
    if mode != CopyMode.NOP:
        image_grouper.move_files_to_cluster_folder()
    else:
        logger.debug("No copy/move operation performed since 'nop' option selected.")


if __name__ == "__main__":
    """Main routine to perform grouping process."""
    parser = argparse.ArgumentParser(description="Purpose of the script")
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
        help="",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-c",
        "--use-existing-clusters",
        help="",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version.__version__}"
    )
    args = parser.parse_args()

    if isinstance(args.watch_dir, str):
        watch_dirs = [args.watch_dir]
    elif isinstance(args.watch_dir, List):
        watch_dirs = args.watch_dir
    else:
        raise TypeError("watch_dirs should be a list")

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
