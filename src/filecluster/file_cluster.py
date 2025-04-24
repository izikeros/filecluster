#!/usr/bin/env python3
"""Main module for image grouping by the event.

This module provides functionality to cluster media files (images and videos)
based on their timestamps, helping organize them into event-based folders.
"""

import argparse
from pathlib import Path
from typing import Any

from filecluster import logger
from filecluster.configuration import (
    CopyMode,
    default_factory,
)
from filecluster.dbase import get_existing_clusters_info
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import ImageReader


def main(
    inbox_dir: str | None = None,
    output_dir: str | None = None,
    watch_dir_list: list[str] | None = None,
    development_mode: bool = False,
    no_operation: bool = False,
    copy_mode: bool = False,
    force_deep_scan: bool | None = None,
    drop_duplicates: bool | None = None,
    use_existing_clusters: bool | None = None,
) -> dict[str, Any]:
    """Run clustering on the media files provided as inbox.

    Groups media files based on their timestamps, organizing them into event-based folders.
    Can optionally check for duplicates and existing clusters in watch directories.

    Args:
        inbox_dir: Input directory containing media files to process
        output_dir: Output directory where clustered media will be placed
        watch_dir_list: List of directories to check for existing clusters and duplicates
        development_mode: Whether to use development configuration
        no_operation: Perform a dry run without making changes to the filesystem
        copy_mode: Copy files instead of moving them
        force_deep_scan: Force recalculation of cluster info for existing clusters
        drop_duplicates: Skip clustering duplicates and store them in a separate folder
        use_existing_clusters: Try to assign media to existing clusters in watch folders

    Returns:
        Dictionary with diagnostic data from the clustering process
    """
    # Get appropriate configuration based on mode
    config = default_factory.get_config(is_development_mode=development_mode)

    # Override configuration with CLI parameters
    logger.info("Applying CLI parameter overrides to configuration")
    config = default_factory.override_from_cli(
        config=config,
        inbox_dir=inbox_dir,
        output_dir=output_dir,
        watch_dir_list=watch_dir_list,
        force_deep_scan=force_deep_scan,
        no_operation=no_operation,
        copy_mode=copy_mode,
        drop_duplicates=drop_duplicates,
        use_existing_clusters=use_existing_clusters,
    )

    # Read cluster info from libraries (or get empty DataFrame if none found)
    logger.info("Reading cluster information from watch directories")
    df_clusters, empty_folders, non_compliant_folders = get_existing_clusters_info(
        config.watch_folders,
        config.skip_duplicated_existing_in_libs,
        config.assign_to_clusters_existing_in_libs,
        config.force_deep_scan,
    )
    results = {
        "df_clusters": df_clusters,
        "empty": empty_folders,
        "non_compliant": non_compliant_folders,
    }

    # Configure image reader and initialize media database
    image_reader = ImageReader(in_dir_name=config.in_dir_name)
    logger.info("Reading media information from inbox files")
    image_reader.get_media_info_from_inbox_files()

    # Configure media grouper and initialize internal dataframes
    image_grouper = ImageGrouper(
        configuration=config,
        df_clusters=df_clusters,  # existing clusters
        inbox_media_df=image_reader.media_df.copy(),  # inbox media
    )

    # Mark duplicates if enabled
    if config.skip_duplicated_existing_in_libs and config.watch_folders:
        logger.info("Identifying duplicates against watch directories")
        dup_files, dup_clusters = image_grouper.mark_inbox_duplicates()
        results.update({"dup_files": dup_files, "dup_clusters": dup_clusters})
    else:
        results.update({"dup_files": 0, "dup_clusters": 0})

    # Assign to existing clusters if enabled
    results.update({"files_existing_cl": None, "existing_cluster_names": None})
    if config.assign_to_clusters_existing_in_libs and config.watch_folders:
        logger.info("Assigning media to existing clusters")
        files_assigned, existing_cluster_names = (
            image_grouper.assign_to_existing_clusters()
        )
        results.update(
            {
                "files_existing_cl": files_assigned,
                "existing_cluster_names": existing_cluster_names,
            }
        )

    # Handle non-clustered items
    logger.info("Calculating time gaps for creating new clusters")
    image_grouper.calculate_gaps()

    # Create new clusters and assign media
    logger.info("Running clustering algorithm")
    new_cluster_df = image_grouper.run_clustering()
    results["new_cluster_df"] = new_cluster_df

    # Assign target folder names for new clusters
    logger.info("Assigning target folder names to new clusters")
    new_folder_names = (
        image_grouper.assign_target_folder_name_and_file_count_to_new_clusters(
            method=config.assign_date_to_clusters_method
        )
    )
    results["new_folder_names"] = new_folder_names

    # Assign target folder names for existing clusters
    logger.info("Assigning target folder names to existing clusters")
    image_grouper.assign_target_folder_name_to_existing_clusters()

    # Add cluster info to media records
    logger.info("Adding cluster information to media records")
    image_grouper.add_cluster_info_from_clusters_to_media()

    # Add target directories for duplicates if enabled
    if config.skip_duplicated_existing_in_libs:
        logger.info("Assigning target directories for duplicates")
        image_grouper.add_target_dir_for_duplicates()

    # Move or copy files to their target folders
    if config.mode != CopyMode.NOP:
        logger.info(
            f"{'Copying' if config.mode == CopyMode.COPY else 'Moving'} files to cluster folders"
        )
        image_grouper.move_files_to_cluster_folder()
    else:
        logger.info("Dry run mode - no files were moved or copied")

    return results


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser for CLI usage.

    Returns:
        Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Group media files by event based on their timestamps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-i", "--inbox-dir", help="Directory with input media files to process"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        help="Output directory where clustered media will be placed",
    )
    parser.add_argument(
        "-w",
        "--watch-dir",
        help="Directory with structured media (official media repository)",
        action="append",
        dest="watch_dirs",
    )
    parser.add_argument(
        "-t",
        "--development-mode",
        help="Run with development configuration using test directories",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-n",
        "--no-operation",
        help="Perform a dry run without making changes to the filesystem",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-y",
        "--copy-mode",
        help="Copy files instead of moving them",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-f",
        "--force-deep-scan",
        help="Force recalculation of cluster info for each existing cluster",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-d",
        "--drop-duplicates",
        help="Do not cluster duplicates, store them in a separate folder",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-c",
        "--use-existing-clusters",
        help="Try to assign media to existing clusters in watch folders",
        action="store_true",
        default=False,
    )

    return parser


def process_watch_dirs(watch_dirs: list[str] | None) -> list[str]:
    """Process and validate watch directories.

    Args:
        watch_dirs: List of watch directories or None

    Returns:
        Validated list of watch directories (empty list if None)

    Raises:
        TypeError: If watch_dirs is not a list or None
    """
    if watch_dirs is None:
        return []
    elif isinstance(watch_dirs, list):
        return watch_dirs
    else:
        raise TypeError("Watch directories must be provided as a list")


def run_from_cli():
    """Execute the application from command line interface."""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Process and validate arguments
    watch_dirs = process_watch_dirs(args.watch_dirs)

    # Convert path strings to ensure they're valid
    inbox_dir = str(Path(args.inbox_dir)) if args.inbox_dir else None
    output_dir = str(Path(args.output_dir)) if args.output_dir else None

    # Run the main function with parsed arguments
    main(
        inbox_dir=inbox_dir,
        output_dir=output_dir,
        watch_dir_list=watch_dirs,
        development_mode=args.development_mode,
        no_operation=args.no_operation,
        copy_mode=args.copy_mode,
        force_deep_scan=args.force_deep_scan,
        drop_duplicates=args.drop_duplicates,
        use_existing_clusters=args.use_existing_clusters,
    )


if __name__ == "__main__":
    run_from_cli()
