#!/usr/bin/env python3
"""Main module for image grouping by the event.

This module provides functionality to cluster media files (images and videos)
based on their timestamps, helping organize them into event-based folders.
"""

from collections.abc import Callable
from time import perf_counter
from typing import Any

from filecluster import logger
from filecluster.configuration import (
    CopyMode,
    default_factory,
)
from filecluster.dbase import get_existing_clusters_info
from filecluster.file_operations import FileOperationPlan, execute_plan
from filecluster.image_grouper import ImageGrouper
from filecluster.image_reader import InboxReader
from filecluster.ui import NullReporter, Reporter, fmt_count


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
    restore_original_names: bool | None = None,
    limit: int | None = None,
    flat: bool | None = None,
    reporter: Reporter | None = None,
    confirm: Callable[[Any, Any], bool] | None = None,
    banner: Callable[[Any], None] | None = None,
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
        restore_original_names: Revert copy-suffixed file names (e.g. "-Kopiuj(1)")
            to their originals when moving/copying into cluster folders
        limit: Ingest at most this many inbox files. Only the first *limit*
            files in name order are read, which keeps a trial run on a large
            inbox both quick and repeatable.
        flat: Only process top-level inbox files (do not scan subdirectories).
        reporter: Renders progress and phase results. Defaults to a silent
            reporter so library callers produce no terminal output.
        confirm: Called with ``(plan, config)`` before files are written. When it
            returns False the run stops without touching the filesystem.
        banner: Called with the resolved config before any scanning starts, so
            the caller can show where files will go before a long run begins.

    Returns:
        Dictionary with diagnostic data from the clustering process
    """
    ui = reporter or NullReporter()
    started = perf_counter()

    # Get appropriate configuration based on mode
    config = default_factory.get_config(is_development_mode=development_mode)

    # Override configuration with CLI parameters
    logger.debug("Applying CLI parameter overrides to configuration")
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
        restore_original_names=restore_original_names,
        limit=limit,
        flat=flat,
    )

    if banner is not None:
        banner(config)

    # Read cluster info from libraries (or get empty DataFrame if none found)
    uses_library = bool(config.watch_folders) and (
        config.skip_duplicated_existing_in_libs
        or config.assign_to_clusters_existing_in_libs
    )
    # Without watch folders the call is a no-op, so it reports through a silent
    # reporter rather than claiming a phase that did no work.
    scan_reporter = ui if uses_library else NullReporter()
    with scan_reporter.phase("Scanned library") as phase:
        df_clusters, empty_folders, non_compliant_folders = get_existing_clusters_info(
            config.watch_folders,
            config.skip_duplicated_existing_in_libs,
            config.assign_to_clusters_existing_in_libs,
            config.force_deep_scan,
            progress=phase,
        )
        phase.detail = f"{fmt_count(len(df_clusters))} clusters"
    results: dict[str, Any] = {
        "df_clusters": df_clusters,
        "empty": empty_folders,
        "non_compliant": non_compliant_folders,
        "aborted": False,
        "config": config,
    }

    # Configure image reader and initialize media database
    image_reader = InboxReader(
        in_dir_name=config.in_dir_name,
        limit=config.inbox_limit,
        recursive=config.recursive_inbox,
    )
    with ui.phase("Read inbox") as phase:
        image_reader.get_media_files_info(progress=phase)
        n_read = len(image_reader.media_df)
        phase.detail = f"{fmt_count(n_read)} files"
        if n_read < image_reader.n_available:
            phase.detail += f" of {fmt_count(image_reader.n_available)}"
    results["n_files_read"] = n_read
    results["n_files_available"] = image_reader.n_available

    # Configure media grouper and initialize internal dataframes
    image_grouper = ImageGrouper(
        configuration=config,
        df_clusters=df_clusters,  # existing clusters
        inbox_media_df=image_reader.media_df.copy(),  # inbox media
    )

    # Mark duplicates if enabled
    results.update({"dup_files": [], "dup_clusters": []})
    if config.skip_duplicated_existing_in_libs and config.watch_folders:
        with ui.phase("Duplicate check") as phase:
            dup_files, dup_clusters = image_grouper.mark_inbox_duplicates(
                progress=phase
            )
            phase.detail = f"{fmt_count(len(dup_files))} duplicates"
        results.update({"dup_files": dup_files, "dup_clusters": dup_clusters})

    # Assign to existing clusters if enabled
    results.update({"files_existing_cl": None, "existing_cluster_names": None})
    if config.assign_to_clusters_existing_in_libs and config.watch_folders:
        with ui.phase("Existing clusters") as phase:
            files_assigned, existing_cluster_names = (
                image_grouper.assign_to_existing_clusters()
            )
            phase.detail = f"{fmt_count(len(files_assigned))} files assigned"
        results.update(
            {
                "files_existing_cl": files_assigned,
                "existing_cluster_names": existing_cluster_names,
            }
        )

    with ui.phase("Clustered") as phase:
        # Handle non-clustered items
        logger.debug("Calculating time gaps for creating new clusters")
        image_grouper.calculate_gaps()

        # Create new clusters and assign media
        logger.debug("Running clustering algorithm")
        results["new_cluster_df"] = image_grouper.run_clustering()

        # Assign target folder names for new clusters
        logger.debug("Assigning target folder names to new clusters")
        new_folder_names = (
            image_grouper.assign_target_folder_name_and_file_count_to_new_clusters(
                method=config.assign_date_to_clusters_method
            )
        )
        results["new_folder_names"] = new_folder_names

        # Assign target folder names for existing clusters
        logger.debug("Assigning target folder names to existing clusters")
        image_grouper.assign_target_folder_name_to_existing_clusters()

        # Add cluster info to media records
        logger.debug("Adding cluster information to media records")
        image_grouper.add_cluster_info_from_clusters_to_media()

        # Add target directories for duplicates if enabled
        if config.skip_duplicated_existing_in_libs:
            logger.debug("Assigning target directories for duplicates")
            image_grouper.add_target_dir_for_duplicates()

        phase.detail = f"{fmt_count(len(new_folder_names))} new clusters"

    # Build the plan in every mode: it is both the execution input and the
    # single source of truth for the summary, dry-run preview and report.
    plan = image_grouper.build_file_operation_plan()
    results["file_operation_plan"] = plan
    results["cluster_sizes"] = _cluster_sizes(image_grouper)
    logger.debug(plan.summary())

    if (
        config.mode != CopyMode.NOP
        and confirm is not None
        and not confirm(plan, config)
    ):
        logger.debug("Aborted before any file was written")
        results["aborted"] = True
        results["elapsed"] = perf_counter() - started
        return results

    if config.mode != CopyMode.NOP:
        label = "Copied files" if config.mode == CopyMode.COPY else "Moved files"
        with ui.phase(label) as phase:
            execute_plan(plan, progress=phase)
            phase.detail = _write_detail(plan)
    else:
        logger.debug("Dry run mode - no files were moved or copied")

    results["elapsed"] = perf_counter() - started
    return results


def _write_detail(plan: FileOperationPlan) -> str:
    """Summarize what a write phase did, in one short line."""
    written = plan.n_moves + plan.n_copies
    detail = f"{fmt_count(written)} files"
    if plan.n_renamed:
        detail += f" · {fmt_count(plan.n_renamed)} renamed"
    return detail


def _cluster_sizes(image_grouper: ImageGrouper) -> list[tuple[str, int]]:
    """Return ``(folder, file_count)`` per target folder.

    Derived from the media frame rather than the plan so the counts are
    identical in dry-run, copy and move modes.
    """
    if "target_path" not in image_grouper.inbox_media_df.columns:
        return []
    counts = image_grouper.inbox_media_df["target_path"].value_counts()
    return [(str(name), int(count)) for name, count in counts.items()]


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
