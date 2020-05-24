import logging
from pathlib import Path

import pandas as pd

from filecluster.image_groupper import ImageGroupper

logger = logging.getLogger(__name__)


def override_config_with_cli_params(config, inbox_dir, no_operation, output_dir, db_driver):
    """Use CLI arguments to override default configuration.
    :param config:
    :param inbox_dir:
    :param no_operation:
    :param output_dir:
    :return:
    """
    if inbox_dir:
        config.in_dir_name = inbox_dir
    if output_dir:
        config.out_dir_name = output_dir
    if no_operation:
        config.mode = 'nop'
    if db_driver:
        config.db_driver = db_driver
    return config


def set_db_paths_in_config(config, db_dir):
    config.db_file = Path(db_dir) / 'filecluster_db'
    config.db_file_clusters = Path(db_dir) / 'clusters.p'  # TODO: KS: 2020-05-23: do not use picke
    config.db_file_media = Path(db_dir) / 'media.p'  # TODO: KS: 2020-05-23: do not use picke
    return config


def read_timestamps_form_media_files(config, image_reader):
    row_list = image_reader.get_data_from_files()

    # convert to data frame
    new_media_df = pd.DataFrame(row_list)
    image_reader.cleanup_data_frame_timestamps()

    duplicates = image_reader.check_import_for_duplicates_in_existing_clusters(new_media_df)

    # --- initialize media grouper
    image_groupper = ImageGroupper(configuration=config,
                                   image_df=image_reader.image_df)
    return image_groupper


def run_clustering_no_prior(config, image_groupper):
    """Perform clustering."""
    logger.info("Calculating gaps")
    image_groupper.calculate_gaps(date_col='date', delta_col='date_delta')
    # actual clustering takes place here:
    cluster_list = image_groupper.add_tmp_cluster_id_to_files_in_data_frame()
    image_groupper.save_cluster_data_to_data_frame(cluster_list)
    image_groupper.assign_representative_date_to_clusters(
        method=config.assign_date_to_clusters_method)
    return image_groupper


def run_clustering_with_prior(config, image_groupper):
    """Add images to exising clusters or create new clusters if needed."""
    logger.debug(f'Running clustering with existing clusters.')
    return image_groupper
