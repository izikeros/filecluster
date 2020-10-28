import logging
from pathlib import Path

import pandas as pd

from filecluster.configuration import CopyMode
from filecluster.image_reader import cleanup_data_frame_timestamps

log_fmt = '%(levelname).1s %(message)s'
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def override_config_with_cli_params(config, inbox_dir, no_operation, output_dir, db_driver, watch_dirs):
    """Use CLI arguments to override default configuration.
    :param config:
    :param inbox_dir:
    :param no_operation:
    :param output_dir:
    :return:
    """
    # TODO: KS: 2020-05-25: Consider using kwargs instead long list of input arguments
    if inbox_dir:
        config.in_dir_name = inbox_dir
    if output_dir:
        config.out_dir_name = output_dir
    if no_operation:
        config.mode = CopyMode.NOP
    if db_driver:
        config.db_driver = db_driver
    if watch_dirs:
        config.watch_folders = watch_dirs
    return config


def set_db_paths_in_config(config, db_dir):
    config.db_file = Path(db_dir) / 'filecluster_db'
    config.db_file_clusters = Path(db_dir) / 'clusters.p'  # TODO: KS: 2020-05-23: do not use picke
    config.db_file_media = Path(db_dir) / 'media.p'  # TODO: KS: 2020-05-23: do not use picke
    return config


def get_media_info_from_imported_files(image_reader):
    row_list = image_reader.get_data_from_files()
    logger.debug(f"Read info from {len(row_list)} files.")
    # convert to data frame
    new_media_df = pd.DataFrame(row_list)
    new_media_df = cleanup_data_frame_timestamps(new_media_df)
    return new_media_df

