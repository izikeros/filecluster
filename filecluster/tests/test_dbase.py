import os
from pathlib import Path

from filecluster.configuration import get_default_config
from filecluster.dbase import db_create_media_df


def test_db_create_media_df__no_exists():
    config = get_default_config()
    # delete media db file if exists
    if os.path.isfile(config.db_file_media):
        os.remove(config.db_file_media)
    res = db_create_media_df(config)
    assert res is True


def test_db_create_media_df__exists():
    config = get_default_config()
    # create media db file if not exists
    if not os.path.isfile(config.db_file_media):
        Path(config.db_file_media).touch()
    res = db_create_media_df(config)
    assert res is False
