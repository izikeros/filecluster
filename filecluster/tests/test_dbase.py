import pytest

from filecluster.configuration import get_default_config
from filecluster.dbase import db_create_media_df

@pytest.mark.skip(reason='not implemented')
def test_db_create_media_df():
    config = get_default_config()
    db_create_media_df(config)
    assert False
