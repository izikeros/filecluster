import pytest

from filecluster.configuration import Driver
from filecluster.file_cluster import main


def test_main_dataframe():
    main(inbox_dir='inbox_test_a_orig', output_dir='/tmp/output_dir', watch_dir_list=[''],
         db_dir_str='/tmp/', db_driver=Driver['dataframe'.upper()], development_mode=True,
         no_operation=False)


@pytest.mark.skip(reason='not implemented')
def test_save_media_and_cluster_info_to_database():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_read_timestamps_form_media_files():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_run_clustering():
    assert False
