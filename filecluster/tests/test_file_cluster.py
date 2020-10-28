import pytest

from filecluster.file_cluster import main

@pytest.mark.skip(reason='not implemented')
def test_main_sqlite():
    main(inbox_dir='inbox_test_a_orig',
         output_dir='/tmp/output_dir',
         db_dir='/tmp/',
         db_driver='sqlite',
         development_mode=True,
         no_operation=False)


def test_main_dataframe():
    main(inbox_dir='inbox_test_a_orig',
         output_dir='/tmp/output_dir',
         db_dir='/tmp/',
         db_driver='dataframe',
         development_mode=True,
         no_operation=False)


@pytest.mark.skip(reason='not implemented')
def test_override_config_with_cli_params():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_save_media_and_cluster_info_to_database():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_read_timestamps_form_media_files():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_run_clustering():
    assert False
