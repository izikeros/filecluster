import pytest

from filecluster.clustering import override_config_with_cli_params
from filecluster.configuration import get_default_config, CopyMode, Driver
from filecluster.file_cluster import main


@pytest.mark.skip(reason='not implemented')
def test_main_sqlite():
    main(inbox_dir='inbox_test_a_orig',
         output_dir='/tmp/output_dir',
         db_dir='/tmp/',
         db_driver=Driver['sqlite'.upper()],
         development_mode=True,
         no_operation=False,
         watch_dirs=[''])


def test_main_dataframe():
    main(inbox_dir='inbox_test_a_orig',
         output_dir='/tmp/output_dir',
         db_dir='/tmp/',
         db_driver=Driver['dataframe'.upper()],
         development_mode=True,
         no_operation=False,
         watch_dirs=[''])


def test_override_config_with_cli_params():
    config = get_default_config()
    nc = override_config_with_cli_params(
        config=config,
        inbox_dir='aaa',
        no_operation=True,
        output_dir='bbb',
        db_driver=Driver['sqlite'.upper()],
        watch_dirs=['/home/user/Pictures']
    )
    assert nc.in_dir_name == 'aaa'
    assert nc.mode == CopyMode.NOP
    assert nc.out_dir_name == 'bbb'
    assert nc.db_driver == Driver.SQLITE
    assert nc.watch_folders[0] == '/home/user/Pictures'


@pytest.mark.skip(reason='not implemented')
def test_save_media_and_cluster_info_to_database():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_read_timestamps_form_media_files():
    assert False


@pytest.mark.skip(reason='not implemented')
def test_run_clustering():
    assert False
