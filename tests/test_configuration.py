from filecluster.configuration import Config
from filecluster.configuration import CopyMode
from filecluster.configuration import configure_paths_for_this_os
from filecluster.configuration import configure_watch_folder_paths
from filecluster.configuration import get_default_config
from filecluster.configuration import get_development_config
from filecluster.configuration import get_proper_mode_config
from filecluster.configuration import override_config_with_cli_params


def test_get_default_config():
    config = get_default_config()
    assert isinstance(config, Config)


def test_configure_watch_folder_paths__runs():
    configure_watch_folder_paths()


def test_get_proper_mode_config__runs():
    get_proper_mode_config(is_development_mode=True)
    get_proper_mode_config(is_development_mode=False)


def test_get_development_config__runs():
    get_development_config()


def test_override_config_with_cli_params():
    config = get_default_config()
    nc = override_config_with_cli_params(
        config=config,
        copy_mode=None,
        inbox_dir="aaa",
        no_operation=True,
        output_dir="bbb",
        watch_dir_list=["/home/user/Pictures"],
    )
    assert nc.in_dir_name == "aaa"
    assert nc.mode == CopyMode.NOP
    assert nc.out_dir_name == "bbb"
    assert nc.watch_folders[0] == "/home/user/Pictures"


def test_configure_paths_for_this_os__runs():
    _, _, _ = configure_paths_for_this_os()
