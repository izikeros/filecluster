from filecluster.configuration import (
    get_default_config,
    Config,
    configure_watch_folder_paths,
    get_proper_mode_config,
    get_development_config,
    override_config_with_cli_params,
    CopyMode,
    configure_paths_for_this_os,
)


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
        copy_mode=True,
        inbox_dir="aaa",
        no_operation=True,
        output_dir="bbb",
        watch_dir_list=["/home/user/Pictures"],
    )
    assert nc.in_dir_name == "aaa"
    assert nc.mode == CopyMode.NOP
    assert nc.out_dir_name == "bbb"
    assert nc.watch_folders[0] == "/home/user/Pictures"
    # TODO: KS: 2020-12-15: assert file_cluster_dbs property - it is also modified


def test_configure_paths_for_this_os__runs():
    _, _, _ = configure_paths_for_this_os()
