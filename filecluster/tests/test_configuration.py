from filecluster.configuration import (
    get_default_config,
    Config,
    configure_db_path,
    configure_watch_folder_paths,
    setup_directory_for_database,
    get_proper_mode_config,
    get_development_config,
    override_config_with_cli_params,
    Driver,
    CopyMode,
    configure_paths_for_this_os,
)


def test_get_default_config():
    config = get_default_config()
    assert isinstance(config, Config)


def test_configure_db_path__runs():
    configure_db_path()


def test_configure_watch_folder_paths__runs():
    configure_watch_folder_paths()


def test_setup_directory_for_database__dir_provided():
    config = get_default_config()
    dir_name = "my_dir"
    config_after = setup_directory_for_database(config, db_dir=dir_name)
    assert dir_name in config_after.db_file_media
    assert dir_name in config_after.db_file_clusters


def test_setup_directory_for_database__dir_not_provided():
    config = get_default_config()
    db_file_media = config.db_file_media
    db_file_clusters = config.db_file_clusters
    dir_name = None
    config_after = setup_directory_for_database(config, db_dir=dir_name)
    assert config.out_dir_name in config_after.db_file_media
    assert config.out_dir_name in config_after.db_file_clusters


def test_get_proper_mode_config__runs():
    get_proper_mode_config(is_development_mode=True)
    get_proper_mode_config(is_development_mode=False)


def test_get_development_config__runs():
    get_development_config()


def test_override_config_with_cli_params():
    config = get_default_config()
    nc = override_config_with_cli_params(
        config=config,
        inbox_dir="aaa",
        no_operation=True,
        output_dir="bbb",
        db_driver=Driver["dataframe".upper()],
        watch_dir_list=["/home/user/Pictures"],
    )
    assert nc.in_dir_name == "aaa"
    assert nc.mode == CopyMode.NOP
    assert nc.out_dir_name == "bbb"
    assert nc.db_driver == Driver.DATAFRAME
    assert nc.watch_folders[0] == "/home/user/Pictures"


def test_configure_paths_for_this_os__runs():
    _, _, _ = configure_paths_for_this_os()
