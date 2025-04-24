import pytest

from filecluster.configuration import get_development_config
from filecluster.dbase import get_existing_clusters_info


def check_lists_equal(list_1, list_2) -> bool:
    """Check if two lists are equal."""
    return len(list_1) == len(list_2) and sorted(list_1) == sorted(list_2)


def test_get_existing_clusters_info__gives_the_same_dataframe_cols(assets_dir):
    config_1 = get_development_config()
    config_1.watch_folders = []
    df_blank, _, _ = get_existing_clusters_info(
        watch_folders=config_1.watch_folders,
        skip_duplicated_existing_in_libs=config_1.skip_duplicated_existing_in_libs,
        assign_to_clusters_existing_in_libs=config_1.assign_to_clusters_existing_in_libs,
        force_deep_scan=config_1.force_deep_scan,
    )

    config_2 = get_development_config()
    config_2.watch_folders = [assets_dir / "zdjecia", assets_dir / "clusters"]
    config_2.force_deep_scan = True
    config_2.assign_to_clusters_existing_in_libs = True
    df_clusters, _, _ = get_existing_clusters_info(
        watch_folders=config_2.watch_folders,
        skip_duplicated_existing_in_libs=config_2.skip_duplicated_existing_in_libs,
        assign_to_clusters_existing_in_libs=config_2.assign_to_clusters_existing_in_libs,
        force_deep_scan=config_2.force_deep_scan,
    )

    cols_blank = df_blank.columns
    cols = df_clusters.columns
    assert check_lists_equal(cols, cols_blank), f"clusters:{cols}, blank:{cols_blank}"


def test_get_existing_clusters_info__ids_are_uniq(assets_dir):
    config = get_development_config()
    config.watch_folders = [assets_dir / "zdjecia", assets_dir / "clusters"]
    config.force_deep_scan = True
    config.assign_to_clusters_existing_in_libs = True
    df_clusters, _, _ = get_existing_clusters_info(
        watch_folders=config.watch_folders,
        skip_duplicated_existing_in_libs=config.skip_duplicated_existing_in_libs,
        assign_to_clusters_existing_in_libs=config.assign_to_clusters_existing_in_libs,
        force_deep_scan=config.force_deep_scan,
    )

    ids = df_clusters.cluster_id.values

    ids_are_unique = len(ids) == len(set(ids))
    assert ids_are_unique


@pytest.mark.skip(reason="not implemented")
def test_get_new_cluster_id_from_dataframe():
    raise AssertionError
