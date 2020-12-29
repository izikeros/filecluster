import pytest

from filecluster.update_clusters import is_year_folder


@pytest.mark.parametrize(
    "folder_path,expected",
    [  # ("c:\\2019", True),
        # ("photos\\2019", True),
        ("photos\\2019\\[2019_01_02]_some_event", False),
        ("/home/2019", True),
        ("/home/2019/[2019_01_02]_some_event", False),
    ],
)
def test_is_year_folder(folder_path, expected):
    result = is_year_folder(folder_path)
    assert result is expected
