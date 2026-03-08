from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from filecluster.configuration import FileClusterSettings
from filecluster.update_clusters import (
    dict_from_ini_range_section,
    fast_scandir,
    get_or_create_library_cluster_ini_as_dataframe,
    get_this_ini,
    identify_folder_types,
    initialize_cluster_info_dict,
    is_event,
    is_event_folder,
    is_event_subcategory_folder,
    is_sel_folder,
    is_year_folder,
    read_cluster_ini_as_dict,
    save_cluster_ini,
    str_to_bool,
)


class TestStrToBool:
    """Tests for the str_to_bool function."""

    def test_str_true_returns_true(self):
        """Test that 'True' string is converted to True boolean."""
        result = str_to_bool("True")
        assert result is True

    def test_str_false_returns_false(self):
        """Test that 'False' string is converted to False boolean."""
        result = str_to_bool("False")
        assert result is False

    def test_invalid_string_raises_value_error(self):
        """Test that invalid strings raise ValueError."""
        with pytest.raises(ValueError):
            str_to_bool("not_a_bool")


class TestInitializeClusterInfoDict:
    """Tests for the initialize_cluster_info_dict function."""

    def test_initialize_cluster_info_dict_basic(self):
        """Test basic cluster info initialization."""
        start = "2020-01-01 12:00:00"
        stop = "2020-01-02 12:00:00"
        median = "2020-01-01 18:00:00"
        is_continuous = True
        file_count = 10

        result = initialize_cluster_info_dict(
            start=start,
            stop=stop,
            is_continuous=is_continuous,
            median=median,
            file_count=file_count,
        )

        assert isinstance(result, ConfigParser)
        assert "Range" in result
        assert result["Range"]["start_date"] == start
        assert result["Range"]["end_date"] == stop
        assert result["Range"]["is_continuous"] == str(is_continuous)
        assert result["Range"]["median"] == median
        assert result["Range"]["file_count"] == str(file_count)


class TestSaveClusterIni:
    """Tests for the save_cluster_ini function."""

    def test_save_cluster_ini(self, tmp_path):
        """Test saving cluster ini file."""
        cluster_ini = ConfigParser()
        cluster_ini["Range"] = {
            "start_date": "2020-01-01 12:00:00",
            "end_date": "2020-01-02 12:00:00",
            "is_continuous": "True",
            "median": "2020-01-01 18:00:00",
            "file_count": "10",
        }

        with patch.object(
            FileClusterSettings,
            "__new__",
            return_value=MagicMock(ini_filename=".cluster.ini"),
        ):
            save_cluster_ini(cluster_ini, tmp_path)

            ini_path = tmp_path / ".cluster.ini"
            assert ini_path.exists()

            saved_config = ConfigParser()
            saved_config.read(ini_path)

            assert "Range" in saved_config
            assert saved_config["Range"]["start_date"] == "2020-01-01 12:00:00"
            assert saved_config["Range"]["end_date"] == "2020-01-02 12:00:00"
            assert saved_config["Range"]["is_continuous"] == "True"


class TestReadClusterIniAsDict:
    """Tests for the read_cluster_ini_as_dict function."""

    def test_read_existing_cluster_ini(self, tmp_path):
        """Test reading an existing cluster ini file."""
        # Create a test ini file
        ini_path = tmp_path / ".cluster.ini"
        with open(ini_path, "w") as f:
            f.write("""[Range]
start_date = 2020-01-01 12:00:00
end_date = 2020-01-02 12:00:00
is_continuous = True
median = 2020-01-01 18:00:00
file_count = 10
""")

        with patch("pathlib.Path.__truediv__", return_value=ini_path):
            result = read_cluster_ini_as_dict(tmp_path)

            assert "Range" in result
            assert isinstance(result["Range"]["start_date"], datetime)
            assert result["Range"]["start_date"].year == 2020
            assert result["Range"]["start_date"].month == 1
            assert result["Range"]["start_date"].day == 1
            assert result["Range"]["is_continuous"] == "True"
            assert result["Range"]["file_count"] == "10"

    def test_read_nonexistent_cluster_ini(self, tmp_path):
        """Test reading a nonexistent cluster ini file returns None."""
        with patch("pathlib.Path.__truediv__", return_value=tmp_path / ".cluster.ini"):
            result = read_cluster_ini_as_dict(tmp_path)
            assert result is None


class TestDictFromIniRangeSection:
    """Tests for the dict_from_ini_range_section function."""

    def test_dict_from_ini_range_section(self):
        """Test converting ini range section to dictionary with proper types."""
        cluster_ini_r = {
            "Range": {
                "start_date": "2020-01-01 12:00:00",
                "end_date": "2020-01-02 12:00:00",
                "is_continuous": "True",
                "median": "2020-01-01 18:00:00",
                "file_count": "10",
            }
        }
        path = Path("/test/path")

        result = dict_from_ini_range_section(cluster_ini_r, path)

        assert result["is_continuous"] is True  # Converted to bool
        assert isinstance(result["median"], datetime)  # Converted to datetime
        assert result["file_count"] == 10  # Converted to int
        assert result["path"] == path  # Path is added


class TestFastScandir:
    """Tests for the fast_scandir function."""

    def test_fast_scandir_empty_dir(self, tmp_path):
        """Test scanning an empty directory."""
        result = fast_scandir(str(tmp_path))
        assert result == []

    def test_fast_scandir_with_subdirs(self, tmp_path):
        """Test scanning a directory with subdirectories."""
        # Create subdirectories
        subdir1 = tmp_path / "subdir1"
        subdir2 = tmp_path / "subdir2"
        subdir3 = subdir1 / "subdir3"

        subdir1.mkdir()
        subdir2.mkdir()
        subdir3.mkdir()

        # Create a file (should be ignored)
        (tmp_path / "file.txt").touch()

        result = fast_scandir(str(tmp_path))

        # Normalize paths for comparison
        normalized_result = [Path(p) for p in result]

        assert len(normalized_result) == 3
        assert subdir1 in normalized_result
        assert subdir2 in normalized_result
        assert subdir3 in normalized_result


class TestFolderTypeIdentification:
    """Tests for the folder type identification functions."""

    def test_is_year_folder(self):
        """Test identification of year folders."""
        assert is_year_folder("2020") is True
        assert is_year_folder("1999") is True
        assert is_year_folder("2099") is True
        assert is_year_folder("3000") is False
        assert is_year_folder("20202") is False
        assert is_year_folder("abcd") is False

    def test_is_event_folder(self):
        """Test identification of event folders."""
        # with patch(
        #     "filecluster.update_clusters.Path.parts",
        #     property(lambda self: ["root", "2020", "event_name"]),
        # ):
        assert is_event_folder("2020/event_name") is True

        # with patch(
        #     "filecluster.update_clusters.Path.parts",
        #     property(lambda self: ["root", "not_year", "event_name"]),
        # ):
        assert is_event_folder("not_year/event_name") is False

    def test_is_sel_folder(self):
        """Test identification of selection folders."""
        assert is_sel_folder("path/to/sel") is True
        assert is_sel_folder("path/to/selection") is False
        assert is_sel_folder("sel/subfolder") is False

    def test_is_event_subcategory_folder(self):
        """Test identification of event subcategory folders."""
        # This is currently not implemented and always returns False
        assert is_event_subcategory_folder("any/path") is False

    def test_identify_folder_types(self):
        """Test labeling of various folder types."""
        with (
            patch(
                "filecluster.update_clusters.is_year_folder",
                side_effect=lambda x: x == "2020",
            ),
            patch(
                "filecluster.update_clusters.is_sel_folder",
                side_effect=lambda x: x == "2020/event/sel",
            ),
            patch(
                "filecluster.update_clusters.is_event_folder",
                side_effect=lambda x: x == "2020/event",
            ),
            patch(
                "filecluster.update_clusters.is_event_subcategory_folder",
                return_value=False,
            ),
        ):
            folders = ["2020", "2020/event", "2020/event/sel", "other"]
            result = identify_folder_types(folders)

            expected = [
                ("2020", "year"),
                ("2020/event", "event"),
                ("2020/event/sel", "sel"),
                ("other", "unknown"),
            ]
            assert result == expected

    def test_is_event(self):
        """Test identification of event items in labeled tuples."""
        assert is_event(("2020/event", "event")) is True
        assert is_event(("2020", "year")) is False
        assert is_event(("path/to/sel", "sel")) is False


class TestGetThisIni:
    """Tests for the get_this_ini function."""

    @patch("filecluster.update_clusters.os.path.isfile")
    @patch("filecluster.update_clusters.os.listdir")
    @patch("filecluster.update_clusters.configure_inbox_reader")
    @patch("filecluster.update_clusters.get_media_df")
    @patch("filecluster.update_clusters.get_media_stats")
    @patch("filecluster.update_clusters.initialize_cluster_info_dict")
    @patch("filecluster.update_clusters.save_cluster_ini")
    @patch("filecluster.update_clusters.read_cluster_ini_as_dict")
    @patch("filecluster.update_clusters.dict_from_ini_range_section")
    def test_get_this_ini_with_existing_files(
        self,
        mock_dict_from_ini,
        mock_read_ini,
        mock_save_ini,
        mock_init_dict,
        mock_get_stats,
        mock_get_media_df,
        mock_configure_reader,
        mock_listdir,
        mock_isfile,
    ):
        """Test get_this_ini with existing files and no force scan."""
        # Setup
        event_dir = "2020/event"
        force_deep_scan = False
        library_path = "/test/lib"

        mock_isfile.return_value = True
        mock_listdir.return_value = ["file1.jpg", "file2.jpg"]
        mock_reader = MagicMock()
        mock_reader.in_dir_name = "/test/lib/2020/event"
        mock_reader.time_granularity.total_seconds.return_value = 3600
        mock_configure_reader.return_value = mock_reader
        mock_get_media_df.return_value = pd.DataFrame(
            {"path": ["file1.jpg", "file2.jpg"]}
        )
        mock_get_stats.return_value = {
            "date_min": "2020-01-01 12:00:00",
            "date_max": "2020-01-02 12:00:00",
            "is_time_consistent": True,
            "date_median": "2020-01-01 18:00:00",
            "file_count": 2,
        }
        mock_read_ini.return_value = {"Range": {"key": "value"}}
        mock_dict_from_ini.return_value = {
            "path": "/test/lib/2020/event",
            "file_count": 2,
        }

        # Act
        result = get_this_ini(event_dir, force_deep_scan, library_path)

        # Assert
        assert isinstance(result, dict)
        assert result == {"path": "/test/lib/2020/event", "file_count": 2}
        mock_read_ini.assert_called_once()
        mock_dict_from_ini.assert_called_once()

    @patch("filecluster.update_clusters.os.path.isfile")
    @patch("filecluster.update_clusters.os.listdir")
    def test_get_this_ini_empty_directory(self, mock_listdir, mock_isfile):
        """Test get_this_ini with an empty directory."""
        mock_isfile.return_value = False
        mock_listdir.return_value = []

        event_dir = "2020/event"
        force_deep_scan = True
        library_path = "/test/lib"

        with patch(
            "filecluster.update_clusters.configure_inbox_reader"
        ) as mock_configure:
            mock_reader = MagicMock()
            mock_reader.in_dir_name = "/test/lib/2020/event"
            mock_configure.return_value = mock_reader

            with (
                patch("filecluster.update_clusters.logger.debug") as mock_logger,
                patch(
                    "filecluster.update_clusters.read_cluster_ini_as_dict",
                    return_value=None,
                ),
            ):
                result = get_this_ini(event_dir, force_deep_scan, library_path)

                assert isinstance(result, Path)
                mock_logger.assert_called_with(
                    f" - directory {mock_reader.in_dir_name} is empty."
                )


class TestGetOrCreateLibraryClusterIniAsDataframe:
    """Tests for the get_or_create_library_cluster_ini_as_dataframe function."""

    @patch("filecluster.update_clusters.fast_scandir")
    @patch("filecluster.update_clusters.identify_folder_types")
    @patch("multiprocessing.pool.Pool.starmap")
    def test_get_or_create_library_cluster(
        self, mock_starmap, mock_identify, mock_scandir
    ):
        """Test getting library cluster information as a dataframe."""
        # Setup
        library_path = "/test/lib"
        pool = MagicMock()

        mock_scandir.return_value = [
            "/test/lib/2020",
            "/test/lib/2020/event1",
            "/test/lib/2020/event2",
        ]
        mock_identify.return_value = [
            ("2020", "year"),
            ("2020/event1", "event"),
            ("2020/event2", "event"),
        ]

        # Return two dictionaries and one empty directory
        mock_starmap.return_value = [
            {
                "path": "/test/lib/2020/event1",
                "file_count": 10,
                "median": datetime(2020, 1, 1),
            },
            {
                "path": "/test/lib/2020/event2",
                "file_count": 5,
                "median": datetime(2020, 1, 2),
            },
            Path("/test/lib/2020/empty_event"),
        ]
        pool.starmap.return_value = mock_starmap.return_value

        # Act
        df, empty_dirs = get_or_create_library_cluster_ini_as_dataframe(
            library_path, pool, force_deep_scan=False
        )

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "target_path" in df.columns
        assert "new_file_count" in df.columns
        assert len(empty_dirs) == 1
        assert empty_dirs[0] == Path("/test/lib/2020/empty_event")
