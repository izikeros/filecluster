"""Tests for the update_clusters module.

Covers folder type identification, directory scanning, cluster ini file
read/write/round-trip, str_to_bool conversion, library cluster dataframe
creation, and library structure validation.

All file system tests use tmp_path or real assets — no mocking of internal
business logic.
"""

from configparser import ConfigParser
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from filecluster.update_clusters import (
    dict_from_ini_range_section,
    fast_scandir,
    get_or_create_library_cluster_ini_as_dataframe,
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
    validate_library_structure,
)


# ---------------------------------------------------------------------------
# str_to_bool
# ---------------------------------------------------------------------------
class TestStrToBool:
    """Tests for string-to-boolean conversion.

    Business rule: cluster ini files store booleans as 'True'/'False' strings.
    """

    def test_true_string(self):
        assert str_to_bool("True") is True

    def test_false_string(self):
        assert str_to_bool("False") is False

    @pytest.mark.parametrize("value", ["true", "false", "yes", "no", "1", "0", ""])
    def test_invalid_strings_raise_value_error(self, value):
        """
        Test Description: Only exact 'True'/'False' are accepted.

        Purpose: Loose parsing could silently corrupt cluster continuity flags.
        """
        with pytest.raises(ValueError):
            str_to_bool(value)


# ---------------------------------------------------------------------------
# initialize_cluster_info_dict
# ---------------------------------------------------------------------------
class TestInitializeClusterInfoDict:
    """Tests for creating a ConfigParser object from cluster stats."""

    def test_produces_configparser_with_range_section(self):
        """
        Test Description: Output is a ConfigParser with 'Range' section
        containing start_date, end_date, is_continuous, median, file_count.

        Purpose: save_cluster_ini relies on this structure.
        """
        result = initialize_cluster_info_dict(
            start="2020-01-01 12:00:00",
            stop="2020-01-02 12:00:00",
            is_continuous=True,
            median="2020-01-01 18:00:00",
            file_count=10,
        )
        assert isinstance(result, ConfigParser)
        assert "Range" in result
        assert result["Range"]["start_date"] == "2020-01-01 12:00:00"
        assert result["Range"]["end_date"] == "2020-01-02 12:00:00"
        assert result["Range"]["is_continuous"] == "True"
        assert result["Range"]["file_count"] == "10"


# ---------------------------------------------------------------------------
# save / read cluster ini (round-trip)
# ---------------------------------------------------------------------------
class TestClusterIniRoundTrip:
    """Tests for saving and reading cluster ini files.

    No mocking of FileClusterSettings — uses real settings with real tmp_path.
    """

    def _create_test_ini(self, tmp_path):
        """Helper: save a test ini to tmp_path."""
        ini = initialize_cluster_info_dict(
            start="2020-01-01 12:00:00",
            stop="2020-01-02 12:00:00",
            is_continuous=True,
            median="2020-01-01 18:00:00",
            file_count=10,
        )
        save_cluster_ini(ini, tmp_path)
        return tmp_path / ".cluster.ini"

    def test_save_creates_ini_file(self, tmp_path):
        """save_cluster_ini writes a .cluster.ini file to disk."""
        ini_path = self._create_test_ini(tmp_path)
        assert ini_path.exists()

    def test_read_returns_dict_with_range_section(self, tmp_path):
        """
        Test Description: After saving, reading the ini returns a dict with
        'Range' section and correctly parsed dates.

        Purpose: Round-trip integrity is essential — the ini is the persistent
        cache of cluster info.
        """
        self._create_test_ini(tmp_path)
        result = read_cluster_ini_as_dict(tmp_path)
        assert result is not None
        assert "Range" in result
        assert isinstance(result["Range"]["start_date"], datetime)
        assert result["Range"]["start_date"].year == 2020

    def test_read_nonexistent_returns_none(self, tmp_path):
        """Reading from a directory without .cluster.ini returns None."""
        result = read_cluster_ini_as_dict(tmp_path)
        assert result is None

    def test_full_roundtrip_preserves_data(self, tmp_path):
        """
        Test Description: save then read then convert yields the original values.

        Purpose: Any data loss or type corruption in the round-trip would cause
        incorrect cluster boundaries.
        """
        self._create_test_ini(tmp_path)
        raw = read_cluster_ini_as_dict(tmp_path)
        result = dict_from_ini_range_section(raw, tmp_path)
        assert result["is_continuous"] is True
        assert isinstance(result["median"], datetime)
        assert result["file_count"] == 10
        assert result["path"] == tmp_path


# ---------------------------------------------------------------------------
# dict_from_ini_range_section
# ---------------------------------------------------------------------------
class TestDictFromIniRangeSection:
    """Tests for type conversion of ini range data."""

    def test_converts_types_correctly(self):
        """
        Test Description: is_continuous becomes bool, median becomes datetime,
        file_count becomes int, and path is attached.

        Purpose: Consumers expect Python types, not ini-file strings.
        """
        ini_data = {
            "Range": {
                "start_date": "2020-01-01 12:00:00",
                "end_date": "2020-01-02 12:00:00",
                "is_continuous": "True",
                "median": "2020-01-01 18:00:00",
                "file_count": "10",
            }
        }
        result = dict_from_ini_range_section(ini_data, Path("/test"))
        assert result["is_continuous"] is True
        assert isinstance(result["median"], datetime)
        assert result["file_count"] == 10
        assert result["path"] == Path("/test")

    def test_handles_microsecond_median(self):
        """Median with microseconds is still parsed correctly."""
        ini_data = {
            "Range": {
                "start_date": "2020-01-01 12:00:00",
                "end_date": "2020-01-02 12:00:00",
                "is_continuous": "False",
                "median": "2020-01-01 18:00:00.123456",
                "file_count": "5",
            }
        }
        result = dict_from_ini_range_section(ini_data, Path("/test"))
        assert result["is_continuous"] is False
        assert result["median"].microsecond == 123456


# ---------------------------------------------------------------------------
# fast_scandir
# ---------------------------------------------------------------------------
class TestFastScandir:
    """Tests for recursive directory scanning."""

    def test_empty_directory(self, tmp_path):
        """Empty directory returns empty list."""
        assert fast_scandir(str(tmp_path)) == []

    def test_with_subdirectories(self, tmp_path):
        """Recursively finds all subdirectories."""
        (tmp_path / "a").mkdir()
        (tmp_path / "a" / "b").mkdir()
        (tmp_path / "c").mkdir()
        # Also create a file to verify it's ignored
        (tmp_path / "file.txt").touch()

        result = fast_scandir(str(tmp_path))
        paths = {Path(p) for p in result}
        assert tmp_path / "a" in paths
        assert tmp_path / "a" / "b" in paths
        assert tmp_path / "c" in paths
        assert len(paths) == 3

    def test_empty_string_returns_empty(self):
        """Empty dirname returns empty list."""
        assert fast_scandir("") == []


# ---------------------------------------------------------------------------
# Folder type identification
# ---------------------------------------------------------------------------
class TestFolderTypeIdentification:
    """Tests for is_year_folder, is_event_folder, is_sel_folder, etc.

    Business rules:
    - Year folders match pattern (19|20)XX as the last path component
    - Event folders are directly under a year folder
    - Sel folders have basename 'sel'
    """

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("2020", True),
            ("1999", True),
            ("2099", True),
            ("/home/2019", True),
            ("3000", False),
            ("20202", False),
            ("abcd", False),
            ("202", False),
            ("photos/2019/[2019_01_02]_event", False),
            ("/home/2019/[2019_01_02]_event", False),
        ],
    )
    def test_is_year_folder(self, path, expected):
        assert is_year_folder(path) is expected

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("2020/event_name", True),
            ("not_year/event_name", False),
            ("2020", False),
            ("2020/sub/deeper", False),
        ],
    )
    def test_is_event_folder(self, path, expected):
        assert is_event_folder(path) is expected

    def test_is_sel_folder(self):
        assert is_sel_folder("path/to/sel") is True
        assert is_sel_folder("selection") is False
        assert is_sel_folder("sel/subfolder") is False

    def test_is_event_subcategory_folder_always_false(self):
        """Currently unimplemented — always returns False."""
        assert is_event_subcategory_folder("any/path") is False

    def test_identify_folder_types_labels_correctly(self):
        """
        Test Description: Real folder names are labelled as year/event/sel/unknown
        without mocking the identification functions.

        Purpose: This is the routing logic that determines which folders get
        scanned for cluster info.
        """
        folders = [
            "2020",
            "2020/[2020_01_01]_event",
            "2020/[2020_01_01]_event/sel",
            "random_folder",
        ]
        result = identify_folder_types(folders)
        types = dict(result)
        assert types["2020"] == "year"
        assert types["2020/[2020_01_01]_event"] == "event"
        assert types["2020/[2020_01_01]_event/sel"] == "sel"
        assert types["random_folder"] == "unknown"

    def test_is_event_filters_tuples(self):
        """is_event returns True only for event-type tuples."""
        assert is_event(("2020/event", "event")) is True
        assert is_event(("2020", "year")) is False
        assert is_event(("path/sel", "sel")) is False


# ---------------------------------------------------------------------------
# validate_library_structure
# ---------------------------------------------------------------------------
class TestValidateLibraryStructure:
    """Tests for library structure validation."""

    def test_valid_structure(self, tmp_path):
        """A well-formed library with year/event/sel passes validation."""
        year = tmp_path / "2020"
        year.mkdir()
        event = year / "[2020_01_01]_event"
        event.mkdir()
        sel = event / "sel"
        sel.mkdir()
        assert validate_library_structure(tmp_path) is True

    def test_invalid_structure(self, tmp_path):
        """A library with unknown folder types fails validation."""
        (tmp_path / "random").mkdir()
        assert validate_library_structure(tmp_path) is False

    def test_empty_library_passes(self, tmp_path):
        """An empty directory has no unknown folders => valid."""
        assert validate_library_structure(tmp_path) is True


# ---------------------------------------------------------------------------
# Integration: get_or_create_library_cluster_ini_as_dataframe
# ---------------------------------------------------------------------------
class TestGetOrCreateLibraryClusterIni:
    """Integration test using real test assets — no mocking."""

    def test_scans_real_library(self, assets_dir):
        """
        Test Description: Scanning the test library (assets/zdjecia) produces
        a DataFrame with cluster info rows.

        Purpose: End-to-end verification that library scanning, ini reading,
        and DataFrame construction work together.

        Mocking Strategy: None — uses real files and multiprocessing.
        """
        import multiprocessing

        library = assets_dir / "zdjecia"
        with multiprocessing.Pool(processes=1) as pool:
            df, empty_dirs = get_or_create_library_cluster_ini_as_dataframe(
                library_path=str(library),
                pool=pool,
                force_deep_scan=True,
            )
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "target_path" in df.columns
        assert "new_file_count" in df.columns
