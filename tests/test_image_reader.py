"""Tests for the image_reader module.

Covers Metadata model, timestamp disambiguation (multiple_timestamps_to_one),
row initialization, InboxReader file scanning, media stats calculation, and
the configure_inbox_reader helper.
"""

import os

import pandas as pd
from numpy import dtype

from filecluster.configuration import CopyMode, Status
from filecluster.filecluster_types import MediaDataFrame
from filecluster.image_reader import (
    InboxReader,
    Metadata,
    configure_inbox_reader,
    get_media_df,
    get_media_stats,
    initialize_row_dict,
    multiple_timestamps_to_one,
    prepare_new_row_with_meta,
)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------
class TestMetadata:
    """Tests for the Metadata Pydantic model."""

    def test_default_values(self):
        """
        Test Description: A freshly created Metadata has sensible defaults.

        Purpose: Downstream code (prepare_new_row_with_meta) fills fields
        incrementally — defaults must not cause errors before fields are set.
        """
        meta = Metadata()
        assert meta.file_name == ""
        assert meta.file_size == 0
        assert meta.status == Status.UNKNOWN
        assert meta.is_image is True
        assert meta.duplicated_to == []
        assert meta.duplicated_cluster == []

    def test_custom_values_roundtrip(self):
        """Fields set at construction are retrievable."""
        meta = Metadata(file_name="test.jpg", file_size=12345, is_image=False)
        assert meta.file_name == "test.jpg"
        assert meta.file_size == 12345
        assert meta.is_image is False


# ---------------------------------------------------------------------------
# initialize_row_dict
# ---------------------------------------------------------------------------
class TestInitializeRowDict:
    """Tests for the initialize_row_dict function."""

    def test_returns_dict_with_expected_keys(self):
        """
        Test Description: The dict contains exactly the columns needed for the
        media DataFrame.

        Purpose: A missing key here would cause a KeyError when building the DF.
        """
        meta = Metadata()
        row = initialize_row_dict(meta)
        expected_keys = {
            "file_name",
            "m_date",
            "c_date",
            "exif_date",
            "date",
            "size",
            "hash_value",
            "is_image",
            "cluster_id",
            "status",
            "duplicated_to",
            "duplicated_cluster",
        }
        assert set(row.keys()) == expected_keys

    def test_values_match_metadata_fields(self):
        """
        Test Description: Row values are taken from the Metadata, not invented.

        Purpose: Ensures the mapping between Metadata fields and dict keys is
        correct — a wrong mapping silently produces incorrect data.
        """
        meta = Metadata(
            file_name="photo.jpg",
            m_time="2020-01-01",
            c_time="2020-01-02",
            exif_date="2020-01-03",
            file_size=9999,
            hash_value=42,
            is_image=True,
            status=Status.NEW_CLUSTER,
        )
        row = initialize_row_dict(meta)
        assert row["file_name"] == "photo.jpg"
        assert row["m_date"] == "2020-01-01"
        assert row["c_date"] == "2020-01-02"
        assert row["exif_date"] == "2020-01-03"
        assert row["size"] == 9999
        assert row["hash_value"] == 42
        assert row["is_image"] is True
        assert row["status"] == Status.NEW_CLUSTER


# ---------------------------------------------------------------------------
# multiple_timestamps_to_one
# ---------------------------------------------------------------------------
class TestMultipleTimestampsToOne:
    """Tests for the timestamp disambiguation logic.

    Business rules:
      - rule='m_date': prefer exif_date, fall back to m_date when exif is missing
      - rule='earliest': take the minimum of m_date, c_date, exif_date
      - drop_columns=True removes the raw date columns
    """

    def test_drops_raw_date_columns_by_default(self):
        """After disambiguation only the 'date' column remains."""
        df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
        result = multiple_timestamps_to_one(df)
        assert list(result.columns) == ["date"]

    def test_keeps_raw_columns_when_requested(self):
        """drop_columns=False preserves all four date columns."""
        df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
        result = multiple_timestamps_to_one(df, drop_columns=False)
        assert "m_date" in result.columns
        assert "c_date" in result.columns
        assert "exif_date" in result.columns
        assert "date" in result.columns

    def test_date_column_is_datetime64(self):
        """The output 'date' column must be datetime64 for time arithmetic."""
        df = MediaDataFrame(pd.DataFrame({"exif_date": [], "c_date": [], "m_date": []}))
        result = multiple_timestamps_to_one(df)
        assert result.dtypes["date"] == dtype("<M8[ns]")

    def test_m_date_rule_prefers_exif_then_mdate(self, media_df_with_timestamps):
        """
        Test Description: Under the 'm_date' rule, exif_date is preferred, and
        m_date is used as fallback when exif_date is NaT.

        Purpose: Core business logic — wrong date selection means files end up
        in the wrong cluster.

        Edge Cases Covered:
        - Row with valid exif_date => exif_date used
        - Row with missing exif_date => m_date used as fallback
        """
        result = multiple_timestamps_to_one(
            media_df_with_timestamps.copy(), rule="m_date", drop_columns=False
        )
        # Row 0: exif_date=13:55 is not NaT => date should be exif_date
        assert result.loc[0, "date"] == pd.Timestamp("2020-01-10 13:55:00")
        # Row 1: exif_date is NaT => date should fall back to m_date=15:00
        assert result.loc[1, "date"] == pd.Timestamp("2020-01-10 15:00:00")
        # Row 2: exif_date=15:55 => date should be exif_date
        assert result.loc[2, "date"] == pd.Timestamp("2020-01-10 15:55:00")
        # Row 3: exif_date is NaT => date should fall back to m_date=17:00
        assert result.loc[3, "date"] == pd.Timestamp("2020-01-10 17:00:00")

    def test_earliest_rule_takes_minimum(self, media_df_with_timestamps):
        """
        Test Description: Under 'earliest' rule, the minimum across all three
        date columns is selected.

        Purpose: Conservative date selection that avoids future-dated artefacts.
        """
        result = multiple_timestamps_to_one(
            media_df_with_timestamps.copy(), rule="earliest", drop_columns=False
        )
        # Row 0: min(14:00, 14:05, 13:55) = 13:55
        assert result.loc[0, "date"] == pd.Timestamp("2020-01-10 13:55:00")
        # Row 1: exif is NaT => min(15:00, 15:05) = 15:00
        assert result.loc[1, "date"] == pd.Timestamp("2020-01-10 15:00:00")

    def test_real_files_produce_valid_dates(self, assets_dir):
        """
        Test Description: End-to-end test with real image files from test assets.

        Purpose: Verifies the full pipeline: read files => build DataFrame =>
        disambiguate timestamps.
        """
        reader = InboxReader(in_dir_name=assets_dir / "set_1")
        rows = reader.get_data_from_files_as_list_of_rows()
        df = MediaDataFrame(pd.DataFrame(rows))
        result = multiple_timestamps_to_one(df, drop_columns=False)

        assert "date" in result.columns
        # Every file must get a date (no NaTs remaining)
        assert result["date"].notna().all(), (
            "Some files have no date after disambiguation"
        )


# ---------------------------------------------------------------------------
# prepare_new_row_with_meta
# ---------------------------------------------------------------------------
class TestPrepareNewRowWithMeta:
    """Tests for prepare_new_row_with_meta using real image files."""

    def test_returns_dict_with_all_fields(self, assets_dir):
        """
        Test Description: For a real JPEG, the returned dict contains all
        expected keys with non-trivial values.

        Purpose: This is the primary entry point for reading a single file's
        metadata. Incorrect output corrupts the entire media DataFrame.
        """
        meta = Metadata()
        row = prepare_new_row_with_meta(
            media_file_name="IMG_3784.jpg",
            accepted_media_file_extensions=[".jpg", ".jpeg"],
            in_dir_name=assets_dir / "set_1",
            meta=meta,
        )
        assert row["file_name"] == "IMG_3784.jpg"
        assert row["size"] > 0
        assert row["hash_value"] is not None and row["hash_value"] != 0
        assert row["is_image"] is True
        assert row["cluster_id"] is None  # not yet assigned
        assert row["status"] == Status.UNKNOWN
        assert row["date"] is None  # placeholder, filled later

    def test_video_file_detected_as_non_image(self, assets_dir):
        """MOV file is not an image even though it's a valid media file."""
        meta = Metadata()
        row = prepare_new_row_with_meta(
            media_file_name="IMG_2250.MOV",
            accepted_media_file_extensions=[".jpg", ".jpeg"],
            in_dir_name=assets_dir / "set_1",
            meta=meta,
        )
        assert row["is_image"] is False


# ---------------------------------------------------------------------------
# InboxReader
# ---------------------------------------------------------------------------
class TestInboxReader:
    """Tests for InboxReader file scanning."""

    def test_reads_all_files_from_directory(self, assets_dir):
        """
        Test Description: InboxReader discovers all media files in the inbox.

        Purpose: Missing files mean missing cluster assignments.
        """
        in_dir = assets_dir / "set_1"
        reader = InboxReader(in_dir_name=in_dir)
        rows = reader.get_data_from_files_as_list_of_rows()
        all_files = os.listdir(str(in_dir))
        assert len(rows) == len(all_files)

    def test_get_media_files_info_populates_dataframe(self, assets_dir):
        """
        Test Description: get_media_files_info fills self.media_df with the
        disambiguated date column.

        Purpose: This is the main entry point for the reader; the resulting DF
        is passed to ImageGrouper.
        """
        reader = InboxReader(in_dir_name=assets_dir / "set_1")
        reader.get_media_files_info()
        df = reader.media_df
        assert len(df) > 0
        assert "date" in df.columns
        assert "file_name" in df.columns
        assert df["date"].notna().all()

    def test_initializes_with_existing_dataframe(self, assets_dir, sample_media_df):
        """InboxReader accepts a pre-built DataFrame."""
        reader = InboxReader(in_dir_name=assets_dir / "set_1", media_df=sample_media_df)
        assert len(reader.media_df) == len(sample_media_df)

    def test_initializes_with_empty_dataframe_when_none(self, assets_dir):
        """Without a provided DataFrame, reader starts with an empty DF."""
        reader = InboxReader(in_dir_name=assets_dir / "set_1")
        assert len(reader.media_df) == 0


# ---------------------------------------------------------------------------
# get_media_df
# ---------------------------------------------------------------------------
class TestGetMediaDf:
    """Tests for the get_media_df convenience function."""

    def test_returns_dataframe_for_non_empty_dir(self, assets_dir):
        """Non-empty directory returns a DataFrame with 'date' column."""
        df = get_media_df(assets_dir / "set_1")
        assert df is not None
        assert "date" in df.columns
        assert len(df) > 0

    def test_returns_none_for_empty_dir(self, tmp_path):
        """Empty directory returns None."""
        result = get_media_df(tmp_path)
        assert result is None


# ---------------------------------------------------------------------------
# get_media_stats
# ---------------------------------------------------------------------------
class TestGetMediaStats:
    """Tests for the get_media_stats function.

    Business rules:
    - is_time_consistent is True when all consecutive timestamps are within
      time_granularity seconds
    - file_count equals the number of files
    """

    def test_stats_structure(self, assets_dir):
        """
        Test Description: Stats dict contains all expected keys.

        Purpose: Consumers rely on these keys to characterize clusters.
        """
        df = get_media_df(assets_dir / "set_1")
        stats = get_media_stats(df, time_granularity=3600)
        assert "date_min" in stats
        assert "date_max" in stats
        assert "date_median" in stats
        assert "is_time_consistent" in stats
        assert "file_count" in stats

    def test_file_count_matches_dataframe_length(self, assets_dir):
        """file_count must match the actual number of files."""
        df = get_media_df(assets_dir / "set_1")
        stats = get_media_stats(df, time_granularity=3600)
        assert stats["file_count"] == len(df)

    def test_date_range_correctness(self, assets_dir):
        """date_min <= date_median <= date_max."""
        df = get_media_df(assets_dir / "set_1")
        stats = get_media_stats(df, time_granularity=3600)
        assert stats["date_min"] <= stats["date_median"] <= stats["date_max"]

    def test_time_consistent_false_for_spread_data(self):
        """
        Test Description: Files separated by more than the granularity make
        is_time_consistent False.

        Purpose: This flag drives whether a cluster can accept new files.
        """
        data = {
            "file_name": ["a.jpg", "b.jpg"],
            "date": pd.to_datetime(["2020-01-01 10:00", "2020-01-01 15:00"]),
        }
        df = pd.DataFrame(data)
        stats = get_media_stats(df, time_granularity=3600)
        # 5-hour gap > 1-hour granularity => not consistent
        assert stats["is_time_consistent"] is False

    def test_time_consistent_true_for_tight_data(self):
        """Files within the granularity are time-consistent."""
        data = {
            "file_name": ["a.jpg", "b.jpg", "c.jpg"],
            "date": pd.to_datetime(
                ["2020-01-01 10:00", "2020-01-01 10:30", "2020-01-01 10:50"]
            ),
        }
        df = pd.DataFrame(data)
        stats = get_media_stats(df, time_granularity=3600)
        assert stats["is_time_consistent"] is True


# ---------------------------------------------------------------------------
# configure_inbox_reader
# ---------------------------------------------------------------------------
class TestConfigureInboxReader:
    """Tests for the configure_inbox_reader helper."""

    def test_sets_in_dir_and_nop_mode(self, assets_dir):
        """
        Test Description: The helper sets in_dir_name and NOP mode for scanning.

        Purpose: Library scanning should never move/copy files.
        """
        conf = configure_inbox_reader(in_dir_name=str(assets_dir / "set_1"))
        assert conf.in_dir_name == assets_dir / "set_1"
        assert conf.mode == CopyMode.NOP
        assert str(conf.out_dir_name) in (".", "")
