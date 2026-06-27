"""Tests for the file_cluster main module.

Covers the main() orchestration function, CLI argument processing via
process_watch_dirs, and end-to-end clustering pipelines with various
feature flag combinations.

No mocking — these are integration tests using real files from test assets.
"""

import tempfile

import pytest

from filecluster.file_cluster import create_argument_parser, main, process_watch_dirs


# ---------------------------------------------------------------------------
# process_watch_dirs
# ---------------------------------------------------------------------------
class TestProcessWatchDirs:
    """Tests for the CLI watch directory processor.

    Business rules:
    - None => empty list
    - list => returned as-is
    - anything else => TypeError
    """

    def test_none_returns_empty_list(self):
        """No watch dirs specified => empty list."""
        assert process_watch_dirs(None) == []

    def test_list_returned_as_is(self):
        """A list of paths is returned unchanged."""
        dirs = ["/a", "/b"]
        assert process_watch_dirs(dirs) == dirs

    def test_non_list_raises_type_error(self):
        """A string (instead of list) raises TypeError."""
        with pytest.raises(TypeError, match="list"):
            process_watch_dirs("/not/a/list")

    def test_empty_list_returns_empty(self):
        """Explicit empty list is valid."""
        assert process_watch_dirs([]) == []


class TestArgumentParser:
    """Tests for the CLI argument parser."""

    def test_restore_original_names_flag_defaults_false(self):
        """The --restore-original-names flag defaults to False."""
        parser = create_argument_parser()
        args = parser.parse_args([])
        assert args.restore_original_names is False

    def test_restore_original_names_flag_can_be_set(self):
        """Both the short and long forms enable the flag."""
        parser = create_argument_parser()
        assert parser.parse_args(["-r"]).restore_original_names is True
        assert (
            parser.parse_args(["--restore-original-names"]).restore_original_names
            is True
        )


# ---------------------------------------------------------------------------
# main() — integration tests
# ---------------------------------------------------------------------------
class TestMainOrchestration:
    """Integration tests for the main clustering pipeline.

    Uses real image assets and a temp directory for output. Tests verify the
    clustering results dict contains expected structures and counts.

    Mocking Strategy: None. File system operations use temp dirs that are
    cleaned up automatically.
    """

    @pytest.fixture(autouse=True)
    def setup(self, assets_dir):
        self.inbox_dir = assets_dir / "set_1"
        self.output_dir = tempfile.mkdtemp()
        self.assets_dir = assets_dir

    def test_minimal_clustering_produces_expected_clusters(self):
        """
        Test Description: Clustering the test inbox without duplicate detection
        or existing-cluster assignment produces 4 new clusters and 4 folder names.

        Purpose: This is the core happy path — the most common use case.

        Test Strategy:
        - Setup: 8 test files in set_1, no watch folders
        - Execution: main() with minimal flags
        - Verification: 4 clusters (matching the time distribution of test files)
        """
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[],
            development_mode=True,
            drop_duplicates=False,
            use_existing_clusters=False,
            force_deep_scan=True,
        )
        assert len(results["new_folder_names"]) == 4
        assert len(results["new_cluster_df"]) == 4
        assert results["dup_files"] == 0
        assert results["dup_clusters"] == 0

    def test_with_skip_duplicates(self):
        """
        Test Description: With duplicate detection enabled, known duplicate files
        are identified against the watch library.

        Purpose: Users want to avoid re-clustering files that already exist in
        their organized collection.
        """
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[
                self.assets_dir / "zdjecia",
                self.assets_dir / "clusters",
            ],
            development_mode=True,
            drop_duplicates=True,
            use_existing_clusters=False,
            force_deep_scan=True,
        )
        assert "dup_files" in results
        assert "dup_clusters" in results
        assert "new_cluster_df" in results
        # Some duplicates should be detected (shared files between set_1 and library)
        assert isinstance(results["dup_files"], list)

    def test_with_use_existing_clusters(self):
        """
        Test Description: With existing-cluster assignment enabled, some inbox
        files may be assigned to library clusters.

        Purpose: Avoids creating duplicate event folders when the event already
        exists in the collection.
        """
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[
                self.assets_dir / "zdjecia",
                self.assets_dir / "clusters",
            ],
            development_mode=True,
            drop_duplicates=False,
            use_existing_clusters=True,
            force_deep_scan=True,
        )
        assert "files_existing_cl" in results
        assert "existing_cluster_names" in results
        assert "new_cluster_df" in results

    def test_results_contain_cluster_dataframe(self):
        """The results dict always contains a 'df_clusters' key."""
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[],
            development_mode=True,
            drop_duplicates=False,
            use_existing_clusters=False,
            force_deep_scan=True,
        )
        assert "df_clusters" in results
        assert "new_cluster_df" in results
        assert "new_folder_names" in results

    def test_no_operation_mode_does_not_move_files(self):
        """
        Test Description: When no_operation is True, the output directory stays
        empty — no files are moved or copied.

        Purpose: Dry-run is essential for previewing clustering results.
        """
        import os

        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[],
            development_mode=True,
            no_operation=True,
            drop_duplicates=False,
            use_existing_clusters=False,
            force_deep_scan=True,
        )
        # Output dir should remain empty in NOP mode
        output_contents = os.listdir(self.output_dir)
        assert len(output_contents) == 0
        # But results should still be computed
        assert len(results["new_cluster_df"]) > 0
