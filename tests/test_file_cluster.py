import tempfile

import pytest

from filecluster import logger
from filecluster.file_cluster import main


class TestMain:
    @pytest.fixture(autouse=True)
    def setup_class(self, assets_dir):
        self.inbox_dir = assets_dir / "set_1"
        # use temp dir for output
        tmpdir = tempfile.mkdtemp()
        self.output_dir = tmpdir
        self.development_mode = True
        self.force_deep_scan = True

    def test_main__minimal(self):
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[],
            development_mode=self.development_mode,
            drop_duplicates=False,
            use_existing_clusters=False,
            force_deep_scan=self.force_deep_scan,
        )
        n_new_folder_res = len(results["new_folder_names"])
        n_new_folder_exp = 4
        assert n_new_folder_res == n_new_folder_exp

        n_new_cluster_res = len(results["new_cluster_df"])
        n_new_cluster_exp = 4
        logger.info(f"output_dir: {self.output_dir}")
        assert n_new_cluster_res == n_new_cluster_exp

    def test_main__with_skip_duplicates(self, assets_dir):
        _ = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[assets_dir / "zdjecia", assets_dir / "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=False,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__with_use_existing_clusters(self, assets_dir):
        _ = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[assets_dir / "zdjecia", assets_dir / "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=False,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )

    # @pytest.mark.skip(reason="fix expectations")
    def test_main__with_skip_duplicates_and_use_existing_clusters(self, assets_dir):
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[assets_dir / "zdjecia", assets_dir / "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )
        self._assert_result_len(results, "new_folder_names")
        self._assert_result_len(results, "new_cluster_df")

    # TODO Rename this here and in `test_main__with_skip_duplicates_and_use_existing_clusters`
    def _assert_result_len(self, results, arg1, value=15):
        n_new_folder_res = len(results[arg1])
        n_new_folder_exp = value
        assert n_new_folder_res == n_new_folder_exp
