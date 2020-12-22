import pytest

from filecluster.file_cluster import main


class TestMain:
    def setup_class(self):
        self.inbox_dir = "inbox_test_a"
        self.output_dir = "/tmp/output_dir"
        self.development_mode = True
        self.force_deep_scan = True

    def test_main__with_skip_duplicates(self):
        main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=False,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__with_use_existing_clusters(self):
        main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=False,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__with_skip_duplicates_and_use_existing_clusters(self):
        main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__minimal(self):
        main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=[],
            development_mode=self.development_mode,
            drop_duplicates=False,
            use_existing_clusters=False,
            force_deep_scan=self.force_deep_scan,
        )


@pytest.mark.skip(reason="not implemented")
def test_save_media_and_cluster_info_to_database():
    assert False


@pytest.mark.skip(reason="not implemented")
def test_read_timestamps_form_media_files():
    assert False
