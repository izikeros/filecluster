from filecluster.file_cluster import main


class TestMain:
    def setup_class(self):
        self.inbox_dir = "inbox_test_a"
        self.output_dir = "/tmp/output_dir"
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
        n_new_folder_exp = 17
        assert n_new_folder_res == n_new_folder_exp

        n_new_cluster_res = len(results["new_cluster_df"])
        n_new_cluster_exp = 17
        assert n_new_cluster_res == n_new_cluster_exp

    def test_main__with_skip_duplicates(self):
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=False,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__with_use_existing_clusters(self):
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=False,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )

    def test_main__with_skip_duplicates_and_use_existing_clusters(self):
        results = main(
            inbox_dir=self.inbox_dir,
            output_dir=self.output_dir,
            watch_dir_list=["zdjecia", "clusters"],
            development_mode=self.development_mode,
            drop_duplicates=True,
            use_existing_clusters=True,
            force_deep_scan=self.force_deep_scan,
        )
        n_new_folder_res = len(results["new_folder_names"])
        n_new_folder_exp = 15
        assert n_new_folder_res == n_new_folder_exp

        n_new_cluster_res = len(results["new_cluster_df"])
        n_new_cluster_exp = 15
        assert n_new_cluster_res == n_new_cluster_exp
