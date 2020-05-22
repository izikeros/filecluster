from filecluster.file_cluster import main


def test_main():
    main(inbox_dir='inbox_test_a_orig',
         output_dir='/tmp/output_dir',
         db_dir='/tmp/',
         development_mode=True,
         no_operation=False)
