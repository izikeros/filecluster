import os
from datetime import timedelta

DELETE_DB = True
GENERATE_THUMBNAIL = False


def get_development_config():
    """ Configuration for development"""
    print("Warning: Using development configuration")

    # get defaults
    config = get_default_config()

    # move/copy/nop
    config['mode'] = 'move'

    # overwrite defaults with development specific params
    if os.name == 'nt':
        pth = 'h:\\incomming'
    else:
        pth = '/home/izik/bulk/fc_data'
        pth = '/home/safjan/Pictures'

    inbox_dir = 'inbox_test_a'
    outbox_dir = 'inbox_clust_test'

    config['inDirName'] = os.path.join(pth, inbox_dir)
    config['outDirName'] = os.path.join(pth, outbox_dir)

    config['db_file'] = os.path.join(pth, 'filecluster_db.sqlite3')
    config['db_file_media'] = os.path.join(pth, 'media.p')
    config['db_file_clusters'] = os.path.join(pth, 'clusters.p')
    return config


def get_default_config():
    # path to files to be clustered

    # Configure inbox
    if os.name == 'nt':
        pth = 'h:\\incomming\\'
    else:
        pth = '/media/root/Foto/incomming/'

    inbox_dir = 'inbox'
    outbox_dir = 'inbox_clust'

    inbox_path = os.path.join(pth, inbox_dir)
    outbox_path = os.path.join(pth, outbox_dir)

    # Configure database
    if os.name == 'nt':
        pth = 'h:\\zdjecia\\'
    else:
        pth = '/media/root/Foto/zdjecia/'

    db_file = os.path.join(pth, 'filecluster_db.sqlite3')
    db_file_clusters = os.path.join(pth, 'clusters.p')
    db_file_media = os.path.join(pth, 'media.p')

    # Filename extensions in scope of clustering
    image_extensions = ['.jpg', '.jpeg', '.dng', '.cr2']
    video_extensions = ['.mp4', '.3gp', 'mov']

    # Minimum gap that separate two events
    max_gap = timedelta(minutes=60)

    # method that is used to group images, default: assume different events
    # are separated by significant time gape (max_gap config parameter)
    clustering_method = 'time_gap'

    assign_date_to_clusters_method = 'random'

    config = {
        'inDirName': inbox_path,
        'outDirName': outbox_path,
        'db_file_clusters': db_file_clusters,
        'db_file_media': db_file_media,
        'db_file': db_file,
        'image_extensions': image_extensions,
        'video_extensions': video_extensions,
        'granularity_minutes': max_gap,
        'cluster_col': 'cluster_id',
        'assign_date_to_clusters_method': assign_date_to_clusters_method,
        'clustering_method': clustering_method,
        'mode': 'move',  # move | copy | nop
        'db_driver': 'sqlite',  # dataframe | sqlite
    }

    # ensure extensions are lowercase
    config['image_extensions'] = [xx.lower() for xx in
                                  config['image_extensions']]
    config['video_extensions'] = [xx.lower() for xx in
                                  config['video_extensions']]
    return config
