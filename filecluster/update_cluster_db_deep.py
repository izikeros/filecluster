#!/usr/bin/env python3
"""Scan recursively directory and get information on clusters."""
import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from filecluster.cluster_scaner import (
    initialize_cluster_info_dict,
    save_cluster_ini,
    read_cluster_ini_as_dict,
    fast_scandir,
    identify_folder_types,
    is_event,
    INI_FILENAME,
)
from filecluster.image_reader import configure_im_reader, get_media_df, get_media_stats

# Usage of the clustering information
# from datetime import datetime
# img_time_str = "2018-10-26 18:35:08"
# img_time = datetime.strptime(img_time_str, "%Y-%m-%d %H:%M:%S")  # 06-12
# candidate_clusters = df[(df.start <= img_time) & (df.stop >= img_time)]


log_fmt = "%(levelname).1s %(message)s"
logging.basicConfig(format=log_fmt)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def scan_library_dir(library_path: str, force_deep_scan=False):
    """Scan folder.

    Args:
        library_path:
        quick_mode:

    Returns:
        None
    """
    # strip trailing '/' if any
    library_path = library_path.rstrip("/")

    subfolders = fast_scandir(library_path)

    subfolders_root = [s.replace(library_path + "/", "") for s in subfolders]
    subs_labeled = identify_folder_types(subfolders_root)

    event_dirs = list(filter(is_event, subs_labeled))

    # reading
    ds = []
    for i, ed in tqdm(enumerate(event_dirs)):
        pth = Path(library_path) / ed[0]
        is_ini = os.path.isfile(Path(pth) / INI_FILENAME)
        if force_deep_scan or not is_ini:
            # calculate ini
            conf = configure_im_reader(in_dir_name=pth)
            media_df = get_media_df(conf)
            time_granularity = int(conf.time_granularity.total_seconds())
            media_stats = get_media_stats(media_df, time_granularity)
            cluster_ini = initialize_cluster_info_dict(
                start=media_stats[0], stop=media_stats[1], is_continous=media_stats[2]
            )
            save_cluster_ini(cluster_ini, pth)
        # read existing ini
        cluster_ini_r = read_cluster_ini_as_dict(pth)
        d = cluster_ini_r["Range"]
        d["path"] = pth
        ds.append(d)

    df = pd.DataFrame(ds)
    df.to_csv(Path(library_path) / ".clusters.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan media library locations and get information on existing clusters."
    )

    # named parameters
    parser.add_argument(
        "-l",
        "--library",
        help="top-level directory of the media library.",
        type=str,
        action="append",
    )

    parser.add_argument(
        "-f",
        "--force-recalc",
        help="recalculate cluster info even if .cluster.ini files exists",
        type=bool,
        default=False,
    )

    args = parser.parse_args()
    libs = args.library
    for lib in libs:
        scan_library_dir(library_path=lib, force_deep_scan=args.force_recalc)
