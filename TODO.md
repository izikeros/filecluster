TODO list
==========
- [FEAT] write a script that is detecting videos in folder with wrong date (different day)
- [FEAT] rich directories (with many files) sortable to the top (cover 20%/50%/80% of files? All above 10?)
- [FEAT] read config from the dot file as here: https://www.foxinfotech.in/2019/01/how-to-read-config-file-in-python.html
- [FEAT] add mechanism to detect that cluster folder was updated and ini file needs recalculation
- [DONE] [IMPR] better indicate progress while scanning library
- [DONE] [IMPR] better handle log level (single setting on package level)
- [REFA] use dataenforce and perhaps great expectations package to better control dataframes format and content
- [FEAT] integrate library scanning with diary - add reference to events dir in the diary note for given date. Create note for the day if not existing. Write python script for that. Might be too complex for bash script.
- [TASK] use github issues and tools for project planning
- [REFA] use `inbox_media_df` everywhere instead of `media_df`
- [REFA] use `cluster_id` as index in cluster_df data frame
- [REFA] rename `duplicated_cluster` in `inbox_media_df` to `ref_cluster` that should be filled in in case of duplicates and existing clusters and used to create target path
- [IMPR] take minimum date from mtime, ctime, exif date

Curation cascade (`src/filecluster/curation`, see docs/curation.md for detail):
- [TASK] build a labelled evaluation set from the own library (split by event, not at random) - blocks calibration
- [TASK] calibrate weights and thresholds, and fit a calibration map so the scores become probabilities
- [TASK] validate the SigLIP semantic stage on real files: per-label precision/recall, prompt-bank iteration, MPS vs CPU throughput
- [FEAT] close the feedback loop: capture user corrections, store embeddings, train the preference model
- [FEAT] implement the aesthetic provider (NIMA / MUSIQ / TOPIQ); the seam is ready
- [FEAT] implement the local VLM provider; response parsing and prompt are done
- [FEAT] wire `curate` into the main CLI, then `filecluster run --curate` sharing one inbox pass
- [FEAT] `curate cache` subcommand for stats / prune / vacuum (catalog methods already exist)
- [IMPR] batch the semantic stage instead of one image per forward pass
- [IMPR] decode one keyframe so videos get pixel and semantic signals instead of always going to review

FIXMEs:
- for the iphone videos (or videos in general) take creation date, not modification date
- why '[2020_01_23]_Wystep_Hani' has no date info. In case exif not available, mdate/cdate should be taken. Is this windows problem?
seems that under windows do not read creation/modification date
- E [WinError 183] Nie można utworzyć pliku, który już istnieje: 'h:\\zdjecia\\2020\\[2020_09_26]_Runmageddon\\Piotrek'
- E [WinError 183] Nie można utworzyć pliku, który już istnieje: 'h:\\zdjecia\\2020\\[2020_09_26]_Runmageddon\\Hania'
- case with same video file name but different size and contents -> found as duplicates
- case with new cluster gathering media from various events but mapping to existing. Existing was not continous? .cluster.ini: start_date = 2020-09-27 09:45:51
end_date = 2020-10-27 08:03:00
