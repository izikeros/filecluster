TODO list
==========

- for some reason after cluster id=1 there is cluster id=3 created - debug this
- rich directories (with many files) sortable to the top (cover 20%/50%/80% of files? All above 10?)
- check for duplicates before import
- add photos to cluster existing in watch folder (structured repo)
- read config from the dot file as here: https://www.foxinfotech.in/2019/01/how-to-read-config-file-in-python.html
- add mechanism to detect that cluster folder was updated and ini file needs recalculation
- use cluster_is as index in cluster_df
- better indicate progress while scanning library

FIXME:
- why '[2020_01_23]_Wystep_Hani' has no date info. In case exif not available, mdate/cdate should be taken. Is this windows problem.
seems that under windows do not read creation/modification date
- E [WinError 183] Nie można utworzyć pliku, który już istnieje: 'h:\\zdjecia\\2020\\[2020_09_26]_Runmageddon\\Piotrek'
- E [WinError 183] Nie można utworzyć pliku, który już istnieje: 'h:\\zdjecia\\2020\\[2020_09_26]_Runmageddon\\Hania'
- video files are not handled in any means
