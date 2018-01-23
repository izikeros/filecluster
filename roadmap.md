[1] APPEND functionality - add new photos to the repository that already cover 
time when photos were taken. Usually case for late adding photos from iphone
 when photos from the other 'family media devices' were already downloaded 
 and groupped by event

[2] DUPLICATES - check if photos in given directory (e.g. photo_1)were already 
added to main photo repository. Put duplicates in `checkme_duplicates` and 
unique in `checkme_unique` (in subfolder photo_1 and replicate structure)
 
[3] TOP_EVENTS - find directories with more than n photos (can be 20 for 
first run and 10 for second run) and put them in special subfolder (e.g 
`20_plus` or `10_plus` respectively). These directories will be labelled in 
first place. Alternative scheme can be finding 80th centile - most populated
 folders covering 20% of pictures.

* Manual post-processing on miniatures
    * (A) generate thumbnails of inbox_clustered (in inbox_clustered_mini) to 
allow manual rename and refine clustering
    * rearrange original inbox_clusterred to mimic structure in 
inbox_clustered_mini after rearrangement
* add web UI to merge subsequent clusters
* generate basic sidecar files
* add sidecar with location to cr2 files if location available in photos 
from phone
* deep learning:
    * detect blurry photos
    * face detection
    * detect documents
    * detect Natalia's selfies
    * detect photo series (similar shots) (low resolution comparison?)
    * learn which is best photo (representative photo)
    
        
