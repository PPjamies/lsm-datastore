# Datastore that uses LSM Tree

* Database stores (key, val) in an append only log.txt
* Index stores (key, offset, operation) in append only offset.txt used to restore indexes
* The last offset in database is stored in offset.txt

This datastore will add the following functionalities:
* segmentation
* compaction
* merging 
