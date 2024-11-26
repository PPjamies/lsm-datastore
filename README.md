# A Simple Datastore Built in Rust

* Database stores (key, val) in an append only log.txt
* Index stores (key, offset, operation) in append only offset.txt used to restore indexes
* The last offset in database is stored in offset.txt