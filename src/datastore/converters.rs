use crate::datastore::DBIndex;
use crate::datastore::store::IndexBucket;

pub fn convert_db_index_to_index_bucket(db_index: &DBIndex) -> IndexBucket {
    IndexBucket {
        offset: db_index.offset,
        length: db_index.length,
    }
}