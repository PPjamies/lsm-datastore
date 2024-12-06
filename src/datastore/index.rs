use crate::datastore::indexable::Indexable;
use crate::datastore::operation::Operation;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DBIndex {
    pub key: String,
    pub offset: u64,
    pub length: usize,
    pub operation: Operation,
    pub timestamp: i64,
}

impl DBIndex {
    pub fn new(
        key: String,
        offset: u64,
        length: usize,
        operation: Operation,
        timestamp: i64,
    ) -> Self {
        Self {
            key,
            offset,
            length,
            operation,
            timestamp,
        }
    }
}

impl Indexable for DBIndex {
    fn key(&self) -> &str {
        &self.key
    }
    fn operation(&self) -> &Operation {
        &self.operation
    }
    fn timestamp(&self) -> &i64 {
        &self.timestamp
    }
}
