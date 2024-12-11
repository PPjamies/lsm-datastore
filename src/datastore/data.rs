use crate::datastore::indexable::Indexable;
use crate::datastore::operation::Operation;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DBData {
    pub key: String,
    pub val: String,
    pub operation: Operation,
    pub timestamp: i64,
}

impl DBData {
    pub fn new(key: String, val: String, operation: Operation, timestamp: i64) -> Self {
        Self {
            key,
            val,
            operation,
            timestamp,
        }
    }
}

impl Indexable for DBData {
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
