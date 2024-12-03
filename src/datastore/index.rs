use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum Operation {
    ADD,
    UPDATE,
    DELETE,
}

#[derive(Serialize, Deserialize)]
pub struct DBIndex {
    key: String,
    offset: u64,
    length: usize,
    operation: Operation,
}

impl DBIndex {
    pub fn new(key: String, offset: u64, length: usize, operation: Operation) -> Self {
        Self {
            key,
            offset,
            length,
            operation,
        }
    }
    pub fn print(&self) -> String {
        format!(
            "Key: {}, Offset: {}, Length: {}, Operation: {:?}",
            self.key, self.offset, self.length, self.operation
        )
    }
}
