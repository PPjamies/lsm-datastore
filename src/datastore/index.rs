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
    offset: String,
    operation: Operation,
}

impl DBIndex {
    pub fn new(key: String, offset: String, operation: Operation) -> Self {
        Self {
            key,
            offset,
            operation,
        }
    }
    pub fn get_key(&self) -> &String {
        &self.key
    }
    pub fn get_offset(&self) -> &String {
        &self.offset
    }
    pub fn get_operation(&self) -> &Operation {
        &self.operation
    }
    pub fn print(&self) -> String {
        format!(
            "Key: {}, Offset: {}, Operation: {:?}",
            self.key, self.offset, self.operation
        )
    }
}
