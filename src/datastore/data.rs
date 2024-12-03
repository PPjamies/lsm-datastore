use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DBData {
    pub key: String,
    pub val: String,
}

impl DBData {
    pub fn new(key: String, val: String) -> Self {
        Self { key, val }
    }
    pub fn print(&self) -> String {
        format!("Key: {}, Val: {}", self.key, self.val)
    }
}
