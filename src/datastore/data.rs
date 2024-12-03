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
    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }
    pub fn get_key(&self) -> &str {
        &self.key
    }
    pub fn set_val(&mut self, val: String) {
        self.val = val;
    }
    pub fn get_val(&self) -> &str {
        &self.val
    }
    pub fn print(&self) -> String {
        format!("Key: {}, Val: {}", self.key, self.val)
    }
}
