use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
pub struct Data {
    pub key: String,
    pub val: String,
}

impl Data {
    pub fn new(key: String, val: String) -> Data {
        Data { key, val }
    }
    pub fn get_key(&self) -> &str {
        &self.key
    }
    pub fn set_key(&mut self, key: &str) {
        self.key = key.to_string();
    }
    pub fn get_val(&self) -> &str {
        &self.val
    }
    pub fn set_val(&mut self, val: &str) {
        self.val = val.to_string();
    }
    pub fn print_data(&self) -> String {
        format!("Key: {}, Val: {}", self.key, self.val)
    }
}

pub fn read(file: &mut File) -> std::io::Result<Data> {}
pub fn write(data: &Data, file: &mut File) -> std::io::Result<()> {}

