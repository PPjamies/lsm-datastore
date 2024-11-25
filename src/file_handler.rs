use bincode;
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
    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }
    pub fn get_val(&self) -> &str {
        &self.val
    }
    pub fn set_val(&mut self, val: String) {
        self.val = val;
    }
    pub fn print_data(&self) -> String {
        format!("Key: {}, Val: {}", self.key, self.val)
    }
}

pub fn read(file_path: &str) -> io::Result<Data> {
    let file: File = File::open(file_path)?;
    let mut buff_reader: BufReader<File> = BufReader::new(file);

    let mut contents: Vec<u8> = Vec::new();
    buff_reader.read_to_end(&mut contents)?;

    let data: Data = bincode::deserialize(&contents)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(data)
}

pub fn write(data: &Data, file_path: &str) -> io::Result<()> {
    let file: File = File::create(file_path)?;
    let mut buff_writer: BufWriter<File> = BufWriter::new(file);

    bincode::serialize_into(&mut buff_writer, data)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(())
}

