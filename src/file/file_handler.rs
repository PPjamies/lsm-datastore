use crate::file::{deserialize_string, serialize};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Result, Write};

pub fn create_dir(path: &str) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub fn load_from_json<T>(path: &str) -> Result<T>
where
    T: Deserialize,
{
    let file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let mut content: String = String::new();
    reader.read_to_string(&mut content)?;

    let data: T = deserialize_string(&content);

    Ok(data)
}

pub fn flush<T>(path: &str, data: &T, is_json: bool) -> Result<()>
where
    T: Serialize,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    let data: Vec<u8> = serialize(&data, is_json)?;

    file.write_all(&data)?;
    file.sync_all()?;

    Ok(())
}
