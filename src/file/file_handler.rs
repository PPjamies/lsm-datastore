use crate::file::{deserialize_string, serialize};
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Result, Write};

pub fn load_from_json<T>(path: &str) -> Result<Option<T>>
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

    // file empty or was created
    if content.trim().is_empty() {
        return Ok(None);
    }

    let data: T = deserialize_string(&content);

    Ok(Some(data))
}

pub fn load_from_bytes<T>(path: &str) -> Result<Option<T>>
where
    T: Deserialize,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?;

    let mut bytes: Vec<u8> = Vec::new();
    file.read_to_end(&mut bytes)?;

    // file empty or was created
    if bytes.is_empty() {
        return Ok(None);
    }

    let data: T = bincode::deserialize(&bytes);

    Ok(Some(data))
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

pub fn delete(path: &str) -> Result<()> {
    fs::remove_file(path)?;
    Ok(())
}
