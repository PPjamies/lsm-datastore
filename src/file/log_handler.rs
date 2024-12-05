use crate::datastore::indexable::Indexable;
use bincode;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};

/// This function reads data from the given byte offset
pub fn read<T>(file_path: &str, offset: u64, length: usize) -> Result<T>
where
    T: DeserializeOwned + Indexable,
{
    let mut file: File = File::open(file_path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer: Vec<u8> = vec![0; length];
    file.read_exact(&mut buffer)?;

    bincode::deserialize_from(&buffer).map_err(Error::new)
}

/// This function appends data to end of file and returns its byte offset, length, and timestamp
pub fn write<T>(file_path: &str, data: T) -> Result<(u64, usize, i64)>
where
    T: Serialize + Indexable,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)?;

    let offset: u64 = file.seek(SeekFrom::End(0))?;

    let data: Vec<u8> = bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let length: usize = data.len();

    file.write_all(&data)?;
    file.write_all(b"\n")?;

    let timestamp: i64 = Utc::now().timestamp_millis();

    Ok((offset, length, timestamp))
}

/// This function scans an append only log file for a given key and returns the newest data entry
/// This function also returns the newest data entry's offset and length
pub fn scan<T>(file_path: &str, key: &str) -> Result<(T, u64, usize)>
where
    T: Indexable,
{
    let file: File = File::open(file_path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let mut newest_data: Option<T> = None;
    let mut offset: u64 = 0;
    let mut length: usize = 0;

    let mut current_offset: u64 = 0;
    for line in reader.lines() {
        let line = line?;
        let bytes = line.as_bytes();

        let data: T =
            bincode::deserialize(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if key == data.key() {
            let is_newer_data = match &newest_data {
                Some(old_data) => data.timestamp() > old_data.timestamp(),
                None => true,
            };

            if is_newer_data {
                newest_data = Some(data);
                offset = current_offset;
                length = bytes.len();
            }
        }

        current_offset += bytes.len() as u64;
    }

    Ok((newest_data, offset, length))
}

/// This function reads everything from the database into a vector
pub fn restore<T>(file_path: &str) -> Result<Vec<T>>
where
    T: DeserializeOwned + Indexable,
{
    let file: File = File::open(file_path)?;
    let reader: BufReader<File> = BufReader::new(file);

    bincode::deserialize_from(reader).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}
