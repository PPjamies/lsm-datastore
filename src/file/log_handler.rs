use crate::datastore::converters::convert_db_index_to_index_bucket;
use crate::datastore::indexable::Indexable;
use crate::datastore::operation::Operation;
use crate::datastore::store::IndexBucket;
use crate::datastore::DBIndex;
use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};

/// This function appends data to end of file and returns its byte offset and length
pub fn write<T>(file_path: &str, data: &T) -> Result<(u64, usize)>
where
    T: Indexable + Serialize,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)?;

    let offset: u64 = file.seek(SeekFrom::End(0))?;

    let data: Vec<u8> = bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let length: usize = data.len();

    file.write_all(&data)?;

    Ok((offset, length))
}

/// This function reads data from the given byte offset
pub fn read<T>(file_path: &str, offset: u64, length: usize) -> Result<T>
where
    T: Indexable + DeserializeOwned,
{
    let mut file: File = File::open(file_path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer: Vec<u8> = vec![0; length];
    file.read_exact(&mut buffer)?;

    bincode::deserialize_from(&*buffer).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

/// This function scans a log file for a given key and returns the newest data entry as well as its offset and length
pub fn scan<T>(file_path: &str, key: &str) -> Result<Option<(T, u64, usize)>>
where
    T: Indexable + Serialize + DeserializeOwned,
{
    let file: File = File::open(file_path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let mut newest_data: Option<T> = None;
    let mut offset: u64 = 0;
    let mut length: usize = 0;

    let mut current_offset: u64 = 0;
    loop {
        match bincode::deserialize_from(&mut reader) {
            Ok(data) => {
                let data: T = data;

                if key == data.key() {
                    let is_newer_data = match &newest_data {
                        Some(old_data) => data.timestamp() > old_data.timestamp(),
                        None => true,
                    };

                    if is_newer_data {
                        length = bincode::serialized_size(&data)
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                            as usize;
                        newest_data = Some(data);
                        offset = current_offset;
                    }
                }
                current_offset += length as u64;
            }
            Err(_e) => {
                break; //eof
            }
        }
    }

    match newest_data {
        Some(data) => Ok(Some((data, offset, length))),
        None => Ok(None), // no key exists
    }
}

/// This function reads through an index log and restores an in memory index map
pub fn restore_indexes(file_path: &str) -> Result<HashMap<String, IndexBucket>> {
    let file: File = File::open(file_path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let mut map: HashMap<String, IndexBucket> = HashMap::new();

    loop {
        match bincode::deserialize_from(&mut reader) {
            Ok(data) => {
                let data: DBIndex = data;

                match &data.operation() {
                    Operation::ADD | Operation::UPDATE => {
                        map.insert(data.key.clone(), convert_db_index_to_index_bucket(&data));
                    }
                    Operation::DELETE => {
                        map.remove(&data.key.clone());
                    }
                }
            }
            Err(_e) => {
                break; //eof
            }
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::data::DBData;
    use crate::datastore::operation::Operation;
    use crate::file::log_handler::{scan, write};
    use chrono::Utc;
    use std::fs;
    use std::fs::File;
    use std::path::Path;

    fn setup() -> Result<(String, DBData, u64)> {
        let temp_dir = std::env::temp_dir();
        let temp_db_path = temp_dir.join("test_log_db.txt");
        if !temp_db_path.exists() {
            File::create(&temp_db_path)?;
        }

        let db_path = temp_db_path.to_string_lossy().to_string();
        let db_data: DBData = DBData::new(
            String::from("test-key"),
            String::from("test-value"),
            Operation::ADD,
            Utc::now().timestamp_millis(),
        );
        let db_offset: u64 = 0;

        Ok((db_path, db_data, db_offset))
    }

    fn tear_down(path: &str) -> Result<()> {
        let path = Path::new(path);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    #[test]
    fn read_test() {
        let (db_path, db_data, db_offset) = setup().unwrap();

        match write::<DBData>(&db_path, &db_data) {
            Ok((offset, length)) => {
                assert_eq!(offset, db_offset, "Offset mismatch");

                match read::<DBData>(&db_path, offset, length) {
                    Ok(data) => {
                        assert_eq!(data, db_data, "Data mismatch");
                    }
                    Err(err) => panic!("Read failed: {}", err),
                }
            }
            Err(err) => panic!("Write failed: {}", err),
        }

        tear_down(&db_path).unwrap();
    }

    #[test]
    fn scan_test() {
        let (db_path, db_data, db_offset) = setup().unwrap();

        match write(&db_path, &db_data) {
            Ok((offset, length)) => {
                assert_eq!(offset, db_offset, "Offset mismatch");

                match scan::<DBData>(&db_path, &db_data.key) {
                    Ok(Some((scanned_data, scanned_offset, scanned_length))) => {
                        assert_eq!(scanned_data, db_data, "Data mismatch");
                        assert_eq!(scanned_offset, offset, "Offset mismatch");
                        assert_eq!(scanned_length, length, "Length mismatch");
                    }
                    Ok(None) => panic!("Scan did not find the data"),
                    Err(err) => panic!("Scan failed: {}", err),
                }
            }
            Err(err) => panic!("Write failed: {}", err),
        }

        tear_down(&db_path).unwrap();
    }
}
