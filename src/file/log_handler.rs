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
pub fn write<T>(path: &str, data: &T) -> Result<(u64, usize)>
where
    T: Indexable + Serialize,
{
    let mut file: File = OpenOptions::new().write(true).append(true).open(path)?;

    let offset: u64 = file.seek(SeekFrom::End(0))?;

    let data: Vec<u8> = bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let length: usize = data.len();

    file.write_all(&data)?;

    Ok((offset, length))
}

/// This function reads data from the given byte offset
pub fn read<T>(path: &str, offset: u64, length: usize) -> Result<T>
where
    T: Indexable + DeserializeOwned,
{
    let mut file: File = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer: Vec<u8> = vec![0; length];
    file.read_exact(&mut buffer)?;

    bincode::deserialize_from(&*buffer).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

/// This function scans a log file for a given key and returns the newest data entry as well as its offset and length
pub fn scan<T>(path: &str, key: &str) -> Result<Option<(T, u64, usize)>>
where
    T: Indexable + Serialize + DeserializeOwned,
{
    let file: File = File::open(path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let mut newest_data: Option<T> = None;
    let mut offset: u64 = 0;
    let mut length: usize = 0;

    let mut current_offset: u64 = 0;
    let mut current_length: usize = 0;
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
                        newest_data = Some(data);
                        length = current_length;
                        offset = current_offset;

                        continue;
                    }
                }

                current_length = bincode::serialized_size(&data)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
                    as usize;

                current_offset += current_length as u64;
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
pub fn restore_indexes(path: &str) -> Result<HashMap<String, IndexBucket>> {
    let file: File = File::open(path)?;
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
    use std::path::Path;

    fn setup() -> Result<(String, DBData, DBData)> {
        let path = String::from("test_log.txt");
        File::create(Path::new(&path))?;

        let old_data: DBData = DBData::new(
            String::from("test-key-1"),
            String::from("test-value-1"),
            Operation::ADD,
            Utc::now().timestamp_millis(),
        );
        let new_data: DBData = DBData::new(
            String::from("test-key-2"),
            String::from("test-value-2"),
            Operation::ADD,
            Utc::now().timestamp_millis(),
        );

        Ok((path, old_data, new_data))
    }

    fn tear_down(path: &str) -> Result<()> {
        if Path::new(&path).exists() {
            fs::remove_file(&path)?;
        }

        Ok(())
    }

    fn scanner(path: &str, key: &str, data: (&DBData, u64, usize)) -> Result<()> {
        let (data, offset, length) = data;

        match scan::<DBData>(path, key) {
            Ok(Some((actual_data, actual_offset, actual_length))) => {
                assert_eq!(actual_data, *data, "Data mismatch");
                assert_eq!(actual_offset, offset, "Offset mismatch");
                assert_eq!(actual_length, length, "Length mismatch");
            }
            Ok(None) => panic!("Scan did not find the data"),
            Err(err) => panic!("Scan failed: {}", err),
        }

        Ok(())
    }

    #[test]
    fn read_test() {
        let (path, data, _) = setup().unwrap();

        let result = || -> Result<()> {
            let (offset, length) = write(&path, &data)?;
            assert_eq!(offset, 0, "Offset mismatch");

            let actual_data = read::<DBData>(&path, offset, length)?;
            assert_eq!(actual_data, data, "Data mismatch");

            Ok(())
        }();

        let _ = fs::remove_file(path);

        match result {
            Ok(_) => {}
            Err(err) => panic!("Read failed: {}", err),
        }
    }

    #[test]
    fn scan_test() {
        let (path, old_data, new_data) = setup().unwrap();

        let result = || -> Result<()> {
            let (old_offset, old_length) = write(&path, &old_data)?;
            let (new_offset, new_length) = write(&path, &new_data)?;

            scanner(&path, new_data.key(), (&new_data, new_offset, new_length))?;

            Ok(())
        }();

        let _ = fs::remove_file(path);

        match result {
            Ok(_) => {}
            Err(err) => panic!("Scan failed: {}", err),
        }
    }

    #[test]
    fn restore_indexes_test() {
        let (path, _, _) = setup().unwrap();

        let old_data: DBIndex = DBIndex::new(
            String::from("test-key-1"),
            0u64,
            10usize,
            Operation::ADD,
            Utc::now().timestamp_millis(),
        );

        let new_data: DBIndex = DBIndex::new(
            String::from("test-key-1"),
            0u64,
            10usize,
            Operation::DELETE,
            Utc::now().timestamp_millis(),
        );

        let result = || -> Result<()> {
            write(&path, &old_data)?;

            let mut map = restore_indexes(&path)?;
            assert!(!map.is_empty(), "Map should contain one index");
            assert!(map.contains_key(&old_data.key), "Map should contain index key");

            write(&path, &new_data)?;

            map = restore_indexes(&path)?;
            assert!(map.is_empty(), "Map should be empty after adding and deleting index.");

            Ok(())
        }();

        let _ = fs::remove_file(path);

        match result {
            Ok(_) => {}
            Err(err) => panic!("Restoring indexes failed: {}", err),
        }
    }
}
