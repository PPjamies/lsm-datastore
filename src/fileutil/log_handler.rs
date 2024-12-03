use crate::datastore::{DBData, DBIndex};
use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};

/// functions read and write into log file and index file
pub fn read<T>(file_path: &str, offset: u64, length: usize) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut file: File = File::open(file_path)?;
    file.seek(SeekFrom::Start(offset))?;

    let mut buffer: Vec<u8> = vec![0; length];
    file.read_exact(&mut buffer)?;

    bincode::deserialize_from(&buffer).map_err(Error::new)
}

pub fn write<T>(file_path: &str, data: &T) -> Result<(u64, usize)>
where
    T: Serialize,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)?;

    let offset = file.seek(SeekFrom::End(0))?;

    let data: Vec<u8> = bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let length: usize = data.len();

    file.write_all(&data)?;

    Ok((offset, length))
}

/// This function returns a list of DB Index objects
fn get_log_indexes(file_path: &str) -> Result<Vec<DBIndex>> {
    let file: File = File::open(file_path)?;
    let reader: BufReader<File> = BufReader::new(file);

    let indexes: Vec<DBIndex> =
        bincode::deserialize_from(reader).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

    Ok(indexes)
}

/// scan log file
pub fn find_data_in_log(file_path: &str, search_key: &str) -> Result<Option<DBData>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?; // read line as string
        let bytes = line.as_bytes(); //convert to byte

        let data: DBData =
            bincode::deserialize(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if search_key == data.key {
            return Ok(Some(data));
        }
    }

    Ok(None)
}
