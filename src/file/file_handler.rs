use crate::file::serializer::serialize;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Result, Write};

/// create a file if it doesn't exist or open existing file
/// overwrite the file with data
pub fn flush_to_file<T>(path: &str, data: &T) -> Result<()>
where
    T: Serialize,
{
    let mut file: File = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    let data: Vec<u8> = serialize(data)?;

    file.write_all(&data)?;
    file.sync_all()?;

    Ok(())
}
