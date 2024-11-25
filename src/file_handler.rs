use crate::data::Data;
use bincode;
use std::fs::File;
use std::io::{BufReader, BufWriter, Error, ErrorKind, Result};

pub fn read(file_path: &str) -> Result<Data> {
    let file: File = File::open(file_path)?;
    let mut reader: BufReader<File> = BufReader::new(file);

    let data: Data =
        bincode::deserialize_from(&mut reader).map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(data)
}

pub fn write(data: &Data, file_path: &str) -> Result<()> {
    let file: File = File::create(file_path)?;
    let mut writer: BufWriter<File> = BufWriter::new(file);

    bincode::serialize_into(&mut writer, data).map_err(|e| Error::new(ErrorKind::Other, e))?;
    Ok(())
}
