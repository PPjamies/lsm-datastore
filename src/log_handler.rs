use crate::data::Data;
use bincode;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Result, Write};

pub fn append_to_log(file_path: &str, data: &Data) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;

    let mut writer = BufWriter::new(file);

    let serialized_data = bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e))?;

    writer.write_all(&serialized_data)?;
    writer.write_all(b"\n")?;

    Ok(())
}

pub fn find_in_log(file_path: &str, search_key: &str) -> Result<Option<Data>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?; // read line as string
        let bytes = line.as_bytes(); //convert to byte

        let data: Data =
            bincode::deserialize(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        if search_key == data.key {
            return Ok(Some(data));
        }
    }

    Ok(None)
}
