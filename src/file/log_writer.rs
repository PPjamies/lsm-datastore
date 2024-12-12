use crate::datastore::Operation;
use crate::file::metadata::{Metadata, SegmentMetadata};
use chrono::Utc;
use serde::Serialize;
use std::fs::{create_dir_all, metadata, File, OpenOptions};
use std::io::{Error, ErrorKind, Result, Seek, SeekFrom, Write};

#[derive(Debug, Serialize)]
pub struct LogEntry<T> {
    key: String,
    value: T,
    operation: Operation,
    timestamp: u64,
}

#[derive(Serialize)]
pub struct LogWriter {
    logs_dir: String,
    log_path: String,
    metadata_path: String,
    metadata: Metadata,
}

impl LogWriter {
    //const MAX_SEGMENT_SIZE: u64 = 10 * 1024 * 1024; // 10 MB

    /// create log writer
    pub fn new(logs_dir: &str, metadata_path: &str) -> Result<Self> {
        let mut metadata: Metadata = Metadata::load(metadata_path)?;

        create_dir_all(logs_dir)?;

        let log_path: String = format!(
            "{}/segment_{}.log",
            logs_dir,
            metadata.generate_segment_id()
        );

        File::create(&log_path)?;

        Ok(Self {
            logs_dir: logs_dir.to_string(),
            log_path,
            metadata_path: metadata_path.to_string(),
            metadata,
        })
    }

    /// append data to log file
    /// input (log_entry: &LogEntry<T>)
    /// output (byte_offset, byte_length)
    pub fn append<T>(&mut self, log_entry: &LogEntry<T>) -> Result<(u64, usize)>
    where
        T: Serialize,
    {
        let mut file: File = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.log_path)?;

        let offset: u64 = file.seek(SeekFrom::End(0))?;
        let data: Vec<u8> =
            bincode::serialize(log_entry).map_err(|e| Error::new(ErrorKind::Other, e))?;
        let length: usize = data.len();

        file.write_all(&data)?;
        file.sync_all()?;

        Ok((offset, length))
    }

    /// creates new log file and stores old one as a segment file
    fn rotate(&mut self, threshold: u64) -> Result<()> {
        let log_file_size: u64 = metadata(&self.log_path)?.len();
        if log_file_size <= threshold {
            return Ok(());
        }

        let new_segment_id: u32 = self.metadata.generate_segment_id();
        let new_log_path: String = format!("{}/segment_{}.log", self.logs_dir, new_segment_id);

        File::create(&new_log_path)?;

        self.metadata.add_segment(SegmentMetadata {
            id: new_segment_id,
            size: 0,
            compacted: false,
            timestamp: Utc::now().timestamp_millis(),
        });

        self.metadata.save(&self.metadata_path)?;
        self.log_path = new_log_path;

        Ok(())
    }
}
