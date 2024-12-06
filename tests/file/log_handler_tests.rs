#[derive(cfgtest)]
mod tests {
    use crate::datastore::operation::Operation;
    use crate::datastore::{DBConfig, DBData, DBStore};
    use crate::file::log_handler::{read, scan, write};
    use chrono::Utc;
    use std::fs::File;

    fn setup() -> Result<(String, DBData, u64)> {
        let temp_dir = std::env::temp_dir();
        let temp_db_path = temp_dir.join("log_db.txt");
        if !temp_db_path.exists() {
            fs::File::create(&temp_db_path)?;
        }

        let db_path = db_path.to_string_lossy().to_string();
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
        let (db_path, db_data, db_offset) = setup();

        // write to db
        match log_handler::write::<DBData>(&db_path, &db_data) {
            Ok(offset, length) => {
                assert_eq!(offset, db_offset);

                // read from db
                match log_handler::read::<DBData>(&db_path, offset, length) {
                    Ok(data) => {
                        assert_eq!(data, db_data);
                    }
                    Err(err) => panic!("Read failed: {}", err),
                }
            }
            Err(err) => panic!("Write failed: {}", err),
        }

        tear_down(&db_path)?;
    }

    #[test]
    fn scan_test() {
        let (db_path, db_data, db_offset) = setup();

        match write(&db_path, &db_data) {
            Ok((offset, length)) => {
                assert_eq!(offset, db_offset);

                match scan::<DBData>(&db_path, &db_data.key) {
                    Ok(Some((scanned_data, scanned_offset, scanned_length))) => {
                        assert_eq!(scanned_data, db_data);
                        assert_eq!(scanned_offset, offset);
                        assert_eq!(scanned_length, length);
                    }
                    Ok(None) => panic!("Scan did not find the data"),
                    Err(err) => panic!("Scan failed: {}", err),
                }
            }
            Err(err) => panic!("Write failed: {}", err),
        }

        tear_down(&db_path)?;
    }
}
