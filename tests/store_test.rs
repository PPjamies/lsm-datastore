use simple_datastore::{DBConfig, DBStore, DBData, DBIndex, Operation};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::fs::File;
    use std::path::Path;
    use chrono::Utc;

    fn setup() -> std::io::Result<(DBStore, DBData, DBData)> {
        let log_path = String::from("test_log.txt");
        File::create(Path::new(&log_path))?;

        let index_path = String::from("test_index_log.txt");
        File::create(Path::new(&index_path))?;

        let mut db: DBStore = DBStore::new(DBConfig::new(log_path, index_path));

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

        Ok((db, old_data, new_data))
    }

    fn tear_down(log_path: &str, index_path: &str) -> std::io::Result<()> {
        if Path::new(&log_path).exists() {
            fs::remove_file(&log_path)?;
        }

        if Path::new(&index_path).exists() {
            fs::remove_file(&index_path)?;
        }

        Ok(())
    }

    #[test]
    fn put_no_index() {
        let (mut db, old_data, new_data) = setup().unwrap();

        let key: &str = old_data.key();

        if let Err(err) = db.put(old_data) {
            panic!("Put failed: {}", err);
        }

        if let Err(err) = db.put(new_data) {
            panic!("Put failed: {}", err);
        }

        match db.get(key) {
            Ok(data) => {
                assert_eq!(
                    data, new_data,
                    "Data mismatch. Scanning did not return the latest data."
                );
            }
            Err(err) => panic!("Get failed: {}", err),
        }

        tear_down(&db.config.log_path_db, &db.config.log_path_index).unwrap()
    }

    #[test]
    fn put_index() {
        let (mut db, old_data, new_data) = setup().unwrap();

        let key: String = old_data.key.clone();

        if let Err(err) = db.put(old_data) {
            panic!("Put failed: {}", err);
        }

        if let Err(err) = db.put(new_data) {
            panic!("Put failed: {}", err);
        }

        match db.create_index(&key) {
            Ok(_) => {
                assert_eq!(db.indexes.len(), 1);
                assert!(db.indexes.contains_key(&key));
            }
            Err(err) => panic!("Create index failed: {}", err),
        }

        // attempt to create index again should fail
        let create_index_result = db.create_index(&key);
        assert!(create_index_result.is_err(), "Expected an error but got {:?}", create_index_result);

        match db.get(&key) {
            Ok(data) => {
                assert_eq!(data, new_data, "Data mismatch. Reading from index did not return the latest data.");
            }
            Err(err) => panic!("Get failed: {}", err),
        }

        tear_down(&db.config.log_path_db, &db.config.log_path_index).unwrap()
    }
}
