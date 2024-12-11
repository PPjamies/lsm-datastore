use simple_datastore::indexable::Indexable;
use simple_datastore::{DBConfig, DBStore};

#[cfg(test)]
mod tests {
    use super::*;
    use simple_datastore::{DBData, Operation};
    use std::fs;
    use std::fs::File;
    use std::io::Result;
    use std::path::Path;

    fn setup() -> Result<(DBStore, String, String, String)> {
        let log_path = String::from("test_log.txt");
        File::create(Path::new(&log_path))?;

        let index_path = String::from("test_index_log.txt");
        File::create(Path::new(&index_path))?;

        let db: DBStore = DBStore::new(DBConfig::new(log_path, index_path));

        let key: String = "key".to_string();
        let old_val: String = "val1".to_string();
        let new_val: String = "val2-bigger".to_string();

        Ok((db, key, old_val, new_val))
    }

    fn tear_down(log_path: &str, index_path: &str) -> Result<()> {
        if Path::new(&log_path).exists() {
            fs::remove_file(&log_path)?;
        }

        if Path::new(&index_path).exists() {
            fs::remove_file(&index_path)?;
        }

        Ok(())
    }

    #[test]
    fn update_index_test() {
        let (mut db, key, old_val, new_val) = setup().unwrap();

        let result = || -> Result<()> {
            db.update_index(&key, 10, 10, Operation::ADD)?;
            assert!(!db.indexes.is_empty());
            assert!(db.indexes.contains_key(&key));

            db.update_index(&key, 12, 12, Operation::DELETE)?;
            assert!(db.indexes.is_empty());

            Ok(())
        }();

        tear_down(&db.config.log_path_db, &db.config.log_path_index).unwrap();

        match result {
            Ok(_) => {}
            Err(e) => panic!("Failed to update index: {}", e),
        }
    }

    #[test]
    fn create_index_test() {
        let (mut db, key, old_val, new_val) = setup().unwrap();

        let result = || -> Result<()> {
            let (old_offset, old_length) = db.put(key.clone(), old_val.clone())?;
            let (new_offset, new_length) = db.put(key.clone(), new_val.clone())?;

            let (index_offset, index_length) = db.create_index(&key)?;
            assert!(db.indexes.contains_key(&key));
            assert_eq!(index_offset, new_offset);
            assert_eq!(index_length, new_length);

            let data: DBData = db.get(&key)?;
            assert_eq!(&data.key, &key, "Key mismatch");
            assert_eq!(&data.val, &new_val, "Val mismatch");

            db.delete_index(&key)?;
            assert!(db.indexes.is_empty());

            Ok(())
        }();

        tear_down(&db.config.log_path_db, &db.config.log_path_index).unwrap();

        match result {
            Ok(_) => {}
            Err(e) => panic!("Put/Get (where key is not indexed) failed: {}", e),
        }
    }
}
