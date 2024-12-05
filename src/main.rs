mod datastore;
mod file;

use chrono::Utc;
use datastore::data::DBData;
use datastore::{DBConfig, DBStore};
use crate::datastore::indexable::Operation;

fn db_init() -> DBStore {
    let db_config: DBConfig = DBConfig::new(
        String::from("Simple Datastore"),
        String::from("<path-to-log-file>"),
        String::from("<path-to-log-index-file>"),
    );

    DBStore::new(db_config)
}

fn main() {
    println!("Welcome to Simple DataStore!");

    println!("Enter key:");
    let mut key = String::new();
    std::io::stdin()
        .read_line(&mut key)
        .expect("Failed to read key");

    println!("Enter value:");
    let mut val = String::new();
    std::io::stdin()
        .read_line(&mut val)
        .expect("Failed to read value");

    let mut db: DBStore = db_init();
    db.put(DBData::new(
        key,
        val,
        Operation::ADD,
        Utc::now().timestamp_millis()
    ));

    println!("Key and value stored!");
}
