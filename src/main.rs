mod datastore;
mod fileutil;

use datastore::data::DBData;
use datastore::{DBConfig, DBStore};

fn db_init() -> DBStore {
    let segments = vec![
        String::from("<path-to-first-segment>"),
        String::from("<path-to-second-segment>"),
    ];

    let db_config: DBConfig = DBConfig::new(
        String::from("Simple Datastore"),
        String::from("<path-to-log-file>"),
        "".to_string(),
    );

    DBStore::new(db_config)
}

fn main() {
    println!("Welcome to Simple DataStore!");

    let db: DBStore = db_init();

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

    let data: DBData = DBData::new(key, val);

    println!("Key and value stored!");
}
