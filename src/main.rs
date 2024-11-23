mod simple_datastore;
mod file_handler;

use file_handler::Data;
use simple_datastore::DBConfig;
use simple_datastore::SimpleDatastore;
use std::io;

fn db_init() -> SimpleDatastore {
    let segments = vec![
        String::from("<path-to-first-segment>"),
        String::from("<path-to-second-segment>")
    ];

    let db_config: DBConfig = DBConfig::new(
        String::from("Simple Datastore"),
        String::from("<path-to-log-file>"),
        segments,
        String::from("<path-to-crash-recovery-file>"),
    );

    SimpleDatastore::new(db_config)
}

fn main() {
    println!("Welcome to Simple DataStore!");

    let db: SimpleDatastore = db_init();

    println!("Enter key:");
    let mut key = String::new();
    io::stdin().read_line(&mut key)
        .expect("Failed to read key");

    println!("Enter value:");
    let mut val = String::new();
    io::stdin().read_line(&mut val)
        .expect("Failed to read value");

    let data: Data = Data::new(key, val);

    println!("Key and value stored!");
}
