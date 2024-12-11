mod datastore;
mod file;

use datastore::{DBConfig, DBStore};

fn db_init() -> DBStore {
    DBStore::new(DBConfig::new(
        String::from("<path-to-log-file>"),
        String::from("<path-to-log-index-file>"),
    ))
}

fn main() {
    println!("Welcome to Simple DataStore!");

    let _db: DBStore = db_init();
}
