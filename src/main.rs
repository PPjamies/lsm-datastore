use crate::index::Memtable;

mod file;
mod index;

fn main() {
    println!("Welcome to LSM DataStore!");

    let mut memtable = Memtable::new();

    let key = "a".to_string();
    let val = "b".to_string();

    memtable.put(key.clone(), val.clone());
    memtable.get(&key);
    memtable.delete(&key);
}
