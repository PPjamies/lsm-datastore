use std::collections::HashMap;
use crate::index::{SSTable, SSTableIndex};

mod index;
mod file;

fn main() {
    println!("Welcome to LSM DataStore!");

    // Simulate multiple SSTables
    let mut sstable1 = SSTable {
        file_name: "sstable1.txt".to_string(),
        data: HashMap::new(),
    };
    sstable1.data.insert("key1".to_string(), "value1".to_string());

    let mut sstable2 = SSTable {
        file_name: "sstable2.txt".to_string(),
        data: HashMap::new(),
    };
    sstable2.data.insert("key2".to_string(), "value2".to_string());

    // Simulate first compaction
    let mut index = SSTableIndex::new();
    index.add_sstable(sstable1.file_name.clone(), "key1".to_string(), "key1".to_string());
    index.add_sstable(sstable2.file_name.clone(), "key2".to_string(), "key2".to_string());
    let merged_sstable1 = SSTable::merge(vec![sstable1, sstable2], &mut index).unwrap();

    // Simulate another SSTable with overlapping data
    let mut sstable3 = SSTable {
        file_name: "sstable3.txt".to_string(),
        data: HashMap::new(),
    };
    sstable3.data.insert("key1".to_string(), "new_value1".to_string());

    // Simulate second compaction
    let merged_sstable2 = SSTable::merge(vec![merged_sstable1, sstable3], &mut index).unwrap();

    // Output merged SSTable and index
    println!("After 2 compactions: {:?}", merged_sstable2);
    println!("Updated SSTable index: {:?}", index);
}
