use skiplist::SkipMap;
use std::collections::HashMap;

pub fn convert_skipmap_to_vec(map: &SkipMap<u64, String>) -> Vec<(u64, String)> {
    map.iter().collect()
}

pub fn convert_vec_to_skipmap(v: &Vec<(u64, String)>) -> SkipMap<u64, String> {
    let mut map = SkipMap::new();
    for (k, v) in v {
        map.insert(k.clone(), v.clone());
    }
    map
}

pub fn convert_hashmap_to_vec(map: &HashMap<u64, String>) -> Vec<(u64, String)> {
    map.iter().collect()
}

pub fn convert_vec_to_hashmap(v: &Vec<(u64, String)>) -> HashMap<u64, String> {
    let mut map: HashMap<u64, String> = HashMap::new();
    for (k, v) in v {
        map.insert(k.clone(), v.clone());
    }
    map
}
