#[derive(Debug)]
pub struct DBConfig {
    pub name: String,
    pub log_path_db: String,
    pub log_path_index: String,
}

impl DBConfig {
    pub fn new(name: String, log_path_db: String, log_path_index: String) -> Self {
        Self {
            name,
            log_path_db,
            log_path_index,
        }
    }
}
