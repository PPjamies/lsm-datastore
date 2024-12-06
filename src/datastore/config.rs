#[derive(Debug)]
pub struct DBConfig {
    pub log_path_db: String,
    pub log_path_index: String,
}

impl DBConfig {
    pub fn new(log_path_db: String, log_path_index: String) -> Self {
        Self {
            log_path_db,
            log_path_index,
        }
    }
}
