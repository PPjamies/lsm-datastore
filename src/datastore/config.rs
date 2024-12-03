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
    pub fn get_log_path_db(&self) -> &str {
        &self.log_path_db
    }
    pub fn get_log_path_index(&self) -> &str {
        &self.log_path_index
    }
    pub fn print(&self) -> String {
        format!(
            "Database: {}, DB Log Path: {}, Index Log Path: {}",
            self.name, self.log_path_db, self.log_path_index
        )
    }
}
