pub struct DBConfig {
    pub db_name: String,
    pub db_path: String,
    pub db_segments: Vec<String>,
    pub db_recovery: String,
}

impl DBConfig {
    pub fn new(db_name: String, db_path: String, db_segments: Vec<String>, db_recovery: String) -> DBConfig {
        DBConfig {
            db_name,
            db_path,
            db_segments,
            db_recovery,
        }
    }
    pub fn print(&self) -> String {
        format!(
            "Database: {}, Path: {}, Recovery: {}, Segments: {:?}",
            self.db_name, self.db_path, self.db_recovery, self.db_segments
        )
    }
}