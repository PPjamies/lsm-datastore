use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    ADD,
    UPDATE,
    DELETE,
}
