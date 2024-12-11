use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Operation {
    ADD,
    UPDATE,
    DELETE,
}
