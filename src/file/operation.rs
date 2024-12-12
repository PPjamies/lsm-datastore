use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Eq)]
pub enum Operation {
    ADD,
    UPDATE,
    DELETE,
}
