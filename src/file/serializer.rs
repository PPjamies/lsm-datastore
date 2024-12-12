use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Error, ErrorKind, Result};

pub fn serialize<T>(data: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    Ok(bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e)))?
}

pub fn deserialize<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    Ok(bincode::deserialize(bytes).map_err(|e| Error::new(ErrorKind::Other, e)))?
}
