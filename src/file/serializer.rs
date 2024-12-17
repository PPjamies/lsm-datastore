use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

pub fn serialized_size<T>(data: &T) -> Result<u64>
where
    T: Serialize,
{
    Ok(bincode::serialized_size(data).map_err(|e| Error::new(ErrorKind::Other, e)))?
}

pub fn serialize<T>(data: &T, is_json: bool) -> Result<Vec<u8>>
where
    T: Serialize,
{
    if is_json {
        let str_data: String = serde_json::to_string(&data)?;
        return Ok(str_data.as_bytes().to_vec());
    }

    Ok(bincode::serialize(data).map_err(|e| Error::new(ErrorKind::Other, e)))?
}

pub fn deserialize_string<T>(data: &String) -> Result<T>
where
    T: Deserialize,
{
    Ok(serde_json::from_str(data).map_err(|e| Error::new(ErrorKind::Other, e)))?
}

pub fn deserialize_bytes<T>(data: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    Ok(bincode::deserialize(data).map_err(|e| Error::new(ErrorKind::Other, e)))?
}
