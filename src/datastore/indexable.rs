use crate::datastore::operation::Operation;

pub trait Indexable {
    fn key(&self) -> &str;
    fn operation(&self) -> &Operation;
    fn timestamp(&self) -> &i64;
}
