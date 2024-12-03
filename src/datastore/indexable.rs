#[derive(Debug)]
pub enum Operation {
    ADD,
    UPDATE,
    DELETE,
}

pub trait Indexable {
    fn key(&self) -> &str;
    fn operation(&self) -> &Operation;
    fn timestamp(&self) -> &i64;
}
