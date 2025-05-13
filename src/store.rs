use std::{u8, vec};

use crate::error::Error;

pub type AppendCallback = dyn Fn(Result<u64, Error>) -> () + Send + Sync + 'static;
pub type DataType = Vec<u8>;
pub struct Entry {
    // auto increment id
    pub version: u8,
    pub id: u64,
    pub stream_id: u64,
    pub offset: u64,
    pub data: DataType,
    pub callback: Box<AppendCallback>,
}
pub trait StreamReader {
    fn read(&mut self, size: u64) -> Result<DataType, Error>;
    fn offset(&self) -> Result<u64, Error>;
    fn seek(&mut self, offset: u64) -> Result<(), Error>;
    fn seek_to_end(&mut self) -> Result<(), Error>;
    fn seek_to_begin(&mut self) -> Result<(), Error>;
    fn get_stream_range(&mut self) -> Result<(u64, u64), Error>;
    fn get_stream_id(&self) -> Result<u64, Error>;
}
pub trait Store {
    // append data to stream
    fn append(&self, stream_id: u64, data: DataType, callback: Box<AppendCallback>);
    fn read(&mut self, stream_id: u64, offset: u64, size: u64) -> Result<DataType, Error>;
    fn get_reader(&mut self, stream_id: u64) -> Result<Box<dyn StreamReader>, Error>;
    fn truncate(&mut self, stream_id: u64, offset: u64) -> Result<(), Error>;
    fn get_stream_range(&mut self, stream_id: u64) -> Result<(u64, u64), Error>;
}
