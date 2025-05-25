use crate::{entry::DataType, error::Error};
use anyhow::Result;

pub trait StreamReader {
    fn read(&mut self, size: u64) -> Result<DataType, Error>;
    fn offset(&self) -> Result<u64, Error>;
    fn seek(&mut self, offset: u64) -> Result<(), Error>;
    fn seek_to_end(&mut self) -> Result<(), Error>;
    fn seek_to_begin(&mut self) -> Result<(), Error>;
    fn get_stream_range(&mut self) -> Result<(u64, u64), Error>;
    fn get_stream_id(&self) -> Result<u64, Error>;
}
