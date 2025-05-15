use std::{sync::atomic::AtomicU64, u8, vec};

use crate::{error::Error, wal::Wal};

pub type AppendCallback = dyn Fn(Result<u64, Error>) -> () + Send + Sync;
pub type DataType = Vec<u8>;

pub struct Entry {
    // auto increment id
    pub version: u8,
    pub id: u64,
    pub stream_id: u64,
    pub data: DataType,
    pub callback: Option<Box<AppendCallback>>,
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
    fn append(
        &self,
        stream_id: u64,
        data: DataType,
        callback: Box<AppendCallback>,
    ) -> Result<(), Error>;
    fn read(&mut self, stream_id: u64, offset: u64, size: u64) -> Result<DataType, Error>;
    fn get_reader(&mut self, stream_id: u64) -> Result<Box<dyn StreamReader>, Error>;
    fn truncate(&mut self, stream_id: u64, offset: u64) -> Result<(), Error>;
    fn get_stream_range(&mut self, stream_id: u64) -> Result<(u64, u64), Error>;
}

struct StreamStore {
    wal: Wal,
    entry_index: AtomicU64,
}

struct Options {
    wal_path: String,
    segment_path: String,
    max_table_size: u64,
}

impl StreamStore {
    pub fn open(options: Options) -> Result<Self, Error> {
        todo!()
    }
    pub fn start(&self) -> Result<(), Error> {
        // start the wal

        Ok(())
    }
}

impl Store for StreamStore {
    fn append(
        &self,
        stream_id: u64,
        data: DataType,
        callback: Box<AppendCallback>,
    ) -> Result<(), Error> {
        let id = self
            .entry_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.wal.write(Entry {
            version: 1,
            id: id,
            stream_id,
            data,
            callback: Some(callback),
        })
    }

    fn read(&mut self, stream_id: u64, offset: u64, size: u64) -> Result<DataType, Error> {
        todo!()
    }

    fn get_reader(&mut self, stream_id: u64) -> Result<Box<dyn StreamReader>, Error> {
        todo!()
    }

    fn truncate(&mut self, stream_id: u64, offset: u64) -> Result<(), Error> {
        todo!()
    }

    fn get_stream_range(&mut self, stream_id: u64) -> Result<(u64, u64), Error> {
        todo!()
    }
}

impl Entry {
    pub fn default() -> Self {
        Entry {
            version: 0,
            id: 0,
            stream_id: 0,
            data: Vec::new(),
            callback: None,
        }
    }
}
