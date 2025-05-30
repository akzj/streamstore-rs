use std::{io, sync::Arc};

use crate::{
    mem_table::MemTableArc,
    store::{SegmentArc, StreamStoreInner},
};

pub enum StreamReadState {
    None,
    MemTable,
    MemTables,
    Segment,
    EndOfStream,
}

pub struct StreamReader {
    stream_id: u64,
    inner: Arc<StreamStoreInner>,
    offset: std::sync::atomic::AtomicU64,
    read_memtable: Option<MemTableArc>,
    read_segment: Option<SegmentArc>,
    read_state: std::sync::Mutex<StreamReadState>,
}

impl StreamReader {
    pub fn new(inner: Arc<StreamStoreInner>, stream_id: u64) -> Self {
        Self {
            inner,
            stream_id,
            read_memtable: None,
            read_segment: None,
            read_state: std::sync::Mutex::new(StreamReadState::None),
            offset: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn offset(&self) -> u64 {
        self.offset.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn reset_read_state(&mut self) {
        self.read_memtable = None;
        self.read_segment = None;
        *self.read_state.lock().unwrap() = StreamReadState::None;
    }
}

impl io::Read for StreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        todo!()
    }
}

impl io::Seek for StreamReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let (begin, end) = self.inner.get_stream_range(self.stream_id).map_err(|e| {
            log::error!("Failed to get stream range: {:?}", e);
            io::Error::new(io::ErrorKind::Other, "Failed to get stream range")
        })?;
        let (base_offset, change) = match pos {
            io::SeekFrom::Start(offset) => {
                if offset < begin || offset > end {
                    log::error!(
                        "Seek position out of range: {} not in [{}, {}]",
                        offset,
                        begin,
                        end
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Seek position out of range",
                    ));
                }
                self.reset_read_state();
                self.offset
                    .store(offset, std::sync::atomic::Ordering::Relaxed);
                return Ok(offset);
            }
            io::SeekFrom::End(offset) => (end, offset),
            io::SeekFrom::Current(offset) => (
                self.offset.load(std::sync::atomic::Ordering::Relaxed),
                offset,
            ),
        };

        match base_offset.checked_add_signed(change) {
            Some(new_offset) => {
                if new_offset >= begin && new_offset <= end {
                    self.reset_read_state();
                    self.offset
                        .store(new_offset, std::sync::atomic::Ordering::Relaxed);
                    return Ok(new_offset);
                } else {
                    log::error!(
                        "Seek position out of range: {} not in [{}, {}]",
                        new_offset,
                        begin,
                        end
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Seek position out of range",
                    ));
                }
            }
            _ => {
                log::error!(
                    "invalid seek to a negative or overflowing position: current_offset {} offset {} ",
                    base_offset,
                    change
                );
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ));
            }
        };
    }
}
