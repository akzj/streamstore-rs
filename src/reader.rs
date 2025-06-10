use std::{io, sync::Arc};

use crate::{
    mem_table::MemTableArc,
    metrics,
    store::{SegmentWeak, StreamStoreInner},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamReadState {
    None,
    Segment,
    MemTables,
    MemTable,
}

pub struct StreamReader {
    stream_id: u64,
    inner: Arc<StreamStoreInner>,
    offset: std::sync::atomic::AtomicU64,
    read_memtable: Option<MemTableArc>,
    read_segment: Option<SegmentWeak>,
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

    fn read_from_segments(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_bytes_all = 0;
        if let Some(segment) = &self.read_segment {
            if let Some(segment) = segment.upgrade() {
                metrics::read_segment_hit_count.inc();
                let (begin, end) = segment.get_stream_range(self.stream_id).unwrap();
                assert!(
                    begin <= self.offset() && self.offset() <= end,
                    "Offset {} out of range[{}, {}) for Stream ID {}",
                    self.offset(),
                    begin,
                    end,
                    self.stream_id
                );

                let bytes_read = segment.read_stream(self.stream_id, self.offset(), buf)?;
                if bytes_read > 0 {
                    self.offset
                        .fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
                    read_bytes_all += bytes_read;
                    if read_bytes_all >= buf.len() {
                        return Ok(read_bytes_all); // Stop if we filled the buffer
                    }
                }
            }
        }
        loop {
            let begin_ts = std::time::Instant::now();
            match self.inner.find_segment(self.stream_id, self.offset()) {
                Some(segment) => {
                    metrics::read_segment_miss_count.inc();
                    metrics::find_segment_time_seconds.observe(begin_ts.elapsed().as_secs_f64());
                    let bytes_read = segment.read_stream(
                        self.stream_id,
                        self.offset(),
                        &mut buf[read_bytes_all..],
                    )?;
                    self.offset
                        .fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
                    read_bytes_all += bytes_read;
                    if read_bytes_all >= buf.len() {
                        self.read_segment = Some(Arc::downgrade(&segment));
                        return Ok(read_bytes_all); // Stop if we filled the buffer
                    }
                }
                None => {
                    log::debug!(
                        "No more segments found for Stream ID {} at offset {}",
                        self.stream_id,
                        self.offset()
                    );
                    break; // No more segments to read
                }
            }
        }

        *self.read_state.lock().unwrap() = StreamReadState::MemTables;
        Ok(read_bytes_all)
    }

    fn read_from_tables(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_bytes_all = 0;
        if let Some(memtable) = &self.read_memtable {
            let bytes_read =
                memtable.read_stream(self.stream_id, self.offset(), &mut buf[read_bytes_all..])?;
            self.offset
                .fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
            read_bytes_all += bytes_read;
            if read_bytes_all >= buf.len() {
                return Ok(read_bytes_all); // Stop if we filled the buffer
            }
        }

        let memtables = self.inner.mem_tables.read().unwrap();
        for memtable in memtables.iter() {
            if let Some((begin, end)) = memtable.get_stream_range(self.stream_id) {
                if begin <= self.offset() && self.offset() < end {
                    log::debug!(
                        "Reading from MemTable for Stream ID {} at offset {} begin {} end {}",
                        self.stream_id,
                        self.offset(),
                        begin,
                        end
                    );
                    let read_buf = &mut buf[read_bytes_all..];
                    let bytes_read =
                        memtable.read_stream(self.stream_id, self.offset(), read_buf)?;
                    self.offset
                        .fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
                    read_bytes_all += bytes_read;

                    if read_bytes_all >= buf.len() {
                        self.read_memtable = Some(memtable.clone());
                        return Ok(read_bytes_all); // Stop if we filled the buffer
                    }
                }
            }
        }

        *self.read_state.lock().unwrap() = StreamReadState::MemTable;

        Ok(read_bytes_all)
    }
}

impl io::Read for StreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_bytes_all = 0;

        loop {
            let state = { *self.read_state.lock().unwrap() };

            match state {
                StreamReadState::None => {
                    log::info!(
                        "Reader Stream ID {} offset{} not initialized, checking MemTable and segments...",
                        self.stream_id,
                        self.offset()
                    );
                    // Initialize the read state
                    match self.inner.table.load().get_stream_range(self.stream_id) {
                        Some((begin, end)) => {
                            if begin <= self.offset() && self.offset() < end {
                                *self.read_state.lock().unwrap() = StreamReadState::MemTable;
                                log::debug!(
                                    "Stream ID {} found in MemTable, range: [{}, {})",
                                    self.stream_id,
                                    begin,
                                    end
                                );
                                continue;
                            }
                        }
                        None => {
                            log::debug!(
                                "Stream ID {} offset {} not found in MemTable, checking segments...",
                                self.stream_id,
                                self.offset()
                            );
                        }
                    };

                    match self.inner.mem_tables.read().unwrap().iter().find(|mt| {
                        if let Some((begin, end)) = mt.get_stream_range(self.stream_id) {
                            begin <= self.offset() && self.offset() < end
                        } else {
                            false
                        }
                    }) {
                        Some(memtable) => {
                            let (begin, end) = memtable.get_stream_range(self.stream_id).unwrap();
                            log::info!(
                                "Stream ID {} offset {} found in MemTable begin {} end {}",
                                self.stream_id,
                                self.offset(),
                                begin,
                                end
                            );
                            self.read_memtable = Some(memtable.clone());
                            *self.read_state.lock().unwrap() = StreamReadState::MemTables;
                            continue;
                        }
                        None => {
                            log::debug!(
                                "Stream ID {} offset {} not found in any MemTable, checking segments...",
                                self.stream_id,
                                self.offset()
                            );
                        }
                    }

                    // If we reach here, we need to check the segments
                    if let Some(segment) = self.inner.find_segment(self.stream_id, self.offset()) {
                        let (begin, end) = segment.get_stream_range(self.stream_id).unwrap();
                        log::debug!(
                            "Stream ID {} offset {} found in Segment file {} at offset {} [{}, {})",
                            self.stream_id,
                            self.offset(),
                            segment.filename().display(),
                            self.offset(),
                            begin,
                            end
                        );
                        self.read_segment = Some(Arc::downgrade(&segment));
                        *self.read_state.lock().unwrap() = StreamReadState::Segment;
                        continue;
                    } else {
                        log::debug!(
                            "Segment file for Stream ID {} offset {} is no longer available",
                            self.stream_id,
                            self.offset()
                        );
                    }

                    let (begin, end) =
                        self.inner.get_stream_range(self.stream_id).map_err(|e| {
                            log::error!("Failed to get stream range: {:?}", e);
                            io::Error::new(io::ErrorKind::Other, "Failed to get stream range")
                        })?;

                    log::info!(
                        "Stream ID {} offset {} not found in any MemTable or Segment, range: [{}, {}]",
                        self.stream_id,
                        self.offset(),
                        begin,
                        end
                    );

                    // If we reach here, we have no data to read
                    return Ok(0); // No data to read
                }
                StreamReadState::MemTable => {
                    let memtable = self.inner.table.load();

                    let bytes_read = memtable.read_stream(
                        self.stream_id,
                        self.offset(),
                        &mut buf[read_bytes_all..],
                    )?;
                    self.offset
                        .fetch_add(bytes_read as u64, std::sync::atomic::Ordering::Relaxed);
                    read_bytes_all += bytes_read;
                    return Ok(read_bytes_all); // Stop if we filled the buffer
                }
                StreamReadState::MemTables => {
                    // If we read less than the buffer size, we may need to check other MemTables
                    let bytes_read = self.read_from_tables(&mut buf[read_bytes_all..])?;
                    read_bytes_all += bytes_read;
                    if read_bytes_all >= buf.len() {
                        return Ok(read_bytes_all); // Stop if we filled the buffer
                    }
                }
                StreamReadState::Segment => {
                    let bytes_read = self.read_from_segments(&mut buf[read_bytes_all..])?;
                    read_bytes_all += bytes_read;
                    if read_bytes_all >= buf.len() {
                        return Ok(read_bytes_all); // Stop if we filled the buffer
                    }
                }
            }
        }
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
