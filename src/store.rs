use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicU64},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    u8, vec,
};

use arc_swap::ArcSwap;
use log::Log;

use crate::{
    config::Options,
    entry::{AppendEntryCallback, DataType, Entry},
    error::Error,
    mem_table::{MemTable, MemTableArc},
    reload::{self, reload_segments},
    segments::{Segment, generate_segment},
    wal::Wal,
};

pub trait StreamReader {
    fn read(&mut self, size: u64) -> Result<DataType, Error>;
    fn offset(&self) -> Result<u64, Error>;
    fn seek(&mut self, offset: u64) -> Result<(), Error>;
    fn seek_to_end(&mut self) -> Result<(), Error>;
    fn seek_to_begin(&mut self) -> Result<(), Error>;
    fn get_stream_range(&mut self) -> Result<(u64, u64), Error>;
    fn get_stream_id(&self) -> Result<u64, Error>;
}

type SegmentArc = Arc<Segment>;

pub struct StreamStoreInner {
    // segment files
    wal: Mutex<Wal>,
    options: Options,
    entry_index: AtomicU64,
    table: ArcSwap<MemTable>,
    tables: Mutex<Vec<MemTableArc>>,
    segment_files: Mutex<Vec<SegmentArc>>,
    entry_receiver: Mutex<Receiver<Vec<Entry>>>,
    write_segment_sender: Arc<SyncSender<(String, MemTableArc)>>,
    write_segment_receiver: Mutex<Receiver<(String, MemTableArc)>>,
    is_stop: atomic::AtomicBool,
}

#[derive(Clone)]
pub struct Store(Arc<StreamStoreInner>);

impl std::ops::Deref for Store {
    type Target = StreamStoreInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Store {
    fn append(
        &self,
        stream_id: u64,
        data: DataType,
        callback: Box<AppendEntryCallback>,
    ) -> Result<(), Error> {
        let id = self
            .entry_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.wal.lock().unwrap().write(Entry {
            version: 1,
            id: id,
            stream_id,
            data,
            callback: Some(callback),
        })
    }

    fn read(&mut self, stream_id: u64, offset: u64, size: u64) -> Result<DataType, Error> {
        let table = self.table.load();
        let stream_range = table.get_stream_range(stream_id);
        if let Some((_start, end)) = stream_range {
            if offset > end {
                return Err(Error::new_io_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Offset out of range",
                )));
            }
            let data = table.read_stream_data(stream_id, offset, size)?;
            return Ok(data);
        }

        Err(Error::new_io_error(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Stream not found",
        )))
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

    fn reload_segments(&mut self) -> Result<(), Error> {
        // read segment file from dir
        let mut segment_files = self.segment_files.lock().unwrap();
        *segment_files = reload_segments(&self.options.segment_path)?;
        // reload segment files
        Ok(())
    }

    fn get_last_segment_entry_index(&self) -> Result<u64, Error> {
        let segment_files = self.segment_files.lock().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.last().unwrap();
        Ok(last_segment.entry_index().1)
    }

    pub fn reload_wal(&mut self) -> Result<(), Error> {
        let (memtable, file) = reload::reload_wals(
            &self.options.wal_path,
            self.get_last_segment_entry_index()?,
            self.options.max_table_size,
        )?;

        self.table.store(memtable.into());

        let (sender, receiver) = sync_channel::<Vec<Entry>>(100);
        *self.wal.lock().unwrap() = Wal::new(
            file,
            self.options.wal_path.clone(),
            self.options.max_wal_size,
            sender,
            Box::new(|err| {
                // handle error
                println!("Error: {}", err);
            }),
        );

        *self.entry_receiver.lock().unwrap() = receiver;

        Ok(())
    }

    pub fn run_segment_generater(&self) -> Result<(), Error> {
        loop {
            if self.is_stop.load(atomic::Ordering::SeqCst) {
                log::info!("Stop segment generator");
                break;
            }

            let write_segment_receiver = self.write_segment_receiver.lock().unwrap();
            let (file_name, table) = write_segment_receiver.recv().unwrap();
            generate_segment(&file_name, &table).unwrap();
            // update segment list
            let segment = Segment::open(&file_name).unwrap();

            let mut segment_files_guard = self.segment_files.lock().unwrap();
            segment_files_guard.push(Arc::new(segment));
            if self.tables.lock().unwrap().len() > self.options.max_tables_count as usize {
                self.tables.lock().unwrap().remove(0);
            }
        }
        Ok(())
    }

    pub fn run(&self) -> () {
        loop {
            let entries = self.entry_receiver.lock().unwrap().recv();
            if entries.is_err() {
                log::info!(
                    "error {} happen commit entry loop exit",
                    entries.err().unwrap()
                );
                break;
            }

            for entry in entries.unwrap() {
                let table = self.table.load();
                // Append the memory table
                table.append(&entry).unwrap();

                // Check if the table size is greater than the max size
                if table.get_size() > self.options.max_table_size {
                    self.tables.lock().unwrap().push(table.clone());
                    self.table.store(Arc::new(MemTable::new()));
                    let filename = std::path::Path::new(&self.options.segment_path)
                        .join(format!("{}.segment", table.get_first_entry()));
                    // notify to create a new segment
                    self.write_segment_sender
                        .send((filename.to_str().unwrap().to_string(), table.clone()))
                        .unwrap();
                    // Reset the memory table
                }
            }
        }
    }

    pub fn reload(&mut self) -> Result<(), Error> {
        // reload the segments
        self.reload_segments()?;
        // start the wal
        self.reload_wal()?;

        // start the segment generator
        let _self = self.clone();
        let _ = std::thread::Builder::new()
            .name("store::run".to_string())
            .spawn(move || {
                _self.run();
            });
        let _self = self.clone();
        let _ = std::thread::Builder::new()
            .name("run_segment_generater".to_string())
            .spawn(move || {
                _self.run_segment_generater().unwrap();
            });

        Ok(())
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

impl Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("version", &self.version)
            .field("id", &self.id)
            .field("stream_id", &self.stream_id)
            .field("data", &self.data)
            .finish()
    }
}
