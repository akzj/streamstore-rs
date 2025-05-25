use std::sync::{
    Arc, Mutex,
    atomic::{self, AtomicU64},
    mpsc::{Receiver, SyncSender, sync_channel},
};

use anyhow::Result;
use arc_swap::ArcSwap;

use crate::{
    entry::{AppendEntryCallback, DataType, Entry},
    error::Error,
    mem_table::{MemTable, MemTableArc},
    options::{self, Options},
    reader::StreamReader,
    reload::{self, reload_segments},
    segments::{Segment, generate_segment},
    wal::Wal,
};

type SegmentArc = Arc<Segment>;

pub struct StreamStoreInner {
    // segment files
    wal: Wal,
    config: Options,
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
    pub fn append(
        &self,
        stream_id: u64,
        data: DataType,
        callback: Option<Box<AppendEntryCallback>>,
    ) -> Result<()> {
        let id = self
            .entry_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        self.wal.write(Entry {
            version: 1,
            id: id,
            stream_id,
            data,
            callback: callback,
        })
    }

    pub fn read(&mut self, stream_id: u64, offset: u64, size: u64) -> Result<DataType, Error> {
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

    pub fn get_stream_range(&mut self, stream_id: u64) -> Option<(u64, u64)> {
        let mut begin = self
            .segment_files
            .lock()
            .unwrap()
            .iter()
            .find_map(|segment| match segment.get_stream_range(stream_id) {
                Some((begin, _end)) => return Some(begin),
                None => return None,
            });

        if begin.is_none() {
            begin = self.tables.lock().unwrap().iter().find_map(|table| {
                match table.get_stream_range(stream_id) {
                    Some((begin, _end)) => return Some(begin),
                    None => return None,
                }
            });
        }

        if begin.is_none() {
            begin = match self.table.load().get_stream_range(stream_id) {
                Some((begin, _end)) => Some(begin),
                None => None,
            };
        }

        if begin.is_none() {
            return None;
        }

        // reverse the order
        let mut end = match self.table.load().get_stream_range(stream_id) {
            Some((_begin, end)) => Some(end),
            None => None,
        };

        if end.is_none() {
            end = self.tables.lock().unwrap().iter().find_map(|table| {
                match table.get_stream_range(stream_id) {
                    Some((_begin, end)) => return Some(end),
                    None => return None,
                }
            });
        }

        if end.is_none() {
            end = self
                .segment_files
                .lock()
                .unwrap()
                .iter()
                .find_map(|segment| match segment.get_stream_range(stream_id) {
                    Some((_begin, end)) => return Some(end),
                    None => return None,
                });
        }

        assert!(
            begin.is_some() && end.is_some(),
            "Stream range not found for stream_id: {}",
            stream_id
        );

        Some((begin.unwrap(), end.unwrap()))
    }

    fn get_last_segment_entry_index(&self) -> Result<u64, Error> {
        let segment_files = self.segment_files.lock().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.last().unwrap();
        Ok(last_segment.entry_index().1)
    }

    fn run_segment_generater(&self) -> Result<(), Error> {
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
            if self.tables.lock().unwrap().len() > self.config.max_tables_count as usize {
                self.tables.lock().unwrap().remove(0);
            }
        }
        Ok(())
    }

    fn run(&self) -> () {
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
                if table.get_size() > self.config.max_table_size {
                    self.tables.lock().unwrap().push(table.clone());
                    self.table.store(Arc::new(MemTable::new()));
                    let filename = std::path::Path::new(&self.config.segment_path)
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

    fn start(&self) -> () {
        self.wal.start();

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
    }

    pub fn reload(config: &Options) -> Result<Self> {
        let (entries_sender, entries_receiver) = sync_channel::<Vec<Entry>>(100);

        let mut last_segment_entry_index = 0;
        let segment_files = reload_segments(&config.segment_path)?;
        if !segment_files.is_empty() {
            last_segment_entry_index = segment_files.last().unwrap().entry_index().1;
        }

        let (memtable, file) = reload::reload_wals(
            &config.wal_path,
            last_segment_entry_index,
            config.max_table_size,
        )?;

        let last_log_entry = memtable.get_last_entry();
        let wal = Wal::new(
            file,
            config.wal_path.clone(),
            config.max_wal_size,
            last_log_entry,
            entries_sender,
            Box::new(|err| {
                // handle error
                println!("Error: {}", err);
            }),
        );

        log::info!("last log entry: {}", last_log_entry);

        let (write_segment_sender, write_segment_receiver) =
            sync_channel::<(String, MemTableArc)>(10);

        let storeinner = StreamStoreInner {
            wal: wal,
            config: config.clone(),
            entry_index: AtomicU64::new(last_log_entry + 1),
            table: ArcSwap::new(Arc::new(memtable)),
            tables: Mutex::new(Vec::new()),
            segment_files: Mutex::new(segment_files),
            entry_receiver: Mutex::new(entries_receiver),
            write_segment_sender: Arc::new(write_segment_sender),
            write_segment_receiver: Mutex::new(write_segment_receiver),
            is_stop: atomic::AtomicBool::new(false),
        };

        // reload the segments
        let store = Store(Arc::new(storeinner));

        // start background thread
        store.start();

        Ok(store)
    }
}
