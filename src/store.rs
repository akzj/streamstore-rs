use std::{
    collections::HashMap,
    path,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicU64},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

use anyhow::Result;
use arc_swap::ArcSwap;

use crate::{
    entry::{AppendEntryResultFn, DataType, Entry},
    errors::{self, Error, new_stream_not_found, new_stream_offset_invalid},
    mem_table::{GetStreamOffsetFn, MemTable, MemTableArc},
    options::Options,
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
    mem_tables: Mutex<Vec<MemTableArc>>,
    segment_files: Mutex<Vec<SegmentArc>>,
    offsets: Arc<Mutex<HashMap<u64, u64>>>,
    entry_receiver: Mutex<Receiver<Vec<Entry>>>,
    write_segment_sender: Arc<SyncSender<(path::PathBuf, MemTableArc)>>,
    write_segment_receiver: Mutex<Receiver<(path::PathBuf, MemTableArc)>>,
    is_readonly: Arc<atomic::AtomicBool>,
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
        callback: Option<AppendEntryResultFn>,
    ) -> Result<()> {
        // Check if the store is read-only
        if self.is_readonly.load(atomic::Ordering::SeqCst) {
            return Err(errors::new_store_is_read_only());
        }
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

    pub fn read(&self, stream_id: u64, offset: u64, size: u64) -> Result<DataType> {
        let table = self.table.load();
        match table.get_stream_range(stream_id) {
            Some((start, _end)) => {
                if offset >= start {
                    return table.read_stream_data(stream_id, offset - start, size);
                } else {
                    return Err(new_stream_offset_invalid(stream_id, offset));
                }
            }
            None => {
                // If not found in the main table, check the memtables
                for mem_table in self.mem_tables.lock().unwrap().iter() {
                    match mem_table.get_stream_range(stream_id) {
                        Some((start, _end)) => {
                            if offset >= start {
                                return mem_table.read_stream_data(stream_id, offset - start, size);
                            } else {
                                return Err(new_stream_offset_invalid(stream_id, offset));
                            }
                        }
                        None => continue,
                    }
                }
            },
        };
        Err(new_stream_not_found(stream_id))
    }

    fn get_reader(&mut self, stream_id: u64) -> Result<Box<dyn StreamReader>, Error> {
        todo!()
    }

    pub fn get_stream_end(&self, stream_id: u64) -> Result<u64> {
        // reverse the order
        let mut end = match self.table.load().get_stream_range(stream_id) {
            Some((_begin, end)) => Some(end),
            None => None,
        };

        if end.is_none() {
            end = self.mem_tables.lock().unwrap().iter().find_map(|table| {
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
        if end.is_none() {
            return Err(new_stream_not_found(stream_id));
        }
        return Ok(end.unwrap());
    }

    pub fn get_stream_begin(&self, stream_id: u64) -> Result<u64> {
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
            begin = self.mem_tables.lock().unwrap().iter().find_map(|table| {
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
            return Err(new_stream_not_found(stream_id));
        }
        Ok(begin.unwrap())
    }

    pub fn get_stream_range(&mut self, stream_id: u64) -> Result<(u64, u64)> {
        let res = self.get_stream_begin(stream_id);
        match res {
            Ok(begin) => match self.get_stream_end(stream_id) {
                Ok(end) => Ok((begin, end)),
                Err(e) => return Err(e),
            },
            Err(e) => Err(e),
        }
    }

    fn get_last_segment_entry_index(&self) -> Result<u64> {
        let segment_files = self.segment_files.lock().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.last().unwrap();
        Ok(last_segment.entry_index().1)
    }

    fn reload_offset(&self) {
        // reload the segments
        for segment in self.segment_files.lock().unwrap().iter() {
            for stream_header in segment.get_stream_headers() {
                let stream_id = stream_header.stream_id;
                let offset = stream_header.offset + stream_header.size;
                self.offsets.lock().unwrap().insert(stream_id, offset);
                log::info!(
                    "Reloaded stream {} with offset {} from segment {} ",
                    stream_id,
                    offset,
                    segment.filename().display()
                );
            }
        }

        // reload the memtable
        for mem_table in self.mem_tables.lock().unwrap().iter() {
            let stream_tables = mem_table.get_stream_tables();
            for (stream_id, stream_table) in &*stream_tables {
                match stream_table.get_stream_range() {
                    Some((_begin, end)) => {
                        self.offsets.lock().unwrap().insert(stream_id.clone(), end);
                        log::info!(
                            "Reloaded stream {} with offset {} from memtable",
                            stream_id,
                            end
                        );
                    }
                    None => {
                        log::warn!("Stream {} not found in memtable", stream_id);
                    }
                }
            }
        }

        //reload from the memtable
        let memtable = self.table.load();
        let stream_tables = memtable.get_stream_tables();
        for (stream_id, stream_table) in &*stream_tables {
            match stream_table.get_stream_range() {
                Some((_begin, end)) => {
                    self.offsets.lock().unwrap().insert(stream_id.clone(), end);
                    log::info!(
                        "Reloaded stream {} with offset {} from memtable",
                        stream_id,
                        end
                    );
                }
                None => {
                    log::warn!("Stream {} not found in memtable", stream_id);
                }
            }
        }
    }

    fn run_segment_generater(&self) -> Result<()> {
        loop {
            if self.is_readonly.load(atomic::Ordering::SeqCst) {
                log::info!("Stop segment generator");
                break;
            }

            let write_segment_receiver = self.write_segment_receiver.lock().unwrap();
            let (file_name, table) = write_segment_receiver.recv().unwrap();
            match generate_segment(&file_name, &table) {
                Ok(_) => {
                    log::info!("Segment generated: {}", file_name.display());
                    match self.wal.gc(table.get_last_entry()) {
                        Ok(_) => {
                            log::info!(
                                "WAL garbage collection completed for segment: {}",
                                file_name.display()
                            );
                        }
                        Err(e) => {
                            log::error!("Failed to garbage collect WAL: {:?}", e);
                            // If WAL garbage collection fails, set the store to readonly
                            self.is_readonly.store(true, atomic::Ordering::SeqCst);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    // If segment generation fails, set the store to readonly
                    self.is_readonly.store(true, atomic::Ordering::SeqCst);
                    log::error!(
                        "Failed to generate segment {}: {:?}",
                        file_name.display(),
                        e
                    );
                    return Err(e);
                }
            }
            // update segment list
            let segment = Segment::open(file_name).unwrap();

            let mut segment_files_guard = self.segment_files.lock().unwrap();
            segment_files_guard.push(Arc::new(segment));
            if self.mem_tables.lock().unwrap().len() > self.config.max_tables_count as usize {
                self.mem_tables.lock().unwrap().remove(0);
            }
        }
        Ok(())
    }

    fn run(&self) -> () {
        let get_stream_offset_fn: GetStreamOffsetFn = Arc::new(Box::new({
            let offsets = self.offsets.clone();
            move |stream_id| match offsets.lock().unwrap().get(&stream_id) {
                Some(offset) => Ok(*offset),
                None => Ok(0), // Default to 0 if not found
            }
        }));

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
                match table.append(&entry) {
                    Ok(offset) => {
                        self.offsets.lock().unwrap().insert(entry.stream_id, offset);
                        match entry.callback {
                            Some(callback) => {
                                callback(Ok(offset));
                            }
                            None => {}
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to append entry: {:?}", e);
                        return;
                    }
                }

                // Check if the table size is greater than the max size
                if table.get_size() > self.config.max_table_size {
                    self.mem_tables.lock().unwrap().push(table.clone());
                    self.table
                        .store(Arc::new(MemTable::new(&get_stream_offset_fn.clone())));

                    let filename = std::path::Path::new(&self.config.segment_path)
                        .join(format!("{}.seg", table.get_first_entry()));
                    // notify to create a new segment
                    self.write_segment_sender
                        .send((filename, table.clone()))
                        .unwrap();
                    // Reset the memory table
                }
            }
        }
    }

    pub fn reload(config: &Options) -> Result<Self> {
        let offset_map = Arc::new(Mutex::new(HashMap::new()));

        let get_stream_offset_fn: GetStreamOffsetFn = Arc::new(Box::new({
            let offset_map = offset_map.clone();
            move |stream_id| match offset_map.lock().unwrap().get(&stream_id) {
                Some(offset) => Ok(*offset),
                None => Ok(0), // Default to 0 if not found
            }
        }));

        let (entries_sender, entries_receiver) = sync_channel::<Vec<Entry>>(100);

        let mut last_segment_entry_index = 0;
        let mut segment_files = reload_segments(&config.segment_path)?;
        if !segment_files.is_empty() {
            last_segment_entry_index = segment_files.last().unwrap().entry_index().1;
        }

        let (mut memtables, files, file) = reload::reload_wals(
            &config.wal_path,
            last_segment_entry_index,
            config.max_table_size,
            get_stream_offset_fn,
        )?;
        let memtable = memtables.pop().unwrap();

        // generate the segment files from the memtable
        for table in memtables {
            let filename = std::path::Path::new(&config.segment_path)
                .join(format!("{}.seg", table.get_first_entry()));

            generate_segment(&filename, &table)?;
            let segment = Segment::open(filename)?;
            segment_files.push(Arc::new(segment));
        }

        let is_readonly = Arc::new(atomic::AtomicBool::new(false));
        let last_log_entry = memtable.get_last_entry();

        let wal = Wal::new(
            file,
            config.wal_path.clone(),
            config.max_wal_size,
            last_log_entry,
            entries_sender,
            files,
            {
                let is_readonly = is_readonly.clone();
                Box::new(move |err| {
                    // handle error
                    log::error!("WAL error: {}", err);
                    // stop the store
                    is_readonly.store(true, atomic::Ordering::SeqCst);
                })
            },
        );

        log::info!("last log entry: {}", last_log_entry);

        let (write_segment_sender, write_segment_receiver) =
            sync_channel::<(path::PathBuf, MemTableArc)>(10);

        // unwrap the Rc to get the inner memtable
        let memtable = Rc::into_inner(memtable).unwrap();

        let inner = StreamStoreInner {
            wal: wal,
            config: config.clone(),
            entry_index: AtomicU64::new(last_log_entry + 1),
            table: ArcSwap::new(Arc::new(memtable)),
            mem_tables: Mutex::new(Vec::new()),
            segment_files: Mutex::new(segment_files),
            entry_receiver: Mutex::new(entries_receiver),
            write_segment_sender: Arc::new(write_segment_sender),
            write_segment_receiver: Mutex::new(write_segment_receiver),
            offsets: offset_map.clone(),
            is_readonly: is_readonly.clone(),
        };

        let store = Store(Arc::new(inner));

        store.reload_offset();
        // start background thread
        store.start();

        Ok(store)
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
}
