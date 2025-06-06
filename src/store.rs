use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ops::ControlFlow,
    os::unix::thread,
    path,
    rc::Rc,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{self, AtomicU64},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::sleep,
};

use anyhow::Result;
use arc_swap::ArcSwap;

use crate::{
    entry::{AppendEntryResultFn, DataType, Entry},
    errors::{self, new_stream_not_found},
    mem_table::{GetStreamOffsetFn, MemTable, MemTableArc},
    options::Options,
    reader::StreamReader,
    reload::{self, reload_segments},
    segments::{Segment, generate_segment, merge_segments},
    wal::Wal,
};

pub(crate) type SegmentArc = Arc<Segment>;
pub(crate) type SegmentWeak = Weak<Segment>;

pub struct StreamStoreInner {
    // segment files
    wal: Wal,
    config: Options,
    entry_index: AtomicU64,
    write_segment_receiver: Mutex<Receiver<(path::PathBuf, MemTableArc)>>,
    entry_receiver: Mutex<Receiver<Vec<Entry>>>,
    write_segment_sender: Arc<SyncSender<(path::PathBuf, MemTableArc)>>,

    pub(crate) table: ArcSwap<MemTable>,
    pub(crate) mem_tables: std::sync::RwLock<Vec<MemTableArc>>,
    pub(crate) segment_files: RwLock<VecDeque<SegmentArc>>,
    pub(crate) offsets: Arc<Mutex<HashMap<u64, u64>>>,
    pub(crate) segment_offset_index:
        Arc<RwLock<HashMap<u64, Arc<RwLock<BTreeMap<u64, SegmentWeak>>>>>>,
    pub(crate) is_readonly: Arc<atomic::AtomicBool>,
}

#[derive(Clone)]
pub struct Store(Arc<StreamStoreInner>);

impl std::ops::Deref for Store {
    type Target = StreamStoreInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StreamStoreInner {
    // print segment files
    pub fn print_segment_files(&self) {
        let segment_files = self.segment_files.read().unwrap();
        log::info!("Segment files:");
        for segment in segment_files.iter() {
            log::info!("  - {}", segment.filename().display());
        }
    }

    pub fn get_stream_begin(&self, stream_id: u64) -> Result<u64> {
        let mut begin = self
            .segment_files
            .read()
            .unwrap()
            .iter()
            .find_map(|segment| match segment.get_stream_range(stream_id) {
                Some((begin, _end)) => return Some(begin),
                None => return None,
            });

        if begin.is_none() {
            begin = self.mem_tables.read().unwrap().iter().find_map(|table| {
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
    pub fn get_stream_end(&self, stream_id: u64) -> Result<u64> {
        // reverse the order
        let mut end = match self.table.load().get_stream_range(stream_id) {
            Some((_begin, end)) => Some(end),
            None => None,
        };

        if end.is_none() {
            end = self.mem_tables.read().unwrap().iter().find_map(|table| {
                match table.get_stream_range(stream_id) {
                    Some((_begin, end)) => return Some(end),
                    None => return None,
                }
            });
        }

        if end.is_none() {
            end = self
                .segment_files
                .read()
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

    pub fn get_stream_range(&self, stream_id: u64) -> Result<(u64, u64)> {
        let res = self.get_stream_begin(stream_id);
        match res {
            Ok(begin) => match self.get_stream_end(stream_id) {
                Ok(end) => Ok((begin, end)),
                Err(e) => return Err(e),
            },
            Err(e) => Err(e),
        }
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

    pub fn new_stream_reader(&self, stream_id: u64) -> Result<StreamReader> {
        self.offsets.lock().unwrap().get(&stream_id).map_or_else(
            || Err(new_stream_not_found(stream_id)),
            |_offset| Ok(StreamReader::new(self.0.clone(), stream_id)),
        )
    }

    pub fn get_stream_end(&self, stream_id: u64) -> Result<u64> {
        self.0.get_stream_end(stream_id)
    }

    pub fn get_stream_begin(&self, stream_id: u64) -> Result<u64> {
        self.0.get_stream_begin(stream_id)
    }

    pub fn get_stream_range(&self, stream_id: u64) -> Result<(u64, u64)> {
        self.0.get_stream_range(stream_id)
    }

    fn get_last_segment_entry_index(&self) -> Result<u64> {
        let segment_files = self.segment_files.read().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.back().unwrap();
        Ok(last_segment.entry_index().1)
    }

    fn reload_offset(&self) {
        // reload the segments
        for segment in self.segment_files.read().unwrap().iter() {
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
        for mem_tables in self.mem_tables.read().unwrap().iter() {
            let stream_tables = mem_tables.get_stream_tables();
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

    pub fn merge_segments(&self) -> Result<()> {
        for level in 0..self.config.max_segment_merge_level {
            loop {
                match self.merge_segments_with_level(level) {
                    Ok(true) => {
                        log::info!("Merged segments at level {}", level);
                    }
                    Ok(false) => {
                        log::info!("No segments to merge at level {}", level);
                        break; // No more segments to merge at this level
                    }
                    Err(e) => {
                        log::error!("Failed to merge segments at level {}: {:?}", level, e);
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn merge_segments_with_level(&self, level: u32) -> Result<bool> {
        let to_merge_segment = match self.segment_files.read().unwrap().iter().try_fold(
            Vec::new(),
            |mut acc, segment| {
                if segment.get_segment_header().level == level {
                    acc.push(segment.clone());
                    if acc.len() >= self.config.segment_merge_count as usize {
                        log::info!("Find to merge {} segments at level {}", acc.len(), level);
                        return ControlFlow::Break(acc);
                    }
                }
                ControlFlow::Continue(acc)
            },
        ) {
            ControlFlow::Break(segments) => segments,
            ControlFlow::Continue(_) => {
                log::info!("No segments to merge at level {}", level);
                return Ok(false);
            }
        };

        // Generate the new segment file name
        let new_segment_file_name = std::path::Path::new(&self.config.segment_path).join(format!(
            "{}-{}.seg",
            to_merge_segment
                .first()
                .unwrap()
                .get_segment_header()
                .first_entry,
            to_merge_segment
                .last()
                .unwrap()
                .get_segment_header()
                .last_entry,
        ));

        let segment_merged = match merge_segments(&new_segment_file_name, &to_merge_segment) {
            Ok(segment) => {
                log::info!(
                    "Merged {} segments into new segment: {}",
                    &to_merge_segment
                        .iter()
                        .map(|s| s.filename().to_str().unwrap().to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    new_segment_file_name.display()
                );
                segment
            }
            Err(e) => {
                log::error!("Failed to merge segments: {:?}", e);
                return Err(e);
            }
        };

        // Update the segment files list
        let mut segment_files_guard = self.segment_files.write().unwrap();
        segment_files_guard.push_back(Arc::new(segment_merged));

        // Remove the merged segments from the list
        for segment in to_merge_segment {
            segment_files_guard.retain(|s| s.filename() != segment.filename());
            segment.set_drop_delete(true);
        }

        segment_files_guard
            .make_contiguous()
            .sort_by_key(|s| s.entry_index().1);

        log::info!(
            "Segment merge completed, new segment file: {}",
            new_segment_file_name.display()
        );

        // Update the segment offset index
        return Ok(true);
    }

    fn run_segment_merger(&self) -> Result<()> {
        // Start the segment generator thread
        loop {
            if self.is_readonly.load(atomic::Ordering::SeqCst) {
                log::info!("Stop segment merger");
                break;
            }

            // Wait for the segment files to be ready
            sleep(std::time::Duration::from_secs(1));

            match self.merge_segments() {
                Ok(_) => {
                    log::info!("Segment merge completed successfully");
                }
                Err(e) => {
                    log::error!("Segment merge failed: {:?}", e);
                    // If segment merge fails, set the store to readonly
                    self.is_readonly.store(true, atomic::Ordering::SeqCst);
                    return Err(e);
                }
            }
        }
        Ok(())
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
            let segment = Arc::new(Segment::open(file_name).unwrap());

            let mut segment_files_guard = self.segment_files.write().unwrap();
            segment_files_guard.push_back(segment.clone());

            for stream_header in segment.get_stream_headers() {
                let stream_id = stream_header.stream_id;
                let offset = stream_header.offset + stream_header.size;
                self.segment_offset_index
                    .write()
                    .unwrap()
                    .entry(stream_id)
                    .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
                    .write()
                    .unwrap()
                    .insert(offset, Arc::downgrade(&segment));
            }

            let mut memtables = self.mem_tables.write().unwrap();
            if memtables.len() > self.config.max_tables_count as usize {
                memtables.remove(0);
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
                    self.mem_tables.write().unwrap().push(table.clone());
                    self.table
                        .store(Arc::new(MemTable::new(&get_stream_offset_fn.clone())));

                    let filename = std::path::Path::new(&self.config.segment_path).join(format!(
                        "{}-{}.seg",
                        table.get_first_entry(),
                        table.get_last_entry()
                    ));
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
            last_segment_entry_index = segment_files.back().unwrap().entry_index().1;
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
            segment_files.push_back(Arc::new(segment));
        }

        let mut segment_offset_index = HashMap::new();
        for segment in segment_files.iter() {
            log::info!("Reloaded segment file: {}", segment.get_segment_header());
            for stream_header in segment.get_stream_headers() {
                let stream_id = stream_header.stream_id;
                let offset = stream_header.offset + stream_header.size;
                segment_offset_index
                    .entry(stream_id)
                    .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
                    .write()
                    .unwrap()
                    .insert(offset, Arc::downgrade(segment));
            }
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
            offsets: offset_map.clone(),
            is_readonly: is_readonly.clone(),
            entry_index: AtomicU64::new(last_log_entry + 1),
            table: ArcSwap::new(Arc::new(memtable)),
            mem_tables: RwLock::new(Vec::new()),
            segment_files: RwLock::new(segment_files),
            entry_receiver: Mutex::new(entries_receiver),
            write_segment_sender: Arc::new(write_segment_sender),
            write_segment_receiver: Mutex::new(write_segment_receiver),
            segment_offset_index: Arc::new(RwLock::new(segment_offset_index)),
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
