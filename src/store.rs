use std::{
    collections::{HashMap, VecDeque},
    ops::ControlFlow,
    path,
    rc::Rc,
    sync::{
        Arc, Condvar, Mutex, RwLock, Weak,
        atomic::{self, AtomicU64},
        mpsc::{Receiver, SyncSender, sync_channel},
    },
};

use anyhow::Result;
use arc_swap::ArcSwap;

use crate::{
    entry::{AppendEntryResultFn, DataType, Entry},
    errors::{self, new_stream_not_found},
    futures::AppendFuture,
    mem_table::{GetStreamOffsetFn, MemTable, MemTableArc},
    metrics::{self},
    options::Options,
    reader::StreamReader,
    reload::{self, reload_segments},
    segments::{Segment, generate_segment, merge_segments},
    wal::{Wal, WalInner},
};

pub(crate) type SegmentArc = Arc<Segment>;
pub(crate) type SegmentWeak = Weak<Segment>;

pub struct StreamStoreInner {
    // segment files
    wal_inner: Arc<WalInner>,
    config: Options,
    entry_index: AtomicU64,
    entry_receiver: Mutex<Receiver<Vec<Entry>>>,

    pub(crate) table: ArcSwap<MemTable>,
    pub(crate) mem_tables: std::sync::RwLock<Vec<MemTableArc>>,
    pub(crate) segment_files: RwLock<VecDeque<SegmentArc>>,
    pub(crate) offsets: Arc<Mutex<HashMap<u64, u64>>>,
    pub(crate) is_readonly: Arc<atomic::AtomicBool>,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<StreamStoreInner>,
    wal: Arc<Wal>,
}

impl std::ops::Deref for Store {
    type Target = StreamStoreInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl StreamStoreInner {
    // print segment files
    pub fn print_segment_files(&self) {
        let segment_files = self.segment_files.read().unwrap();
        log::debug!("Segment files:");
        for segment in segment_files.iter() {
            log::debug!("  - {}", segment.filename().display());
        }
    }

    pub(crate) fn find_segment(&self, stream_id: u64, offset: u64) -> Option<SegmentArc> {
        self.segment_files
            .read()
            .unwrap()
            .iter()
            .find(|segment| match segment.get_stream_range(stream_id) {
                Some((begin, end)) => begin <= offset && offset < end,
                None => false,
            })
            .cloned()
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

    fn memtable_writer(
        &self,
        write_segment_sender: SyncSender<(path::PathBuf, MemTableArc)>,
    ) -> () {
        let get_stream_offset_fn: GetStreamOffsetFn = Arc::new(Box::new({
            let offsets = self.offsets.clone();
            move |stream_id| match offsets.lock().unwrap().get(&stream_id) {
                Some(offset) => Ok(*offset),
                None => Ok(0), // Default to 0 if not found
            }
        }));

        loop {
            let entries = match self.entry_receiver.lock().unwrap().recv() {
                Ok(entries) => entries,
                Err(_) => {
                    log::info!("Entry receiver closed, exiting memtable writer");
                    self.is_readonly.store(true, atomic::Ordering::SeqCst);
                    break;
                }
            };

            for entry in entries {
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
                    write_segment_sender
                        .send((filename, table.clone()))
                        .unwrap();
                    // Reset the memory table
                }
            }
        }
    }

    fn run_segment_generater(
        &self,
        receiver: Receiver<(path::PathBuf, MemTableArc)>,
    ) -> Result<()> {
        loop {
            if self.is_readonly.load(atomic::Ordering::SeqCst) {
                log::info!("Stop segment generator");
                break;
            }
            let (file_name, table) = match receiver.recv() {
                Ok((file_name, table)) => (file_name, table),
                Err(_) => {
                    log::info!("Segment generator receiver closed, exiting segment generator");
                    return Ok(());
                }
            };
            match generate_segment(&file_name, &table) {
                Ok(_) => {
                    log::info!("Segment generated: {}", file_name.display());
                    match self.wal_inner.gc(table.get_last_entry()) {
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
            let segment = Arc::new(Segment::open(&file_name).unwrap());

            let mut segment_files_guard = self.segment_files.write().unwrap();
            segment_files_guard.push_back(segment.clone());

            let mut memtables = self.mem_tables.write().unwrap();
            if memtables.len() > self.config.max_tables_count as usize {
                memtables.remove(0);
            }
        }
        Ok(())
    }

    pub fn merge_segments(&self) -> Result<()> {
        for level in 0..self.config.max_segment_merge_level {
            loop {
                match self.merge_segments_with_level(level) {
                    Ok(true) => {
                        log::info!("Merged segments at level {}", level);
                    }
                    Ok(false) => {
                        // log::info!("No segments to merge at level {}", level);
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
        let to_merges = match self.segment_files.read().unwrap().iter().try_fold(
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
                // log::info!("No segments to merge at level {}", level);
                return Ok(false);
            }
        };

        let begin_ts = std::time::Instant::now();

        // Generate the new segment file name
        let file_name = std::path::Path::new(&self.config.segment_path).join(format!(
            "{}-{}.seg",
            to_merges.first().unwrap().get_segment_header().first_entry,
            to_merges.last().unwrap().get_segment_header().last_entry,
        ));

        let segment = match merge_segments(&file_name, &to_merges) {
            Ok(segment) => {
                log::info!(
                    "Merged {:?} segments into new segment: {}",
                    &to_merges
                        .iter()
                        .map(|s| (
                            s.filename().to_str().unwrap().to_string(),
                            s.get_segment_header().level
                        ))
                        .collect::<Vec<_>>(),
                    file_name.display()
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
        segment_files_guard.push_back(Arc::new(segment));

        // Remove the merged segments from the list
        for segment in to_merges {
            segment_files_guard.retain(|s| s.filename() != segment.filename());
            segment.set_drop_delete(true);
        }

        segment_files_guard
            .make_contiguous()
            .sort_by_key(|s| s.entry_index().1);

        log::info!(
            "Segment merge completed, new segment file: {} took {} ms",
            file_name.display(),
            begin_ts.elapsed().as_millis()
        );

        // Update the segment offset index
        return Ok(true);
    }

    // Start the segment generator thread
    fn run_segment_merger(&self, stop_cond: Arc<(Mutex<u64>, Condvar)>) -> Result<()> {
        loop {
            let result = stop_cond
                .1
                .wait_timeout(
                    stop_cond.0.lock().unwrap(),
                    std::time::Duration::from_secs(1),
                )
                .unwrap();
            if result.1.timed_out() == false {
                log::info!("Segment merger stopped by condition variable");
                break;
            }

            match self.merge_segments() {
                Ok(_) => {
                    // log::info!("Segment merge completed successfully");
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

    pub async fn append_async(&self, stream_id: u64, data: DataType) -> Result<u64> {
        // Check if the store is read-only
        if self.is_readonly.load(atomic::Ordering::SeqCst) {
            return Err(errors::new_store_is_read_only());
        }
        let id = self
            .entry_index
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let f = AppendFuture::new();

        let result = self.wal.write(Entry {
            version: 1,
            id: id,
            stream_id,
            data,
            callback: Some(Box::new({
                let f = f.clone();
                move |result| {
                    f.set_result(result);
                }
            })),
        });
        if result.is_err() {
            return Err(result.err().unwrap());
        }
        // Wait for the future to complete
        f.await
    }

    pub fn new_stream_reader(&self, stream_id: u64) -> Result<StreamReader> {
        self.offsets.lock().unwrap().get(&stream_id).map_or_else(
            || Err(new_stream_not_found(stream_id)),
            |_offset| Ok(StreamReader::new(self.inner.clone(), stream_id)),
        )
    }

    pub fn get_stream_end(&self, stream_id: u64) -> Result<u64> {
        self.inner.get_stream_end(stream_id)
    }

    pub fn get_stream_begin(&self, stream_id: u64) -> Result<u64> {
        self.inner.get_stream_begin(stream_id)
    }

    pub fn get_stream_range(&self, stream_id: u64) -> Result<(u64, u64)> {
        self.inner.get_stream_range(stream_id)
    }

    #[allow(dead_code)]
    fn get_last_segment_entry_index(&self) -> Result<u64> {
        let segment_files = self.segment_files.read().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.back().unwrap();
        Ok(last_segment.entry_index().1)
    }

    pub fn reload(options: &Options) -> Result<Self> {
        let mut offset_map = Arc::new(Mutex::new(HashMap::new()));

        let get_stream_offset_fn: GetStreamOffsetFn = Arc::new(Box::new({
            let offset_map = offset_map.clone();
            move |stream_id| match offset_map.lock().unwrap().get(&stream_id) {
                Some(offset) => Ok(*offset),
                None => Ok(0), // Default to 0 if not found
            }
        }));

        let (entries_sender, entries_receiver) = sync_channel::<Vec<Entry>>(100);

        let mut last_segment_entry_index = 0;
        let mut segment_files = reload_segments(&options.segment_path, options.reload_check_crc)?;
        if !segment_files.is_empty() {
            last_segment_entry_index = segment_files.back().unwrap().entry_index().1;
        }

        // reload the offsets from the segment files
        log::info!("Reloading offsets from segment files");
        for segment in segment_files.iter() {
            for stream_header in segment.get_stream_headers() {
                let stream_id = stream_header.stream_id;
                let offset = stream_header.offset + stream_header.size;
                offset_map.lock().unwrap().insert(stream_id, offset);
                log::debug!(
                    "Reloaded stream {} with offset {} from segment {} ",
                    stream_id,
                    offset,
                    segment.filename().display()
                );
            }
        }

        let (mut mem_tables, files, file) = reload::reload_wals(
            &options.wal_path,
            last_segment_entry_index,
            options.max_table_size,
            get_stream_offset_fn,
        )?;

        // reload the offsets from the memtables
        log::info!("Reloading offsets from memtables");
        for mem_tables in mem_tables.iter() {
            let stream_tables = mem_tables.get_stream_tables();
            for (stream_id, stream_table) in &*stream_tables {
                match stream_table.get_stream_range() {
                    Some((_begin, end)) => {
                        offset_map.lock().unwrap().insert(stream_id.clone(), end);
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

        let mem_table = mem_tables.pop().unwrap();
        // generate the segment files from the memtable
        for table in mem_tables {
            let filename = std::path::Path::new(&options.segment_path)
                .join(format!("{}.seg", table.get_first_entry()));

            generate_segment(&filename, &table)?;
            let segment = Segment::open(&filename)?;
            segment_files.push_back(Arc::new(segment));
        }

        let is_readonly = Arc::new(atomic::AtomicBool::new(false));
        let last_log_entry = mem_table.get_last_entry();

        let wal = Wal::new(
            file,
            options.wal_path.clone(),
            options.max_wal_size,
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

        // unwrap the Rc to get the inner memtable
        let memtable = Rc::into_inner(mem_table).unwrap();

        let inner = StreamStoreInner {
            wal_inner: wal.clone_inner(),
            config: options.clone(),
            offsets: offset_map.clone(),
            is_readonly: is_readonly.clone(),
            entry_index: AtomicU64::new(last_log_entry + 1),
            table: ArcSwap::new(Arc::new(memtable)),
            mem_tables: RwLock::new(Vec::new()),
            segment_files: RwLock::new(segment_files),
            entry_receiver: Mutex::new(entries_receiver),
        };

        let store = Store {
            inner: Arc::new(inner),
            wal: Arc::new(wal),
        };

        // start background thread
        store.start();

        Ok(store)
    }

    pub fn print_metrics(&self) {
        println!("{}", metrics::encode_metrics());
    }

    pub fn get_metrics(&self) -> String {
        metrics::encode_metrics()
    }

    fn start(&self) -> () {
        self.wal.start();

        let (sender, receiver) = sync_channel::<(path::PathBuf, MemTableArc)>(10);
        let cond = Arc::new((Mutex::new(0 as u64), Condvar::new()));

        let _ = std::thread::Builder::new()
            .name("store::run".to_string())
            .spawn({
                let _self = self.inner.clone();
                let cond = cond.clone();
                move || {
                    _self.memtable_writer(sender);
                    cond.1.notify_all();
                }
            });

        let _ = std::thread::Builder::new()
            .name("run_segment_generater".to_string())
            .spawn({
                let _self = self.inner.clone();
                move || {
                    _self.run_segment_generater(receiver).unwrap();
                }
            });

        let _ = std::thread::Builder::new()
            .name("run_segment_merger".to_string())
            .spawn({
                let _self = self.inner.clone();
                move || {
                    _self.run_segment_merger(cond).unwrap();
                }
            });
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        log::info!("Dropping Store");
        // Set the store to read-only
        // ref count of Arc
    }
}
