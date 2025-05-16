use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
    sync::{
        Arc, Mutex,
        atomic::AtomicU64,
        mpsc::{Receiver, sync_channel},
    },
    u8, vec,
};

use arc_swap::ArcSwap;

use crate::{
    error::Error,
    segments::{Segment, generate_segment},
    table::{MemTable, MemTableArc},
    wal::{Wal, WalInner},
};

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
type SegmentArc = Arc<Segment>;

struct StreamStoreInner {
    // segment files
    segment_files: Mutex<Vec<SegmentArc>>,
    table: ArcSwap<MemTable>,
    tables: Mutex<Vec<MemTableArc>>,
    entry_receiver: Mutex<Receiver<Vec<Entry>>>,
}

pub struct StreamStore {
    wal: Wal,
    options: Options,
    entry_index: AtomicU64,
    inner: Arc<StreamStoreInner>,
}

#[derive(Clone)]
pub struct Options {
    wal_path: String,
    segment_path: String,
    max_table_size: u64,
    max_wal_size: u64,
}

impl StreamStore {
    pub fn open(options: Options) -> Result<Self, Error> {
        todo!()
    }

    fn reload_segments(&mut self) -> Result<(), Error> {
        // read segment file from dir
        let mut segment_files = vec![];
        for entry in std::fs::read_dir(&self.options.segment_path).map_err(Error::new_io_error)? {
            let entry = entry.map_err(Error::new_io_error)?;
            if !entry.file_type().map_err(Error::new_io_error)?.is_file() {
                continue;
            }
            //segment path join file name
            if let Some(filename) = std::path::Path::new(&self.options.segment_path)
                .join(entry.file_name())
                .to_str()
            {
                // check if file name is valid
                if !filename.ends_with(".segment") {
                    println!("Invalid segment file name: {}", filename);
                    continue;
                }

                let segment = Segment::open(filename)?;
                segment_files.push(std::sync::Arc::new(segment));
            }
        }

        segment_files
            .sort_by(|a, b| -> std::cmp::Ordering { a.entry_index().0.cmp(&b.entry_index().0) });

        self.inner.segment_files.lock().unwrap().clear();
        self.inner
            .segment_files
            .lock()
            .unwrap()
            .extend(segment_files);

        // reload segment files
        Ok(())
    }

    fn get_last_segment_entry_index(&self) -> Result<u64, Error> {
        let segment_files = self.inner.segment_files.lock().unwrap();
        if segment_files.is_empty() {
            return Ok(0);
        }
        let last_segment = segment_files.last().unwrap();
        Ok(last_segment.entry_index().1)
    }

    pub fn reload_wal(&mut self) -> Result<(), Error> {
        use crate::wal::Decoder;

        let mut wals = vec![];

        // read file from wal dir
        for entry in std::fs::read_dir(&self.options.wal_path).map_err(Error::new_io_error)? {
            let entry = entry.map_err(Error::new_io_error)?;
            if !entry.file_type().map_err(Error::new_io_error)?.is_file() {
                continue;
            }
            //segment path join file name
            if let Some(filename) = std::path::Path::new(&self.options.wal_path)
                .join(entry.file_name())
                .to_str()
            {
                // check if file name is valid
                if !filename.ends_with(".wal") {
                    println!("Invalid wal file name: {}", filename);
                    continue;
                }

                // Open the WAL file
                let mut file = File::open(filename).map_err(Error::new_io_error)?;
                let mut entry_index = 0;
                // Decode the entries from the WAL file
                file.decode(Box::new(|entry| {
                    // Handle the entry
                    entry_index = entry.id;
                    Ok(false)
                }))?;

                wals.push((filename.to_string(), entry_index));
            }
        }
        wals.sort_by(|a, b| a.1.cmp(&b.1));

        let last_segment_entry_index = self.get_last_segment_entry_index()?;
        let mut table = MemTable::new();
        // Reload the WAL files
        for (filename, _entry_index) in &wals {
            let mut file = File::open(&filename).map_err(Error::new_io_error)?;
            file.decode(Box::new(|entry| {
                // Handle the entry
                if entry.id < last_segment_entry_index {
                    return Ok(true);
                }
                table.append(&entry)?;

                // check table size > max_table_size
                if table.get_size() > self.options.max_table_size {
                    // create new segment
                    let segment_file_name = format!("{}.segment", table.get_first_entry());
                    generate_segment(&table, &segment_file_name)?;
                    table = MemTable::new();
                }
                self.entry_index
                    .store(entry.id, std::sync::atomic::Ordering::SeqCst);

                Ok(true)
            }))?;
        }

        //self.table.lock().unwrap().box

        let file_name = if wals.is_empty() {
            std::path::Path::new(&self.options.wal_path).join(format!(
                "{}.wal",
                self.entry_index.load(std::sync::atomic::Ordering::SeqCst)
            ))
        } else {
            std::path::Path::new(&wals.last().unwrap().0).to_path_buf()
        };

        // open wal for writing
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_name)
            .map_err(Error::new_io_error)?;

        // seek to the end of the file
        file.seek(SeekFrom::End(0)).map_err(Error::new_io_error)?;

        let wal_inner = WalInner::new(
            file,
            self.options.max_wal_size,
            self.options.wal_path.clone(),
        );

        let mut wal = Wal::new(Box::new(|err| {}));

        let (sender, receive) = sync_channel(1000);

        *self.inner.entry_receiver.lock().unwrap() = receive;

        wal.start(wal_inner, sender)?;

        Ok(())
    }

    pub fn start(&mut self) -> Result<(), Error> {
        // reload the segments
        self.reload_segments()?;

        // start the wal
        self.reload_wal()?;

        let options = self.options.clone();

        let inner = self.inner.clone();

        std::thread::spawn(move || {
            let receiver = inner.entry_receiver.lock().unwrap();
            loop {
                let entries = receiver.recv().unwrap();
                for entry in entries {
                    let table = inner.table.load();

                    // Append the memory table
                    table.append(&entry).unwrap();

                    // Check if the table size is greater than the max size
                    if table.get_size() > options.max_table_size {
                        // Create a new segment
                        inner.tables.lock().unwrap().push(table.clone());

                        let segment_file_name = format!("{}.segment", table.get_first_entry());

                        let segment_file_name =
                            std::path::Path::new(&options.segment_path).join(segment_file_name);

                        std::thread::spawn(move || {
                            generate_segment(table.as_ref(), segment_file_name.to_str().unwrap())
                                .unwrap();
                        });

                        // Reset the memory table
                        inner.table.store(Arc::new(MemTable::new()));
                    }
                }
            }
        });

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
