use std::{
    fs::{File, OpenOptions},
    io::{self, Seek},
    sync::Arc,
};

use crate::{
    entry::Decoder,
    error::Error,
    mem_table::MemTable,
    segments::{Segment, generate_segment},
};

pub fn reload_segments(segment_path: &str) -> Result<Vec<Arc<Segment>>, Error> {
    let mut segment_files = vec![];
    for entry in std::fs::read_dir(&segment_path).map_err(Error::new_io_error)? {
        let entry = entry.map_err(Error::new_io_error)?;
        if !entry.file_type().map_err(Error::new_io_error)?.is_file() {
            continue;
        }
        //segment path join file name
        if let Some(filename) = std::path::Path::new(&segment_path)
            .join(entry.file_name())
            .to_str()
        {
            // check if file name is valid
            if !filename.ends_with(".segment") {
                log::warn!("Invalid segment file name: {}", filename);
                continue;
            }

            let segment = Segment::open(filename)?;
            segment_files.push(std::sync::Arc::new(segment));
        }
    }

    segment_files
        .sort_by(|a, b| -> std::cmp::Ordering { a.entry_index().0.cmp(&b.entry_index().0) });

    Ok(segment_files)
}

fn list_wal_files(wal_path: &str) -> Result<Vec<(String, u64)>, Error> {
    let mut wals = vec![];

    // read file from wal dir
    for entry in std::fs::read_dir(&wal_path).map_err(Error::new_io_error)? {
        let entry = entry.map_err(Error::new_io_error)?;
        if !entry.file_type().map_err(Error::new_io_error)?.is_file() {
            continue;
        }
        //segment path join file name
        if let Some(filename) = std::path::Path::new(&wal_path)
            .join(entry.file_name())
            .to_str()
        {
            // check if file name is valid
            if !filename.ends_with(".wal") {
                // println!("Invalid wal file name: {}", filename);
                log::warn!("Invalid wal file name: {}", filename);
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
    Ok(wals)
}

pub fn reload_wals(
    wal_path: &str,
    last_segment_entry_index: u64,
    max_table_size: u64,
) -> Result<(MemTable, File), Error> {
    let wals = list_wal_files(wal_path)?;
    let mut entry_index = 0;
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
            if table.get_size() > max_table_size {
                // create new segment
                let segment_file_name = format!("{}.segment", table.get_first_entry());
                generate_segment(&segment_file_name, &table)?;
                table = MemTable::new();
            }
            entry_index = entry.id;

            Ok(true)
        }))?;
    }

    //self.table.lock().unwrap().box

    let file_name = if wals.is_empty() {
        std::path::Path::new(&wal_path).join(format!("{}.wal", entry_index + 1))
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
    file.seek(io::SeekFrom::End(0))
        .map_err(Error::new_io_error)?;

    // let wal_inner = WalInner::new(file, max_wal_size, wal_path.to_string());

    // let mut wal = Wal::new(Box::new(|err| {}));

    // let (sender, receive) = std::sync::mpsc::sync_channel(1000);

    // *self.inner.entry_receiver.lock().unwrap() = receive;

    // wal.start(wal_inner, sender)?;

    Ok((table, file))
}
