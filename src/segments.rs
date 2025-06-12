use crate::{errors, mem_table::MemTable, store::SegmentArc};
use anyhow::Result;
use crc::Crc;
use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{self, Write},
    path::{self},
    rc::Rc,
    sync::atomic,
};

const SEGMENT_STREAM_HEADER_SIZE: u64 = std::mem::size_of::<SegmentStreamHeader>() as u64;
const SEGMENT_HEADER_SIZE: u64 = std::mem::size_of::<SegmentHeader>() as u64;
const SEGMENT_STREAM_HEADER_VERSION_V1: u64 = 1;
const SEGMENT_HEADER_VERSION_V1: u32 = 1;

#[derive(Default, Debug, Clone)]
#[repr(C)]
pub struct SegmentStreamHeader {
    pub(crate) version: u64,
    // The stream id
    pub(crate) stream_id: u64,
    // The offset of the stream
    pub(crate) offset: u64,
    // The offset of the stream in the file
    pub(crate) file_offset: u64,
    // The size of the stream in the file
    pub(crate) size: u64,
    // checksum of the stream
    pub(crate) crc64: u64,
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct SegmentHeader {
    pub(crate) version: u32,
    pub(crate) level: u32,
    pub(crate) last_entry: u64,
    pub(crate) first_entry: u64,
    pub(crate) stream_headers_offset: u64,
    pub(crate) stream_headers_count: u64,
    _pading: [u8; 88], // Padding to ensure the size is 128 bytes
}

impl Default for SegmentHeader {
    fn default() -> Self {
        SegmentHeader {
            version: SEGMENT_HEADER_VERSION_V1,
            level: 0,
            last_entry: 0,
            first_entry: 0,
            stream_headers_offset: SEGMENT_HEADER_SIZE,
            stream_headers_count: 0,
            _pading: [0; 88],
        }
    }
}

impl Display for SegmentHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SegmentHeader {{ version: {}, level: {}, first_entry: {},last_entry: {}, stream_headers_count: {} }}",
            self.version, self.level, self.first_entry, self.last_entry, self.stream_headers_count
        )
    }
}

pub struct Segment {
    #[allow(dead_code)]
    pub filename: path::PathBuf,
    file: Option<File>,
    data: Option<memmap2::Mmap>,
    drop_delete: atomic::AtomicBool,
}

impl Segment {
    pub fn open(file_name: &path::PathBuf) -> Result<Segment> {
        let file = File::open(&file_name).map_err(errors::new_io_error)?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(errors::new_io_error)?;
        let segment = Segment {
            file: Some(file),
            data: Some(mmap),
            filename: file_name.clone(),
            drop_delete: atomic::AtomicBool::new(false),
        };
        Ok(segment)
    }

    pub fn check_crc(&self) -> Result<bool> {
        let header = self.get_segment_header();
        if header.version != SEGMENT_HEADER_VERSION_V1 {
            return Err(anyhow::anyhow!(
                "Invalid segment header version: {}",
                header.version
            ));
        }

        for stream_header in self.get_stream_headers() {
            let stream_data = self.stream_data(stream_header.stream_id);
            if let Some(data) = stream_data {
                let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
                let mut hash = crc64.digest();
                hash.update(data);
                if hash.finalize() != stream_header.crc64 {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn set_drop_delete(&self, drop_delete: bool) {
        self.drop_delete
            .store(drop_delete, atomic::Ordering::Relaxed);
    }

    pub fn entry_index(&self) -> (u64, u64) {
        let header = self.get_segment_header();
        (header.first_entry, header.last_entry)
    }

    pub fn filename(&self) -> path::PathBuf {
        self.filename.clone()
    }

    pub fn level(&self) -> u32 {
        self.get_segment_header().level
    }

    pub fn get_segment_header(&self) -> SegmentHeader {
        unsafe { &*(self.data() as *const SegmentHeader) }.clone()
    }

    pub fn get_stream_headers(&self) -> &[SegmentStreamHeader] {
        let header = self.get_segment_header();
        unsafe {
            std::slice::from_raw_parts(
                self.data().add(header.stream_headers_offset as usize)
                    as *const SegmentStreamHeader,
                header.stream_headers_count as usize,
            )
        }
    }

    pub fn get_stream_range(&self, stream_id: u64) -> Option<(u64, u64)> {
        let stream_header = self.find_stream_header(stream_id)?;
        let offset = stream_header.offset;
        let size = stream_header.size;
        Some((offset, offset + size))
    }

    pub fn find_stream_header(&self, stream_id: u64) -> Option<SegmentStreamHeader> {
        self.get_stream_headers()
            .binary_search_by(|b| {
                if b.stream_id < stream_id {
                    std::cmp::Ordering::Less
                } else if b.stream_id > stream_id {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
            .map(|index| self.get_stream_headers()[index].clone())
    }

    fn data(&self) -> *const u8 {
        self.data.as_ref().unwrap().as_ptr()
    }

    pub fn read_stream(&self, stream_id: u64, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let stream_header = self.find_stream_header(stream_id);
        return match stream_header {
            Some(stream_header) => {
                if stream_header.offset <= offset
                    && offset < stream_header.size + stream_header.offset
                {
                    let stream_data = unsafe {
                        std::slice::from_raw_parts(
                            self.data().add(stream_header.file_offset as usize) as *const u8,
                            stream_header.size as usize,
                        )
                    };

                    let start = (offset - stream_header.offset) as usize;
                    let end = (start + buf.len()).min(stream_data.len());

                    let data_to_copy = &stream_data[start..end];
                    let bytes_to_copy = data_to_copy.len();
                    if bytes_to_copy == 0 {
                        return Ok(0); // No data to copy
                    }
                    buf[..bytes_to_copy].copy_from_slice(data_to_copy);
                    Ok(bytes_to_copy)
                } else {
                    Ok(0)
                }
            }
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Stream ID {} not found", stream_id),
            )),
        };
    }

    pub fn stream_data(&self, stream_id: u64) -> Option<&[u8]> {
        let stream_header = self.find_stream_header(stream_id)?;
        let offset = stream_header.file_offset;
        let size = stream_header.size;

        let data = unsafe {
            std::slice::from_raw_parts(self.data().add(offset as usize) as *const u8, size as usize)
        };
        Some(data)
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.drop_delete.load(atomic::Ordering::Relaxed) {
            log::debug!("Deleting segment file: {}", self.filename.display());

            // Ensure the file and data are properly released
            self.data.take();
            self.file.take();

            match std::fs::remove_file(&self.filename) {
                Err(e) => {
                    log::warn!(
                        "Failed to delete segment file: {}. Error: {}",
                        self.filename.display(),
                        e
                    );
                }
                Ok(_) => {
                    log::info!("Segment file deleted: {}", self.filename.display());
                }
            }
        }
    }
}

unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

pub(crate) fn generate_segment(
    segment_file_path: &path::PathBuf,
    table: &MemTable,
) -> Result<Segment> {
    assert!(align_of::<SegmentHeader>() <= 8);

    let temp_file_path = segment_file_path.with_extension("tmp");
    let mut file = File::create(&temp_file_path).map_err(errors::new_io_error)?;

    let mut segment_stream_headers = Vec::new();

    // delete temp file if errors happen
    let temp_filename_clone = temp_file_path.clone();
    defer::defer!({
        // check if the file exists
        if std::fs::metadata(&temp_filename_clone).is_ok() {
            // delete the file
            if std::fs::remove_file(&temp_filename_clone).is_err() {
                log::warn!("Failed to delete temp file: {:?}", &temp_filename_clone);
            }
        }
    });

    table
        .get_stream_tables()
        .iter()
        .for_each(|(_, stream_table)| {
            let stream_header = SegmentStreamHeader {
                size: stream_table.size,
                crc64: stream_table.crc64(),
                offset: stream_table.offset,
                stream_id: stream_table.stream_id,
                ..Default::default()
            };
            segment_stream_headers.push(stream_header);
        });
    segment_stream_headers.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));

    let segment_header = SegmentHeader {
        first_entry: table.get_first_entry(),
        last_entry: table.get_last_entry(),
        stream_headers_count: segment_stream_headers.len() as u64,
        ..Default::default()
    };

    log::debug!(
        "Segment {} Header: first_entry: {}, last_entry: {}, stream_headers_count: {}",
        segment_file_path.display(),
        segment_header.first_entry,
        segment_header.last_entry,
        segment_header.stream_headers_count
    );

    // Write the segment stream headers to the file
    file.write_all(unsafe {
        std::slice::from_raw_parts(
            &segment_header as *const SegmentHeader as *const u8,
            SEGMENT_HEADER_SIZE as usize,
        )
    })
    .map_err(errors::new_io_error)?;

    let mut offset = SEGMENT_HEADER_SIZE as u64;
    offset += SEGMENT_STREAM_HEADER_SIZE as u64 * segment_stream_headers.len() as u64;

    // update the file offset
    for stream_header in segment_stream_headers.iter_mut() {
        stream_header.file_offset = offset;
        offset += stream_header.size;
    }

    let stream_header = segment_stream_headers.as_ptr() as *const SegmentStreamHeader;
    let data = unsafe {
        std::slice::from_raw_parts(
            stream_header as *const SegmentStreamHeader as *const u8,
            SEGMENT_STREAM_HEADER_SIZE as usize * segment_stream_headers.len() as usize,
        )
    };
    file.write_all(data).map_err(errors::new_io_error)?;

    // Verify that the segment stream headers are written correctly
    {
        let temp_stream_headers = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *const SegmentStreamHeader,
                segment_stream_headers.len(),
            )
        };
        assert!(temp_stream_headers.len() == segment_stream_headers.len());
        for (i, stream_header) in temp_stream_headers.iter().enumerate() {
            assert!(stream_header.stream_id == segment_stream_headers[i].stream_id);
            assert!(stream_header.file_offset == segment_stream_headers[i].file_offset);
            assert!(stream_header.size == segment_stream_headers[i].size);
            assert!(stream_header.offset == segment_stream_headers[i].offset);
            assert!(stream_header.version == segment_stream_headers[i].version);
            assert!(stream_header.crc64 == segment_stream_headers[i].crc64);
        }
    }

    // Write the stream data to the file
    for (_, stream_table) in table.get_stream_tables().iter() {
        for stream_data in stream_table.stream_datas.iter() {
            file.write_all(unsafe {
                std::slice::from_raw_parts(
                    stream_data.data.as_ptr() as *const u8,
                    stream_data.size() as usize,
                )
            })
            .map_err(errors::new_io_error)?;
        }
    }

    // flush the file to disk
    file.flush().map_err(errors::new_io_error)?;
    file.sync_all().map_err(errors::new_io_error)?;

    // close the file
    drop(file);

    // rename the file
    std::fs::rename(temp_file_path, segment_file_path).map_err(errors::new_io_error)?;

    Ok(Segment::open(segment_file_path)?)
}

pub(crate) fn merge_segments(
    segment_file_path: &path::PathBuf,
    segments: &[SegmentArc],
) -> Result<Segment> {
    assert!(!segments.is_empty(), "No segments to merge");

    assert!(align_of::<SegmentHeader>() <= 8);

    let begin = std::time::Instant::now();
    let temp_file_path = segment_file_path.with_extension("tmp");

    let mut file = File::create(&temp_file_path).map_err(errors::new_io_error)?;

    // delete temp file if errors happen
    let temp_filename_clone = temp_file_path.clone();
    defer::defer!({
        // check if the file exists
        if std::fs::metadata(&temp_filename_clone).is_ok() {
            // delete the file
            if std::fs::remove_file(&temp_filename_clone).is_err() {
                log::warn!("Failed to delete temp file: {:?}", &temp_filename_clone);
            }
        }
    });

    // Calculate the CRC64 checksum for the segment data
    // Use the Redis CRC64 algorithm

    let mut segment_data_map = HashMap::new();

    let mut header_map = HashMap::new();

    for segment in segments.iter() {
        for header in segment.get_stream_headers() {
            header_map
                .entry(&header.stream_id)
                .or_insert_with(|| SegmentStreamHeader {
                    version: SEGMENT_STREAM_HEADER_VERSION_V1,
                    offset: header.offset,
                    stream_id: header.stream_id,
                    ..Default::default()
                })
                .size += header.size;

            let entry = segment_data_map
                .entry(&header.stream_id)
                .or_insert_with(|| Vec::new());
            let data = segment.stream_data(header.stream_id).unwrap();
            entry.push(data);
        }
    }

    for (stream_id, datas) in segment_data_map.iter() {
        let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
        let mut hash = crc64.digest();
        for data in datas.iter() {
            hash.update(data);
        }
        header_map.get_mut(stream_id).unwrap().crc64 = hash.finalize();
    }

    let mut segment_stream_headers = header_map
        .into_iter()
        .map(|(_, header)| header)
        .collect::<Vec<SegmentStreamHeader>>();

    segment_stream_headers.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));

    let segment_header = SegmentHeader {
        level: segments[0].get_segment_header().level + 1,
        first_entry: segments[0].get_segment_header().first_entry,
        last_entry: segments.last().unwrap().get_segment_header().last_entry,
        stream_headers_count: segment_stream_headers.len() as u64,
        ..Default::default()
    };

    log::debug!(
        "Segment {} Header: first_entry: {}, last_entry: {}, stream_headers_count: {}",
        segment_file_path.display(),
        segment_header.first_entry,
        segment_header.last_entry,
        segment_header.stream_headers_count
    );

    // Write the segment stream headers to the file
    file.write_all(unsafe {
        std::slice::from_raw_parts(
            &segment_header as *const SegmentHeader as *const u8,
            SEGMENT_HEADER_SIZE as usize,
        )
    })
    .map_err(errors::new_io_error)?;

    let mut offset = SEGMENT_HEADER_SIZE as u64;
    offset += SEGMENT_STREAM_HEADER_SIZE as u64 * segment_stream_headers.len() as u64;

    // update the file offset
    for stream_header in segment_stream_headers.iter_mut() {
        stream_header.file_offset = offset;
        offset += stream_header.size;
    }

    let stream_header = segment_stream_headers.as_ptr() as *const SegmentStreamHeader;
    let data = unsafe {
        std::slice::from_raw_parts(
            stream_header as *const SegmentStreamHeader as *const u8,
            SEGMENT_STREAM_HEADER_SIZE as usize * segment_stream_headers.len() as usize,
        )
    };
    file.write_all(data).map_err(errors::new_io_error)?;

    // Verify that the segment stream headers are written correctly
    {
        let temp_stream_headers = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *const SegmentStreamHeader,
                segment_stream_headers.len(),
            )
        };
        assert!(temp_stream_headers.len() == segment_stream_headers.len());
        for (i, stream_header) in temp_stream_headers.iter().enumerate() {
            assert!(stream_header.stream_id == segment_stream_headers[i].stream_id);
            assert!(stream_header.file_offset == segment_stream_headers[i].file_offset);
            assert!(stream_header.size == segment_stream_headers[i].size);
            assert!(stream_header.offset == segment_stream_headers[i].offset);
            assert!(stream_header.version == segment_stream_headers[i].version);
            assert!(stream_header.crc64 == segment_stream_headers[i].crc64);
        }
    }

    for header in segment_stream_headers.iter() {
        for segment in segments.iter() {
            if let Some(stream_data) = segment.stream_data(header.stream_id) {
                file.write_all(stream_data).map_err(errors::new_io_error)?;
            }
        }
    }

    // flush the file to disk
    file.flush().map_err(errors::new_io_error)?;
    file.sync_all().map_err(errors::new_io_error)?;

    // close the file
    drop(file);

    // rename the file
    std::fs::rename(temp_file_path, &segment_file_path).map_err(errors::new_io_error)?;

    log::debug!(
        "Segment {} merged in {} ms",
        segment_file_path.display(),
        begin.elapsed().as_millis()
    );

    Ok(Segment::open(&segment_file_path)?)
}

#[test]
fn test_segment_header_size() {
    env_logger::init();
    assert_eq!(
        SEGMENT_HEADER_SIZE,
        std::mem::size_of::<SegmentHeader>() as u64
    );
    assert_eq!(
        SEGMENT_STREAM_HEADER_SIZE,
        std::mem::size_of::<SegmentStreamHeader>() as u64
    );

    let get_stream_offset: crate::mem_table::GetStreamOffsetFn =
        std::sync::Arc::new(Box::new(|_stream_id| Ok(0)));

    let memtable = MemTable::new(&get_stream_offset);

    let mut entry_id = 0;
    for i in 0..10 {
        for _j in 0..1000 {
            let stream_id = i + 1;
            let data = "hello world".as_bytes().to_vec();
            entry_id += 1;
            memtable
                .append(&crate::entry::Entry {
                    version: 1,
                    id: entry_id,
                    stream_id: stream_id,
                    data: data,
                    callback: None,
                })
                .unwrap();
        }
    }

    let segment_file_path = path::PathBuf::from("test_segment.bin");
    let segment = generate_segment(&segment_file_path, &memtable).unwrap();

    let seg_header = segment.get_segment_header();
    assert!(seg_header.version == SEGMENT_HEADER_VERSION_V1);
    assert!(seg_header.first_entry == 1);
    assert!(seg_header.last_entry == entry_id);
    assert!(seg_header.stream_headers_offset == SEGMENT_HEADER_SIZE);
    assert!(seg_header.stream_headers_count == 10);

    let mut file_offset =
        SEGMENT_HEADER_SIZE + SEGMENT_STREAM_HEADER_SIZE * seg_header.stream_headers_count;
    for (index, header) in segment.get_stream_headers().iter().enumerate() {
        assert!(header.version == SEGMENT_STREAM_HEADER_VERSION_V1);
        assert!(header.stream_id == index as u64 + 1);
        assert!(header.offset == 0);
        assert!(
            header.file_offset == file_offset,
            "Stream ID: {}, expected file_offset: {}, got: {}",
            header.stream_id,
            file_offset,
            header.file_offset
        );
        assert!(
            header.size == "hello world".as_bytes().len() as u64 * 1000,
            "header.size {}",
            header.size
        ); // 1000 entries * 10 bytes each
        assert!(header.crc64 == 0); // checksum is not calculated in this test

        file_offset += header.size;
    }
}
