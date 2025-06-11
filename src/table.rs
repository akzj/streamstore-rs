use std::io;

use anyhow::Result;

const STREAM_DATA_BUFFER_CAP: u64 = 128 << 10; // 128KB

pub struct StreamData {
    pub stream_id: u64,
    pub offset: u64,
    pub data: Vec<u8>,
}

impl StreamData {
    pub fn new(stream_id: u64, offset: u64, buffer_cap: u64) -> Self {
        StreamData {
            stream_id,
            offset,
            data: Vec::with_capacity(buffer_cap as usize),
        }
    }

    // Fill the buffer with data
    // If the buffer is full, return the remaining data
    // If the buffer is not full, return None
    pub fn fill<'a>(&mut self, data: &'a [u8]) -> Result<(usize, Option<&'a [u8]>)> {
        let available = self.cap_remaining().min(data.len());
        self.data.extend_from_slice(&data[..available as usize]);

        let remaining_data = if available < data.len() {
            Some(&data[available as usize..])
        } else {
            None
        };

        Ok((available, remaining_data))
    }

    pub fn get_stream_range(&self) -> Option<(u64, u64)> {
        if self.data.is_empty() {
            return None;
        }
        let start = self.offset;
        let end = self.offset + self.data.len() as u64;
        Some((start, end))
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn cap_remaining(&self) -> usize {
        STREAM_DATA_BUFFER_CAP as usize - self.data.len()
    }
}

pub struct StreamTable {
    pub stream_id: u64,
    pub offset: u64,
    pub size: u64,
    pub stream_datas: Vec<StreamData>,
}

impl StreamTable {
    pub fn new(stream_id: u64, offset: u64) -> Self {
        StreamTable {
            stream_id,
            offset: offset,
            size: 0,
            stream_datas: Vec::new(),
        }
    }

    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
        if self.stream_datas.is_empty() || self.stream_datas.last().unwrap().cap_remaining() == 0 {
            if !self.stream_datas.is_empty() {
                assert_eq!(
                    self.stream_datas.last().unwrap().size(),
                    STREAM_DATA_BUFFER_CAP
                );
            }

            self.stream_datas.push(StreamData::new(
                self.stream_id,
                self.offset + self.size,
                STREAM_DATA_BUFFER_CAP,
            ));
        }

        let stream_data = self.stream_datas.last_mut().unwrap();
        let (size, remain_buffer) = stream_data.fill(data)?;
        self.size += size as u64;

        // If the buffer is full, we need to create a new buffer
        if let Some(buffer) = remain_buffer {
            return self.append(buffer);
        }

        Ok(self.offset + self.size)
    }

    pub fn get_stream_range(&self) -> Option<(u64, u64)> {
        if self.stream_datas.is_empty() {
            return None;
        }
        return Some((self.offset, self.offset + self.size));
    }

    pub fn print_stream_meta(&self) {
        for (i, stream_data) in self.stream_datas.iter().enumerate() {
            assert!(stream_data.stream_id == self.stream_id);
            if i != self.stream_datas.len() - 1 {
                assert_eq!(stream_data.size(), STREAM_DATA_BUFFER_CAP);
            }
        }
    }

    pub fn crc64(&self) -> u64 {
        let crc64 = crc::Crc::<u64>::new(&crc::CRC_64_REDIS);
        let mut digest = crc64.digest();
        for stream_data in &self.stream_datas {
            digest.update(&stream_data.data);
        }
        digest.finalize()
    }

    pub fn read_stream(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.print_stream_meta();

        let mut offset = offset;
        let mut size = buf.len() as u64;
        let mut copied_size = 0;

        // find the first stream data that offset <= offset by quick search
        let res = self.stream_datas.binary_search_by(|stream_data| {
            let (_begin, end) = stream_data.get_stream_range().unwrap();
            end.cmp(&offset)
        });
        let mut index = match res {
            Ok(index) => index + 1, // we want the first stream data that starts after the offset
            Err(index) => index,
        };

        if index >= self.stream_datas.len() {
            log::debug!(
                "Offset {} find index {} is beyond the last stream data [{},{}), returning 0 bytes read",
                offset,
                index,
                self.stream_datas.last().unwrap().offset,
                self.stream_datas.last().unwrap().offset + self.stream_datas.last().unwrap().size()
            );
            return Ok(0);
        }
        // read the data from the stream data
        while index < self.stream_datas.len() && size > 0 {
            let stream_data = &self.stream_datas[index];
            let stream_data_offset = stream_data.offset;
            let stream_data_size = stream_data.size();

            assert!(
                stream_data_offset <= offset && offset <= stream_data_offset + stream_data_size
            );
            // we can read the data from this stream data

            let start = (offset - stream_data_offset) as usize;
            let end = (start + size as usize).min(stream_data_size as usize);

            // copy the data to the buffer
            let data_to_copy = &stream_data.data[start..end];
            let bytes_to_copy = data_to_copy.len();
            buf[copied_size as usize..(copied_size as u64 + bytes_to_copy as u64) as usize]
                .copy_from_slice(data_to_copy);

            copied_size += bytes_to_copy;
            size -= (bytes_to_copy) as u64;
            offset += bytes_to_copy as u64;
            index += 1;
        }

        Ok(copied_size)
    }
}

#[test]
fn test_stream_data_fill() {
    // set rust_log to use the environment variable RUST_LOG

    let mut table = StreamTable::new(1, 0);
    let count = 1000;
    let mut next_offset = 0;
    let crc64 = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);
    let mut digest = crc64.digest();
    for i in 0..count {
        let data = format!("hello world {}\n", i);
        digest.update(data.as_bytes());
        next_offset += data.len() as u64;
        let offset = table.append(data.as_bytes()).unwrap();
        assert_eq!(offset, next_offset);
    }

    let checksum = digest.finalize();
    log::debug!("Checksum: {}", checksum);

    let mut buf = vec![0u8; next_offset as usize];
    let read_size = table.read_stream(0, &mut buf).unwrap();
    assert_eq!(read_size, next_offset as usize);
    let read_checksum = crc64.checksum(&buf);
    log::debug!("Read Checksum: {}", read_checksum);
    assert_eq!(read_checksum, checksum);

    let mut read_bytes = Vec::new();
    loop {
        let mut buf = vec![0u8; rand::random::<u64>() as usize % 64 + 1];
        let read_size = table
            .read_stream(read_bytes.len() as u64, &mut buf)
            .unwrap();
        if read_size == 0 {
            break;
        }
        buf.truncate(read_size);
        read_bytes.extend_from_slice(buf.as_slice());
        if read_bytes.len() as u64 >= next_offset {
            break;
        }

        assert_eq!(read_size, buf.len() as usize);
    }

    let read_checksum = crc64.checksum(&read_bytes);
    log::debug!("Multi Read Checksum: {}", read_checksum);
    assert_eq!(read_bytes.len() as u64, next_offset);
    assert_eq!(read_checksum, checksum);
}
