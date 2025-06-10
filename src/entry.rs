use std::{
    fs::File,
    io::Read,
};

use anyhow::{Error, anyhow};

use crate::errors;
use anyhow::{Context, Result};

pub type AppendEntryResultFn = Box<dyn Fn(Result<u64>) -> () + Send + Sync>;
pub type DataType = Vec<u8>;

pub struct Entry {
    // auto increment id
    pub version: u8,
    pub id: u64,
    pub stream_id: u64,
    pub data: DataType,
    pub callback: Option<AppendEntryResultFn>,
}

pub trait Encoder {
    fn encode(&self) -> Vec<u8>;
}

impl Encoder for Entry {
    fn encode(&self) -> Vec<u8> {
        // Encode the item into bytes
        let mut data = Vec::new();
        data.extend_from_slice(&self.version.to_le_bytes());

        if self.version == 1 {
            data.extend_from_slice(&self.id.to_le_bytes());
            data.extend_from_slice(&self.stream_id.to_le_bytes());
            data.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
            data.extend_from_slice(&self.data);
        } else {
            panic!("Unsupported version");
        }
        data
    }
}

pub trait Decoder<'a> {
    fn decode(&mut self, closure: Box<dyn FnMut(Entry) -> Result<bool, Error> + 'a>) -> Result<()>;
}

impl<'a> Decoder<'a> for File {
    fn decode(
        &mut self,
        mut closure: Box<dyn FnMut(Entry) -> Result<bool, Error> + 'a>,
    ) -> Result<()> {
        // Decode the item from bytes
        loop {
            let mut entry = Entry::default();

            let mut version = [0u8; 1];
            match self.read_exact(&mut version) {
                Ok(()) => {
                    entry.version = u8::from_le_bytes(version);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // End of file
                Err(e) => return Err(anyhow!(e)),
            }
            if entry.version == 1 {
                let mut id_buf = [0u8; 8];
                self.read_exact(&mut id_buf).context("")?;
                entry.id = u64::from_le_bytes(id_buf);

                let mut stream_id_buf = [0u8; 8];
                self.read_exact(&mut stream_id_buf)
                    .context("Failed to read stream_id")?;
                entry.stream_id = u64::from_le_bytes(stream_id_buf);

                let mut data_size_buf = [0u8; 4];
                self.read_exact(&mut data_size_buf)
                    .context("Failed to read data size")?;

                let data_size = u32::from_le_bytes(data_size_buf);

                entry.data.resize(data_size as usize, 0);
                self.read_exact(&mut entry.data)
                    .map_err(errors::new_io_error)?;
            } else {
                log::error!("Unsupported version: {}", entry.version);
                return Err(anyhow!(errors::new_invalid_data()));
            }
            // Call the closure with the decoded entry
            if !closure(entry)? {
                break;
            }
        }
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

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("version", &self.version)
            .field("id", &self.id)
            .field("stream_id", &self.stream_id)
            .field("data", &self.data)
            .finish()
    }
}

#[test]
fn test_entry_encode() {
    let entry = Entry {
        version: 1,
        id: 1,
        stream_id: 1,
        data: "hello world".as_bytes().to_vec(),
        callback: None,
    };

    let encoded = entry.encode();

    use std::io::Write;
    // write the encoded data to a file
    let mut file = File::create("test_entry.bin").expect("Failed to create file");
    file.write_all(&encoded).expect("Failed to write to file");

    file.sync_all().expect("Failed to flush file");
    drop(file);

    // Read the file back and decode
    let mut file = File::open("test_entry.bin").expect("Failed to open file");

    let meta = file.metadata().expect("Failed to read metadata");
    assert!(meta.len() > 0, "File should not be empty");

    file.decode(Box::new(|decoded_entry| {
        assert_eq!(decoded_entry.version, 1);
        assert_eq!(decoded_entry.id, 1);
        assert_eq!(decoded_entry.stream_id, 1);
        assert_eq!(decoded_entry.data, b"hello world");
        Ok(true)
    }))
    .expect("Failed to decode entry");
}
