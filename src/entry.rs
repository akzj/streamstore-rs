use std::{fs::File, io::Read};

use crate::error::Error;

pub type AppendEntryCallback = dyn Fn(Result<u64, Error>) -> () + Send + Sync;
pub type DataType = Vec<u8>;

pub struct Entry {
    // auto increment id
    pub version: u8,
    pub id: u64,
    pub stream_id: u64,
    pub data: DataType,
    pub callback: Option<Box<AppendEntryCallback>>,
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
    fn decode(
        &mut self,
        closure: Box<dyn FnMut(Entry) -> Result<bool, Error> + 'a>,
    ) -> Result<(), Error>;
}

impl<'a> Decoder<'a> for File {
    fn decode(
        &mut self,
        mut closure: Box<dyn FnMut(Entry) -> Result<bool, Error> + 'a>,
    ) -> Result<(), Error> {
        // Decode the item from bytes
        loop {
            let mut entry = Entry::default();
            match self.read(&mut entry.version.to_le_bytes()) {
                Ok(0) => break, // EOF
                Ok(_) => {}
                Err(e) => {
                    return Err(Error::new_io_error(e));
                }
            }
            if entry.version == 1 {
                self.read(&mut entry.id.to_le_bytes())
                    .map_err(Error::new_io_error)?;
                self.read(&mut entry.stream_id.to_le_bytes())
                    .map_err(Error::new_io_error)?;
                let data_size = u32::default();
                self.read(&mut data_size.to_le_bytes())
                    .map_err(Error::new_io_error)?;
                entry.data.resize(data_size as usize, 0);
                self.read_exact(&mut entry.data)
                    .map_err(Error::new_io_error)?;
            } else {
                return Err(Error::InvalidData);
            }
            // Call the closure with the decoded entry
            if !closure(entry)? {
                break;
            }
        }
        Ok(())
    }
}
