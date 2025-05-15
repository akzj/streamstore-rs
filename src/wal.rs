use crate::{error::Error, store::Entry};

use std::{
    fs::File,
    io::{Read, Write},
    sync::mpsc::{Receiver, SyncSender},
    thread,
};

pub struct WalInner {
    file: File,
    max_size: u64,
    index: u64,
}

pub struct Wal {
    sender: Option<SyncSender<Entry>>,
    err_handler: std::sync::Arc<std::sync::Mutex<Box<dyn Fn(Error) + Send + Sync>>>,
}

impl WalInner {
    pub fn new(file: File, max_size: u64, index: u64) -> Self {
        WalInner {
            file,
            index,
            max_size,
        }
    }

    pub fn close(&mut self) -> Result<(), Error> {
        // Close the WAL file
        self.file.sync_all().map_err(Error::new_wal_file_io_error)?;
        Ok(())
    }

    pub fn batch_write(&mut self, items: &Vec<Entry>) -> Result<(), Error> {
        // Append data to the stream
        let mut buffer = Vec::new();
        for item in items {
            let data = item.encode();
            buffer.extend_from_slice(&data);
        }

        match self.file.write_all(&buffer) {
            Ok(_) => {}
            Err(e) => {
                return Err(Error::new_wal_file_io_error(e));
            }
        }
        // Flush the file to ensure all data is written
        self.file.flush().map_err(Error::new_wal_file_io_error)?;

        // rotate the WAL file if necessary
        self.try_to_rotate(self.max_size)?;

        Ok(())
    }

    // Rotate the WAL file
    pub fn try_to_rotate(&mut self, rotate_size: u64) -> Result<(), Error> {
        let metadata = self.file.metadata().map_err(Error::new_wal_file_io_error)?;
        // check if the file size is greater than the rotate size
        if metadata.len() < rotate_size {
            return Ok(());
        }
        self.file.sync_all().map_err(Error::new_wal_file_io_error)?;

        // Close the current file
        self.file = File::create("wal_new").map_err(Error::new_wal_file_io_error)?;
        Ok(())
    }
}

impl Wal {
    pub fn write(&self, item: Entry) -> Result<(), Error> {
        // Append data to the stream
        self.sender
            .as_ref()
            .unwrap()
            .send(item)
            .map_err(|_| Error::WalChannelSendError)?;
        Ok(())
    }

    pub fn start(
        &mut self,
        mut inner: WalInner,
        next: SyncSender<Vec<Entry>>,
    ) -> Result<(), Error> {
        // Start the WAL with the given sender
        let (sender, receiver) = std::sync::mpsc::sync_channel(1024);
        self.sender = Some(sender);

        // Spawn a thread to handle the WAL writing

        let err_handler = self.err_handler.clone();

        thread::spawn(move || {
            loop {
                let item = receiver.recv().unwrap();

                let mut items = vec![item];
                loop {
                    match receiver.try_recv() {
                        Ok(item) => {
                            items.push(item);
                            if items.len() >= 128 {
                                break;
                            }
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                // Write the items to the file
                match inner.batch_write(&items) {
                    Ok(_) => {}
                    Err(e) => {
                        // Handle the error
                        err_handler.lock().unwrap()(e);
                        return;
                    }
                }

                match next.send(items) {
                    Ok(_) => {}
                    Err(e) => {
                        // Handle the error
                        println!("Error sending to next: {:?}", e);
                        err_handler.lock().unwrap()(Error::new_wal_channel_send_error());
                        return;
                    }
                }
            }
        });

        Ok(())
    }
}

trait Encoder {
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

trait Decoder {
    fn decode(&mut self, closure: Box<dyn Fn(Entry)>) -> Result<(), Error>;
}

impl Decoder for File {
    fn decode(&mut self, closure: Box<dyn Fn(Entry)>) -> Result<(), Error> {
        // Decode the item from bytes

        loop {
            let mut entry = Entry::default();
            match self.read(&mut entry.version.to_le_bytes()) {
                Ok(0) => break, // EOF
                Ok(_) => {}
                Err(e) => {
                    return Err(Error::new_wal_file_io_error(e));
                }
            }
            if entry.version == 1 {
                self.read(&mut entry.id.to_le_bytes())
                    .map_err(Error::new_wal_file_io_error)?;
                self.read(&mut entry.stream_id.to_le_bytes())
                    .map_err(Error::new_wal_file_io_error)?;
                let data_size = u32::default();
                self.read(&mut data_size.to_le_bytes())
                    .map_err(Error::new_wal_file_io_error)?;
                entry.data.resize(data_size as usize, 0);
                self.read_exact(&mut entry.data)
                    .map_err(Error::new_wal_file_io_error)?;
            } else {
                return Err(Error::InvalidData);
            }
            // Call the closure with the decoded entry
            closure(entry);
        }
        Ok(())
    }
}
