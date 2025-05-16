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
    dir: String,
    last_entry: u64,
}

pub struct Wal {
    sender: Option<SyncSender<Entry>>,
    err_handler: std::sync::Arc<std::sync::Mutex<Box<dyn Fn(Error) + Send + Sync>>>,
}

impl WalInner {
    pub fn new(file: File, max_size: u64, dir: String) -> Self {
        WalInner {
            dir,
            file,
            max_size,
            last_entry: 0,
        }
    }

    pub fn close(&mut self) -> Result<(), Error> {
        // Close the WAL file
        self.file.sync_all().map_err(Error::new_io_error)?;
        Ok(())
    }

    pub fn batch_write(&mut self, items: &[Entry]) -> Result<(), Error> {
        assert!(!items.is_empty(), "Items cannot be empty");
        assert!(items[0].id == self.last_entry + 1, "Items must be in order");

        // Append data to the stream
        let mut buffer = Vec::new();
        for item in items {
            let data = item.encode();
            buffer.extend_from_slice(&data);
            self.last_entry = item.id;
        }

        match self.file.write_all(&buffer) {
            Ok(_) => {}
            Err(e) => {
                return Err(Error::new_io_error(e));
            }
        }
        // Flush the file to ensure all data is written
        self.file.flush().map_err(Error::new_io_error)?;

        self.try_to_rotate()?;

        Ok(())
    }

    // Rotate the WAL file
    pub fn try_to_rotate(&mut self) -> Result<(), Error> {
        let metadata = self.file.metadata().map_err(Error::new_io_error)?;
        // check if the file size is greater than the rotate size
        if metadata.len() < self.max_size {
            return Ok(());
        }
        self.file.sync_all().map_err(Error::new_io_error)?;

        let filename = std::path::Path::new(&self.dir).join(format!("{}.wal", self.last_entry + 1));
        // Close the current file
        self.file = File::create(&filename).map_err(Error::new_io_error)?;

        println!("WAL file rotated to {}", filename.to_str().unwrap());

        Ok(())
    }
}

impl Wal {
    pub fn new(
        err_handler: Box<dyn Fn(Error) + Send + Sync>,
    ) -> Self {
        Wal {
            sender: None,
            err_handler: std::sync::Arc::new(std::sync::Mutex::new(err_handler)),
        }
    }

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
