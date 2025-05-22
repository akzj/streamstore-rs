use crate::{
    entry::{Encoder, Entry},
    error::Error,
};

use std::{
    cell::Cell,
    fs::File,
    io::{Read, Write},
    sync::{
        Arc, Mutex, atomic,
        mpsc::{Receiver, SyncSender},
    },
    thread,
};

pub struct WalInner {
    file: Mutex<File>,
    dir: String,
    max_size: u64,
    last_entry: atomic::AtomicU64,
    sender: SyncSender<Entry>,
    next: SyncSender<Vec<Entry>>,
    receiver: Mutex<Receiver<Entry>>,
    err_handler: std::sync::Mutex<Box<dyn Fn(Error) + Send + Sync>>,
}

impl WalInner {
    pub fn close(&mut self) -> Result<(), Error> {
        // Close the WAL file
        self.file
            .lock()
            .unwrap()
            .sync_all()
            .map_err(Error::new_io_error)?;
        Ok(())
    }

    pub fn batch_write(&self, items: &[Entry]) -> Result<(), Error> {
        assert!(!items.is_empty(), "Items cannot be empty");
        assert!(
            items[0].id == self.last_entry.load(atomic::Ordering::Relaxed) + 1,
            "Items must be in order"
        );

        // Append data to the stream
        let mut buffer = Vec::new();
        for item in items {
            let data = item.encode();
            buffer.extend_from_slice(&data);
            self.last_entry.store(item.id, atomic::Ordering::SeqCst);
        }

        match self.file.lock().unwrap().write_all(&buffer) {
            Ok(_) => {}
            Err(e) => {
                return Err(Error::new_io_error(e));
            }
        }
        // Flush the file to ensure all data is written
        self.file
            .lock()
            .unwrap()
            .flush()
            .map_err(Error::new_io_error)?;

        self.try_to_rotate()?;

        Ok(())
    }

    // Rotate the WAL file
    pub fn try_to_rotate(&self) -> Result<(), Error> {
        let mut file_guard = self.file.lock().unwrap();

        let metadata = file_guard.metadata().map_err(Error::new_io_error)?;
        // check if the file size is greater than the rotate size
        if metadata.len() < self.max_size {
            return Ok(());
        }
        file_guard.sync_all().map_err(Error::new_io_error)?;

        let filename = std::path::Path::new(&self.dir).join(format!(
            "{}.wal",
            self.last_entry.fetch_add(1, atomic::Ordering::SeqCst)
        ));
        // Close the current file
        *file_guard = File::create(&filename).map_err(Error::new_io_error)?;

        println!("WAL file rotated to {}", filename.to_str().unwrap());

        Ok(())
    }

    pub fn run(&self) -> Result<(), Error> {
        // Start the WAL thread
        loop {
            let item = self.receiver.lock().unwrap().recv().unwrap();

            let mut items = vec![item];
            loop {
                match self.receiver.lock().unwrap().try_recv() {
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
            match self.batch_write(&items) {
                Ok(_) => {}
                Err(e) => {
                    // Handle the error
                    self.err_handler.lock().unwrap()(e);
                    //log.warn!("Error writing to WAL: {:?}", e);
                }
            }

            match self.next.send(items) {
                Ok(_) => {}
                Err(e) => {
                    // Handle the error
                    println!("Error sending to next: {:?}", e);
                    self.err_handler.lock().unwrap()(Error::new_wal_channel_send_error());
                }
            }
        }
    }
}

unsafe impl Sync for WalInner {}
unsafe impl Send for WalInner {}

#[derive(Clone)]
pub struct Wal(Arc<WalInner>);

impl std::ops::Deref for Wal {
    type Target = WalInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Wal {
    pub fn new(
        file: File,
        dir: String,
        max_size: u64,
        next: SyncSender<Vec<Entry>>,
        err_handler: Box<dyn Fn(Error) + Send + Sync>,
    ) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1024);
        Wal(Arc::new(WalInner {
            file: Mutex::new(file),
            dir,
            max_size,
            last_entry: atomic::AtomicU64::new(0),
            sender,
            next,
            receiver: Mutex::new(receiver),
            err_handler: std::sync::Mutex::new(err_handler),
        }))
    }

    pub fn write(&self, item: Entry) -> Result<(), Error> {
        // Append data to the stream
        self.sender
            .send(item)
            .map_err(|_| Error::WalChannelSendError)?;
        Ok(())
    }

    pub fn start(&self) -> () {
        // Start the WAL with the given sender
        let _self = self.clone();
        thread::spawn(move || {
            _self.run().unwrap();
        });
    }
}
