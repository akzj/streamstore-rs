use crc::Crc;
use std::env;
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use streamstore::entry::AppendEntryResultFn;
fn main() {
    // Initialize the logger
    // env_logger::init();

    // set rust_log to use the environment variable RUST_LOG
    let log_level = "RUST_LOG";
    let env = env::var(log_level).unwrap_or_else(|_| "debug".to_string());
    unsafe {
        env::set_var(log_level, env);
    }

    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} level:{} {}",
                record.file().unwrap(),
                record.line().unwrap(),
                record.level(),
                record.args()
            )
        })
        .init();
    log::info!("Starting streamstore example");

    let mut options = streamstore::options::Options::default();
    options.max_wal_size(1024);
    options.max_table_size(3000);

    println!("options {:?}", &options);

    let store = match options.open_store() {
        Ok(store) => store,
        Err(e) => {
            log::error!("Failed to open store: {:?}", e);
            return;
        }
    };

    let cond = Arc::new((Mutex::new(0 as u64), Condvar::new()));

    let make_callback = |cond: Arc<(Mutex<u64>, Condvar)>| {
        let cond_clone = cond.clone();
        // Increment the count in the condition variable
        let (lock, _) = &*cond_clone;
        let mut count = lock.lock().unwrap();
        *count += 1;

        Some(Box::new(move |result| match result {
            Ok(entry) => {
                //                log::info!("Append success: {:?}", entry);
                let (lock, cvar) = &*cond;
                let mut count = lock.lock().unwrap();
                *count -= 1;
                cvar.notify_all();
            }
            Err(e) => {
                log::error!("Append failed: {:?}", e);
            }
        }) as AppendEntryResultFn)
    };

    // create crc32 table

    let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
    let mut hash = crc64.digest();

    let count = 10000;
    for i in 0..count {
        let data = format!("hello world {}", i);
        //log::info!("Appending entry: {}", data);

        hash.update(data.as_bytes());

        store
            .append(1, data.into(), make_callback(cond.clone()))
            .unwrap();
    }

    // Wait for the callbacks to be called

    let (lock, cvar) = &*cond;
    let mut count = lock.lock().unwrap();
    while *count > 0 {
        count = cvar.wait(count).unwrap();
    }

    let write_check_sum = hash.finalize();

    log::info!(
        "All append operations completed check sum: {}",
        write_check_sum
    );

    store.print_segment_files();

    let begin = store.get_stream_begin(1).unwrap();
    log::info!("Stream begin: {:?}", begin);

    let end = store.get_stream_end(1).unwrap();
    log::info!("Stream end: {:?}", end);

    let mut buffer = vec![0u8; (end - begin) as usize];
    let mut reader = store.new_stream_reader(1).unwrap();
    let bytes_read = reader.read(&mut buffer).unwrap();

    buffer.truncate(bytes_read);

    let read_check_sum = Crc::<u64>::new(&crc::CRC_64_REDIS).checksum(&buffer);

    log::info!(
        "Read {} bytes from stream 1 hash {}",
        bytes_read,
        read_check_sum
    );

    if read_check_sum != write_check_sum {
        log::error!(
            "Checksum mismatch: expected {}, got {}",
            write_check_sum,
            read_check_sum
        );
    } else {
        log::info!("Checksum match: {}", read_check_sum);
    }

    // log::info!("Stream data: {:?}", String::from_utf8_lossy(&buffer));
}
