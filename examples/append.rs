use core::time;
use crc::Crc;
use std::env;
use std::io::{Read, Seek, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::sleep;
use streamstore::entry::AppendEntryResultFn;
fn main() {
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
    options.max_wal_size(320);
    options.max_table_size(640);

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
        let data = format!("hello world {}\n", i);
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

    reader.seek(std::io::SeekFrom::Start(begin)).unwrap();

    let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
    let mut hash = crc64.digest();

    let mut read_buffer = vec![];

    loop {
        let rand_sizd = rand::random::<u64>() % 1024 + 1;
        let mut buffer = vec![0; rand_sizd as usize];
        let bytes_read = reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            log::info!("End of stream reached");
            break;
        }

        buffer.truncate(bytes_read);

        read_buffer.extend_from_slice(&buffer);

        hash.update(&buffer);
    }

    let multi_read_check_sum = hash.finalize();

    log::info!("Multi read check sum: {}", multi_read_check_sum);
    if multi_read_check_sum != write_check_sum {
        panic!(
            "Multi read checksum mismatch: expected {}, got {}",
            write_check_sum, multi_read_check_sum
        );
    } else {
        log::info!("Multi read checksum match: {}", multi_read_check_sum);
    }

    // store.print_metrics();
    drop(reader);
    drop(store);

    // wait for a while to ensure drop is complete
    sleep(time::Duration::from_secs(1));

    let store = options.reload_check_crc(true).open_store().unwrap();

    store.print_segment_files();
    store.print_mem_tables();

    let begin = store.get_stream_begin(1).unwrap();
    log::info!("Stream begin: {:?}", begin);

    let end = store.get_stream_end(1).unwrap();
    log::info!("Stream end: {:?}", end);

    let mut reader = store.new_stream_reader(1).unwrap();

    let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
    let mut hash = crc64.digest();

    let mut read_buffer = vec![];

    loop {
        let rand_sizd = rand::random::<u64>() % 1024 + 1;
        let mut buffer = vec![0; rand_sizd as usize];
        let bytes_read = reader.read(&mut buffer).unwrap();
        if bytes_read == 0 {
            log::info!("End of stream reached");
            break;
        }

        buffer.truncate(bytes_read);

        read_buffer.extend_from_slice(&buffer);

        hash.update(&buffer);
    }

    let multi_read_check_sum = hash.finalize();

    log::info!("reload multi read check sum: {}", multi_read_check_sum);
    if multi_read_check_sum != write_check_sum {
        panic!(
            "reload multi read checksum mismatch: expected {}, got {}",
            write_check_sum, multi_read_check_sum
        );
    } else {
        log::info!("Reload multi read checksum match: {}", multi_read_check_sum);
    }

    drop(store);
    drop(reader);
    sleep(time::Duration::from_secs(1));

    log::info!("Example completed successfully");
}
