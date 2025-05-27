use std::env;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::{thread::sleep, time::Duration};
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
                log::info!("Append success: {:?}", entry);
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

    for i in 0..1 {
        let data = format!("hello world {}", i);
        log::info!("Appending entry: {}", data);
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
    log::info!("All append operations completed");

    let begin = store.get_stream_begin(1).unwrap();
    log::info!("Stream begin: {:?}", begin);

    let end = store.get_stream_end(1).unwrap();
    log::info!("Stream end: {:?}", end);

    let res = store.read(1, 0, end);
    match res {
        Ok(data) => {
            // vec to string
            log::info!("Read entries: {}", String::from_utf8(data).unwrap().len());
        }
        Err(e) => {
            log::error!("Failed to read entries: {:?}", e);
        }
    }
}
