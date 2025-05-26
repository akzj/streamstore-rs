use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};
use std::{thread::sleep, time::Duration};
use streamstore::entry::AppendEntryCallback;
fn main() {
    // Initialize the logger
    // env_logger::init();
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

    let options = streamstore::options::Options::default();

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
        Some(Box::new(move |result| match result {
            Ok(entry) => {
                log::info!("Append success: {:?}", entry);
                let (lock, cvar) = &*cond;
                let mut count = lock.lock().unwrap();
                *count += 1;
                cvar.notify_all();
            }
            Err(e) => {
                log::error!("Append failed: {:?}", e);
            }
        }) as AppendEntryCallback)
    };

    store
        .append(1, "hello world".into(), make_callback(cond.clone()))
        .unwrap();
    store
        .append(1, "hello world".into(), make_callback(cond.clone()))
        .unwrap();
    store
        .append(1, "hello world".into(), make_callback(cond.clone()))
        .unwrap();
    store
        .append(1, "hello world".into(), make_callback(cond.clone()))
        .unwrap();

    sleep(Duration::from_secs(3));
}
