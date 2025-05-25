use std::io::Write;
use std::{thread::sleep, time::Duration};

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

    store.append(1, "hello world".into(), None).unwrap();
    store.append(1, "hello world".into(), None).unwrap();
    store.append(1, "hello world".into(), None).unwrap();
    store.append(1, "hello world".into(), None).unwrap();

    sleep(Duration::from_secs(3));
}
