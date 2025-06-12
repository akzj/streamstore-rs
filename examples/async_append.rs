use crc::Crc;
use std::io::Write;
use std::io::Read;

#[tokio::main]
async fn main() {
    // Initialize the logger
    // env_logger::init();

    // set rust_log to use the environment variable RUST_LOG

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

    let mut options = streamstore::options::Options::new_with_data_path("data/async");
    options.max_wal_size(32 * 1024);
    options.max_table_size(64 * 1024);

    println!("options {:?}", &options);

    let store = match options.open_store() {
        Ok(store) => store,
        Err(e) => {
            log::error!("Failed to open store: {:?}", e);
            return;
        }
    };

    let crc64 = Crc::<u64>::new(&crc::CRC_64_REDIS);
    let mut hash = crc64.digest();

    let count = 100000;
    for i in 0..count {
        let data = format!("hello world {}\n", i);
        //log::info!("Appending entry: {}", data);
        hash.update(data.as_bytes());
        let result = store.append_async(1, data.into()).await;
        match result {
            Ok(_offset) => {
                // log::debug!("Append success: {} {}", i, offset);
            }
            Err(e) => {
                log::error!("Append failed: {:?}", e);
            }
        }
    }

    let write_check_sum = hash.finalize();

    log::info!(
        "All append operations completed check sum: {}",
        write_check_sum
    );

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
        panic!(
            "Checksum mismatch: expected {}, got {}",
            write_check_sum,
            read_check_sum
        );
    } else {
        log::info!("Checksum match: {}", read_check_sum);
    }
}
