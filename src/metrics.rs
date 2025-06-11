use std::sync::Mutex;

use lazy_static::lazy_static;
use prometheus_client::{
    encoding::text::encode,
    metrics::{
        counter::Counter,
        histogram::{Histogram, exponential_buckets},
    },
    registry::Registry,
};

lazy_static! {
    pub static ref registry: Mutex<Registry> = Mutex::new(Registry::default());
    pub static ref wal_write_file_seconds: Histogram = {
        let h = Histogram::new(exponential_buckets(0.00001, 1.25, 60));
        registry.lock().unwrap().register(
            "wal_write_log_seconds",
            "Duration of Wal Write in seconds",
            h.clone(),
        );
        h
    };
    pub static ref wal_recv_entry_seconds: Histogram = {
        let h = Histogram::new(exponential_buckets(0.00001, 1.25, 60));
        registry.lock().unwrap().register(
            "wal_recv_entry_seconds",
            "Duration of Wal Read entrys in seconds",
            h.clone(),
        );
        h
    };
    pub static ref read_segment_hit_count: Counter = {
        let c: Counter = Default::default();
        registry.lock().unwrap().register(
            "read_segment_hit_count",
            "Count of read segment hits",
            c.clone(),
        );
        c
    };
    pub static ref read_segment_miss_count: Counter = {
        let c: Counter = Default::default();
        registry.lock().unwrap().register(
            "read_segment_miss_count",
            "Count of read segment misses",
            c.clone(),
        );
        c
    };
    pub static ref find_segment_time_seconds: Histogram = {
        let h = Histogram::new(exponential_buckets(0.0000001, 2.0, 25));
        registry.lock().unwrap().register(
            "find_segment_time_seconds",
            "Duration of find segment in seconds",
            h.clone(),
        );
        h
    };
}

pub fn encode_metrics() -> String {
    let reg = registry.lock().unwrap();
    let mut buf = String::new();
    encode(&mut buf, &reg).unwrap();
    buf
}
