use std::any;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Stream already exists")]
    AlreadyExists,

    #[error("path {path} is invalid")]
    InValidPath { path: std::path::PathBuf },

    #[error("invalid data")]
    InvalidData,

    #[error("internal error")]
    InternalError,

    #[error("is closed")]
    CloseError,

    #[error("channel is closed")]
    WalChannelSendError,

    #[error("IO error")]
    IoError(std::io::Error),

    #[error("Stream {stream_id} offset {offset} is invalid")]
    StreamOffsetInvalid { stream_id: u64, offset: u64 },

    #[error("Stream {stream_id} Not Found")]
    StreamNotFound { stream_id: u64 },
}

pub fn new_stream_offset_invalid(stream_id: u64, offset: u64) -> anyhow::Error {
    anyhow::anyhow!(Error::StreamOffsetInvalid { stream_id, offset })
}

pub fn new_stream_not_found(stream_id: u64) -> anyhow::Error {
    anyhow::anyhow!(Error::StreamNotFound { stream_id })
}

pub fn new_io_error(e: std::io::Error) -> anyhow::Error {
    anyhow::anyhow!(Error::IoError(e))
}

pub fn new_invalid_path(path: std::path::PathBuf) -> anyhow::Error {
    anyhow::anyhow!(Error::InValidPath { path })
}

pub fn new_invalid_data() -> anyhow::Error {
    anyhow::anyhow!(Error::InvalidData)
}
