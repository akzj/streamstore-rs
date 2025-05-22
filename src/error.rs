use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Stream not found")]
    NotFound,
    #[error("Stream already exists")]
    AlreadyExists,
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
    #[error("Stream offset is invalid")]
    StreamOffsetInvalid,
    #[error("Stream Not Found")]
    StreamNotFound,
}

impl Error {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }

    pub fn is_already_exists(&self) -> bool {
        matches!(self, Error::AlreadyExists)
    }

    pub fn is_invalid_data(&self) -> bool {
        matches!(self, Error::InvalidData)
    }

    pub fn is_internal_error(&self) -> bool {
        matches!(self, Error::InternalError)
    }

    pub fn new_wal_channel_send_error() -> Self {
        Error::WalChannelSendError
    }
    pub fn new_io_error(e: std::io::Error) -> Self {
        Error::IoError(e)
    }
}
