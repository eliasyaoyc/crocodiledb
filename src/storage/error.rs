use prost::DecodeError;
use std::io;
use std::sync::{PoisonError, RwLockReadGuard};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
    #[error("{0}")]
    TooLong(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFilename(String),
    #[error("Invalid prost data: {0}")]
    Decode(#[source] Box<prost::DecodeError>),
    #[error("Invalid data: {0}")]
    VarDecode(&'static str),
    #[error("Database Closed")]
    DBClose,
    #[error("{0}")]
    LogRead(String),
    #[error("capacity must be at least 2")]
    ParamCapacityrErr,
    #[error("{0}")]
    InternalErr(String),
    #[error("{0}")]
    InternalTnx(String),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(Box::new(e))
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::Decode(Box::new(e))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(e: PoisonError<T>) -> Self {
        Self::InternalErr(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
