use bincode::ErrorKind;
use serde_derive::{Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::io;
use std::string::FromUtf8Error;
use std::sync::{PoisonError, RwLockReadGuard};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("Empty key")]
    EmptyKey,
    #[error("{0}")]
    TooLong(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFilename(String),
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
    #[error("Serialization failure, retry transaction")]
    Serialization,
}

impl<T> From<PoisonError<T>> for Error {
    fn from(e: PoisonError<T>) -> Self {
        Self::InternalErr(e.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        Self::InternalErr(e.to_string())
    }
}

impl From<TryFromSliceError> for Error {
    fn from(e: TryFromSliceError) -> Self {
        Self::InternalErr(e.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Self::InternalErr(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
