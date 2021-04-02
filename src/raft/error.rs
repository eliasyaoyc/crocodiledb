use thiserror::Error;

#[derive(Debug, Error)]
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


pub type Result<T> = std::result::Result<T, Error>;
