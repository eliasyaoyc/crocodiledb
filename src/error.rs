use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found.")]
    NotFound,
    #[error("dir not exist.")]
    DirNotExist,
    #[error("{0}")]
    Corruption(&'static str),
    #[error("{0}")]
    CorruptionString(String),
    #[error("I/O operation error: {0}")]
    IO(#[from] std::io::Error),
    #[error("compressed error:{0}")]
    CompressedFailed(#[from] snap::Error),
    #[error("UTF8 error:{0}")]
    UTF8Error(#[from] std::string::FromUtf8Error),
}


pub type IResult<T> = std::result::Result<T, Error>;