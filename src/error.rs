use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found.")]
    NotFound,
    #[error("dir not exist.")]
    DirNotExist,
    #[error("I/O operation error: {0}")]
    IO(#[from] std::io::Error),
}


pub type IResult<T> = std::result::Result<T, Error>;