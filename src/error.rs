use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found.")]
    NotFound,
}


pub type IResult<T> = std::result::Result<T, Error>;