use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {}

pub type IResult<T> = std::result::Result<T, Error>;