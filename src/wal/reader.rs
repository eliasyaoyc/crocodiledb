use std::sync::Arc;
use crate::IResult;

pub trait Reporter {
    /// Some corruption was detected. `size` is the approximate number
    /// of bytes dropped due to the corruption.
    fn corruption(&self, bytes: usize) -> IResult<()>;
}

pub struct LogReader {
    reporter: Option<Box<dyn Reporter>>,
    // whether check sum for the record or not.
    checksum: bool,
}