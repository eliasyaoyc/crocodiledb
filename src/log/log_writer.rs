use crate::log::Log;

pub struct LogWriter<L: Log> {
    log: L,
}

impl<L: Log> Log for LogWriter<L> {}