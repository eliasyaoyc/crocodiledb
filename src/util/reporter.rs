use crate::{Error, IResult};
use std::cell::RefCell;
use std::rc::Rc;
use crate::wal::reader::Reporter;

#[derive(Clone)]
pub struct LogReporter {
    inner: Rc<RefCell<LogReporterInner>>,
}

struct LogReporterInner {
    ok: bool,
    reason: String,
}

impl LogReporter {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(LogReporterInner {
                ok: true,
                reason: "".to_owned(),
            })),
        }
    }
    pub fn result(&self) -> IResult<()> {
        let inner = self.inner.borrow();
        if inner.ok {
            Ok(())
        } else {
            Err(Error::CorruptionString(inner.reason.clone()))
        }
    }
}

impl Reporter for LogReporter {
    fn corruption(&mut self, _bytes: u64, reason: &str) -> IResult<()> {
        self.inner.borrow_mut().ok = false;
        self.inner.borrow_mut().reason = reason.to_owned();
        Ok(())
    }
}
