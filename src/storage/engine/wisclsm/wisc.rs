use std::fmt;
use std::fmt::{Display, Formatter};

pub struct Wisc {}

impl Display for Wisc {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "wisc")
    }
}
