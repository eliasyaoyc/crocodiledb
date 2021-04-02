use bytes::Bytes;
use crate::storage::error::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct FencePointer {
    pub minimum: Bytes,
    pub maximum: Bytes,
}

impl FencePointer {
    pub fn new() -> Self {
        FencePointer::default()
    }

    pub fn with_min_max(minimum: Bytes, maximum: Bytes) -> Self {
        Self {
            minimum,
            maximum,
        }
    }

    /// Return the minimum key in current SST,normally this value will not be empty.
    pub fn minimum(self) -> Option<Bytes> {
        if self.minimum.is_empty() {
            return None;
        }
        Some(self.minimum)
    }

    /// Return the maximum key in current SST,normally this value wil not be empty.
    pub fn maximum(self) -> Option<Bytes> {
        if self.maximum.is_empty() {
            return None;
        }
        Some(self.maximum)
    }

    /// Sets the minimum key for SST.
    pub fn set_minimum(&mut self, min: Bytes) -> Result<()> {
        self.minimum = min;
        Ok(())
    }

    /// Set the maximum key for SST.
    pub fn set_maximum(&mut self, max: Bytes) -> Result<()> {
        self.maximum = max;
        Ok(())
    }
}