use std::sync::Arc;
use crate::version::version_edit::FileMetaData;

pub struct VersionEdit {}

/// Calculate the total size of given files.
#[inline]
pub fn total_file_size(files: &[Arc<FileMetaData>]) -> u64 {
    files.iter().fold(0, |acc, file| acc + file.file_size)
}