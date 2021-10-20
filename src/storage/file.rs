use std::fs::{create_dir_all, OpenOptions, read_dir, remove_dir, remove_dir_all, remove_file, rename};
use std::path::{Path, PathBuf};
use crate::error::Error;
use crate::IResult;
use crate::storage::{File, Storage};
use std::fs::File as SysFile;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use fs2::FileExt;

#[derive(Default)]
pub struct FileStorage;

impl Storage for FileStorage {
    type F = SysFile;

    fn create<P: AsRef<Path>>(&self, name: P) -> IResult<Self::F> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(name)
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn open<P: AsRef<Path>>(&self, name: P) -> IResult<Self::F> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .open(name)
        {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn remove<P: AsRef<Path>>(&self, name: P) -> IResult<()> {
        match remove_file(name)
        {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> IResult<()> {
        match if recursively {
            remove_dir_all(dir)
        } else {
            remove_dir(dir)
        }
        {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn exists<P: AsRef<Path>>(&self, name: P) -> bool {
        name.as_ref().exists()
    }

    fn list<P: AsRef<Path>>(&self, dir: P) -> IResult<Vec<PathBuf>> {
        if dir.as_ref().is_dir() {
            let mut v = vec![];
            match read_dir(dir) {
                Ok(rd) => {
                    for entry in rd {
                        match entry {
                            Ok(p) => v.push(p.path()),
                            Err(e) => return Err(Error::IO(e)),
                        }
                    }
                    return Ok(v);
                }
                Err(e) => return Err(Error::IO(e)),
            }
        }
        Ok(vec![])
    }

    fn rename<P: AsRef<Path>>(&self, src: P, target: P) -> IResult<()> {
        match rename(src, target) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e)),
        }
    }

    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> IResult<()> {
        match create_dir_all(dir) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e))
        }
    }
}

impl File for SysFile {
    fn lock_file(&self) -> IResult<()> {
        match SysFile::try_lock_exclusive(self) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn unlock_file(&self) -> IResult<()> {
        match SysFile::unlock(self) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e)),
        }
    }

    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> IResult<usize> {
        match std::os::unix::prelude::FileExt::read_at(self, buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> IResult<usize> {
        match std::os::windows::prelude::FileExt::seek_read(self, buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> IResult<usize> {
        let mut reader = BufReader::new(self);
        match reader.read(buf) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn read_all(&mut self, buf: &mut Vec<u8>) -> IResult<usize> {
        let mut reader = BufReader::new(self);
        match reader.read_to_end(buf) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn write(&mut self, buf: &[u8]) -> IResult<usize> {
        let mut write = BufWriter::new(self);
        match write.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn flush(&mut self) -> IResult<()> {
        let mut write = BufWriter::new(self);
        match write.flush() {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> IResult<u64> {
        match Seek::seek(self, pos) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn len(&self) -> IResult<u64> {
        match SysFile::metadata(self) {
            Ok(n) => Ok(n.len()),
            Err(e) => Err(Error::IO(e))
        }
    }

    fn close(&mut self) -> IResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_exact_at() {
        let mut f = SysFile::create("test").unwrap();
        f.write_all("hello world".as_bytes()).unwrap();
        f.sync_all().unwrap();
        let tests = vec![
            (0, "hello world"),
            (0, ""),
            (1, "ello"),
            (4, "o world"),
            (100, ""),
        ];
        let rf = SysFile::open("test").unwrap();
        let mut buffer = vec![];
        for (offset, expect) in tests {
            buffer.resize(expect.as_bytes().len(), 0u8);
            rf.read_exact_at(buffer.as_mut_slice(), offset).unwrap();
            assert_eq!(buffer, Vec::from(String::from(expect)));
        }
        // EOF case
        buffer.resize(100, 0u8);
        rf.read_exact_at(buffer.as_mut_slice(), 2)
            .expect_err("failed to fill whole buffer");
        remove_file("test").unwrap();
    }
}