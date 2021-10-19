use std::fs::{OpenOptions, read_dir, remove_dir, remove_dir_all, remove_file, rename};
use std::path::{Path, PathBuf};
use crate::error::Error;
use crate::IResult;
use crate::storage::{File, Storage};
use std::fs::File as SysFile;

pub struct FileStorage;


impl Storage for FileStorage {
    type F = SysFile;

    fn create<P: AsRef<Path>>(&mut self, name: P) -> IResult<Self::F> {
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

    fn rename<P: AsRef<Path>>(&mut self, src: P, target: P) -> IResult<()> {
        match rename(src, target) {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::IO(e)),
        }
    }
}

impl File for SysFile {
    fn lock_file(&self) -> IResult<()> {
        todo!()
    }

    fn unlock_file(&self) -> IResult<()> {
        todo!()
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> IResult<usize> {
        todo!()
    }

    fn read(&self, buf: &mut [u8]) -> IResult<usize> {
        todo!()
    }

    fn read_all(&self, buf: &mut Vec<u8>) -> IResult<usize> {
        todo!()
    }

    fn write(&mut self, buf: &[u8]) -> IResult<usize> {
        todo!()
    }

    fn flush(&mut self) -> IResult<()> {
        todo!()
    }

    fn close(&mut self) -> IResult<()> {
        todo!()
    }
}