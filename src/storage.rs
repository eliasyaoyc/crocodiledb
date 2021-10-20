use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, SeekFrom, Write};
use crate::IResult;
use std::path::{Path, PathBuf};
use crate::error::Error;

mod file;
mod memory;

pub trait Storage: Sync + Send {
    type F: File + 'static;

    /// Create the specified path file/directory.
    fn create<P: AsRef<Path>>(&self, name: P) -> IResult<Self::F>;

    /// Open a file for writing and reading.
    fn open<P: AsRef<Path>>(&self, name: P) -> IResult<Self::F>;

    /// Delete the named file
    fn remove<P: AsRef<Path>>(&self, name: P) -> IResult<()>;

    /// Removes a directory at this path. If `recursively` , removes all its contents.
    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> IResult<()>;


    /// Returns true iff the name file or dir exists.
    fn exists<P: AsRef<Path>>(&self, name: P) -> bool;

    /// Returns a list of the path to each file in given directory,
    ///
    /// If not found the given directory then return `Error::DirNotFound`.
    fn list<P: AsRef<Path>>(&self, dir: P) -> IResult<Vec<PathBuf>>;

    /// Rename a file or directory to a new name, replacing the original file if
    /// `new` already exists.
    fn rename<P: AsRef<Path>>(&self, src: P, target: P) -> IResult<()>;

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> IResult<()>;
}

pub trait File: Sync + Send {
    /// Lock the file for exclusive usage, blocking if the file is currently locked.
    fn lock_file(&self) -> IResult<()>;

    /// UnLock the file
    fn unlock_file(&self) -> IResult<()>;

    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    fn read_at(&self, buf: &mut [u8], offset: u64) -> IResult<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> IResult<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let mut tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => match e {
                    Error::IO(err) => {
                        if err.kind() != std::io::ErrorKind::Interrupted {
                            return Err(Error::IO(err));
                        }
                    }
                    _ => return Err(e),
                }
            }
        }
        if !buf.is_empty() {
            return Err(Error::IO(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to fill whole buffer.")));
        }
        Ok(())
    }

    /// Read bytes from the slice capacity in this source into a buffer, returning how
    /// many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> IResult<usize>;

    /// Read all bytes in this source into a buffer, returning how many
    /// bytes were read.
    fn read_all(&mut self, buf: &mut Vec<u8>) -> IResult<usize>;

    /// Write bytes from the slice capacity in this source into a buffer, returning how
    /// many bytes were write.
    fn write(&mut self, buf: &[u8]) -> IResult<usize>;

    /// Flush buffer bytes to disk.
    fn flush(&mut self) -> IResult<()>;

    fn seek(&mut self, pos: SeekFrom) -> IResult<u64>;

    fn len(&self) -> IResult<u64>;

    /// Close the fd.
    fn close(&mut self) -> IResult<()>;
}


/// write "data" to the named file and flush file to disk iff `should_sync` is true.
pub fn write_string_to_file<S: Storage, P: AsRef<Path>>(
    env: &S,
    data: String,
    file_name: P,
    should_sync: bool,
) -> IResult<()>
{
    let mut file = env.create(&file_name)?;
    file.write(data.as_bytes())?;
    if should_sync {
        file.flush()?;
    }
    if file.close().is_err() {
        env.remove(&file_name)?;
    }
    Ok(())
}