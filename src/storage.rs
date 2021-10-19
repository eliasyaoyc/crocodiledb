use crate::IResult;
use std::fs::File;
use crate::error::Error;

mod file;
mod memory;

pub trait Storage {
    /// Create the specified path file/directory.
    fn create(&mut self, name: &str) -> IResult<()>;

    /// Returns true iff the name file or dir exists.
    fn exists(&self, name: &str) -> bool;

    /// Returns a list of the path to each file in given directory,
    /// If recursive is true, it will recursive inner directory.
    ///
    /// If not found the given directory then return `Error::DirNotFound`.
    fn list(&self, dir: &str, recursive: bool) -> IResult<Vec<&str>>;

    /// Delete the named file/directory.
    ///
    /// If not found the dir then return `Error::DirNotExist`.
    fn remove(&mut self, name: &str) -> IResult<()>;

    /// Rename a file or directory to a new name, replacing the original file if
    /// `new` already exists.
    fn rename(&mut self, src: &str, target: &str) -> IResult<()>;

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
    fn read(&self, buf: &mut [u8]) -> IResult<usize>;

    /// Read all bytes in this source into a buffer, returning how many
    /// bytes were read.
    fn read_all(&self, buf: &mut Vec<u8>) -> IResult<usize>;

    /// Write bytes from the slice capacity in this source into a buffer, returning how
    /// many bytes were write.
    fn write(&mut self, buf: &[u8]) -> IResult<usize>;

    /// Flush buffer bytes to disk.
    fn flush(&mut self) -> IResult<()>;

    /// Close the fd.
    fn close(&mut self) -> IResult<()>;
}

/// write "data" to the named file.
pub fn write_string_to_file<S: Storage>(
    env: &S,
    data: String,
    file_name: &str,
) -> IResult<()>
{
    Ok(())
}

/// read the named file to data.
pub fn read_file_to_string<S: Storage>(
    env: &S,
    file_name: &str,
) -> IResult<String>
{
    Ok(String::new())
}

#[test]
fn test() {
    let v = vec![1, 2, 3, 4];
    let r = &v[v.len()..];
    println!("{:?}", r);
}