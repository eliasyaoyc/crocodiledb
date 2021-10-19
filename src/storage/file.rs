use crate::IResult;
use crate::storage::Storage;

pub struct FileSys;

impl Storage for FileSys {
    fn create(&mut self, name: &str) -> IResult<()> {
        todo!()
    }

    fn exists(&self, name: &str) -> bool {
        todo!()
    }

    fn list(&self, dir: &str, recursive: bool) -> IResult<Vec<&str>> {
        todo!()
    }

    fn remove(&mut self, name: &str) -> IResult<()> {
        todo!()
    }

    fn rename(&mut self, src: &str, target: &str) -> IResult<()> {
        todo!()
    }

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