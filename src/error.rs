use crossbeam_channel::RecvError;
use quick_error::quick_error;

quick_error! {
   #[derive(Debug)]
   pub enum Error {
    NotFound(hint: Option<String>){
          display("key seeking failed: {:?}",hint)
      }
    InvaildArgument(hint: String){
          display("invalid argument: {}",hint)
      }
    DBClosed(hint: String) {
          display("try to operate a closed db: {}",hint)
      }
    IO(err: std::io::Error) {
          display("I/O operation error: {}", err)
      }
    RecvError(err: RecvError){
          display("{:?}",err)
          cause(err)
      }
   }
}

macro_rules! map_io_res {
    ($result:expr) => {
        match $result {
           Ok(v) => Ok(v),
           Err(e) => Err(Error::IO(e)),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;