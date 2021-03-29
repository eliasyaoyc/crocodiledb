use hyper::http;
use crate::error::Error;


#[derive(Clone)]
pub struct Handle(Inner);

enum Inner {
    Disabled,
    Enabled {
        // level: level::Handle,
        // tasks: tasks::Handle,
    },
}

impl Handle {
    pub fn disabled() -> Self {
        Self(Inner::Disabled)
    }
}