mod level;
mod tasks;

use crate::admin::Error;
use futures::future::ok;
use http::Request;
use hyper::body::HttpBody;

#[derive(Clone)]
pub struct Handle(Inner);

#[derive(Clone)]
enum Inner {
    Disabled,
    Enabled {
        level: level::Handle,
        tasks: tasks::Handle,
    },
}

impl Handle {
    /// Returns a new `handle` with tracing disabled.
    pub fn disabled() -> Self {
        Self(Inner::Disabled)
    }

    /// Serve requests that controls the log level. The request is expected to be either a GET or PUT
    /// request. PUT request must have a body that describes the new log level.
    pub async fn serve_level<B>(
        &self,
        req: http::Request<B>,
    ) -> Result<http::Response<hyper::Body>, Error>
    where
        B: HttpBody,
        B::Error: Into<Error>,
    {
        match self.0 {
            Inner::Enabled { ref level, .. } => level.serve(req).await,
            Inner::Disabled => Ok(Self::not_found()),
        }
    }

    pub async fn serve_tasks<B>(
        &self,
        req: http::Request<B>,
    ) -> Result<http::Response<hyper::Body>, Error> {
        match self.0 {
            Inner::Enabled { ref tasks, .. } => tasks.serve(req),
            Inner::Disabled => Ok(Self::not_found()),
        }
    }

    #[inline]
    fn not_found() -> http::Response<hyper::Body> {
        http::Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .expect("Response must be valid")
    }
}
