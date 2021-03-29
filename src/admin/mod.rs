//! Serves an HTTP admin server.
//!
//! * `GET/log-level` -- returns the current serve tracing filter.
//! * `PUT/log-level` -- sets a new tracing filter.
//! * `POST/shutdown` -- shuts down the crocodile.
mod readiness;
mod client_handle;

use futures::future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::future::Future;
use hyper::{body::{Body, HttpBody}, Request, Response, http, StatusCode};
use tokio::sync::mpsc;

use crate::admin::readiness::Readiness;
use crate::{NewService, metrics};
use std::task::{Context, Poll};
use hyper::service::Service;
use crate::admin::client_handle::ClientHandle;
use crate::tracing::Handle;

#[derive(Clone)]
pub struct Admin<M> {
    metrics: metrics::Serve<M>,
    tracing: Handle,
    ready: Readiness,
    shutdown_tx: mpsc::UnboundedSender<()>,
}

pub struct Accept<S> {
    service: S,
    server: hyper::server,
}

pub struct Serve<S> {
    client_addr: SocketAddr,
    inner: S,
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type ResponseFuture = Pin<Box<dyn Future<Output=Result<Response<Body>, Error>> + Send + 'static>>;


impl<M> Admin<M> {
    pub fn new(
        metrics: M,
        ready: Readiness,
        shutdown_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            metrics,
            ready,
            shutdown_tx,
        }
    }

    fn shutdown(&self) -> Response<Body> {
        if self.shutdown_tx.send(()).is_ok() {
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "text/plain")
                .body("shutdown\n".into())
                .expect("builder with known status code must not fail")
        } else {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(http::header::CONTENT_TYPE, "text/plain")
                .body("shutdown listener dropped\n".into())
                .expect("builder with known status code must not fail")
        }
    }

    fn internal_error_rsp(error: impl ToString) -> http::Response<Body> {
        http::Response::builder()
            .status(http::StatusCode::INTERNAL_SERVER_ERROR)
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body(error.to_string().into())
            .expect("builder with known status code should not fail")
    }

    fn not_found() -> Response<Body> {
        Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("builder with known status code must not fail")
    }

    fn forbidden_not_localhost() -> Response<Body> {
        Response::builder()
            .status(http::StatusCode::FORBIDDEN)
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body("Requests are only permitted from localhost.".into())
            .expect("builder with known status code must not fail")
    }

    fn client_is_localhost<B>(req: &Request<B>) -> bool {
        req.extensions()
            .get::<ClientHandle>()
            .map(|a| a.addr.ip().is_loopback())
            .unwrap_or(false)
    }
}

impl<M: FmtMetrics + Clone, T> NewService<T> for Admin<M> {
    type Service = Self;

    fn new_service(&mut self, _: T) -> Self::Service {
        self.clone()
    }
}

impl<M, B> tower::Service<http::request<B>> for Admin<M>
    where
        M: FmtMetrics,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<Error>,
        B::Data: Send,
{
    type Response = http::Response<Body>;
    type Error = Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        match req.uri().path() {
            "/metrics" => {
                let rsp = self.metrics.serve(req).unwrap_or_else(|error| {
                    tracing::error!(%error, "Failed to format metrics");
                    Self::internal_error_rsp(error)
                });
                Box::pin(future::ok(rsp))
            }
            "/log-level" => {
                if Self::client_is_localhost(&req) {
                    let handle = self.tracing.clone();
                    Box::pin(async move {
                    })
                } else {
                    Box::pin(future::ok(Self::forbidden_not_localhost()))
                }
            }
            "/shutdown" => {}
            path if path.starts_with("/tasks") => {
                if self.client_is_localhost(&req) {} else {
                    Box::pin(future::ok(Self::forbidden_not_localhost()))
                }
            }
            _ => Box::pin(future::ok(Self::not_found())),
        }
    }
}

