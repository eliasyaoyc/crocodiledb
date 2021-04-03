/// Returns a `Future` that completes when the crocodiledb should start to shutdown.
pub async fn shutdown() {
    imp::shutdown().await
}

#[cfg(unix)]
mod imp {
    use tokio::signal::unix::{signal, SignalKind};
    use tracing::info;

    pub(super) async fn shutdown() {
        tokio::select! {
            // SIGINT  - To allow Ctrl-c to emulate SIGTERM while developing.
            () = sig(SignalKind::interrupt(), "SIGINT") => {}
            // SIGTERM - Kubernetes sends this to start a graceful shutdown.
            () = sig(SignalKind::terminate(), "SIGTERM") => {}
        }
    }

    async fn sig(kind: SignalKind, name: &'static str) {
        // Create a Future that completes the first time the process receives 'sig'.
        signal(kind)
            .expect("Failed to register signal handler")
            .recv()
            .await;
        info!(
            // use target to remove 'imp' from output
            target: "linkerd_proxy::signal",
            "received {}, starting shutdown",
            name, )
    }
}
