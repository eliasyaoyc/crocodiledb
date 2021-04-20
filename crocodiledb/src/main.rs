use tokio::sync::mpsc;
// use crocodile::signal::shutdown;
use tracing::{debug, error, info, warn};

fn main() {
    // let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    // runtime::build().block_on(async move {
    //     tokio::select! {
    //     _ = shutdown() => {
    //         info!("Received shutdown signal")
    //     }
    //      _ = shutdown_rx.recv() => {
    //         info!("Received shutdown via admin interface");
    //     }
    //    }
    // });
}

mod runtime {
    use tokio::runtime::{Builder, Runtime};

    pub fn build() -> Runtime {
        Builder::new_current_thread()
            .enable_all()
            .thread_name("db")
            .build()
            .expect("failed to build basic runtime!")
    }
}


