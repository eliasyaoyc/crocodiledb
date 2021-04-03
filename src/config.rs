use crate::error::{Error, Result};
use crate::tracing::Handle;
use tokio::sync::mpsc;
use tracing::{debug, info, info_span};

#[derive(Clone, Debug)]
pub struct Config {}

pub struct App {}

impl Config {
    pub async fn build(
        self,
        shutdown_tx: mpsc::UnboundedReceiver<()>,
        log_level: Handle,
    ) -> Result<App> {
        debug!("building app");
        Ok(App {})
    }
}
