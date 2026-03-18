use crate::{Config, TinyObs, TinyObsHandle};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub struct Application {
    port: u16,
    listener: TcpListener,
    handle: TinyObsHandle,
}

impl Application {
    pub async fn build(config: Config) -> Result<Self> {
        let port = config.application.port;
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(addr).await?;
        let port = listener.local_addr()?.port();
        let handle = TinyObs::start(config).await?;

        Ok(Self {
            port,
            listener,
            handle,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn handle(&self) -> &TinyObsHandle {
        &self.handle
    }

    pub fn tinyobs(&self) -> TinyObs {
        self.handle.clone_tinyobs()
    }

    pub fn into_parts(self) -> (TcpListener, TinyObsHandle, u16) {
        (self.listener, self.handle, self.port)
    }

    pub async fn run_until_stopped(self) -> Result<()> {
        let tinyobs = self.handle.clone_tinyobs();
        let app = tinyobs.ingest_router();

        tracing::info!(port = self.port, "Starting TinyObs server");
        tracing::info!("  POST /v1/traces  - OTLP trace ingestion (protobuf & JSON)");
        tracing::info!("  POST /v1/logs    - OTLP logs ingestion (protobuf & JSON)");
        tracing::info!("  POST /v1/metrics - OTLP metrics ingestion (protobuf & JSON)");
        tracing::info!("  GET  /health     - Health check");

        axum::serve(self.listener, app).await?;

        self.handle.shutdown().await
    }
}
