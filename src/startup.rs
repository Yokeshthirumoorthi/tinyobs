//! Application startup and builder pattern
//!
//! Provides a Z2P-style `Application` type that wraps TinyObs initialization
//! and server lifecycle management.

use crate::{Config, TinyObs, TinyObsHandle};
use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// Application wrapper for TinyObs
///
/// Manages the lifecycle of a TinyObs instance including binding to a port
/// and running the HTTP server.
pub struct Application {
    port: u16,
    listener: TcpListener,
    handle: TinyObsHandle,
}

impl Application {
    /// Build a new application from configuration
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

    /// Get the port the application is bound to
    ///
    /// Useful when binding to port 0 for tests to get the actual assigned port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get a reference to the TinyObs handle
    pub fn handle(&self) -> &TinyObsHandle {
        &self.handle
    }

    /// Get a clone of the TinyObs instance
    pub fn tinyobs(&self) -> TinyObs {
        self.handle.clone_tinyobs()
    }

    /// Run the application until stopped
    ///
    /// This starts the HTTP server and blocks until shutdown is requested.
    pub async fn run_until_stopped(self) -> Result<()> {
        let tinyobs = self.handle.clone_tinyobs();
        let app = tinyobs.ingest_router();

        tracing::info!(
            port = self.port,
            "Starting TinyObs server"
        );
        tracing::info!("  POST /v1/traces - OTLP trace ingestion");
        tracing::info!("  GET  /health    - Health check");

        axum::serve(self.listener, app).await?;

        self.handle.shutdown().await
    }
}
