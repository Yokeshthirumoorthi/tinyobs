//! TinyObs MCP server — standalone binary
//!
//! Serves TinyObs query tools over stdio using the Model Context Protocol.
//!
//! Usage:
//!   tinyobs-mcp [--endpoint http://localhost:4318]

use rmcp::{ServiceExt, transport::stdio};
use tinyobs::client::TinyObsClient;
use tinyobs::mcp::TinyObsTools;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs must go to stderr — stdout is the MCP transport
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    // Parse --endpoint arg
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .windows(2)
        .find(|w| w[0] == "--endpoint")
        .map(|w| w[1].as_str())
        .unwrap_or("http://localhost:4318");

    let client = TinyObsClient::new(endpoint);
    let tools = TinyObsTools::new(client);

    tracing::info!("Starting TinyObs MCP server (endpoint: {endpoint})");
    let service = tools.serve(stdio()).await?;
    service.waiting().await?;

    Ok(())
}
