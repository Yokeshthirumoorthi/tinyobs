//! MCP server tools as a library module
//!
//! Exposes TinyObs query tools for the Model Context Protocol.
//! Downstream crates can compose these with their own MCP tools.
//!
//! # Example
//!
//! ```no_run
//! use tinyobs::client::TinyObsClient;
//! use tinyobs::mcp::TinyObsTools;
//!
//! let client = TinyObsClient::new("http://localhost:4318");
//! let tools = TinyObsTools::new(client);
//! // Use tools.list_services(), tools.get_traces(), etc. in your MCP server
//! ```

use crate::client::TinyObsClient;
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};

// ── Tool parameter types ─────────────────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTracesArgs {
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    #[schemars(description = "Maximum number of results (default: 20)")]
    pub limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTraceArgs {
    #[schemars(description = "The trace ID to look up")]
    pub trace_id: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetLogsArgs {
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    #[schemars(description = "Filter by severity level (e.g. ERROR, WARN, INFO)")]
    pub severity: Option<String>,
    #[schemars(description = "Filter by trace ID")]
    pub trace_id: Option<String>,
    #[schemars(description = "Filter by text in log body")]
    pub body_contains: Option<String>,
    #[schemars(description = "Maximum number of results (default: 20)")]
    pub limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetMetricsArgs {
    #[schemars(description = "Filter by service name")]
    pub service: Option<String>,
    #[schemars(description = "Filter by metric name")]
    pub name: Option<String>,
    #[schemars(description = "Maximum number of results (default: 20)")]
    pub limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct RunQueryArgs {
    #[schemars(description = "ClickHouse SQL query to execute")]
    pub sql: String,
    #[schemars(description = "Maximum number of results (default: 100)")]
    pub limit: Option<usize>,
}

// ── MCP Tool Server ──────────────────────────────────────────────────

/// TinyObs MCP tool server.
///
/// Wraps a `TinyObsClient` and exposes observability queries as MCP tools.
/// Can be used standalone or composed into a larger MCP server.
#[derive(Debug, Clone)]
pub struct TinyObsTools {
    client: TinyObsClient,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl TinyObsTools {
    /// Create a new MCP tool server with the given HTTP client
    pub fn new(client: TinyObsClient) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "List all observed services with span counts, error rates, and latency stats")]
    async fn list_services(&self) -> String {
        match self.client.list_services().await {
            Ok(services) => serde_json::to_string_pretty(&services).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Query recent traces/spans. Filter by service name and limit results.")]
    async fn get_traces(&self, Parameters(args): Parameters<GetTracesArgs>) -> String {
        match self
            .client
            .list_traces(args.service.as_deref(), args.limit)
            .await
        {
            Ok(spans) => serde_json::to_string_pretty(&spans).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Get all spans for a specific trace ID. Useful for debugging a request flow.")]
    async fn get_trace(&self, Parameters(args): Parameters<GetTraceArgs>) -> String {
        match self.client.get_trace(&args.trace_id).await {
            Ok(spans) => serde_json::to_string_pretty(&spans).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Query logs. Filter by service, severity level, trace ID, or body text.")]
    async fn get_logs(&self, Parameters(args): Parameters<GetLogsArgs>) -> String {
        match self
            .client
            .list_logs(
                args.service.as_deref(),
                args.severity.as_deref(),
                args.trace_id.as_deref(),
                args.body_contains.as_deref(),
                args.limit,
            )
            .await
        {
            Ok(logs) => serde_json::to_string_pretty(&logs).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Query metrics. Filter by service name or metric name.")]
    async fn get_metrics(&self, Parameters(args): Parameters<GetMetricsArgs>) -> String {
        match self
            .client
            .list_metrics(args.service.as_deref(), args.name.as_deref(), args.limit)
            .await
        {
            Ok(metrics) => serde_json::to_string_pretty(&metrics).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Execute a raw ClickHouse SQL query. Use for advanced analysis or querying custom tables.")]
    async fn run_query(&self, Parameters(args): Parameters<RunQueryArgs>) -> String {
        match self.client.raw_query(&args.sql, args.limit).await {
            Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Check TinyObs server health and backend connectivity")]
    async fn health(&self) -> String {
        match self.client.health().await {
            Ok(h) => serde_json::to_string_pretty(&h).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Discover database schema: tables, attribute keys, and metric names")]
    async fn discover_schema(&self) -> String {
        match self.client.discover_schema().await {
            Ok(schema) => serde_json::to_string_pretty(&schema).unwrap_or_default(),
            Err(e) => format!("Error: {e}"),
        }
    }
}

#[tool_handler]
impl ServerHandler for TinyObsTools {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
                "TinyObs MCP server — query traces, logs, metrics, and run SQL against your observability data."
                    .to_string(),
            )
    }
}
