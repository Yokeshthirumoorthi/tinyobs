//! Shared HTTP API request/response types
//!
//! Used by both server binaries and the HTTP client library.

use serde::{Deserialize, Serialize};

/// Query parameters for trace listing
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TraceQuery {
    pub service: Option<String>,
    pub limit: Option<usize>,
}

/// Query parameters for log listing
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct LogQuery {
    pub service: Option<String>,
    pub severity: Option<String>,
    pub trace_id: Option<String>,
    pub body_contains: Option<String>,
    pub limit: Option<usize>,
}

/// Query parameters for metric listing
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct MetricQuery {
    pub service: Option<String>,
    pub name: Option<String>,
    pub limit: Option<usize>,
}

/// Request body for raw SQL queries
#[derive(Debug, Deserialize, Serialize)]
pub struct RawQueryRequest {
    pub sql: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub backend: bool,
}

/// Response wrapper for query results
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub data: Vec<serde_json::Value>,
}
