//! HTTP client for the TinyObs API
//!
//! Connects to a running tinyobs server (lite or pro) via HTTP.
//! Used by the CLI and MCP server, and available for downstream crates.

use crate::api_types::*;
use crate::backend::types::{Schema, ServiceSummary};
use crate::schema::{LogRecord, Metric, Span};
use anyhow::{Context, Result};
use reqwest::Client;

/// HTTP client for a running TinyObs server
#[derive(Debug, Clone)]
pub struct TinyObsClient {
    base_url: String,
    client: Client,
}

impl TinyObsClient {
    /// Create a new client pointing at the given tinyobs server URL
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    /// Check server health
    pub async fn health(&self) -> Result<HealthResponse> {
        let resp = self
            .client
            .get(format!("{}/api/health", self.base_url))
            .send()
            .await
            .context("Failed to connect to tinyobs server")?;
        resp.json().await.context("Failed to parse health response")
    }

    /// List traces with optional filters
    pub async fn list_traces(
        &self,
        service: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Span>> {
        let mut url = format!("{}/api/traces", self.base_url);
        let mut params = Vec::new();
        if let Some(s) = service {
            params.push(format!("service={}", urlencoded(s)));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        let resp = self.client.get(&url).send().await.context("Failed to list traces")?;
        resp.json().await.context("Failed to parse traces response")
    }

    /// Get all spans for a specific trace
    pub async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>> {
        let resp = self
            .client
            .get(format!("{}/api/traces/{}", self.base_url, urlencoded(trace_id)))
            .send()
            .await
            .context("Failed to get trace")?;
        resp.json().await.context("Failed to parse trace response")
    }

    /// List logs with optional filters
    pub async fn list_logs(
        &self,
        service: Option<&str>,
        severity: Option<&str>,
        trace_id: Option<&str>,
        body_contains: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<LogRecord>> {
        let mut url = format!("{}/api/logs", self.base_url);
        let mut params = Vec::new();
        if let Some(s) = service {
            params.push(format!("service={}", urlencoded(s)));
        }
        if let Some(s) = severity {
            params.push(format!("severity={}", urlencoded(s)));
        }
        if let Some(t) = trace_id {
            params.push(format!("trace_id={}", urlencoded(t)));
        }
        if let Some(b) = body_contains {
            params.push(format!("body_contains={}", urlencoded(b)));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        let resp = self.client.get(&url).send().await.context("Failed to list logs")?;
        resp.json().await.context("Failed to parse logs response")
    }

    /// List metrics with optional filters
    pub async fn list_metrics(
        &self,
        service: Option<&str>,
        name: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Metric>> {
        let mut url = format!("{}/api/metrics", self.base_url);
        let mut params = Vec::new();
        if let Some(s) = service {
            params.push(format!("service={}", urlencoded(s)));
        }
        if let Some(n) = name {
            params.push(format!("name={}", urlencoded(n)));
        }
        if let Some(l) = limit {
            params.push(format!("limit={l}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        let resp = self.client.get(&url).send().await.context("Failed to list metrics")?;
        resp.json().await.context("Failed to parse metrics response")
    }

    /// List all services
    pub async fn list_services(&self) -> Result<Vec<ServiceSummary>> {
        let resp = self
            .client
            .get(format!("{}/api/services", self.base_url))
            .send()
            .await
            .context("Failed to list services")?;
        resp.json().await.context("Failed to parse services response")
    }

    /// Execute a raw SQL query
    pub async fn raw_query(&self, sql: &str, limit: Option<usize>) -> Result<Vec<serde_json::Value>> {
        let body = RawQueryRequest {
            sql: sql.to_string(),
            limit: limit.unwrap_or(100),
        };
        let resp = self
            .client
            .post(format!("{}/api/query", self.base_url))
            .json(&body)
            .send()
            .await
            .context("Failed to execute query")?;
        resp.json().await.context("Failed to parse query response")
    }

    /// Discover database schema
    pub async fn discover_schema(&self) -> Result<Schema> {
        // Schema discovery uses raw_query to introspect tables
        // For now, query the system tables
        let tables_sql = "SELECT name, engine FROM system.tables WHERE database = currentDatabase()";
        let tables = self.raw_query(tables_sql, Some(100)).await?;

        Ok(Schema {
            tables: tables
                .iter()
                .filter_map(|row| {
                    let name = row.get("name")?.as_str()?.to_string();
                    Some(crate::backend::types::TableSchema {
                        name,
                        signal: crate::backend::types::Signal::Traces,
                        columns: vec![],
                    })
                })
                .collect(),
            ..Default::default()
        })
    }
}

fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace(' ', "%20")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('#', "%23")
}
