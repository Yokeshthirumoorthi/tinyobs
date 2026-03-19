//! Backend abstraction for telemetry storage and query

pub mod types;

pub use types::*;

use crate::ch;
use crate::schema::{LogRecord, Metric, Span};
use crate::transport::Transport;
use anyhow::Result;
use async_trait::async_trait;

/// Unified telemetry backend trait
#[async_trait]
pub trait TelemetryBackend: Send + Sync {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>>;
    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>>;
    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>>;
    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>>;
    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>>;
    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>>;
    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>>;
    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>>;
    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison>;
    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>>;
    async fn discover_schema(&self) -> Result<Schema>;
}

/// Backend type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    Chdb,
    ClickHouse,
    Splunk,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendType::Chdb => write!(f, "chdb"),
            BackendType::ClickHouse => write!(f, "clickhouse"),
            BackendType::Splunk => write!(f, "splunk"),
        }
    }
}

/// Extension trait for backends that support ingestion
#[async_trait]
pub trait IngestBackend: TelemetryBackend {
    async fn ingest_span(&self, span: Span) -> Result<()>;
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn ingest_log(&self, log: LogRecord) -> Result<()>;
    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()>;
    async fn ingest_metric(&self, metric: Metric) -> Result<()>;
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
}

/// Extension trait for backends with lifecycle management
#[async_trait]
pub trait ManagedBackend: TelemetryBackend {
    async fn start_background_tasks(&self) -> Result<()>;
    async fn stop_background_tasks(&self) -> Result<()>;
    async fn health_check(&self) -> Result<bool>;
}

// ============================================================================
// Generic ClickHouse backend over any Transport
// ============================================================================

/// Generic ClickHouse backend parameterized by transport
#[derive(Clone)]
pub struct ChBackend<T: Transport> {
    pub transport: T,
}

impl<T: Transport> ChBackend<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl<T: Transport> TelemetryBackend for ChBackend<T> {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>> {
        let sql = ch::build_query_spans_sql(&filter);
        let rows = self.transport.query_json(&sql).await?;
        Ok(rows.iter().filter_map(ch::row_to_span).collect())
    }

    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>> {
        let sql = ch::build_query_logs_sql(&filter);
        let rows = self.transport.query_json(&sql).await?;
        Ok(rows.iter().filter_map(ch::row_to_log).collect())
    }

    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>> {
        let aggregated = filter.aggregation.is_some();
        let sql = ch::build_query_metrics_sql(&filter);
        let rows = self.transport.query_json(&sql).await?;
        Ok(ch::parse_metric_rows(&rows, aggregated))
    }

    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>> {
        let sql = ch::build_get_trace_sql(trace_id);
        let rows = self.transport.query_json(&sql).await?;
        Ok(rows.iter().filter_map(ch::row_to_span).collect())
    }

    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>> {
        let sql = ch::build_get_session_sql(session_id);
        let rows = self.transport.query_json(&sql).await?;
        Ok(rows.iter().filter_map(ch::row_to_span).collect())
    }

    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>> {
        let sql = ch::build_list_services_sql(&time_range);
        let rows = self.transport.query_json(&sql).await?;
        Ok(ch::parse_service_summaries(&rows))
    }

    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>> {
        let sql = ch::build_attribute_keys_sql(signal, service);
        let rows = self.transport.query_json(&sql).await?;
        Ok(ch::parse_attribute_keys(&rows))
    }

    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>> {
        let sql = ch::build_service_health_sql(&time_range);
        let rows = self.transport.query_json(&sql).await?;
        Ok(ch::parse_service_health(&rows))
    }

    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison> {
        let sql_a = ch::build_compare_period_sql(&a, service);
        let sql_b = ch::build_compare_period_sql(&b, service);
        let rows_a = self.transport.query_json(&sql_a).await?;
        let rows_b = self.transport.query_json(&sql_b).await?;
        let pa = ch::parse_period_metrics(&rows_a, a);
        let pb = ch::parse_period_metrics(&rows_b, b);
        Ok(ch::build_comparison(pa, pb))
    }

    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        let sql = ch::build_raw_query_sql(query, limit);
        self.transport.query_json(&sql).await
    }

    async fn discover_schema(&self) -> Result<Schema> {
        let span_keys = self
            .attribute_keys(Signal::Traces, None)
            .await
            .unwrap_or_default();
        let log_keys = self
            .attribute_keys(Signal::Logs, None)
            .await
            .unwrap_or_default();

        let metric_rows = self
            .transport
            .query_json(&ch::build_discover_schema_metric_names_sql())
            .await
            .unwrap_or_default();
        let metric_names = ch::parse_metric_names(&metric_rows);

        let resource_rows = self
            .transport
            .query_json(&ch::build_discover_schema_resource_keys_sql())
            .await
            .unwrap_or_default();
        let resource_keys = ch::parse_attribute_keys(&resource_rows);

        Ok(Schema {
            tables: vec![
                TableSchema {
                    name: "otel_traces".to_string(),
                    signal: Signal::Traces,
                    columns: vec![],
                },
                TableSchema {
                    name: "otel_logs".to_string(),
                    signal: Signal::Logs,
                    columns: vec![],
                },
                TableSchema {
                    name: "otel_metrics".to_string(),
                    signal: Signal::Metrics,
                    columns: vec![],
                },
            ],
            span_attribute_keys: span_keys,
            resource_attribute_keys: resource_keys,
            log_attribute_keys: log_keys,
            metric_names,
        })
    }
}

/// Default IngestBackend for ChBackend — uses SQL INSERT VALUES (suitable for chdb)
#[async_trait]
impl<T: Transport> IngestBackend for ChBackend<T> {
    async fn ingest_span(&self, span: Span) -> Result<()> {
        self.ingest_spans(vec![span]).await
    }

    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        let sql = ch::build_insert_spans_sql(&spans);
        self.transport.execute(&sql).await
    }

    async fn ingest_log(&self, log: LogRecord) -> Result<()> {
        self.ingest_logs(vec![log]).await
    }

    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let sql = ch::build_insert_logs_sql(&logs);
        self.transport.execute(&sql).await
    }

    async fn ingest_metric(&self, metric: Metric) -> Result<()> {
        self.ingest_metrics(vec![metric]).await
    }

    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        let sql = ch::build_insert_metrics_sql(&metrics);
        self.transport.execute(&sql).await
    }
}

/// Default ManagedBackend for ChBackend — no-op lifecycle
#[async_trait]
impl<T: Transport> ManagedBackend for ChBackend<T> {
    async fn start_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn stop_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        self.transport.ping().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_display() {
        assert_eq!(BackendType::Chdb.to_string(), "chdb");
        assert_eq!(BackendType::ClickHouse.to_string(), "clickhouse");
        assert_eq!(BackendType::Splunk.to_string(), "splunk");
    }
}
