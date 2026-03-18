//! ClickHouse backend implementing TelemetryBackend + IngestBackend
//!
//! Uses ClickHouse's HTTP API with JSONEachRow format for queries
//! and VALUES syntax for inserts.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use std::sync::Arc;

use tinyobs_core::backend::*;
use tinyobs_core::schema::{LogRecord, Metric, MetricKind, Span, SpanStatus, SeverityLevel};

use crate::config::ClickHouseConfig;

/// ClickHouse-backed telemetry storage
#[derive(Clone)]
pub struct ClickHouseBackend {
    http: Client,
    config: Arc<ClickHouseConfig>,
}

impl ClickHouseBackend {
    pub fn new(config: ClickHouseConfig) -> Self {
        let http = Client::new();
        Self {
            http,
            config: Arc::new(config),
        }
    }

    /// Check if ClickHouse is reachable
    pub async fn ping(&self) -> Result<bool> {
        let resp = self
            .http
            .get(&self.config.url)
            .query(&[("query", "SELECT 1")])
            .send()
            .await;
        Ok(resp.is_ok())
    }

    /// Execute a query and return JSON rows
    async fn query_json(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let full_sql = format!("{} FORMAT JSONEachRow", sql);
        let resp = self
            .http
            .post(&self.config.url)
            .query(&[("database", &self.config.database)])
            .body(full_sql)
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        let body = resp.text().await.context("Failed to read response body")?;

        if !status.is_success() {
            anyhow::bail!("ClickHouse error ({}): {}", status, body);
        }

        let rows: Vec<serde_json::Value> = body
            .lines()
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();

        Ok(rows)
    }

    /// Execute a statement (INSERT, DDL, etc.) — no result expected
    async fn execute(&self, sql: &str) -> Result<()> {
        let resp = self
            .http
            .post(&self.config.url)
            .query(&[("database", &self.config.database)])
            .body(sql.to_string())
            .send()
            .await
            .context("ClickHouse HTTP request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse error ({}): {}", status, body);
        }

        Ok(())
    }

    // ========================================================================
    // Query helpers
    // ========================================================================

    fn build_span_where_clause(filter: &SpanFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref operation) = filter.operation {
            conditions.push(format!("SpanName = '{}'", escape_ch(operation)));
        }
        if let Some(ref trace_id) = filter.trace_id {
            conditions.push(format!("TraceId = '{}'", escape_ch(trace_id)));
        }
        if let Some(ref session_id) = filter.session_id {
            conditions.push(format!(
                "SpanAttributes['session.id'] = '{}'",
                escape_ch(session_id)
            ));
        }
        if let Some(ref status_filter) = filter.status {
            let code = match status_filter {
                SpanStatusFilter::Ok => 1,
                SpanStatusFilter::Error => 2,
                SpanStatusFilter::Unset => 0,
            };
            conditions.push(format!("StatusCode = {}", code));
        }
        if let Some(min_dur) = filter.min_duration_ns {
            conditions.push(format!("Duration >= {}", min_dur));
        }
        if let Some(max_dur) = filter.max_duration_ns {
            conditions.push(format!("Duration <= {}", max_dur));
        }
        for (key, val) in &filter.attributes {
            conditions.push(format!(
                "SpanAttributes['{}'] = '{}'",
                escape_ch(key),
                escape_ch(val)
            ));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn build_log_where_clause(filter: &LogFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref severity) = filter.severity {
            conditions.push(format!("SeverityText = '{}'", escape_ch(severity)));
        }
        if let Some(min_sev) = filter.min_severity_number {
            conditions.push(format!("SeverityNumber >= {}", min_sev));
        }
        if let Some(ref trace_id) = filter.trace_id {
            conditions.push(format!("TraceId = '{}'", escape_ch(trace_id)));
        }
        if let Some(ref span_id) = filter.span_id {
            conditions.push(format!("SpanId = '{}'", escape_ch(span_id)));
        }
        if let Some(ref body) = filter.body_contains {
            conditions.push(format!(
                "positionCaseInsensitive(Body, '{}') > 0",
                escape_ch(body)
            ));
        }
        for (key, val) in &filter.attributes {
            conditions.push(format!(
                "LogAttributes['{}'] = '{}'",
                escape_ch(key),
                escape_ch(val)
            ));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn build_metric_where_clause(filter: &MetricFilter) -> String {
        let mut conditions = Vec::new();

        let start_micros = filter.time_range.start.timestamp_micros();
        let end_micros = filter.time_range.end.timestamp_micros();
        conditions.push(format!("Timestamp >= {}", start_micros));
        conditions.push(format!("Timestamp <= {}", end_micros));

        if let Some(ref service) = filter.service {
            conditions.push(format!("ServiceName = '{}'", escape_ch(service)));
        }
        if let Some(ref name) = filter.name {
            conditions.push(format!("MetricName = '{}'", escape_ch(name)));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    fn row_to_span(row: &serde_json::Value) -> Option<Span> {
        let obj = row.as_object()?;

        let attrs: Vec<(String, String)> = obj
            .get("SpanAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let resource_attrs: Vec<(String, String)> = obj
            .get("ResourceAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Some(Span {
            trace_id: obj.get("TraceId")?.as_str()?.to_string(),
            span_id: obj.get("SpanId")?.as_str()?.to_string(),
            parent_span_id: obj
                .get("ParentSpanId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            session_id: attrs
                .iter()
                .find(|(k, _)| k == "session.id")
                .map(|(_, v)| v.clone()),
            service_name: obj
                .get("ServiceName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            operation: obj
                .get("SpanName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            start_time: obj
                .get("Timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            duration_ns: obj
                .get("Duration")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            status: SpanStatus::from(
                obj.get("StatusCode")
                    .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                    .unwrap_or(0) as i32,
            ),
            attributes: attrs,
            resource_attrs,
        })
    }

    fn row_to_log(row: &serde_json::Value) -> Option<LogRecord> {
        let obj = row.as_object()?;

        let attrs: Vec<(String, String)> = obj
            .get("LogAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let resource_attrs: Vec<(String, String)> = obj
            .get("ResourceAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let severity_number = obj
            .get("SeverityNumber")
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0) as i32;

        Some(LogRecord {
            timestamp: obj
                .get("Timestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0),
            observed_timestamp: obj
                .get("ObservedTimestamp")
                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))),
            trace_id: obj
                .get("TraceId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            span_id: obj
                .get("SpanId")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            severity_number: SeverityLevel::from(severity_number),
            severity_text: obj
                .get("SeverityText")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string()),
            body: obj
                .get("Body")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            service_name: obj
                .get("ServiceName")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            attributes: attrs,
            resource_attrs,
        })
    }
}

/// Escape single quotes for ClickHouse SQL
fn escape_ch(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

#[async_trait]
impl TelemetryBackend for ClickHouseBackend {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<Span>> {
        let where_clause = Self::build_span_where_clause(&filter);
        let order = filter
            .order_by
            .as_ref()
            .map(|o| {
                let dir = if o.descending { "DESC" } else { "ASC" };
                format!("ORDER BY {} {}", o.field, dir)
            })
            .unwrap_or_else(|| "ORDER BY Timestamp DESC".to_string());

        let sql = format!(
            "SELECT * FROM otel_traces {} {} LIMIT {} OFFSET {}",
            where_clause, order, filter.limit, filter.offset
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<LogRecord>> {
        let where_clause = Self::build_log_where_clause(&filter);

        let sql = format!(
            "SELECT * FROM otel_logs {} ORDER BY Timestamp DESC LIMIT {} OFFSET {}",
            where_clause, filter.limit, filter.offset
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_log).collect())
    }

    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<Metric>> {
        let where_clause = Self::build_metric_where_clause(&filter);

        let sql = if let (Some(agg), Some(bucket)) = (&filter.aggregation, &filter.bucket_secs) {
            let agg_fn = match agg {
                MetricAggregation::Sum => "sum(Value)",
                MetricAggregation::Avg => "avg(Value)",
                MetricAggregation::Min => "min(Value)",
                MetricAggregation::Max => "max(Value)",
                MetricAggregation::Count => "count()",
                MetricAggregation::P50 => "quantile(0.5)(Value)",
                MetricAggregation::P90 => "quantile(0.9)(Value)",
                MetricAggregation::P95 => "quantile(0.95)(Value)",
                MetricAggregation::P99 => "quantile(0.99)(Value)",
            };

            format!(
                "SELECT MetricName as name, ServiceName as service_name, \
                 toStartOfInterval(fromUnixTimestamp64Micro(Timestamp), INTERVAL {} SECOND) as bucket, \
                 {} as value \
                 FROM otel_metrics {} \
                 GROUP BY name, service_name, bucket \
                 ORDER BY bucket DESC LIMIT {}",
                bucket, agg_fn, where_clause, filter.limit
            )
        } else {
            format!(
                "SELECT * FROM otel_metrics {} ORDER BY Timestamp DESC LIMIT {}",
                where_clause, filter.limit
            )
        };

        let rows = self.query_json(&sql).await?;

        if filter.aggregation.is_some() {
            Ok(rows
                .iter()
                .filter_map(|r| {
                    let obj = r.as_object()?;
                    Some(Metric {
                        name: obj.get("name")?.as_str()?.to_string(),
                        description: None,
                        unit: None,
                        kind: MetricKind::Gauge,
                        timestamp: obj
                            .get("bucket")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        service_name: obj
                            .get("service_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        value: obj.get("value").and_then(|v| v.as_f64()),
                        sum: None,
                        count: None,
                        min: None,
                        max: None,
                        quantiles: None,
                        buckets: None,
                        attributes: vec![],
                        resource_attrs: vec![],
                    })
                })
                .collect())
        } else {
            Ok(rows
                .iter()
                .filter_map(|r| {
                    let obj = r.as_object()?;
                    Some(Metric {
                        name: obj.get("MetricName")?.as_str()?.to_string(),
                        description: obj
                            .get("Description")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string()),
                        unit: obj
                            .get("Unit")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string()),
                        kind: MetricKind::Gauge,
                        timestamp: obj
                            .get("Timestamp")
                            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                            .unwrap_or(0),
                        service_name: obj
                            .get("ServiceName")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        value: obj.get("Value").and_then(|v| v.as_f64()),
                        sum: obj.get("Sum").and_then(|v| v.as_f64()),
                        count: obj.get("Count").and_then(|v| v.as_u64()),
                        min: obj.get("Min").and_then(|v| v.as_f64()),
                        max: obj.get("Max").and_then(|v| v.as_f64()),
                        quantiles: None,
                        buckets: None,
                        attributes: vec![],
                        resource_attrs: vec![],
                    })
                })
                .collect())
        }
    }

    async fn get_trace(&self, trace_id: &str) -> Result<Vec<Span>> {
        let sql = format!(
            "SELECT * FROM otel_traces WHERE TraceId = '{}' ORDER BY Timestamp ASC",
            escape_ch(trace_id)
        );
        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn get_session(&self, session_id: &str) -> Result<Vec<Span>> {
        let sql = format!(
            "SELECT * FROM otel_traces WHERE SpanAttributes['session.id'] = '{}' ORDER BY Timestamp ASC",
            escape_ch(session_id)
        );
        let rows = self.query_json(&sql).await?;
        Ok(rows.iter().filter_map(Self::row_to_span).collect())
    }

    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>> {
        let start = time_range.start.timestamp_micros();
        let end = time_range.end.timestamp_micros();

        let sql = format!(
            r#"SELECT
                ServiceName as name,
                count() as span_count,
                uniqExact(SpanName) as operation_count,
                countIf(StatusCode = 2) / count() as error_rate,
                avg(Duration) as avg_duration_ns,
                quantile(0.99)(Duration) as p99_duration_ns,
                min(Timestamp) as first_seen,
                max(Timestamp) as last_seen
            FROM otel_traces
            WHERE Timestamp >= {} AND Timestamp <= {}
            GROUP BY ServiceName
            ORDER BY span_count DESC"#,
            start, end
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                Some(ServiceSummary {
                    name: obj.get("name")?.as_str()?.to_string(),
                    span_count: obj.get("span_count")?.as_u64()?,
                    operation_count: obj.get("operation_count")?.as_u64()?,
                    error_rate: obj.get("error_rate")?.as_f64()?,
                    avg_duration_ns: obj.get("avg_duration_ns")?.as_i64()?,
                    p99_duration_ns: obj.get("p99_duration_ns").and_then(|v| v.as_i64()),
                    first_seen: chrono::DateTime::from_timestamp_micros(
                        obj.get("first_seen").and_then(|v| v.as_i64())?,
                    )
                    .unwrap_or_default(),
                    last_seen: chrono::DateTime::from_timestamp_micros(
                        obj.get("last_seen").and_then(|v| v.as_i64())?,
                    )
                    .unwrap_or_default(),
                })
            })
            .collect())
    }

    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>> {
        let (table, col) = match signal {
            Signal::Traces => ("otel_traces", "SpanAttributes"),
            Signal::Logs => ("otel_logs", "LogAttributes"),
            Signal::Metrics => ("otel_metrics", "Attributes"),
        };

        let service_clause = service
            .map(|s| format!("WHERE ServiceName = '{}'", escape_ch(s)))
            .unwrap_or_default();

        let sql = format!(
            "SELECT DISTINCT arrayJoin(mapKeys({})) as key FROM {} {} ORDER BY key LIMIT 1000",
            col, table, service_clause
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect())
    }

    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>> {
        let start = time_range.start.timestamp_micros();
        let end = time_range.end.timestamp_micros();
        let duration_secs = time_range.duration_secs().max(1) as f64;

        let sql = format!(
            r#"SELECT
                ServiceName as service,
                count() / {dur} as request_rate,
                countIf(StatusCode = 2) / count() as error_rate,
                avg(Duration) / 1000000.0 as avg_latency_ms,
                quantile(0.5)(Duration) / 1000000.0 as p50_latency_ms,
                quantile(0.95)(Duration) / 1000000.0 as p95_latency_ms,
                quantile(0.99)(Duration) / 1000000.0 as p99_latency_ms,
                countIf(StatusCode != 2) / {dur} as throughput
            FROM otel_traces
            WHERE Timestamp >= {start} AND Timestamp <= {end}
            GROUP BY ServiceName"#,
            dur = duration_secs,
            start = start,
            end = end,
        );

        let rows = self.query_json(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                Some(ServiceHealth {
                    service: obj.get("service")?.as_str()?.to_string(),
                    request_rate: obj.get("request_rate")?.as_f64()?,
                    error_rate: obj.get("error_rate")?.as_f64()?,
                    avg_latency_ms: obj.get("avg_latency_ms")?.as_f64()?,
                    p50_latency_ms: obj.get("p50_latency_ms")?.as_f64()?,
                    p95_latency_ms: obj.get("p95_latency_ms")?.as_f64()?,
                    p99_latency_ms: obj.get("p99_latency_ms")?.as_f64()?,
                    throughput: obj.get("throughput")?.as_f64()?,
                    apdex: None,
                })
            })
            .collect())
    }

    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison> {
        let service_clause = service
            .map(|s| format!("AND ServiceName = '{}'", escape_ch(s)))
            .unwrap_or_default();

        let build_period_query = |tr: &TimeRange| {
            let start = tr.start.timestamp_micros();
            let end = tr.end.timestamp_micros();
            let dur = tr.duration_secs().max(1) as f64;
            format!(
                r#"SELECT
                    count() as request_count,
                    countIf(StatusCode = 2) as error_count,
                    countIf(StatusCode = 2) / greatest(count(), 1) as error_rate,
                    avg(Duration) / 1000000.0 as avg_latency_ms,
                    count() / {} as throughput
                FROM otel_traces
                WHERE Timestamp >= {} AND Timestamp <= {} {}"#,
                dur, start, end, service_clause
            )
        };

        let rows_a = self.query_json(&build_period_query(&a)).await?;
        let rows_b = self.query_json(&build_period_query(&b)).await?;

        let parse_period = |rows: &[serde_json::Value], tr: TimeRange| -> PeriodMetrics {
            rows.first()
                .and_then(|r| r.as_object())
                .map(|obj| PeriodMetrics {
                    time_range: tr.clone(),
                    request_count: obj
                        .get("request_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    error_count: obj
                        .get("error_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    error_rate: obj.get("error_rate").and_then(|v| v.as_f64()).unwrap_or(0.0),
                    avg_latency_ms: obj
                        .get("avg_latency_ms")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    throughput: obj.get("throughput").and_then(|v| v.as_f64()).unwrap_or(0.0),
                })
                .unwrap_or(PeriodMetrics {
                    time_range: tr,
                    request_count: 0,
                    error_count: 0,
                    error_rate: 0.0,
                    avg_latency_ms: 0.0,
                    throughput: 0.0,
                })
        };

        let pa = parse_period(&rows_a, a);
        let pb = parse_period(&rows_b, b);

        Ok(Comparison {
            error_rate_delta: pb.error_rate - pa.error_rate,
            latency_delta_ms: pb.avg_latency_ms - pa.avg_latency_ms,
            throughput_delta: pb.throughput - pa.throughput,
            request_count_delta: pb.request_count as i64 - pa.request_count as i64,
            period_a: pa,
            period_b: pb,
        })
    }

    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        let sql = if query.to_uppercase().contains("LIMIT") {
            query.to_string()
        } else {
            format!("{} LIMIT {}", query, limit)
        };

        self.query_json(&sql).await
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
            .query_json("SELECT DISTINCT MetricName FROM otel_metrics ORDER BY MetricName LIMIT 1000")
            .await
            .unwrap_or_default();
        let metric_names: Vec<String> = metric_rows
            .iter()
            .filter_map(|r| r.get("MetricName").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        let resource_rows = self
            .query_json(
                "SELECT DISTINCT arrayJoin(mapKeys(ResourceAttributes)) as key FROM otel_traces ORDER BY key LIMIT 1000",
            )
            .await
            .unwrap_or_default();
        let resource_keys: Vec<String> = resource_rows
            .iter()
            .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

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

#[async_trait]
impl IngestBackend for ClickHouseBackend {
    async fn ingest_span(&self, span: Span) -> Result<()> {
        self.ingest_spans(vec![span]).await
    }

    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let mut sql = String::from(
            "INSERT INTO otel_traces (TraceId, SpanId, ParentSpanId, ServiceName, SpanName, \
             Timestamp, Duration, StatusCode, SpanAttributes, ResourceAttributes) VALUES ",
        );

        let values: Vec<String> = spans
            .iter()
            .map(|s| {
                let attrs = map_to_ch_map(&s.attributes);
                let resource = map_to_ch_map(&s.resource_attrs);
                format!(
                    "('{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {})",
                    escape_ch(&s.trace_id),
                    escape_ch(&s.span_id),
                    s.parent_span_id
                        .as_deref()
                        .map(escape_ch)
                        .unwrap_or_default(),
                    escape_ch(&s.service_name),
                    escape_ch(&s.operation),
                    s.start_time,
                    s.duration_ns,
                    i32::from(s.status),
                    attrs,
                    resource,
                )
            })
            .collect();

        sql.push_str(&values.join(", "));
        self.execute(&sql).await?;

        tracing::debug!(count = spans.len(), "Inserted spans to ClickHouse");
        Ok(())
    }

    async fn ingest_log(&self, log: LogRecord) -> Result<()> {
        self.ingest_logs(vec![log]).await
    }

    async fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let mut sql = String::from(
            "INSERT INTO otel_logs (Timestamp, ObservedTimestamp, TraceId, SpanId, \
             SeverityNumber, SeverityText, Body, ServiceName, LogAttributes, ResourceAttributes) VALUES ",
        );

        let values: Vec<String> = logs
            .iter()
            .map(|l| {
                let attrs = map_to_ch_map(&l.attributes);
                let resource = map_to_ch_map(&l.resource_attrs);
                format!(
                    "({}, {}, '{}', '{}', {}, '{}', '{}', '{}', {}, {})",
                    l.timestamp,
                    l.observed_timestamp.unwrap_or(0),
                    l.trace_id.as_deref().map(escape_ch).unwrap_or_default(),
                    l.span_id.as_deref().map(escape_ch).unwrap_or_default(),
                    l.severity_number as i32,
                    l.severity_text.as_deref().map(escape_ch).unwrap_or_default(),
                    escape_ch(&l.body),
                    escape_ch(&l.service_name),
                    attrs,
                    resource,
                )
            })
            .collect();

        sql.push_str(&values.join(", "));
        self.execute(&sql).await?;

        tracing::debug!(count = logs.len(), "Inserted logs to ClickHouse");
        Ok(())
    }

    async fn ingest_metric(&self, metric: Metric) -> Result<()> {
        self.ingest_metrics(vec![metric]).await
    }

    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }

        let mut sql = String::from(
            "INSERT INTO otel_metrics (MetricName, Description, Unit, Timestamp, ServiceName, \
             Value, Sum, Count, Min, Max, Attributes, ResourceAttributes) VALUES ",
        );

        let values: Vec<String> = metrics
            .iter()
            .map(|m| {
                let attrs = map_to_ch_map(&m.attributes);
                let resource = map_to_ch_map(&m.resource_attrs);
                format!(
                    "('{}', '{}', '{}', {}, '{}', {}, {}, {}, {}, {}, {}, {})",
                    escape_ch(&m.name),
                    m.description.as_deref().map(escape_ch).unwrap_or_default(),
                    m.unit.as_deref().map(escape_ch).unwrap_or_default(),
                    m.timestamp,
                    escape_ch(&m.service_name),
                    m.value.unwrap_or(0.0),
                    m.sum.unwrap_or(0.0),
                    m.count.unwrap_or(0),
                    m.min.unwrap_or(0.0),
                    m.max.unwrap_or(0.0),
                    attrs,
                    resource,
                )
            })
            .collect();

        sql.push_str(&values.join(", "));
        self.execute(&sql).await?;

        tracing::debug!(count = metrics.len(), "Inserted metrics to ClickHouse");
        Ok(())
    }
}

#[async_trait]
impl ManagedBackend for ClickHouseBackend {
    async fn start_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn stop_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        self.ping().await
    }
}

/// Convert Vec<(String, String)> to ClickHouse map literal
fn map_to_ch_map(pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return "map()".to_string();
    }
    let entries: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("'{}', '{}'", escape_ch(k), escape_ch(v)))
        .collect();
    format!("map({})", entries.join(", "))
}
