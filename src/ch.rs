//! Shared ClickHouse SQL builders and JSON row parsers
//!
//! Used by both pro (remote ClickHouse via HTTP) and lite (embedded chdb) binaries.
//! All functions are pure — no network or database dependencies.

use crate::backend::*;
use crate::schema::{LogRecord, Metric, MetricKind, Span, SpanStatus, SeverityLevel};

// ============================================================================
// DDL constants — MergeTree CREATE TABLE statements
// ============================================================================

pub const CREATE_OTEL_TRACES: &str = r#"
CREATE TABLE IF NOT EXISTS otel_traces (
    TraceId String,
    SpanId String,
    ParentSpanId String,
    ServiceName LowCardinality(String),
    SpanName LowCardinality(String),
    Timestamp Int64,
    Duration Int64,
    StatusCode Int32,
    SpanAttributes Map(String, String),
    ResourceAttributes Map(String, String)
) ENGINE = MergeTree()
ORDER BY (ServiceName, Timestamp, TraceId)
"#;

pub const CREATE_OTEL_LOGS: &str = r#"
CREATE TABLE IF NOT EXISTS otel_logs (
    Timestamp Int64,
    ObservedTimestamp Int64,
    TraceId String,
    SpanId String,
    SeverityNumber Int32,
    SeverityText LowCardinality(String),
    Body String,
    ServiceName LowCardinality(String),
    LogAttributes Map(String, String),
    ResourceAttributes Map(String, String)
) ENGINE = MergeTree()
ORDER BY (ServiceName, Timestamp)
"#;

pub const CREATE_OTEL_METRICS: &str = r#"
CREATE TABLE IF NOT EXISTS otel_metrics (
    MetricName LowCardinality(String),
    Description String,
    Unit String,
    Timestamp Int64,
    ServiceName LowCardinality(String),
    Value Float64,
    Sum Float64,
    Count UInt64,
    Min Float64,
    Max Float64,
    Attributes Map(String, String),
    ResourceAttributes Map(String, String)
) ENGINE = MergeTree()
ORDER BY (ServiceName, MetricName, Timestamp)
"#;

// ============================================================================
// Helpers
// ============================================================================

/// Escape single quotes for ClickHouse SQL literals
pub fn escape_ch(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Format a Vec<(K,V)> as a ClickHouse map literal `{'k1':'v1','k2':'v2'}`
pub fn attrs_to_ch_map(attrs: &[(String, String)]) -> String {
    if attrs.is_empty() {
        return "map()".to_string();
    }
    let pairs: Vec<String> = attrs
        .iter()
        .map(|(k, v)| format!("'{}':'{}'", escape_ch(k), escape_ch(v)))
        .collect();
    format!("{{{}}}", pairs.join(","))
}

/// Parse JSONEachRow body into a Vec of serde_json::Value
pub fn parse_json_rows(body: &str) -> Vec<serde_json::Value> {
    body.lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

// ============================================================================
// SQL query builders
// ============================================================================

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

pub fn build_query_spans_sql(filter: &SpanFilter) -> String {
    let where_clause = build_span_where_clause(filter);
    let order = filter
        .order_by
        .as_ref()
        .map(|o| {
            let dir = if o.descending { "DESC" } else { "ASC" };
            format!("ORDER BY {} {}", o.field, dir)
        })
        .unwrap_or_else(|| "ORDER BY Timestamp DESC".to_string());

    format!(
        "SELECT * FROM otel_traces {} {} LIMIT {} OFFSET {}",
        where_clause, order, filter.limit, filter.offset
    )
}

pub fn build_query_logs_sql(filter: &LogFilter) -> String {
    let where_clause = build_log_where_clause(filter);
    format!(
        "SELECT * FROM otel_logs {} ORDER BY Timestamp DESC LIMIT {} OFFSET {}",
        where_clause, filter.limit, filter.offset
    )
}

pub fn build_query_metrics_sql(filter: &MetricFilter) -> String {
    let where_clause = build_metric_where_clause(filter);

    if let (Some(agg), Some(bucket)) = (&filter.aggregation, &filter.bucket_secs) {
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
    }
}

pub fn build_get_trace_sql(trace_id: &str) -> String {
    format!(
        "SELECT * FROM otel_traces WHERE TraceId = '{}' ORDER BY Timestamp ASC",
        escape_ch(trace_id)
    )
}

pub fn build_get_session_sql(session_id: &str) -> String {
    format!(
        "SELECT * FROM otel_traces WHERE SpanAttributes['session.id'] = '{}' ORDER BY Timestamp ASC",
        escape_ch(session_id)
    )
}

pub fn build_list_services_sql(time_range: &TimeRange) -> String {
    let start = time_range.start.timestamp_micros();
    let end = time_range.end.timestamp_micros();

    format!(
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
    )
}

pub fn build_attribute_keys_sql(signal: Signal, service: Option<&str>) -> String {
    let (table, col) = match signal {
        Signal::Traces => ("otel_traces", "SpanAttributes"),
        Signal::Logs => ("otel_logs", "LogAttributes"),
        Signal::Metrics => ("otel_metrics", "Attributes"),
    };

    let service_clause = service
        .map(|s| format!("WHERE ServiceName = '{}'", escape_ch(s)))
        .unwrap_or_default();

    format!(
        "SELECT DISTINCT arrayJoin(mapKeys({})) as key FROM {} {} ORDER BY key LIMIT 1000",
        col, table, service_clause
    )
}

pub fn build_service_health_sql(time_range: &TimeRange) -> String {
    let start = time_range.start.timestamp_micros();
    let end = time_range.end.timestamp_micros();
    let duration_secs = time_range.duration_secs().max(1) as f64;

    format!(
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
    )
}

pub fn build_compare_period_sql(tr: &TimeRange, service: Option<&str>) -> String {
    let start = tr.start.timestamp_micros();
    let end = tr.end.timestamp_micros();
    let dur = tr.duration_secs().max(1) as f64;

    let service_clause = service
        .map(|s| format!("AND ServiceName = '{}'", escape_ch(s)))
        .unwrap_or_default();

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
}

pub fn build_raw_query_sql(query: &str, limit: usize) -> String {
    if query.to_uppercase().contains("LIMIT") {
        query.to_string()
    } else {
        format!("{} LIMIT {}", query, limit)
    }
}

pub fn build_discover_schema_metric_names_sql() -> String {
    "SELECT DISTINCT MetricName FROM otel_metrics ORDER BY MetricName LIMIT 1000".to_string()
}

pub fn build_discover_schema_resource_keys_sql() -> String {
    "SELECT DISTINCT arrayJoin(mapKeys(ResourceAttributes)) as key FROM otel_traces ORDER BY key LIMIT 1000".to_string()
}

// ============================================================================
// SQL insert builders (for chdb's SQL-based inserts)
// ============================================================================

pub fn build_insert_spans_sql(spans: &[Span]) -> String {
    if spans.is_empty() {
        return String::new();
    }
    let mut sql = String::from(
        "INSERT INTO otel_traces (TraceId, SpanId, ParentSpanId, ServiceName, SpanName, \
         Timestamp, Duration, StatusCode, SpanAttributes, ResourceAttributes) VALUES ",
    );
    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "('{}','{}','{}','{}','{}',{},{},{},{},{})",
            escape_ch(&s.trace_id),
            escape_ch(&s.span_id),
            escape_ch(s.parent_span_id.as_deref().unwrap_or("")),
            escape_ch(&s.service_name),
            escape_ch(&s.operation),
            s.start_time,
            s.duration_ns,
            i32::from(s.status),
            attrs_to_ch_map(&s.attributes),
            attrs_to_ch_map(&s.resource_attrs),
        ));
    }
    sql
}

pub fn build_insert_logs_sql(logs: &[LogRecord]) -> String {
    if logs.is_empty() {
        return String::new();
    }
    let mut sql = String::from(
        "INSERT INTO otel_logs (Timestamp, ObservedTimestamp, TraceId, SpanId, \
         SeverityNumber, SeverityText, Body, ServiceName, LogAttributes, ResourceAttributes) VALUES ",
    );
    for (i, l) in logs.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "({},{},'{}','{}',{},'{}','{}','{}',{},{})",
            l.timestamp,
            l.observed_timestamp.unwrap_or(0),
            escape_ch(l.trace_id.as_deref().unwrap_or("")),
            escape_ch(l.span_id.as_deref().unwrap_or("")),
            l.severity_number as i32,
            escape_ch(l.severity_text.as_deref().unwrap_or("")),
            escape_ch(&l.body),
            escape_ch(&l.service_name),
            attrs_to_ch_map(&l.attributes),
            attrs_to_ch_map(&l.resource_attrs),
        ));
    }
    sql
}

pub fn build_insert_metrics_sql(metrics: &[Metric]) -> String {
    if metrics.is_empty() {
        return String::new();
    }
    let mut sql = String::from(
        "INSERT INTO otel_metrics (MetricName, Description, Unit, Timestamp, ServiceName, \
         Value, Sum, Count, Min, Max, Attributes, ResourceAttributes) VALUES ",
    );
    for (i, m) in metrics.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "('{}','{}','{}',{},'{}',{},{},{},{},{},{},{})",
            escape_ch(&m.name),
            escape_ch(m.description.as_deref().unwrap_or("")),
            escape_ch(m.unit.as_deref().unwrap_or("")),
            m.timestamp,
            escape_ch(&m.service_name),
            m.value.unwrap_or(0.0),
            m.sum.unwrap_or(0.0),
            m.count.unwrap_or(0),
            m.min.unwrap_or(0.0),
            m.max.unwrap_or(0.0),
            attrs_to_ch_map(&m.attributes),
            attrs_to_ch_map(&m.resource_attrs),
        ));
    }
    sql
}

// ============================================================================
// Row parsers — JSON → domain types
// ============================================================================

pub fn row_to_span(row: &serde_json::Value) -> Option<Span> {
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

pub fn row_to_log(row: &serde_json::Value) -> Option<LogRecord> {
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

pub fn parse_service_summaries(rows: &[serde_json::Value]) -> Vec<ServiceSummary> {
    rows.iter()
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
        .collect()
}

pub fn parse_service_health(rows: &[serde_json::Value]) -> Vec<ServiceHealth> {
    rows.iter()
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
        .collect()
}

pub fn parse_period_metrics(rows: &[serde_json::Value], tr: TimeRange) -> PeriodMetrics {
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
}

pub fn parse_metric_rows(rows: &[serde_json::Value], aggregated: bool) -> Vec<Metric> {
    if aggregated {
        rows.iter()
            .filter_map(|r| {
                let obj = r.as_object()?;
                Some(Metric {
                    name: obj.get("name")?.as_str()?.to_string(),
                    description: None,
                    unit: None,
                    kind: MetricKind::Gauge,
                    timestamp: obj.get("bucket").and_then(|v| v.as_i64()).unwrap_or(0),
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
            .collect()
    } else {
        rows.iter()
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
                        .and_then(|v| {
                            v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                        })
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
            .collect()
    }
}

pub fn parse_attribute_keys(rows: &[serde_json::Value]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect()
}

pub fn parse_metric_names(rows: &[serde_json::Value]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| {
            r.get("MetricName")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

pub fn build_comparison(pa: PeriodMetrics, pb: PeriodMetrics) -> Comparison {
    Comparison {
        error_rate_delta: pb.error_rate - pa.error_rate,
        latency_delta_ms: pb.avg_latency_ms - pa.avg_latency_ms,
        throughput_delta: pb.throughput - pa.throughput,
        request_count_delta: pb.request_count as i64 - pa.request_count as i64,
        period_a: pa,
        period_b: pb,
    }
}
