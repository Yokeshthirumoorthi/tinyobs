//! TinyObs Pro - Remote ClickHouse binary

use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use tinyobs::api_types::{HealthResponse, LogQuery, MetricQuery, RawQueryRequest, TraceQuery};
use tinyobs::backend::*;
use tinyobs::config::{ApplicationSettings, IngestConfig};
use tinyobs::server::{self, IngestState};
use tinyobs::transport::Transport;
use tinyobs::transport::remote::{
    BackendConfig, ChLogRow, ChMetricRow, ChSpanRow, RemoteTransport,
};

// ============================================================================
// Pro-specific config
// ============================================================================

#[derive(Debug, Clone, serde::Deserialize)]
struct ProConfig {
    #[serde(default)]
    application: ApplicationSettings,
    #[serde(default)]
    backend: BackendConfig,
    #[serde(default)]
    ingest: IngestConfig,
}

impl Default for ProConfig {
    fn default() -> Self {
        Self {
            application: ApplicationSettings::default(),
            backend: BackendConfig::default(),
            ingest: IngestConfig::default(),
        }
    }
}

// ============================================================================
// Pro backend: wraps ChBackend<RemoteTransport> with Inserter-based ingestion
// ============================================================================

/// Pro backend that uses typed Inserters for high-throughput ingestion
#[derive(Clone)]
struct ProBackend {
    inner: Arc<ChBackend<RemoteTransport>>,
}

impl ProBackend {
    fn new(transport: RemoteTransport) -> Self {
        Self {
            inner: Arc::new(ChBackend::new(transport)),
        }
    }
}

#[async_trait::async_trait]
impl TelemetryBackend for ProBackend {
    async fn query_spans(&self, filter: SpanFilter) -> Result<Vec<tinyobs::Span>> {
        self.inner.query_spans(filter).await
    }
    async fn query_logs(&self, filter: LogFilter) -> Result<Vec<tinyobs::LogRecord>> {
        self.inner.query_logs(filter).await
    }
    async fn query_metrics(&self, filter: MetricFilter) -> Result<Vec<tinyobs::Metric>> {
        self.inner.query_metrics(filter).await
    }
    async fn get_trace(&self, trace_id: &str) -> Result<Vec<tinyobs::Span>> {
        self.inner.get_trace(trace_id).await
    }
    async fn get_session(&self, session_id: &str) -> Result<Vec<tinyobs::Span>> {
        self.inner.get_session(session_id).await
    }
    async fn list_services(&self, time_range: TimeRange) -> Result<Vec<ServiceSummary>> {
        self.inner.list_services(time_range).await
    }
    async fn attribute_keys(&self, signal: Signal, service: Option<&str>) -> Result<Vec<String>> {
        self.inner.attribute_keys(signal, service).await
    }
    async fn service_health(&self, time_range: TimeRange) -> Result<Vec<ServiceHealth>> {
        self.inner.service_health(time_range).await
    }
    async fn compare_periods(
        &self,
        a: TimeRange,
        b: TimeRange,
        service: Option<&str>,
    ) -> Result<Comparison> {
        self.inner.compare_periods(a, b, service).await
    }
    async fn raw_query(&self, query: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        self.inner.raw_query(query, limit).await
    }
    async fn discover_schema(&self) -> Result<Schema> {
        self.inner.discover_schema().await
    }
}

/// Override IngestBackend to use typed Inserters instead of SQL INSERT VALUES
#[async_trait::async_trait]
impl IngestBackend for ProBackend {
    async fn ingest_span(&self, span: tinyobs::Span) -> Result<()> {
        self.ingest_spans(vec![span]).await
    }

    async fn ingest_spans(&self, spans: Vec<tinyobs::Span>) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        let mut inserter = self.inner.transport.spans.lock().await;
        for span in &spans {
            inserter.write(&ChSpanRow::from(span)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed spans");
        }
        Ok(())
    }

    async fn ingest_log(&self, log: tinyobs::LogRecord) -> Result<()> {
        self.ingest_logs(vec![log]).await
    }

    async fn ingest_logs(&self, logs: Vec<tinyobs::LogRecord>) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let mut inserter = self.inner.transport.logs.lock().await;
        for log in &logs {
            inserter.write(&ChLogRow::from(log)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed logs");
        }
        Ok(())
    }

    async fn ingest_metric(&self, metric: tinyobs::Metric) -> Result<()> {
        self.ingest_metrics(vec![metric]).await
    }

    async fn ingest_metrics(&self, metrics: Vec<tinyobs::Metric>) -> Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        let mut inserter = self.inner.transport.metrics.lock().await;
        for metric in &metrics {
            inserter.write(&ChMetricRow::from(metric)).await?;
        }
        let quant = inserter.commit().await?;
        if quant.transactions > 0 {
            tracing::debug!(rows = quant.rows, "Flushed metrics");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ManagedBackend for ProBackend {
    async fn start_background_tasks(&self) -> Result<()> {
        Ok(())
    }

    async fn stop_background_tasks(&self) -> Result<()> {
        self.inner.transport.spans.lock().await.force_commit().await?;
        self.inner.transport.logs.lock().await.force_commit().await?;
        self.inner.transport.metrics.lock().await.force_commit().await?;
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        self.inner.transport.ping().await
    }
}

// ============================================================================
// Query API types and handlers
// ============================================================================


async fn api_health(State(backend): State<ProBackend>) -> Json<HealthResponse> {
    let ch_ok = backend.health_check().await.unwrap_or(false);
    Json(HealthResponse {
        status: if ch_ok { "ok".to_string() } else { "degraded".to_string() },
        backend: ch_ok,
    })
}

async fn api_list_traces(
    State(backend): State<ProBackend>,
    Query(q): Query<TraceQuery>,
) -> Result<Json<Vec<tinyobs::Span>>, (StatusCode, String)> {
    let filter = SpanFilter {
        service: q.service,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let spans = backend
        .query_spans(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(spans))
}

async fn api_get_trace(
    State(backend): State<ProBackend>,
    Path(trace_id): Path<String>,
) -> Result<Json<Vec<tinyobs::Span>>, (StatusCode, String)> {
    let spans = backend
        .get_trace(&trace_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(spans))
}

async fn api_list_logs(
    State(backend): State<ProBackend>,
    Query(q): Query<LogQuery>,
) -> Result<Json<Vec<tinyobs::LogRecord>>, (StatusCode, String)> {
    let filter = LogFilter {
        service: q.service,
        severity: q.severity,
        trace_id: q.trace_id,
        body_contains: q.body_contains,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let logs = backend
        .query_logs(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(logs))
}

async fn api_list_metrics(
    State(backend): State<ProBackend>,
    Query(q): Query<MetricQuery>,
) -> Result<Json<Vec<tinyobs::Metric>>, (StatusCode, String)> {
    let filter = MetricFilter {
        service: q.service,
        name: q.name,
        limit: q.limit.unwrap_or(100),
        ..Default::default()
    };
    let metrics = backend
        .query_metrics(filter)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(metrics))
}

async fn api_list_services(
    State(backend): State<ProBackend>,
) -> Result<Json<Vec<ServiceSummary>>, (StatusCode, String)> {
    let time_range = TimeRange::last_day();
    let services = backend
        .list_services(time_range)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(services))
}

async fn api_raw_query(
    State(backend): State<ProBackend>,
    Json(request): Json<RawQueryRequest>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, String)> {
    let rows = backend
        .raw_query(&request.sql, request.limit)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(rows))
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    server::init_telemetry("tinyobs-pro");

    let config: ProConfig = tinyobs::config::get_configuration().unwrap_or_else(|e| {
        tracing::warn!("Failed to load configuration: {}, using defaults", e);
        ProConfig::default()
    });
    tracing::info!("Starting with config: {:?}", config);

    // Create remote transport + pro backend
    let transport = RemoteTransport::new(config.backend.clone());
    let backend = ProBackend::new(transport);

    // Wait for ClickHouse to be ready
    tracing::info!("Waiting for ClickHouse...");
    let mut attempts = 0;
    loop {
        if backend.health_check().await.unwrap_or(false) {
            tracing::info!("ClickHouse is ready");
            break;
        }
        attempts += 1;
        if attempts > 60 {
            anyhow::bail!("ClickHouse not ready after 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // Build ingest state
    let ingest_state = IngestState {
        backend: backend.clone(),
        config: Arc::new(config.ingest.clone()),
    };

    // Query API routes
    let api_router = Router::new()
        .route("/api/traces", get(api_list_traces))
        .route("/api/traces/{trace_id}", get(api_get_trace))
        .route("/api/logs", get(api_list_logs))
        .route("/api/metrics", get(api_list_metrics))
        .route("/api/services", get(api_list_services))
        .route("/api/health", get(api_health))
        .route("/api/query", post(api_raw_query))
        .with_state(backend.clone());

    let full_app = Router::new()
        .merge(server::ingest_router(ingest_state))
        .merge(api_router);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.application.port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("tinyobs-pro listening on :{}", config.application.port);

    axum::serve(listener, full_app).await?;

    backend.stop_background_tasks().await?;
    Ok(())
}
