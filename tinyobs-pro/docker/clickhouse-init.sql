-- ClickHouse schema for tinyobs-pro OTLP storage
-- Auto-executed on first start via /docker-entrypoint-initdb.d/

-- Traces table (OTEL standard schema)
CREATE TABLE IF NOT EXISTS otel_traces
(
    Timestamp          Int64,          -- microseconds since epoch
    TraceId            String,
    SpanId             String,
    ParentSpanId       String          DEFAULT '',
    ServiceName        LowCardinality(String),
    SpanName           String,
    Duration           Int64,          -- nanoseconds
    StatusCode         Int32           DEFAULT 0,
    StatusMessage      String          DEFAULT '',
    SpanAttributes     Map(String, String),
    ResourceAttributes Map(String, String),
    Events             Nested (
        Timestamp      Int64,
        Name           String,
        Attributes     Map(String, String)
    ),
    Links              Nested (
        TraceId        String,
        SpanId         String,
        TraceState     String,
        Attributes     Map(String, String)
    )
)
ENGINE = MergeTree()
PARTITION BY toDate(fromUnixTimestamp64Micro(Timestamp))
ORDER BY (ServiceName, SpanName, toDateTime(fromUnixTimestamp64Micro(Timestamp)))
TTL toDateTime(fromUnixTimestamp64Micro(Timestamp)) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Secondary indexes for traces
ALTER TABLE otel_traces ADD INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE otel_traces ADD INDEX idx_duration Duration TYPE minmax GRANULARITY 1;

-- Logs table
CREATE TABLE IF NOT EXISTS otel_logs
(
    Timestamp          Int64,          -- microseconds since epoch
    ObservedTimestamp  Int64           DEFAULT 0,
    TraceId            String          DEFAULT '',
    SpanId             String          DEFAULT '',
    SeverityNumber     Int32           DEFAULT 0,
    SeverityText       LowCardinality(String) DEFAULT '',
    Body               String,
    ServiceName        LowCardinality(String),
    LogAttributes      Map(String, String),
    ResourceAttributes Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toDate(fromUnixTimestamp64Micro(Timestamp))
ORDER BY (ServiceName, SeverityText, toDateTime(fromUnixTimestamp64Micro(Timestamp)))
TTL toDateTime(fromUnixTimestamp64Micro(Timestamp)) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

ALTER TABLE otel_logs ADD INDEX idx_log_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE otel_logs ADD INDEX idx_log_body Body TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4;

-- Metrics table
CREATE TABLE IF NOT EXISTS otel_metrics
(
    MetricName         LowCardinality(String),
    Description        String          DEFAULT '',
    Unit               String          DEFAULT '',
    Timestamp          Int64,          -- microseconds since epoch
    ServiceName        LowCardinality(String),
    Value              Float64         DEFAULT 0,
    Sum                Float64         DEFAULT 0,
    Count              UInt64          DEFAULT 0,
    Min                Float64         DEFAULT 0,
    Max                Float64         DEFAULT 0,
    Attributes         Map(String, String),
    ResourceAttributes Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toDate(fromUnixTimestamp64Micro(Timestamp))
ORDER BY (ServiceName, MetricName, toDateTime(fromUnixTimestamp64Micro(Timestamp)))
TTL toDateTime(fromUnixTimestamp64Micro(Timestamp)) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
