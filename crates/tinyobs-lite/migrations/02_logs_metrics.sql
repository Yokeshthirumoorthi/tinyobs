-- Logs table for storing OTLP log records
CREATE TABLE logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,              -- unix micros
    observed_timestamp INTEGER,              -- unix micros (when collected)
    trace_id TEXT,                           -- for correlation with traces
    span_id TEXT,                            -- for correlation with spans
    severity_number INTEGER NOT NULL DEFAULT 0,
    severity_text TEXT,
    body TEXT NOT NULL,
    service_name TEXT NOT NULL,
    attributes TEXT,                         -- JSON object
    resource_attrs TEXT,                     -- JSON object
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

-- Index for service-based queries
CREATE INDEX idx_logs_service ON logs(service_name);

-- Index for time-based queries and compaction
CREATE INDEX idx_logs_timestamp ON logs(timestamp);

-- Index for trace correlation
CREATE INDEX idx_logs_trace ON logs(trace_id);

-- Index for severity-based queries
CREATE INDEX idx_logs_severity ON logs(severity_number);

-- Index for created_at (compaction)
CREATE INDEX idx_logs_created ON logs(created_at);

-- Metrics table for storing OTLP metric data points
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    kind TEXT NOT NULL,                      -- gauge, counter, histogram, summary
    timestamp INTEGER NOT NULL,              -- unix micros
    service_name TEXT NOT NULL,
    value REAL,                              -- for gauge/counter
    sum REAL,                                -- for histogram/summary
    count INTEGER,                           -- for histogram/summary
    min REAL,                                -- for histogram
    max REAL,                                -- for histogram
    quantiles TEXT,                          -- JSON array of [quantile, value] pairs
    buckets TEXT,                            -- JSON array of [upper_bound, count] pairs
    attributes TEXT,                         -- JSON object (labels)
    resource_attrs TEXT,                     -- JSON object
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

-- Index for service-based queries
CREATE INDEX idx_metrics_service ON metrics(service_name);

-- Index for metric name queries
CREATE INDEX idx_metrics_name ON metrics(name);

-- Index for time-based queries and compaction
CREATE INDEX idx_metrics_timestamp ON metrics(timestamp);

-- Index for created_at (compaction)
CREATE INDEX idx_metrics_created ON metrics(created_at);

-- Composite index for common metric queries
CREATE INDEX idx_metrics_name_time ON metrics(name, timestamp);
