-- Spans table for storing OTLP trace data
CREATE TABLE spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    session_id TEXT,
    service_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    start_time INTEGER NOT NULL,      -- unix micros
    duration_ns INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    attributes TEXT,                   -- JSON object
    resource_attrs TEXT,               -- JSON object
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (trace_id, span_id)
);

-- Index for service-based queries
CREATE INDEX idx_spans_service ON spans(service_name);

-- Index for session-based queries
CREATE INDEX idx_spans_session ON spans(session_id);

-- Index for time-based queries and compaction
CREATE INDEX idx_spans_start ON spans(start_time);

-- Index for created_at (compaction)
CREATE INDEX idx_spans_created ON spans(created_at);
