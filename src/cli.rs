//! CLI commands as a library module
//!
//! Exposes composable CLI commands that downstream crates can extend.
//!
//! # Example
//!
//! ```no_run
//! use clap::Command;
//! use tinyobs::cli;
//!
//! let app = Command::new("myapp")
//!     .subcommands(cli::base_commands());
//! ```

use crate::client::TinyObsClient;
use anyhow::Result;
use clap::{Arg, ArgMatches, Command};

/// Default tinyobs server endpoint
pub const DEFAULT_ENDPOINT: &str = "http://localhost:4318";

/// Build the full CLI app with all base commands + global flags
pub fn build_cli() -> Command {
    Command::new("tinyobs-cli")
        .about("TinyObs CLI — query and manage your observability data")
        .arg(
            Arg::new("endpoint")
                .long("endpoint")
                .short('e')
                .global(true)
                .default_value(DEFAULT_ENDPOINT)
                .help("TinyObs server URL"),
        )
        .arg(
            Arg::new("format")
                .long("format")
                .short('f')
                .global(true)
                .default_value("table")
                .value_parser(["table", "json"])
                .help("Output format"),
        )
        .subcommands(base_commands())
}

/// Returns the base tinyobs subcommands.
///
/// Downstream crates can compose these with their own commands:
/// ```no_run
/// use clap::Command;
/// use tinyobs::cli::base_commands;
///
/// let app = Command::new("myapp")
///     .subcommands(base_commands())
///     .subcommand(Command::new("custom").about("My custom command"));
/// ```
pub fn base_commands() -> Vec<Command> {
    vec![
        Command::new("health").about("Check server health"),
        Command::new("services").about("List all observed services"),
        Command::new("traces")
            .about("List recent traces")
            .arg(Arg::new("service").long("service").short('s').help("Filter by service name"))
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .default_value("20")
                    .value_parser(clap::value_parser!(usize))
                    .help("Max results"),
            ),
        Command::new("trace")
            .about("Get all spans for a trace")
            .arg(Arg::new("trace_id").required(true).help("Trace ID")),
        Command::new("logs")
            .about("Query logs")
            .arg(Arg::new("service").long("service").short('s').help("Filter by service name"))
            .arg(Arg::new("severity").long("severity").help("Filter by severity (e.g. ERROR, WARN)"))
            .arg(Arg::new("trace-id").long("trace-id").help("Filter by trace ID"))
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .default_value("20")
                    .value_parser(clap::value_parser!(usize))
                    .help("Max results"),
            ),
        Command::new("metrics")
            .about("Query metrics")
            .arg(Arg::new("service").long("service").short('s').help("Filter by service name"))
            .arg(Arg::new("name").long("name").short('n').help("Filter by metric name"))
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .default_value("20")
                    .value_parser(clap::value_parser!(usize))
                    .help("Max results"),
            ),
        Command::new("query")
            .about("Execute raw SQL query")
            .arg(Arg::new("sql").required(true).help("SQL query string"))
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .default_value("100")
                    .value_parser(clap::value_parser!(usize))
                    .help("Max results"),
            ),
        Command::new("schema").about("Discover database schema"),
        Command::new("version").about("Show version information"),
    ]
}

/// Execute a matched tinyobs subcommand against a TinyObsClient.
///
/// Returns `Ok(true)` if a command was handled, `Ok(false)` if the subcommand
/// was not recognized (allowing downstream to handle it).
pub async fn handle_command(
    client: &TinyObsClient,
    matches: &ArgMatches,
    format: OutputFormat,
) -> Result<bool> {
    match matches.subcommand() {
        Some(("health", _)) => {
            let health = client.health().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&health)?),
                OutputFormat::Table => {
                    println!("Status:  {}", health.status);
                    println!("Backend: {}", if health.backend { "connected" } else { "disconnected" });
                }
            }
        }
        Some(("services", _)) => {
            let services = client.list_services().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&services)?),
                OutputFormat::Table => {
                    if services.is_empty() {
                        println!("No services found");
                    } else {
                        println!(
                            "{:<30} {:>8} {:>8} {:>12} {:>10}",
                            "SERVICE", "SPANS", "OPS", "ERROR%", "AVG(ms)"
                        );
                        for s in &services {
                            println!(
                                "{:<30} {:>8} {:>8} {:>11.1}% {:>10.1}",
                                s.name,
                                s.span_count,
                                s.operation_count,
                                s.error_rate * 100.0,
                                s.avg_duration_ns as f64 / 1_000_000.0
                            );
                        }
                    }
                }
            }
        }
        Some(("traces", sub)) => {
            let service = sub.get_one::<String>("service").map(|s| s.as_str());
            let limit = sub.get_one::<usize>("limit").copied();
            let spans = client.list_traces(service, limit).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&spans)?),
                OutputFormat::Table => {
                    if spans.is_empty() {
                        println!("No traces found");
                    } else {
                        println!(
                            "{:<32} {:<25} {:<30} {:>10}",
                            "TRACE ID", "SERVICE", "OPERATION", "DURATION(ms)"
                        );
                        for s in &spans {
                            println!(
                                "{:<32} {:<25} {:<30} {:>10.1}",
                                s.trace_id,
                                s.service_name,
                                s.operation,
                                s.duration_ns as f64 / 1_000_000.0
                            );
                        }
                    }
                }
            }
        }
        Some(("trace", sub)) => {
            let trace_id = sub.get_one::<String>("trace_id").unwrap();
            let spans = client.get_trace(trace_id).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&spans)?),
                OutputFormat::Table => {
                    if spans.is_empty() {
                        println!("No spans found for trace {trace_id}");
                    } else {
                        println!(
                            "{:<20} {:<25} {:<30} {:>10} {:>8}",
                            "SPAN ID", "SERVICE", "OPERATION", "DUR(ms)", "STATUS"
                        );
                        for s in &spans {
                            println!(
                                "{:<20} {:<25} {:<30} {:>10.1} {:>8}",
                                s.span_id,
                                s.service_name,
                                s.operation,
                                s.duration_ns as f64 / 1_000_000.0,
                                format!("{:?}", s.status),
                            );
                        }
                    }
                }
            }
        }
        Some(("logs", sub)) => {
            let service = sub.get_one::<String>("service").map(|s| s.as_str());
            let severity = sub.get_one::<String>("severity").map(|s| s.as_str());
            let trace_id = sub.get_one::<String>("trace-id").map(|s| s.as_str());
            let limit = sub.get_one::<usize>("limit").copied();
            let logs = client.list_logs(service, severity, trace_id, None, limit).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&logs)?),
                OutputFormat::Table => {
                    if logs.is_empty() {
                        println!("No logs found");
                    } else {
                        println!(
                            "{:<10} {:<25} {:<60}",
                            "SEVERITY", "SERVICE", "BODY"
                        );
                        for l in &logs {
                            let body_preview: String = l.body.chars().take(57).collect();
                            let body_display = if l.body.len() > 57 {
                                format!("{body_preview}...")
                            } else {
                                body_preview
                            };
                            println!(
                                "{:<10} {:<25} {:<60}",
                                l.severity_number.as_str(),
                                l.service_name,
                                body_display,
                            );
                        }
                    }
                }
            }
        }
        Some(("metrics", sub)) => {
            let service = sub.get_one::<String>("service").map(|s| s.as_str());
            let name = sub.get_one::<String>("name").map(|s| s.as_str());
            let limit = sub.get_one::<usize>("limit").copied();
            let metrics = client.list_metrics(service, name, limit).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&metrics)?),
                OutputFormat::Table => {
                    if metrics.is_empty() {
                        println!("No metrics found");
                    } else {
                        println!(
                            "{:<30} {:<25} {:<10} {:>12}",
                            "NAME", "SERVICE", "KIND", "VALUE"
                        );
                        for m in &metrics {
                            println!(
                                "{:<30} {:<25} {:<10} {:>12.2}",
                                m.name,
                                m.service_name,
                                format!("{:?}", m.kind),
                                m.value.unwrap_or(0.0),
                            );
                        }
                    }
                }
            }
        }
        Some(("query", sub)) => {
            let sql = sub.get_one::<String>("sql").unwrap();
            let limit = sub.get_one::<usize>("limit").copied();
            let rows = client.raw_query(sql, limit).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&rows)?),
                OutputFormat::Table => {
                    if rows.is_empty() {
                        println!("(empty result)");
                    } else {
                        // Print as JSON lines for non-structured data
                        for row in &rows {
                            println!("{}", serde_json::to_string(row)?);
                        }
                    }
                }
            }
        }
        Some(("schema", _)) => {
            let schema = client.discover_schema().await?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
        Some(("version", _)) => {
            println!("tinyobs-cli {}", env!("CARGO_PKG_VERSION"));
            match client.health().await {
                Ok(h) => println!("server:     {} ({})", h.status, if h.backend { "connected" } else { "disconnected" }),
                Err(_) => println!("server:     unreachable"),
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Output format for CLI commands
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Json,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s {
            "json" => OutputFormat::Json,
            _ => OutputFormat::Table,
        }
    }
}
