//! Telemetry setup for TinyObs
//!
//! Provides Z2P-style tracing subscriber configuration with support for
//! both JSON (production) and pretty (development) log formatting.

use tracing::Subscriber;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt, EnvFilter, Registry};

/// Create a subscriber with JSON/Bunyan formatting (for production)
///
/// Uses `RUST_LOG` environment variable if set, otherwise falls back to `env_filter`.
pub fn get_subscriber<Sink>(
    name: &str,
    env_filter: &str,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));

    let formatting_layer = BunyanFormattingLayer::new(name.into(), sink);

    Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer)
}

/// Create a subscriber with pretty formatting (for development)
///
/// Uses `RUST_LOG` environment variable if set, otherwise falls back to `env_filter`.
pub fn get_subscriber_pretty(name: &str, env_filter: &str) -> impl Subscriber + Send + Sync {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));

    let _ = name; // Not used for pretty output, but kept for API consistency

    Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().pretty())
}

/// Initialize the global tracing subscriber
///
/// # Panics
/// Panics if a global subscriber has already been set.
pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}
