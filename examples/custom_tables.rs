//! Extending TinyObs with custom tables alongside OTEL tables.
//!
//! Run with: cargo run --example custom_tables --features lite

use tinyobs::TinyObs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tinyobs = TinyObs::lite("./example-data")?;

    // Initialize standard OTEL tables
    tinyobs.init_schema().await?;

    // Create a custom table for your app's domain data
    tinyobs
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS app_events (
                timestamp Int64,
                user_id String,
                event_type LowCardinality(String),
                payload String
            ) ENGINE = MergeTree()
            ORDER BY (event_type, timestamp)
            "#,
        )
        .await?;

    // Insert custom data
    tinyobs
        .execute(
            r#"
            INSERT INTO app_events VALUES
                (1700000000000000, 'user-1', 'login', '{"ip": "192.168.1.1"}'),
                (1700000001000000, 'user-2', 'purchase', '{"item": "widget", "amount": 9.99}'),
                (1700000002000000, 'user-1', 'logout', '{}')
            "#,
        )
        .await?;

    // Query custom table
    println!("Custom events:");
    let events = tinyobs
        .query("SELECT * FROM app_events ORDER BY timestamp", 100)
        .await?;
    for event in &events {
        println!("  {}", serde_json::to_string(event)?);
    }

    // Query OTEL tables too (both coexist)
    let tables = tinyobs
        .query(
            "SELECT name FROM system.tables WHERE database = currentDatabase()",
            100,
        )
        .await?;
    println!("\nAll tables:");
    for table in &tables {
        println!("  {}", table["name"].as_str().unwrap_or("?"));
    }

    tinyobs.shutdown().await?;
    std::fs::remove_dir_all("./example-data").ok();

    Ok(())
}
