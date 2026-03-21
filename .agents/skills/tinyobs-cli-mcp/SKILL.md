---
name: tinyobs-cli-mcp
description: Use when working on the CLI, MCP server, HTTP client library, or downstream composability
metadata:
  internal: true
---

# TinyObs CLI, MCP & Client

## Library-First Composability

All three layers are library modules that downstream crates can compose:

| Module | Feature | Purpose |
|--------|---------|---------|
| `src/client.rs` | `client` | `TinyObsClient` — HTTP client for tinyobs API |
| `src/cli.rs` | `cli` | Composable CLI commands (clap) |
| `src/mcp.rs` | `mcp` | Composable MCP tools (rmcp) |
| `src/bin/tinyobs-cli.rs` | `cli` | Standalone CLI binary |
| `src/bin/tinyobs-mcp.rs` | `mcp` | Standalone MCP server binary |

## Client (`src/client.rs`)

```rust
use tinyobs::client::TinyObsClient;
let client = TinyObsClient::new("http://localhost:4318");
```

Methods: `health()`, `list_traces()`, `get_trace()`, `list_logs()`, `list_metrics()`, `list_services()`, `raw_query()`, `discover_schema()`

## CLI (`src/cli.rs`)

Downstream extension pattern:
```rust
use tinyobs::cli::{base_commands, handle_command};
let cli = Command::new("myapp")
    .subcommands(base_commands())
    .subcommand(my_custom_command());
```

Commands: `health`, `services`, `traces`, `trace`, `logs`, `metrics`, `query`, `schema`, `version`

Global flags: `--endpoint URL`, `--format json|table`

## MCP Server (`src/mcp.rs`)

Uses `rmcp` (official Rust MCP SDK). Tools are defined with `#[tool]` / `#[tool_router]` macros.

Downstream extension pattern:
```rust
use tinyobs::mcp::TinyObsTools;
let tools = TinyObsTools::new(client);
// Compose with custom tools in your own #[tool_router] impl
```

MCP tools: `list_services`, `get_traces`, `get_trace`, `get_logs`, `get_metrics`, `run_query`, `health`, `discover_schema`

## MCP Setup

Configure in Claude Desktop or Cursor:
```json
{
  "mcpServers": {
    "tinyobs": {
      "command": "/path/to/tinyobs-mcp",
      "args": ["--endpoint", "http://localhost:4318"]
    }
  }
}
```
