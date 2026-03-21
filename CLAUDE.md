# Agent Instructions

## Build & Development

This repo uses a **justfile** for all build, test, lint, and dev commands. Run `just help` to see all available targets.

- Language: Rust (edition 2021)
- Package manager: cargo
- Feature flags control which binaries build: `lite` (default), `pro`, `cli`, `mcp`
