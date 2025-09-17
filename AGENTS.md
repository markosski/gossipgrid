# Repository Guidelines

## Project Structure & Module Organization
- Root hosts the workspace `Cargo.toml`, helper scripts, and docs like `gossip.md`.
- Core crate lives in `gossipgrid/`; `src/` holds modules such as `web.rs`, `gossip.rs`, and the CLI entry point `cli.rs`.
- Shared data structures reside under `src/store/`.
- End-to-end helpers and integration checks are in `gossipgrid/tests/`.
- Worked examples and experimental crates live under `examples/`; visualization assets sit in `viz/`.

## Build, Test, and Development Commands
- `cargo fmt` keeps Rust sources formatted to the repo standard.
- `cargo clippy --all-targets` runs lint checks across lib, bin, and tests.
- `cargo test` executes unit and integration suites.
- `RUST_LOG=info cargo run <web_port> [node_port] [seed_addr]` launches a node locally; scripts like `./start_local_cluster.sh` spin up multiple nodes.
- `./simulate.sh` runs a scripted cluster scenario useful for manual verification.

## Coding Style & Naming Conventions
- Use Rust 2024 edition defaults: 4-space indentation, snake_case for modules/functions, CamelCase for types.
- Derive traits where practical; prefer explicit `use` paths over glob imports.
- Keep modules small and cohesive; co-locate tests with the module in `node_tests.rs` when logic is tightly coupled.

## Testing Guidelines
- Favor lightweight unit tests near the code plus broader flows under `gossipgrid/tests/`.
- Name integration tests after the behavior under test, e.g., `int_test.rs` contains cluster-level scenarios.
- Ensure new features extend the simulated cluster checks or add fixtures under `tests/helpers.rs`.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative summaries (e.g., “add initial int tests”); include context for multi-file changes in the body when needed.
- PRs should describe the change, list manual verification steps, and link tracking issues.
- Attach logs or screenshots when touching CLI output or HTTP behavior; call out breaking changes clearly.

## Security & Configuration Notes
- Avoid committing `.env` overrides or cluster secrets; rely on `env.rs` helpers for configuration defaults.
- Use `RUST_LOG` levels responsibly—debug logs should not leak sensitive payloads.
