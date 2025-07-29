# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Build and Run
- `cargo build` - Build the server in debug mode
- `cargo build --release` - Build in release mode
- `cargo run` - Run the server locally
- `docker compose up relay db` - Start local dependencies (Neo4j and Nostr relay)
- `docker compose up --build` - Run complete stack including server

### Testing
- `cargo test` - Run all tests
- `cargo test -- --nocapture` - Run tests with println! output visible
- `cargo test [test_name]` - Run specific test

### Linting and Formatting
- `cargo fmt` - Format code according to Rust standards
- `cargo clippy` - Run linter for common mistakes
- `cargo check` - Check code compiles without building

### Deployment
- `./scripts/tag_latest_as_stable.sh` - Promote latest Docker image to stable (triggers deployment)

## Architecture Overview

This server listens to Nostr Follow List (Kind 3) events and tracks follow/unfollow actions. Key architectural components:

### Core Flow
1. **Event Ingestion**: Subscribes to Nostr relays for ContactList events via WebSocket
2. **Follow Diffing**: Compares new contact lists with stored data to detect changes
3. **Change Processing**: Publishes follow/unfollow events to Google Pub/Sub
4. **Data Storage**: Uses Neo4j graph database for storing follower relationships

### Key Components

- **Main Entry** (`src/main.rs`): Initializes all services and manages graceful shutdown
- **WorkerPool**: Concurrent processing using broadcast channels and task workers
- **FollowsDiffer** (`src/domain/follows_differ.rs`): Calculates follow list changes
- **FollowChangeHandler** (`src/follow_change_handler.rs`): Processes and publishes changes
- **Repo** (`src/repo.rs`): Neo4j database interface with caching
- **HTTP Server** (`src/http_server/`): REST API endpoints for health, metrics, and queries
- **RelaySubscriber** (`src/relay_subscriber.rs`): WebSocket connection to Nostr relays

### External Dependencies
- **Neo4j**: Graph database for storing follower relationships
- **Redis**: Caching and vanish subscriber functionality
- **Google Pub/Sub**: Message queue for follow change notifications
- **Nostr Relay**: Source of ContactList events

### Configuration
- Settings loaded from `config/settings.yml` and `config/settings.development.yml`
- Environment variable `APP__ENVIRONMENT` controls which config to use
- Key configs: Neo4j connection, relay URL, worker counts, channel sizes

### Database Migrations
- Cypher migrations in `migrations/` directory
- Applied automatically on startup via `apply_migrations()`