# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) where applicable.

## [0.3.0] - 2025-10-20
### Added
- Pluggable data-provider layer that decouples routes from the legacy SQLite repositories and prepares the web UI for a Meshpipe gRPC backend.
- Configuration flags and documented environment variables for Meshpipe gRPC connectivity (`MALLA_MESHPIPE_USE_GRPC`, `MALLA_MESHPIPE_GRPC_ENDPOINT`, `MALLA_MESHPIPE_GRPC_USE_PROXY`, `MALLA_MESHPIPE_GRPC_PROXY_ENDPOINT`, `MALLA_MESHPIPE_GRPC_TOKEN`, `MALLA_MESHPIPE_GRPC_TIMEOUT_SECONDS`).

### Changed
- Application startup now gates data access behind the `meshpipe_use_grpc` flag, defaulting to SQLite until the Meshpipe gRPC provider ships a production implementation.

[0.3.0]: https://github.com/aminovpavel/meshworks-malla/compare/v0.2.0...v0.3.0
