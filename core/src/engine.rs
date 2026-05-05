//! Engine module — re-exports nothing.
//!
//! Engine functionality is accessed through [`crate::RdmaServerProvider`]:
//! - `worker_count()` — number of workers (1 = no engine, > 1 = engine)
//! - `open_channel()` — canonical server API (shared by engines and direct providers)
//!
//! Set `pool_count > 0` in [`crate::provider::RdmaServerConfig`] when calling
//! [`crate::create_provider`] to get an engine-backed provider.
//!
//! Concrete engine types (`MockRdmaEngine`, `CuObjEngine`) live in their
//! respective crates and are not part of the public API.
