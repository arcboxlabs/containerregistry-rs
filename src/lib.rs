//! Rust client libraries for OCI container registries
//! (a port of go-containerregistry).
//!
//! This crate re-exports the library crates for convenience:
//!
//! - [`image`] — Manifest, index, descriptor, digest, and platform types
//! - [`v1`] — Image, index and layer traits with `mutate`, `tarball`,
//!   `empty`, `random`, `partial`, `stream`, `validate` and `cache`
//! - [`layout`] — OCI image layout read/write, and layouts as v1 sources
//!   and sinks
//! - [`registry`] — HTTP registry client with streaming, ranged, resumable
//!   and parallel blob transfers, and `remote` v1 images
//! - [`auth`] — Keychains, credential helpers and Docker config handling
//! - `server` (feature `server`) — an in-memory or disk-backed registry

pub use containerregistry_auth as auth;
pub use containerregistry_image as image;
pub use containerregistry_layout as layout;
pub use containerregistry_registry as registry;
#[cfg(feature = "server")]
pub use containerregistry_server as server;
pub use containerregistry_v1 as v1;
