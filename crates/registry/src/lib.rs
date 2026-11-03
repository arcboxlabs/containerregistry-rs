//! OCI distribution registry client.
//!
//! This crate provides an HTTP client for interacting with OCI distribution
//! registries, implementing the OCI Distribution Specification.
//!
//! # Overview
//!
//! The main types in this crate are:
//! - [`Client`] - The registry client for manifest and blob operations
//! - [`Reference`] - A parsed container image reference
//! - [`ClientConfig`] - Configuration for the client
//!
//! Blob transfers stream: [`Client::get_blob_stream`] returns an
//! [`tokio::io::AsyncRead`] that verifies the digest as it reads and resumes
//! interrupted connections, [`Client::download_blob`] downloads to a file
//! with resume and parallel range requests, and [`Client::push_blob_stream`]
//! / [`Client::push_blob_file`] upload from a reader or file while computing
//! the digest, optionally in retryable chunks.
//!
//! # Example
//!
//! ```no_run
//! use containerregistry_registry::{Client, Reference};
//!
//! # async fn example() -> containerregistry_registry::Result<()> {
//! let client = Client::new()?;
//!
//! // Parse a reference
//! let reference: Reference = "nginx:1.21".parse()?;
//!
//! // Get a manifest
//! let (manifest, digest) = client.get_manifest(&reference).await?;
//! # Ok(())
//! # }
//! ```

mod blobs;
mod client;
mod error;
mod listing;
mod manifests;
pub mod metrics;
mod progress;
mod reference;
mod upload;

pub use blobs::{BlobReader, DownloadOptions, error_from_io};
pub use client::{Client, ClientConfig, is_local_registry};
pub use error::Error;
pub use listing::{GoogleManifestInfo, GoogleTags, ListOptions, referrers_tag};
pub use manifests::{DEFAULT_MANIFEST_ACCEPT, ManifestOrIndex, PutManifestResponse, RawManifest};
pub use metrics::{MetricsCollector, MetricsSummary, Operation, OperationMetrics, OperationTimer};
pub use progress::{Progress, ProgressFn};
pub use reference::{DEFAULT_REGISTRY, DEFAULT_TAG, Reference, Specifier};
pub use upload::{
    BlobUpload, CheckpointFn, MountResult, UploadCheckpoint, UploadOptions, UploadStatus,
    UploadedBlob,
};

/// Result type for registry operations.
pub type Result<T> = std::result::Result<T, Error>;
