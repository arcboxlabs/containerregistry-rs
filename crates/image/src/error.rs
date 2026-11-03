//! Error types for image operations.

use thiserror::Error;

/// Errors that can occur during image operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid digest format.
    #[error("invalid digest: {0}")]
    InvalidDigest(String),

    /// Digest mismatch during verification.
    #[error("digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },

    /// Size mismatch during verification.
    #[error("size mismatch: expected {expected}, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    /// Unsupported digest algorithm.
    #[error("unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),

    /// Invalid manifest format.
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),

    /// Unsupported media type.
    #[error("unsupported media type: {0}")]
    UnsupportedMediaType(String),

    /// Invalid image config format.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Invalid platform specification.
    #[error("invalid platform: {0}")]
    InvalidPlatform(String),

    /// JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
