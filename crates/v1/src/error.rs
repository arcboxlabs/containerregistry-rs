//! Error type for the v1 image model.

use thiserror::Error;

/// Errors produced by images, indexes and layers.
#[derive(Debug, Error)]
pub enum Error {
    /// A manifest, config or descriptor problem.
    #[error(transparent)]
    Image(#[from] containerregistry_image::Error),

    /// I/O error while reading or writing content.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// The requested blob, layer or manifest does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// A value of a streaming layer is only known after it was consumed
    /// (go-containerregistry `stream.ErrNotComputed`).
    #[error("value not computed until the stream is consumed: {0}")]
    NotComputed(String),

    /// A streaming layer was read twice (`stream.ErrConsumed`).
    #[error("stream was already consumed")]
    Consumed,

    /// Content failed validation.
    #[error("invalid: {0}")]
    Invalid(String),

    /// The operation is not supported by this implementation.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// An error from an underlying source (registry, layout, ...).
    #[error("{0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Wraps an arbitrary error from an image source.
    pub fn source(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Source(Box::new(e))
    }
}

/// Result type for v1 operations.
pub type Result<T> = std::result::Result<T, Error>;
