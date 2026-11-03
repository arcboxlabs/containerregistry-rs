//! Transfer progress reporting (go-containerregistry `v1.Update`).

use std::sync::Arc;

/// A progress update for a transfer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// Total bytes expected (0 when unknown).
    pub total: u64,
    /// Bytes transferred so far.
    pub complete: u64,
}

/// A progress callback. It may be invoked from several tasks concurrently
/// and should return quickly.
pub type ProgressFn = Arc<dyn Fn(Progress) + Send + Sync>;
