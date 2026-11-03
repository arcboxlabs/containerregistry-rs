//! In-memory and disk-backed OCI distribution registry.
//!
//! A Rust port of go-containerregistry's `pkg/registry`: a small,
//! dependency-light registry implementing the [OCI distribution spec],
//! meant for tests and for `crane registry serve`.
//!
//! ```no_run
//! # async fn demo() -> std::io::Result<()> {
//! use containerregistry_server::{Registry, TestRegistry};
//!
//! // A registry in a background task on 127.0.0.1:<random port>.
//! let reg = TestRegistry::spawn().await?;
//! println!("listening on {}", reg.host());
//!
//! // Or serve blobs from disk on a listener of your own.
//! let registry = Registry::builder().disk("/tmp/blobs").build()?;
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:5000").await?;
//! registry.serve(listener).await
//! # }
//! ```
//!
//! [OCI distribution spec]: https://github.com/opencontainers/distribution-spec

mod error;
mod handlers;
mod manifests;
mod storage;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use axum::Router;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use error::RegistryError;

use handlers::Inner;
use storage::{BlobStore, Uploads};

/// Where blob contents are stored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Storage {
    /// Keep blobs in memory (the default).
    #[default]
    Memory,
    /// Keep blobs in files under this directory, laid out as
    /// `<dir>/<algorithm>/<hex>` (go-containerregistry's `--disk`).
    /// Manifests are always kept in memory.
    Disk(PathBuf),
}

/// Registry configuration.
#[derive(Clone, Debug)]
pub struct Options {
    /// Blob storage backend.
    pub storage: Storage,
    /// Serve the OCI 1.1 referrers API (`/v2/<name>/referrers/<digest>`)
    /// and return `OCI-Subject` on manifest pushes. Default: true.
    pub referrers: bool,
    /// `(probability, message)` pairs: each response carries a
    /// `Warning: 299 - "<message>"` header with that probability
    /// (go-containerregistry's `WithWarning`).
    pub warnings: Vec<(f64, String)>,
    /// Log each request through `tracing` (info for successes, warn for
    /// errors). Default: true.
    pub log_requests: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            storage: Storage::Memory,
            referrers: true,
            warnings: Vec::new(),
            log_requests: true,
        }
    }
}

/// An OCI distribution registry.
///
/// Cloning is cheap; clones share state.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("blobs", &self.inner.blobs)
            .field("referrers", &self.inner.referrers)
            .finish_non_exhaustive()
    }
}

impl Registry {
    /// Creates a registry. For [`Storage::Disk`] the directory is created
    /// if missing.
    pub fn new(options: Options) -> io::Result<Self> {
        let blobs = match options.storage {
            Storage::Memory => BlobStore::Memory(RwLock::new(HashMap::new())),
            Storage::Disk(dir) => {
                std::fs::create_dir_all(&dir)?;
                BlobStore::Disk(dir)
            }
        };
        Ok(Self {
            inner: Arc::new(Inner {
                blobs,
                uploads: Uploads::default(),
                links: Mutex::new(HashMap::new()),
                repos: RwLock::new(Default::default()),
                referrers: options.referrers,
                warnings: options.warnings,
                log_requests: options.log_requests,
            }),
        })
    }

    /// Starts building a registry from default [`Options`].
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::default()
    }

    /// Returns an [`axum::Router`] serving the registry. It handles every
    /// path, so mount it at the site root.
    pub fn router(&self) -> Router {
        Router::new()
            .fallback(handlers::handle)
            .with_state(self.inner.clone())
    }

    /// Serves the registry on `listener` until the task is cancelled.
    pub async fn serve(self, listener: TcpListener) -> io::Result<()> {
        axum::serve(listener, self.router()).await
    }
}

/// Serves `registry` on `listener` (shorthand for [`Registry::serve`]).
pub async fn serve(listener: TcpListener, registry: Registry) -> io::Result<()> {
    registry.serve(listener).await
}

/// Builder for [`Registry`].
#[derive(Clone, Debug, Default)]
pub struct RegistryBuilder {
    options: Options,
}

impl RegistryBuilder {
    /// Stores blobs in memory (the default).
    pub fn memory(mut self) -> Self {
        self.options.storage = Storage::Memory;
        self
    }

    /// Stores blobs under `dir`.
    pub fn disk(mut self, dir: impl Into<PathBuf>) -> Self {
        self.options.storage = Storage::Disk(dir.into());
        self
    }

    /// Enables or disables the referrers API (enabled by default).
    pub fn referrers(mut self, enabled: bool) -> Self {
        self.options.referrers = enabled;
        self
    }

    /// Adds a `Warning` header to responses with probability `prob`.
    pub fn warning(mut self, prob: f64, message: impl Into<String>) -> Self {
        self.options.warnings.push((prob, message.into()));
        self
    }

    /// Enables or disables per-request `tracing` logs (enabled by default).
    pub fn log_requests(mut self, enabled: bool) -> Self {
        self.options.log_requests = enabled;
        self
    }

    /// Builds the registry.
    pub fn build(self) -> io::Result<Registry> {
        Registry::new(self.options)
    }
}

/// A registry served on `127.0.0.1:<random port>` from a background tokio
/// task, for tests. The task is aborted on drop.
#[derive(Debug)]
pub struct TestRegistry {
    addr: SocketAddr,
    registry: Registry,
    task: JoinHandle<()>,
}

impl TestRegistry {
    /// Spawns an in-memory registry with default options.
    pub async fn spawn() -> io::Result<Self> {
        Self::spawn_with(Registry::new(Options::default())?).await
    }

    /// Spawns the given registry.
    pub async fn spawn_with(registry: Registry) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let served = registry.clone();
        let task = tokio::spawn(async move {
            if let Err(e) = served.serve(listener).await {
                tracing::error!(error = %e, "test registry stopped");
            }
        });
        Ok(Self {
            addr,
            registry,
            task,
        })
    }

    /// The listening address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `host:port`, usable as a registry name in image references.
    pub fn host(&self) -> String {
        self.addr.to_string()
    }

    /// The served registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Drop for TestRegistry {
    fn drop(&mut self) {
        self.task.abort();
    }
}
