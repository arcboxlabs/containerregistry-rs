//! The [`Layer`] trait (go-containerregistry `v1.Layer`).

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use containerregistry_image::{Descriptor, Digest, MediaType};

use crate::Result;

/// An owned, sendable async byte stream.
pub type BoxReader = Pin<Box<dyn AsyncRead + Send + 'static>>;

/// Where a layer can be cross-repository mounted from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountSource {
    /// Registry host the layer lives in.
    pub registry: String,
    /// Repository the layer lives in.
    pub repository: String,
}

/// A blob of an image: a filesystem layer, an artifact layer, or a config.
///
/// Readers returned by [`Layer::compressed`] and [`Layer::uncompressed`] are
/// independent streams; implementations should avoid buffering whole blobs
/// in memory so that multi-gigabyte layers can be processed.
#[async_trait]
pub trait Layer: Send + Sync {
    /// The digest of the compressed (as stored) content.
    async fn digest(&self) -> Result<Digest>;

    /// The digest of the uncompressed content (the "diff ID").
    async fn diff_id(&self) -> Result<Digest>;

    /// Opens the compressed (as stored) content.
    async fn compressed(&self) -> Result<BoxReader>;

    /// Opens the uncompressed content.
    async fn uncompressed(&self) -> Result<BoxReader>;

    /// Size of the compressed content in bytes.
    async fn size(&self) -> Result<u64>;

    /// Media type of the layer.
    async fn media_type(&self) -> Result<MediaType>;

    /// The descriptor for this layer. Implementations override this to
    /// carry annotations, URLs or other descriptor fields
    /// (go-containerregistry `partial.Describable`).
    async fn descriptor(&self) -> Result<Descriptor> {
        Ok(Descriptor::new(
            self.media_type().await?,
            self.digest().await?,
            self.size().await?,
        ))
    }

    /// Size of the uncompressed content, when known without reading it.
    async fn uncompressed_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// A repository this blob already exists in, enabling cross-repository
    /// mounts instead of uploads (go-containerregistry `remote.MountableLayer`).
    fn mount_source(&self) -> Option<MountSource> {
        None
    }

    /// A local file holding exactly the compressed content, enabling
    /// resumable file uploads.
    fn local_path(&self) -> Option<PathBuf> {
        None
    }
}

#[async_trait]
impl<L: Layer + ?Sized> Layer for Arc<L> {
    async fn digest(&self) -> Result<Digest> {
        (**self).digest().await
    }
    async fn diff_id(&self) -> Result<Digest> {
        (**self).diff_id().await
    }
    async fn compressed(&self) -> Result<BoxReader> {
        (**self).compressed().await
    }
    async fn uncompressed(&self) -> Result<BoxReader> {
        (**self).uncompressed().await
    }
    async fn size(&self) -> Result<u64> {
        (**self).size().await
    }
    async fn media_type(&self) -> Result<MediaType> {
        (**self).media_type().await
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        (**self).descriptor().await
    }
    async fn uncompressed_size(&self) -> Result<Option<u64>> {
        (**self).uncompressed_size().await
    }
    fn mount_source(&self) -> Option<MountSource> {
        (**self).mount_source()
    }
    fn local_path(&self) -> Option<PathBuf> {
        (**self).local_path()
    }
}

/// Reads a whole stream into memory. Intended for small blobs (configs,
/// manifests) and tests.
pub async fn read_all(mut reader: BoxReader) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await?;
    Ok(out)
}
