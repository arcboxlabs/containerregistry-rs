//! Layer caching (go-containerregistry `pkg/v1/cache`).
//!
//! [`image`] and [`index`] wrap a source so that each layer's compressed
//! bytes are fetched once, written to a [`Cache`] and served from it
//! afterwards. [`FilesystemCache`] stores blobs as files and streams them
//! in and out, so it handles layers of any size.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use containerregistry_image::{Descriptor, Digest, Hasher, MediaType};

use crate::compression::{self, Compression};
use crate::layer::{BoxReader, Layer};
use crate::{Error, Image, ImageIndex, Result};

/// A store of layers keyed by digest (`cache.Cache`).
#[async_trait]
pub trait Cache: Send + Sync {
    /// Stores `layer` and returns a layer backed by the cache.
    async fn put(&self, layer: Arc<dyn Layer>) -> Result<Arc<dyn Layer>>;
    /// Returns the cached layer with `digest`, if present.
    async fn get(&self, digest: &Digest) -> Result<Option<Arc<dyn Layer>>>;
    /// Removes the layer with `digest`.
    async fn delete(&self, digest: &Digest) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
struct Meta {
    descriptor: Descriptor,
    diff_id: Option<Digest>,
}

/// A cache storing blobs under a directory (`cache.NewFilesystemCache`).
#[derive(Clone, Debug)]
pub struct FilesystemCache {
    dir: PathBuf,
}

impl FilesystemCache {
    /// Creates a cache in `dir` (created on first write).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn blob_path(&self, d: &Digest) -> PathBuf {
        self.dir.join(format!("{}-{}", d.algorithm(), d.hex()))
    }

    fn meta_path(&self, d: &Digest) -> PathBuf {
        self.dir.join(format!("{}-{}.json", d.algorithm(), d.hex()))
    }
}

/// A layer whose compressed bytes live in a cache file.
struct FileLayer {
    path: PathBuf,
    descriptor: Descriptor,
    diff_id: Option<Digest>,
    inner: Option<Arc<dyn Layer>>,
}

#[async_trait]
impl Layer for FileLayer {
    async fn digest(&self) -> Result<Digest> {
        Ok(self.descriptor.digest.clone())
    }
    async fn diff_id(&self) -> Result<Digest> {
        if let Some(d) = &self.diff_id {
            return Ok(d.clone());
        }
        if let Some(i) = &self.inner {
            return i.diff_id().await;
        }
        match Compression::from_media_type(&self.descriptor.media_type) {
            None | Some(Compression::None) => Ok(self.descriptor.digest.clone()),
            Some(_) => Ok(crate::partial::digest_reader(
                self.uncompressed().await?,
                containerregistry_image::Algorithm::Sha256,
            )
            .await?
            .0),
        }
    }
    async fn compressed(&self) -> Result<BoxReader> {
        Ok(Box::pin(tokio::fs::File::open(&self.path).await?))
    }
    async fn uncompressed(&self) -> Result<BoxReader> {
        let c =
            Compression::from_media_type(&self.descriptor.media_type).unwrap_or(Compression::None);
        Ok(compression::decompress(self.compressed().await?, c))
    }
    async fn size(&self) -> Result<u64> {
        Ok(self.descriptor.size)
    }
    async fn media_type(&self) -> Result<MediaType> {
        Ok(self.descriptor.media_type.clone())
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        Ok(self.descriptor.clone())
    }
    fn mount_source(&self) -> Option<crate::MountSource> {
        self.inner.as_ref().and_then(|i| i.mount_source())
    }
    fn local_path(&self) -> Option<PathBuf> {
        Some(self.path.clone())
    }
}

async fn write_verified(dir: &Path, final_path: &Path, layer: &dyn Layer) -> Result<(Digest, u64)> {
    tokio::fs::create_dir_all(dir).await?;
    let tmp = final_path.with_extension(format!("tmp-{}", std::process::id()));
    let mut r = layer.compressed().await?;
    let mut f = tokio::fs::File::create(&tmp).await?;
    let mut h = Hasher::new(containerregistry_image::Algorithm::Sha256);
    let mut buf = vec![0u8; 256 * 1024];
    let mut n = 0u64;
    let res: Result<()> = async {
        use tokio::io::AsyncReadExt;
        loop {
            let k = r.read(&mut buf).await?;
            if k == 0 {
                break;
            }
            h.update(&buf[..k]);
            f.write_all(&buf[..k]).await?;
            n += k as u64;
        }
        f.flush().await?;
        Ok(())
    }
    .await;
    if let Err(e) = res {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    let digest = h.finalize();
    // Digests are only known after a streaming layer was consumed.
    if let Ok(want) = layer.digest().await
        && want.algorithm() == digest.algorithm()
        && want != digest
    {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(Error::Invalid(format!(
            "cached layer digest mismatch: expected {}, got {}",
            want, digest
        )));
    }
    tokio::fs::rename(&tmp, final_path).await?;
    Ok((digest, n))
}

#[async_trait]
impl Cache for FilesystemCache {
    async fn put(&self, layer: Arc<dyn Layer>) -> Result<Arc<dyn Layer>> {
        let staging = self.dir.join(format!(
            ".staging-{}-{:x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let (digest, size) = write_verified(&self.dir, &staging, layer.as_ref()).await?;
        let path = self.blob_path(&digest);
        tokio::fs::rename(&staging, &path).await?;
        let mut descriptor = layer
            .descriptor()
            .await
            .unwrap_or_else(|_| Descriptor::new(MediaType::OciLayerGzip, digest.clone(), size));
        descriptor.digest = digest.clone();
        descriptor.size = size;
        let diff_id = layer.diff_id().await.ok();
        let meta = Meta {
            descriptor: descriptor.clone(),
            diff_id: diff_id.clone(),
        };
        tokio::fs::write(self.meta_path(&digest), serde_json::to_vec(&meta)?).await?;
        Ok(Arc::new(FileLayer {
            path,
            descriptor,
            diff_id,
            inner: Some(layer),
        }))
    }

    async fn get(&self, digest: &Digest) -> Result<Option<Arc<dyn Layer>>> {
        let path = self.blob_path(digest);
        if tokio::fs::metadata(&path).await.is_err() {
            return Ok(None);
        }
        let meta: Meta = match tokio::fs::read(self.meta_path(digest)).await {
            Ok(b) => serde_json::from_slice(&b)?,
            Err(_) => return Ok(None),
        };
        Ok(Some(Arc::new(FileLayer {
            path,
            descriptor: meta.descriptor,
            diff_id: meta.diff_id,
            inner: None,
        })))
    }

    async fn delete(&self, digest: &Digest) -> Result<()> {
        for p in [self.blob_path(digest), self.meta_path(digest)] {
            match tokio::fs::remove_file(&p).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// A layer that consults the cache before reading its source.
struct LazyLayer {
    inner: Arc<dyn Layer>,
    cache: Arc<dyn Cache>,
}

impl LazyLayer {
    async fn cached(&self) -> Result<Arc<dyn Layer>> {
        let d = self.inner.digest().await?;
        if let Some(l) = self.cache.get(&d).await? {
            return Ok(l);
        }
        self.cache.put(self.inner.clone()).await
    }
}

#[async_trait]
impl Layer for LazyLayer {
    async fn digest(&self) -> Result<Digest> {
        self.inner.digest().await
    }
    async fn diff_id(&self) -> Result<Digest> {
        self.inner.diff_id().await
    }
    async fn compressed(&self) -> Result<BoxReader> {
        self.cached().await?.compressed().await
    }
    async fn uncompressed(&self) -> Result<BoxReader> {
        self.cached().await?.uncompressed().await
    }
    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }
    async fn media_type(&self) -> Result<MediaType> {
        self.inner.media_type().await
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        self.inner.descriptor().await
    }
    fn mount_source(&self) -> Option<crate::MountSource> {
        self.inner.mount_source()
    }
    fn local_path(&self) -> Option<PathBuf> {
        None
    }
}

struct CachedImage {
    inner: Arc<dyn Image>,
    cache: Arc<dyn Cache>,
}

#[async_trait]
impl Image for CachedImage {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        self.inner.raw_manifest().await
    }
    async fn raw_config_file(&self) -> Result<Vec<u8>> {
        self.inner.raw_config_file().await
    }
    async fn layer_by_digest(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        if let Some(l) = self.cache.get(digest).await? {
            return Ok(l);
        }
        Ok(Arc::new(LazyLayer {
            inner: self.inner.layer_by_digest(digest).await?,
            cache: self.cache.clone(),
        }))
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        self.inner.descriptor().await
    }
}

struct CachedIndex {
    inner: Arc<dyn ImageIndex>,
    cache: Arc<dyn Cache>,
}

#[async_trait]
impl ImageIndex for CachedIndex {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        self.inner.raw_manifest().await
    }
    async fn image(&self, digest: &Digest) -> Result<Arc<dyn Image>> {
        Ok(image(self.inner.image(digest).await?, self.cache.clone()))
    }
    async fn image_index(&self, digest: &Digest) -> Result<Arc<dyn ImageIndex>> {
        Ok(index(
            self.inner.image_index(digest).await?,
            self.cache.clone(),
        ))
    }
    async fn blob(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        self.inner.blob(digest).await
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        self.inner.descriptor().await
    }
}

/// Wraps an image so its layers are read through `cache` (`cache.Image`).
pub fn image(img: Arc<dyn Image>, cache: Arc<dyn Cache>) -> Arc<dyn Image> {
    Arc::new(CachedImage { inner: img, cache })
}

/// Wraps an index so its images' layers are read through `cache`
/// (`cache.ImageIndex`).
pub fn index(idx: Arc<dyn ImageIndex>, cache: Arc<dyn Cache>) -> Arc<dyn ImageIndex> {
    Arc::new(CachedIndex { inner: idx, cache })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{random, read_all};

    #[tokio::test]
    async fn caches_layers() {
        let dir = tempfile::tempdir().unwrap();
        let cache: Arc<dyn Cache> = Arc::new(FilesystemCache::new(dir.path()));
        let img = random::image(1024, 2).await.unwrap();
        let cached = image(img.clone(), cache.clone());
        let layers = cached.layers().await.unwrap();
        let d = layers[0].digest().await.unwrap();
        assert!(cache.get(&d).await.unwrap().is_none());
        let a = read_all(layers[0].compressed().await.unwrap())
            .await
            .unwrap();
        let hit = cache.get(&d).await.unwrap().expect("cached");
        assert_eq!(read_all(hit.compressed().await.unwrap()).await.unwrap(), a);
        assert_eq!(
            hit.diff_id().await.unwrap(),
            layers[0].diff_id().await.unwrap()
        );
        crate::validate::image(cached.as_ref(), crate::validate::Options::default())
            .await
            .unwrap();
        cache.delete(&d).await.unwrap();
        assert!(cache.get(&d).await.unwrap().is_none());
    }
}
