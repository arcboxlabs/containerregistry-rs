//! OCI layouts as [`Image`]/[`ImageIndex`] sources and sinks
//! (go-containerregistry `pkg/v1/layout`).
//!
//! Blobs are written by streaming into a temporary file, hashing on the
//! way, and renaming into place only after the digest checks out, so a
//! crash never leaves a corrupt blob. Remote layers are fetched with
//! [`Layer::download_to`] straight to their final path, which makes
//! interrupted multi-gigabyte pulls resumable. Layers of an image are
//! written concurrently.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use containerregistry_image::{
    Algorithm, Descriptor, Digest, Hasher, MediaType, OciIndex, Platform,
};
use containerregistry_v1::matcher::Matcher;
use containerregistry_v1::partial::{Child, CompressedLayer, CompressedLayerCore, children};
use containerregistry_v1::{
    BoxReader, Error as V1Error, Image, ImageIndex, Layer, Result as V1Result,
};

use crate::{Error, Layout, Result};

const INDEX_FILE: &str = "index.json";

impl From<Error> for V1Error {
    fn from(e: Error) -> Self {
        match e {
            Error::BlobNotFound(s) => V1Error::NotFound(s),
            Error::Io(e) => V1Error::Io(e),
            Error::Image(e) => V1Error::Image(e),
            Error::Json(e) => V1Error::Json(e),
            e => V1Error::source(e),
        }
    }
}

/// Descriptor fields to set when adding to `index.json`
/// (go `layout.WithAnnotations`, `WithURLs`, `WithPlatform`).
#[derive(Clone, Debug, Default)]
pub struct AppendOptions {
    /// Annotations merged into the descriptor.
    pub annotations: BTreeMap<String, String>,
    /// URLs for the descriptor.
    pub urls: Vec<String>,
    /// Platform for the descriptor.
    pub platform: Option<Platform>,
}

impl AppendOptions {
    /// Adds an annotation.
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Sets the platform.
    pub fn with_platform(mut self, platform: Platform) -> Self {
        self.platform = Some(platform);
        self
    }

    fn apply(&self, d: &mut Descriptor) {
        d.annotations.extend(self.annotations.clone());
        if !self.urls.is_empty() {
            d.urls = self.urls.clone();
        }
        if let Some(p) = &self.platform {
            d.platform = Some(p.clone());
        }
    }
}

/// How many layers are written concurrently.
const WRITE_JOBS: usize = 4;

fn temp_path(dir: &Path) -> PathBuf {
    dir.join(format!(
        ".tmp-{}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ))
}

impl Layout {
    fn blobs_dir(&self, alg: Algorithm) -> PathBuf {
        self.path().join("blobs").join(alg.to_string())
    }

    /// Opens a blob for async reading.
    pub async fn open_blob(&self, digest: &Digest) -> Result<tokio::fs::File> {
        match tokio::fs::File::open(self.blob_path(digest)).await {
            Ok(f) => Ok(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::BlobNotFound(digest.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Streams `reader` into the layout, hashing as it writes
    /// (go `Path.WriteBlob`). With `expected`, the content must match it
    /// (and an existing blob is not rewritten). Returns the digest and size.
    pub async fn write_blob_stream<R: AsyncRead + Unpin>(
        &self,
        mut reader: R,
        expected: Option<&Digest>,
    ) -> Result<(Digest, u64)> {
        if let Some(d) = expected
            && let Ok(m) = tokio::fs::metadata(self.blob_path(d)).await
        {
            return Ok((d.clone(), m.len()));
        }
        let alg = expected.map(|d| d.algorithm()).unwrap_or(Algorithm::Sha256);
        let dir = self.blobs_dir(alg);
        tokio::fs::create_dir_all(&dir).await?;
        let tmp = temp_path(&dir);
        let result: Result<(Digest, u64)> = async {
            let mut f = tokio::fs::File::create(&tmp).await?;
            let mut h = Hasher::new(alg);
            let mut buf = vec![0u8; 1 << 20];
            let mut n = 0u64;
            loop {
                let k = reader.read(&mut buf).await?;
                if k == 0 {
                    break;
                }
                h.update(&buf[..k]);
                f.write_all(&buf[..k]).await?;
                n += k as u64;
            }
            f.sync_all().await?;
            Ok((h.finalize(), n))
        }
        .await;
        let (digest, size) = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(e);
            }
        };
        if let Some(want) = expected
            && want != &digest
        {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(Error::InvalidLayout(format!(
                "blob digest mismatch: expected {}, got {}",
                want, digest
            )));
        }
        tokio::fs::rename(&tmp, self.blob_path(&digest)).await?;
        Ok((digest, size))
    }

    /// Writes one blob of an image (go `Path.writeLayer`). Existing blobs
    /// are skipped; remote layers download resumably to the final path.
    pub async fn write_layer(&self, layer: &dyn Layer) -> V1Result<()> {
        let digest = match layer.digest().await {
            Ok(d) => Some(d),
            Err(V1Error::NotComputed(_)) => None,
            Err(e) => return Err(e),
        };
        if let Some(d) = &digest {
            if self.has_blob(d) {
                return Ok(());
            }
            let dest = self.blob_path(d);
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if layer.download_to(&dest).await? {
                return Ok(());
            }
        }
        let r = layer.compressed().await?;
        self.write_blob_stream(r, digest.as_ref()).await?;
        Ok(())
    }

    /// Writes an image's blobs and manifest without touching `index.json`
    /// (go `Path.WriteImage`).
    pub async fn write_image(&self, img: &dyn Image) -> V1Result<()> {
        let layers = img.layers().await?;
        let futs: Vec<_> = layers
            .iter()
            .map(|l| self.write_layer(l.as_ref()))
            .collect();
        futures::stream::iter(futs)
            .buffer_unordered(WRITE_JOBS)
            .try_collect::<Vec<_>>()
            .await?;
        let config = img.raw_config_file().await?;
        let cd = img.manifest().await?.config().digest.clone();
        self.write_blob_stream(config.as_slice(), Some(&cd)).await?;
        let raw = img.raw_manifest().await?;
        self.write_blob_stream(raw.as_slice(), None).await?;
        Ok(())
    }

    /// Writes an index, its children and their blobs, without touching
    /// `index.json` (go `Path.writeIndex`).
    pub fn write_index_tree<'a>(
        &'a self,
        idx: &'a dyn ImageIndex,
    ) -> futures::future::BoxFuture<'a, V1Result<()>> {
        Box::pin(async move {
            for (_, child) in children(idx).await? {
                match child {
                    Child::Image(i) => self.write_image(i.as_ref()).await?,
                    Child::Index(i) => self.write_index_tree(i.as_ref()).await?,
                    Child::Blob(b) => self.write_layer(b.as_ref()).await?,
                }
            }
            let raw = idx.raw_manifest().await?;
            self.write_blob_stream(raw.as_slice(), None).await?;
            Ok(())
        })
    }

    fn read_index_lenient(&self) -> Result<OciIndex> {
        let data = std::fs::read(self.path().join(INDEX_FILE))?;
        Ok(OciIndex::from_bytes_unchecked(&data)?)
    }

    fn write_index_atomic(&self, index: &OciIndex) -> Result<()> {
        let tmp = temp_path(self.path());
        std::fs::write(&tmp, index.to_bytes()?)?;
        std::fs::rename(&tmp, self.path().join(INDEX_FILE))?;
        Ok(())
    }

    /// Adds a descriptor to `index.json` (go `Path.AppendDescriptor`).
    pub fn append_descriptor(&self, descriptor: Descriptor) -> Result<()> {
        let mut index = self.read_index_lenient()?;
        index.manifests.push(descriptor);
        self.write_index_atomic(&index)
    }

    /// Writes an image and adds it to `index.json` (go `Path.AppendImage`).
    pub async fn append_image(&self, img: &dyn Image, options: &AppendOptions) -> V1Result<()> {
        self.write_image(img).await?;
        let mut d = img.descriptor().await?;
        options.apply(&mut d);
        Ok(self.append_descriptor(d)?)
    }

    /// Writes an index and adds it to `index.json` (go `Path.AppendIndex`).
    pub async fn append_index(
        &self,
        idx: &dyn ImageIndex,
        options: &AppendOptions,
    ) -> V1Result<()> {
        self.write_index_tree(idx).await?;
        let mut d = idx.descriptor().await?;
        options.apply(&mut d);
        Ok(self.append_descriptor(d)?)
    }

    fn replace_descriptor(&self, d: Descriptor, matcher: &Matcher) -> Result<()> {
        let mut index = self.read_index_lenient()?;
        index.manifests.retain(|m| !matcher(m));
        index.manifests.push(d);
        self.write_index_atomic(&index)
    }

    /// Writes an image and replaces the `index.json` entries matching
    /// `matcher` with it (go `Path.ReplaceImage`).
    pub async fn replace_image(
        &self,
        img: &dyn Image,
        matcher: &Matcher,
        options: &AppendOptions,
    ) -> V1Result<()> {
        self.write_image(img).await?;
        let mut d = img.descriptor().await?;
        options.apply(&mut d);
        Ok(self.replace_descriptor(d, matcher)?)
    }

    /// Writes an index and replaces the matching `index.json` entries with
    /// it (go `Path.ReplaceIndex`).
    pub async fn replace_index(
        &self,
        idx: &dyn ImageIndex,
        matcher: &Matcher,
        options: &AppendOptions,
    ) -> V1Result<()> {
        self.write_index_tree(idx).await?;
        let mut d = idx.descriptor().await?;
        options.apply(&mut d);
        Ok(self.replace_descriptor(d, matcher)?)
    }

    /// Removes the `index.json` entries matching `matcher`; blobs stay
    /// until [`Layout::garbage_collect`] (go `Path.RemoveDescriptors`).
    pub fn remove_descriptors(&self, matcher: &Matcher) -> Result<()> {
        let mut index = self.read_index_lenient()?;
        index.manifests.retain(|m| !matcher(m));
        self.write_index_atomic(&index)
    }

    /// Deletes a blob file (go `Path.RemoveBlob`).
    pub fn remove_blob(&self, digest: &Digest) -> Result<()> {
        match std::fs::remove_file(self.blob_path(digest)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Lists blobs not reachable from `index.json` (go
    /// `Path.GarbageCollect`; pass each to [`Layout::remove_blob`] to free
    /// space, as `crane layout gc` does). Leftover temporary files are
    /// removed.
    pub async fn garbage_collect(&self) -> V1Result<Vec<Digest>> {
        let root = self.image_index_v1()?;
        let mut keep = HashSet::new();
        collect_index(root.as_ref(), &mut keep).await?;
        let mut out = Vec::new();
        let blobs = self.path().join("blobs");
        let mut algs = match tokio::fs::read_dir(&blobs).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(alg) = algs.next_entry().await? {
            if !alg.file_type().await?.is_dir() {
                continue;
            }
            let alg_name = alg.file_name().to_string_lossy().into_owned();
            let mut files = tokio::fs::read_dir(alg.path()).await?;
            while let Some(f) = files.next_entry().await? {
                let name = f.file_name().to_string_lossy().into_owned();
                if name.starts_with(".tmp-") {
                    let _ = tokio::fs::remove_file(f.path()).await;
                    continue;
                }
                let Ok(d) = format!("{}:{}", alg_name, name).parse::<Digest>() else {
                    continue;
                };
                if !keep.contains(&d) {
                    out.push(d);
                }
            }
        }
        out.sort_by_key(|d| d.to_string());
        Ok(out)
    }

    /// The layout's `index.json` as an [`ImageIndex`]
    /// (go `Path.ImageIndex`).
    pub fn image_index_v1(&self) -> Result<Arc<dyn ImageIndex>> {
        let raw = std::fs::read(self.path().join(INDEX_FILE))?;
        Ok(Arc::new(LayoutIndex {
            root: self.path().to_path_buf(),
            raw,
            descriptor: None,
        }))
    }

    /// The image with manifest `digest` (go `Path.Image`).
    pub fn image_v1(&self, digest: &Digest) -> Result<Arc<dyn Image>> {
        let raw = std::fs::read(self.blob_path(digest))
            .map_err(|_| Error::BlobNotFound(digest.to_string()))?;
        Ok(Arc::new(LayoutImage {
            root: self.path().to_path_buf(),
            raw,
            descriptor: None,
        }))
    }

    /// Creates a layout at `path` holding `idx` as its `index.json`
    /// (go `layout.Write`).
    pub async fn write_v1(path: impl AsRef<Path>, idx: &dyn ImageIndex) -> V1Result<Layout> {
        let layout = Layout::create(path)?;
        for (_, child) in children(idx).await? {
            match child {
                Child::Image(i) => layout.write_image(i.as_ref()).await?,
                Child::Index(i) => layout.write_index_tree(i.as_ref()).await?,
                Child::Blob(b) => layout.write_layer(b.as_ref()).await?,
            }
        }
        let raw = idx.raw_manifest().await?;
        let tmp = temp_path(layout.path());
        tokio::fs::write(&tmp, &raw).await?;
        tokio::fs::rename(&tmp, layout.path().join(INDEX_FILE)).await?;
        Ok(layout)
    }
}

fn collect_index<'a>(
    idx: &'a dyn ImageIndex,
    keep: &'a mut HashSet<Digest>,
) -> futures::future::BoxFuture<'a, V1Result<()>> {
    Box::pin(async move {
        for (d, child) in children(idx).await? {
            keep.insert(d.digest.clone());
            match child {
                Child::Image(i) => {
                    let m = i.manifest().await?;
                    keep.insert(m.config().digest.clone());
                    keep.extend(m.layers().iter().map(|l| l.digest.clone()));
                }
                Child::Index(i) => collect_index(i.as_ref(), keep).await?,
                Child::Blob(_) => {}
            }
        }
        Ok(())
    })
}

fn blob_path(root: &Path, d: &Digest) -> PathBuf {
    root.join("blobs")
        .join(d.algorithm().to_string())
        .join(d.hex())
}

struct LayoutBlob {
    root: PathBuf,
    descriptor: Descriptor,
}

#[async_trait]
impl CompressedLayerCore for LayoutBlob {
    async fn digest(&self) -> V1Result<Digest> {
        Ok(self.descriptor.digest.clone())
    }
    async fn compressed(&self) -> V1Result<BoxReader> {
        let p = blob_path(&self.root, &self.descriptor.digest);
        match tokio::fs::File::open(&p).await {
            Ok(f) => Ok(Box::pin(f)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(V1Error::NotFound(format!(
                "blob {}",
                self.descriptor.digest
            ))),
            Err(e) => Err(e.into()),
        }
    }
    async fn size(&self) -> V1Result<u64> {
        Ok(self.descriptor.size)
    }
    async fn media_type(&self) -> V1Result<MediaType> {
        Ok(self.descriptor.media_type.clone())
    }
    async fn descriptor(&self) -> V1Result<Descriptor> {
        Ok(self.descriptor.clone())
    }
}

/// A layout blob as a layer; it also exposes the file for resumable
/// uploads.
struct LayoutLayer(CompressedLayer<LayoutBlob>);

#[async_trait]
impl Layer for LayoutLayer {
    async fn digest(&self) -> V1Result<Digest> {
        self.0.digest().await
    }
    async fn diff_id(&self) -> V1Result<Digest> {
        self.0.diff_id().await
    }
    async fn compressed(&self) -> V1Result<BoxReader> {
        self.0.compressed().await
    }
    async fn uncompressed(&self) -> V1Result<BoxReader> {
        self.0.uncompressed().await
    }
    async fn size(&self) -> V1Result<u64> {
        self.0.size().await
    }
    async fn media_type(&self) -> V1Result<MediaType> {
        self.0.media_type().await
    }
    async fn descriptor(&self) -> V1Result<Descriptor> {
        self.0.descriptor().await
    }
    fn local_path(&self) -> Option<PathBuf> {
        let c = self.0.core();
        Some(blob_path(&c.root, &c.descriptor.digest))
    }
}

fn layout_layer(root: &Path, descriptor: Descriptor) -> Arc<dyn Layer> {
    Arc::new(LayoutLayer(CompressedLayer::new(LayoutBlob {
        root: root.to_path_buf(),
        descriptor,
    })))
}

struct LayoutImage {
    root: PathBuf,
    raw: Vec<u8>,
    descriptor: Option<Descriptor>,
}

#[async_trait]
impl Image for LayoutImage {
    async fn raw_manifest(&self) -> V1Result<Vec<u8>> {
        Ok(self.raw.clone())
    }
    async fn raw_config_file(&self) -> V1Result<Vec<u8>> {
        let d = self.manifest().await?.config().digest.clone();
        Ok(tokio::fs::read(blob_path(&self.root, &d)).await?)
    }
    async fn layer_by_digest(&self, digest: &Digest) -> V1Result<Arc<dyn Layer>> {
        let m = self.manifest().await?;
        let d = containerregistry_v1::partial::find_blob_descriptor(&m, digest)
            .cloned()
            .ok_or_else(|| V1Error::NotFound(format!("blob {} not in manifest", digest)))?;
        Ok(layout_layer(&self.root, d))
    }
    async fn descriptor(&self) -> V1Result<Descriptor> {
        let mut d = match &self.descriptor {
            Some(d) => d.clone(),
            None => Descriptor::from_bytes(self.media_type().await?, &self.raw),
        };
        if d.artifact_type.is_none()
            && let Ok(m) = self.manifest().await
        {
            d.artifact_type = containerregistry_v1::partial::artifact_type(&m);
        }
        Ok(d)
    }
}

struct LayoutIndex {
    root: PathBuf,
    raw: Vec<u8>,
    descriptor: Option<Descriptor>,
}

impl LayoutIndex {
    async fn find(&self, digest: &Digest) -> V1Result<Descriptor> {
        self.index_manifest()
            .await?
            .manifests()
            .iter()
            .find(|d| &d.digest == digest)
            .cloned()
            .ok_or_else(|| V1Error::NotFound(format!("manifest {} not in index", digest)))
    }
}

#[async_trait]
impl ImageIndex for LayoutIndex {
    async fn raw_manifest(&self) -> V1Result<Vec<u8>> {
        Ok(self.raw.clone())
    }
    async fn image(&self, digest: &Digest) -> V1Result<Arc<dyn Image>> {
        let d = self.find(digest).await?;
        let raw = tokio::fs::read(blob_path(&self.root, digest)).await?;
        Ok(Arc::new(LayoutImage {
            root: self.root.clone(),
            raw,
            descriptor: Some(d),
        }))
    }
    async fn image_index(&self, digest: &Digest) -> V1Result<Arc<dyn ImageIndex>> {
        let d = self.find(digest).await?;
        let raw = tokio::fs::read(blob_path(&self.root, digest)).await?;
        Ok(Arc::new(LayoutIndex {
            root: self.root.clone(),
            raw,
            descriptor: Some(d),
        }))
    }
    async fn blob(&self, digest: &Digest) -> V1Result<Arc<dyn Layer>> {
        let d = self.find(digest).await?;
        Ok(layout_layer(&self.root, d))
    }
    async fn descriptor(&self) -> V1Result<Descriptor> {
        match &self.descriptor {
            Some(d) => Ok(d.clone()),
            None => Ok(Descriptor::from_bytes(MediaType::OciIndex, &self.raw)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use containerregistry_v1::{matcher, random, validate};

    #[tokio::test]
    async fn append_read_replace_gc() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::create(dir.path()).unwrap();
        let a = random::image(1024, 3).await.unwrap();
        let b = random::image(1024, 2).await.unwrap();
        layout
            .append_image(
                a.as_ref(),
                &AppendOptions::default().with_annotation(matcher::ANNOTATION_REF_NAME, "a"),
            )
            .await
            .unwrap();
        let idx = random::index(256, 1, 2).await.unwrap();
        layout
            .append_index(idx.as_ref(), &AppendOptions::default())
            .await
            .unwrap();

        let root = layout.image_index_v1().unwrap();
        validate::index(root.as_ref(), validate::Options::default())
            .await
            .unwrap();
        let got = root.image(&a.digest().await.unwrap()).await.unwrap();
        assert_eq!(
            got.raw_manifest().await.unwrap(),
            a.raw_manifest().await.unwrap()
        );
        let l = got.layers().await.unwrap().remove(0);
        assert!(l.local_path().unwrap().exists());

        layout
            .replace_image(
                b.as_ref(),
                &matcher::name("a"),
                &AppendOptions::default().with_annotation(matcher::ANNOTATION_REF_NAME, "a"),
            )
            .await
            .unwrap();
        let garbage = layout.garbage_collect().await.unwrap();
        // a's manifest, config and 3 layers are now unreferenced.
        assert_eq!(garbage.len(), 5);
        for g in &garbage {
            layout.remove_blob(g).unwrap();
        }
        let root = layout.image_index_v1().unwrap();
        validate::index(root.as_ref(), validate::Options::default())
            .await
            .unwrap();

        layout.remove_descriptors(&matcher::name("a")).unwrap();
        assert_eq!(
            layout
                .image_index_v1()
                .unwrap()
                .index_manifest()
                .await
                .unwrap()
                .manifests()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn write_blob_stream_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::create(dir.path()).unwrap();
        let data = vec![7u8; 3 << 20];
        let (d, n) = layout
            .write_blob_stream(data.as_slice(), None)
            .await
            .unwrap();
        assert_eq!((d.clone(), n), (Digest::sha256(&data), data.len() as u64));
        assert!(
            layout
                .write_blob_stream(&b"other"[..], Some(&Digest::sha256(b"x")))
                .await
                .is_err()
        );
        let leftover: Vec<_> = std::fs::read_dir(dir.path().join("blobs/sha256"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".tmp-")
            })
            .collect();
        assert!(leftover.is_empty());
    }
}
