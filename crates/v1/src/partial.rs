//! Helpers for implementing and inspecting images, indexes and layers
//! (go-containerregistry `pkg/v1/partial`).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;

use containerregistry_image::{
    Algorithm, Descriptor, Digest, Hasher, ImageConfig, Manifest, MediaType,
};

use crate::compression::{self, Compression};
use crate::image::{Image, ImageIndex};
use crate::layer::{BoxReader, Layer};
use crate::matcher::Matcher;
use crate::{Error, Result};

/// Hashes and counts everything readable from `reader`.
pub async fn digest_reader(mut reader: BoxReader, algorithm: Algorithm) -> Result<(Digest, u64)> {
    let mut h = Hasher::new(algorithm);
    let mut buf = vec![0u8; 256 * 1024];
    let mut n = 0u64;
    loop {
        let read = reader.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        h.update(&buf[..read]);
        n += read as u64;
    }
    Ok((h.finalize(), n))
}

/// The artifact type of a manifest: its `artifactType`, else its config
/// media type (go-containerregistry `partial.ArtifactType`).
pub fn artifact_type(manifest: &Manifest) -> Option<MediaType> {
    match manifest.artifact_type() {
        Some(at) => Some(at.clone()),
        None => Some(manifest.config().media_type.clone()),
    }
}

/// Maps an uncompressed layer digest to its blob digest using the config's
/// `rootfs.diff_ids` (go-containerregistry `partial.DiffIDToBlob`).
pub async fn diff_id_to_blob<I: Image + ?Sized>(img: &I, diff_id: &Digest) -> Result<Digest> {
    let config = img.config_file().await?;
    let manifest = img.manifest().await?;
    let i = config
        .rootfs
        .diff_ids
        .iter()
        .position(|d| d == diff_id)
        .ok_or_else(|| Error::NotFound(format!("diff id {}", diff_id)))?;
    manifest
        .layers()
        .get(i)
        .map(|d| d.digest.clone())
        .ok_or_else(|| Error::Invalid(format!("no layer for diff id {}", diff_id)))
}

/// Maps a blob digest to its uncompressed digest (`partial.BlobToDiffID`).
pub async fn blob_to_diff_id<I: Image + ?Sized>(img: &I, digest: &Digest) -> Result<Digest> {
    let config = img.config_file().await?;
    let manifest = img.manifest().await?;
    let i = manifest
        .layers()
        .iter()
        .position(|d| &d.digest == digest)
        .ok_or_else(|| Error::NotFound(format!("blob {}", digest)))?;
    config
        .rootfs
        .diff_ids
        .get(i)
        .cloned()
        .ok_or_else(|| Error::Invalid(format!("no diff id for blob {}", digest)))
}

/// The descriptor of a blob (config or layer) in the image's manifest
/// (`partial.BlobDescriptor`).
pub async fn blob_descriptor<I: Image + ?Sized>(img: &I, digest: &Digest) -> Result<Descriptor> {
    let manifest = img.manifest().await?;
    find_blob_descriptor(&manifest, digest)
        .cloned()
        .ok_or_else(|| Error::NotFound(format!("blob {} not in manifest", digest)))
}

/// Finds the descriptor of a blob (config or layer) in a manifest.
pub fn find_blob_descriptor<'a>(manifest: &'a Manifest, digest: &Digest) -> Option<&'a Descriptor> {
    if &manifest.config().digest == digest {
        return Some(manifest.config());
    }
    manifest.layers().iter().find(|d| &d.digest == digest)
}

/// The size of a blob in the image's manifest (`partial.BlobSize`).
pub async fn blob_size<I: Image + ?Sized>(img: &I, digest: &Digest) -> Result<u64> {
    Ok(blob_descriptor(img, digest).await?.size)
}

/// The layer digests of the image (`partial.FSLayers`).
pub async fn fs_layers<I: Image + ?Sized>(img: &I) -> Result<Vec<Digest>> {
    Ok(img
        .manifest()
        .await?
        .layers()
        .iter()
        .map(|d| d.digest.clone())
        .collect())
}

/// The diff IDs from the config (`partial.DiffIDs`).
pub async fn diff_ids<I: Image + ?Sized>(img: &I) -> Result<Vec<Digest>> {
    Ok(img.config_file().await?.rootfs.diff_ids)
}

/// The uncompressed size of a layer, reading it if necessary
/// (`partial.UncompressedSize`).
pub async fn uncompressed_size(layer: &dyn Layer) -> Result<u64> {
    if let Some(n) = layer.uncompressed_size().await? {
        return Ok(n);
    }
    let (_, n) = digest_reader(layer.uncompressed().await?, Algorithm::Sha256).await?;
    Ok(n)
}

/// The descriptors of an index matching `matcher` (`partial.FindManifests`).
pub async fn find_manifests<X: ImageIndex + ?Sized>(
    index: &X,
    matcher: &Matcher,
) -> Result<Vec<Descriptor>> {
    Ok(index
        .index_manifest()
        .await?
        .manifests()
        .iter()
        .filter(|d| matcher(d))
        .cloned()
        .collect())
}

/// The images of an index matching `matcher` (`partial.FindImages`).
pub async fn find_images<X: ImageIndex + ?Sized>(
    index: &X,
    matcher: &Matcher,
) -> Result<Vec<Arc<dyn Image>>> {
    let mut out = Vec::new();
    for d in find_manifests(index, matcher).await? {
        if d.media_type.is_manifest() {
            out.push(index.image(&d.digest).await?);
        }
    }
    Ok(out)
}

/// The nested indexes of an index matching `matcher` (`partial.FindIndexes`).
pub async fn find_indexes<X: ImageIndex + ?Sized>(
    index: &X,
    matcher: &Matcher,
) -> Result<Vec<Arc<dyn ImageIndex>>> {
    let mut out = Vec::new();
    for d in find_manifests(index, matcher).await? {
        if d.media_type.is_index() {
            out.push(index.image_index(&d.digest).await?);
        }
    }
    Ok(out)
}

/// A child of an index.
#[derive(Clone)]
pub enum Child {
    /// An image manifest.
    Image(Arc<dyn Image>),
    /// A nested index.
    Index(Arc<dyn ImageIndex>),
    /// Any other blob (e.g. an attestation).
    Blob(Arc<dyn Layer>),
}

/// Resolves every descriptor of an index into its child
/// (`partial.Manifests` / `partial.ComputeManifests`).
pub async fn children<X: ImageIndex + ?Sized>(index: &X) -> Result<Vec<(Descriptor, Child)>> {
    let manifest = index.index_manifest().await?;
    let mut out = Vec::with_capacity(manifest.manifests().len());
    for d in manifest.manifests() {
        let child = if d.media_type.is_index() {
            Child::Index(index.image_index(&d.digest).await?)
        } else if d.media_type.is_manifest() {
            Child::Image(index.image(&d.digest).await?)
        } else {
            Child::Blob(index.blob(&d.digest).await?)
        };
        out.push((d.clone(), child));
    }
    Ok(out)
}

/// Builds an image config "layer" for `img` (`partial.ConfigLayer`).
pub async fn config_layer<I: Image + ?Sized>(img: &I) -> Result<Arc<dyn Layer>> {
    img.config_layer().await
}

/// Parses the image config of `img`, or returns `None` for artifacts whose
/// config is not an image config.
pub async fn config_file_opt<I: Image + ?Sized>(img: &I) -> Option<ImageConfig> {
    img.config_file().await.ok()
}

/// The minimal surface of a layer whose stored form is compressed
/// (go-containerregistry `partial.CompressedLayer`).
#[async_trait]
pub trait CompressedLayerCore: Send + Sync {
    /// Digest of the stored bytes.
    async fn digest(&self) -> Result<Digest>;
    /// Opens the stored bytes.
    async fn compressed(&self) -> Result<BoxReader>;
    /// Size of the stored bytes.
    async fn size(&self) -> Result<u64>;
    /// Media type.
    async fn media_type(&self) -> Result<MediaType>;
    /// Full descriptor (defaults to media type, digest and size).
    async fn descriptor(&self) -> Result<Descriptor> {
        Ok(Descriptor::new(
            self.media_type().await?,
            self.digest().await?,
            self.size().await?,
        ))
    }
}

/// Extends a [`CompressedLayerCore`] into a [`Layer`], computing the diff ID
/// and uncompressed stream on demand (`partial.CompressedToLayer`).
///
/// Layers with custom (artifact) media types are treated as opaque: their
/// uncompressed form is the stored form.
pub struct CompressedLayer<C> {
    core: C,
    diff_id: OnceCell<Digest>,
}

impl<C: CompressedLayerCore> CompressedLayer<C> {
    /// Wraps a core.
    pub fn new(core: C) -> Self {
        Self {
            core,
            diff_id: OnceCell::new(),
        }
    }

    /// Wraps a core whose diff ID is already known.
    pub fn with_diff_id(core: C, diff_id: Digest) -> Self {
        Self {
            core,
            diff_id: OnceCell::new_with(Some(diff_id)),
        }
    }

    /// The wrapped core.
    pub fn core(&self) -> &C {
        &self.core
    }

    async fn compression(&self) -> Result<Compression> {
        Ok(Compression::from_media_type(&self.core.media_type().await?)
            .unwrap_or(Compression::None))
    }
}

#[async_trait]
impl<C: CompressedLayerCore> Layer for CompressedLayer<C> {
    async fn digest(&self) -> Result<Digest> {
        self.core.digest().await
    }

    async fn diff_id(&self) -> Result<Digest> {
        self.diff_id
            .get_or_try_init(|| async {
                if self.compression().await? == Compression::None {
                    return self.core.digest().await;
                }
                let (d, _) = digest_reader(self.uncompressed().await?, Algorithm::Sha256).await?;
                Ok(d)
            })
            .await
            .cloned()
    }

    async fn compressed(&self) -> Result<BoxReader> {
        self.core.compressed().await
    }

    async fn uncompressed(&self) -> Result<BoxReader> {
        let c = self.compression().await?;
        let r = self.core.compressed().await?;
        Ok(compression::decompress(r, c))
    }

    async fn size(&self) -> Result<u64> {
        self.core.size().await
    }

    async fn media_type(&self) -> Result<MediaType> {
        self.core.media_type().await
    }

    async fn descriptor(&self) -> Result<Descriptor> {
        self.core.descriptor().await
    }
}

/// The minimal surface of a layer whose natural form is uncompressed
/// (go-containerregistry `partial.UncompressedLayer`).
#[async_trait]
pub trait UncompressedLayerCore: Send + Sync {
    /// Digest of the uncompressed bytes.
    async fn diff_id(&self) -> Result<Digest>;
    /// Opens the uncompressed bytes.
    async fn uncompressed(&self) -> Result<BoxReader>;
    /// Media type of the (compressed) layer.
    async fn media_type(&self) -> Result<MediaType>;
}

/// Extends an [`UncompressedLayerCore`] into a [`Layer`] that compresses on
/// the fly (`partial.UncompressedToLayer`). The compressed digest and size
/// are computed once, by compressing the stream, when first needed.
pub struct UncompressedLayer<C> {
    core: C,
    compression: Compression,
    level: Option<i32>,
    computed: OnceCell<(Digest, u64)>,
}

impl<C: UncompressedLayerCore> UncompressedLayer<C> {
    /// Wraps a core, compressing with gzip.
    pub fn new(core: C) -> Self {
        Self::with_compression(core, Compression::Gzip, None)
    }

    /// Wraps a core with a specific compression and level.
    pub fn with_compression(core: C, compression: Compression, level: Option<i32>) -> Self {
        Self {
            core,
            compression,
            level,
            computed: OnceCell::new(),
        }
    }

    async fn computed(&self) -> Result<&(Digest, u64)> {
        self.computed
            .get_or_try_init(|| async {
                digest_reader(self.compressed().await?, Algorithm::Sha256).await
            })
            .await
    }
}

#[async_trait]
impl<C: UncompressedLayerCore> Layer for UncompressedLayer<C> {
    async fn digest(&self) -> Result<Digest> {
        Ok(self.computed().await?.0.clone())
    }

    async fn diff_id(&self) -> Result<Digest> {
        self.core.diff_id().await
    }

    async fn compressed(&self) -> Result<BoxReader> {
        Ok(compression::compress(
            self.core.uncompressed().await?,
            self.compression,
            self.level,
        ))
    }

    async fn uncompressed(&self) -> Result<BoxReader> {
        self.core.uncompressed().await
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.computed().await?.1)
    }

    async fn media_type(&self) -> Result<MediaType> {
        self.core.media_type().await
    }
}

/// An image assembled from a fixed manifest, config and set of blobs.
///
/// This is the building block for in-memory images ([`crate::empty`],
/// [`crate::random`]) and for sources that know every blob up front.
#[derive(Clone)]
pub struct FixedImage {
    manifest: Vec<u8>,
    config: Vec<u8>,
    layers: Vec<Arc<dyn Layer>>,
    descriptor: Option<Descriptor>,
}

impl FixedImage {
    /// Creates an image. `layers` must contain every blob the manifest
    /// references (in any order); blobs are looked up by digest.
    pub fn new(manifest: Vec<u8>, config: Vec<u8>, layers: Vec<Arc<dyn Layer>>) -> Self {
        Self {
            manifest,
            config,
            layers,
            descriptor: None,
        }
    }

    /// Overrides the descriptor (e.g. to carry a platform from an index).
    pub fn with_descriptor(mut self, descriptor: Descriptor) -> Self {
        self.descriptor = Some(descriptor);
        self
    }
}

#[async_trait]
impl Image for FixedImage {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        Ok(self.manifest.clone())
    }

    async fn raw_config_file(&self) -> Result<Vec<u8>> {
        Ok(self.config.clone())
    }

    async fn layer_by_digest(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        let manifest = self.manifest().await?;
        if &manifest.config().digest == digest {
            return self.config_layer().await;
        }
        let want = find_blob_descriptor(&manifest, digest)
            .ok_or_else(|| Error::NotFound(format!("blob {} not in manifest", digest)))?
            .clone();
        for l in &self.layers {
            if &l.digest().await? == digest {
                return Ok(Arc::new(Described::new(l.clone(), want)));
            }
        }
        Err(Error::NotFound(format!("blob {}", digest)))
    }

    async fn descriptor(&self) -> Result<Descriptor> {
        let raw = &self.manifest;
        let mut d = match &self.descriptor {
            Some(d) => d.clone(),
            None => {
                let mt = match crate::image::probe_media_type(raw) {
                    Some(mt) => mt,
                    None => self.media_type().await?,
                };
                Descriptor::new(mt, Digest::sha256(raw), 0)
            }
        };
        d.digest = Digest::sha256(raw);
        d.size = raw.len() as u64;
        if d.artifact_type.is_none()
            && let Ok(m) = Manifest::from_bytes_unchecked(raw, None)
        {
            d.artifact_type = artifact_type(&m);
        }
        Ok(d)
    }
}

/// An index assembled from a fixed index manifest and its children.
#[derive(Clone)]
pub struct FixedIndex {
    manifest: Vec<u8>,
    children: Vec<(Digest, Child)>,
}

impl FixedIndex {
    /// Creates an index from its raw manifest and the children it
    /// references, keyed by digest.
    pub fn new(manifest: Vec<u8>, children: Vec<(Digest, Child)>) -> Self {
        Self { manifest, children }
    }

    fn child(&self, digest: &Digest) -> Result<&Child> {
        self.children
            .iter()
            .find(|(d, _)| d == digest)
            .map(|(_, c)| c)
            .ok_or_else(|| Error::NotFound(format!("manifest {} in index", digest)))
    }
}

#[async_trait]
impl ImageIndex for FixedIndex {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        Ok(self.manifest.clone())
    }

    async fn image(&self, digest: &Digest) -> Result<Arc<dyn Image>> {
        match self.child(digest)? {
            Child::Image(i) => Ok(i.clone()),
            _ => Err(Error::Invalid(format!("{} is not an image", digest))),
        }
    }

    async fn image_index(&self, digest: &Digest) -> Result<Arc<dyn ImageIndex>> {
        match self.child(digest)? {
            Child::Index(i) => Ok(i.clone()),
            _ => Err(Error::Invalid(format!("{} is not an index", digest))),
        }
    }

    async fn blob(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        match self.child(digest)? {
            Child::Blob(b) => Ok(b.clone()),
            _ => Err(Error::Invalid(format!("{} is not a blob", digest))),
        }
    }
}

/// A layer whose descriptor (media type, annotations, URLs, ...) is
/// overridden while its content is delegated to another layer.
pub struct Described {
    inner: Arc<dyn Layer>,
    descriptor: Descriptor,
}

impl Described {
    /// Wraps `inner`, reporting `descriptor` (digest and size are still
    /// taken from `inner`).
    pub fn new(inner: Arc<dyn Layer>, descriptor: Descriptor) -> Self {
        Self { inner, descriptor }
    }
}

#[async_trait]
impl Layer for Described {
    async fn digest(&self) -> Result<Digest> {
        self.inner.digest().await
    }
    async fn diff_id(&self) -> Result<Digest> {
        self.inner.diff_id().await
    }
    async fn compressed(&self) -> Result<BoxReader> {
        self.inner.compressed().await
    }
    async fn uncompressed(&self) -> Result<BoxReader> {
        self.inner.uncompressed().await
    }
    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }
    async fn media_type(&self) -> Result<MediaType> {
        Ok(self.descriptor.media_type.clone())
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        let mut d = self.descriptor.clone();
        d.digest = self.inner.digest().await?;
        d.size = self.inner.size().await?;
        Ok(d)
    }
    async fn uncompressed_size(&self) -> Result<Option<u64>> {
        self.inner.uncompressed_size().await
    }
    fn mount_source(&self) -> Option<crate::MountSource> {
        self.inner.mount_source()
    }
    fn local_path(&self) -> Option<std::path::PathBuf> {
        self.inner.local_path()
    }
}
