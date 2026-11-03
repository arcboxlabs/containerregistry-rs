//! The [`Image`] and [`ImageIndex`] traits (go-containerregistry `v1.Image`
//! and `v1.ImageIndex`).
//!
//! Implementations provide the raw manifest (and, for images, the raw config
//! and a way to look up blobs); everything else has a default derived from
//! those, which is what go-containerregistry's `partial` package does.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use containerregistry_image::{
    Descriptor, Digest, ImageConfig, ImageIndex as IndexManifest, Manifest, MediaType,
};

use crate::layer::Layer;
use crate::static_layer::StaticLayer;
use crate::{Error, Result};

/// Reads the `mediaType` field of a manifest document, if present.
pub fn probe_media_type(raw: &[u8]) -> Option<MediaType> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Probe {
        media_type: Option<MediaType>,
    }
    serde_json::from_slice::<Probe>(raw).ok()?.media_type
}

/// A single-platform image (or OCI artifact): a manifest, a config blob and
/// an ordered list of layers.
#[async_trait]
pub trait Image: Send + Sync {
    /// The serialized manifest, exactly as it is (or will be) stored.
    async fn raw_manifest(&self) -> Result<Vec<u8>>;

    /// The serialized config blob.
    async fn raw_config_file(&self) -> Result<Vec<u8>>;

    /// Looks up a blob of this image (a layer, or the config) by digest.
    async fn layer_by_digest(&self, digest: &Digest) -> Result<Arc<dyn Layer>>;

    /// The parsed manifest.
    async fn manifest(&self) -> Result<Manifest> {
        let raw = self.raw_manifest().await?;
        Ok(Manifest::from_bytes_unchecked(&raw, None)?)
    }

    /// The media type of the manifest.
    async fn media_type(&self) -> Result<MediaType> {
        let raw = self.raw_manifest().await?;
        if let Some(mt) = probe_media_type(&raw) {
            return Ok(mt);
        }
        Ok(Manifest::from_bytes_unchecked(&raw, None)?.media_type())
    }

    /// The SHA-256 digest of the manifest.
    async fn digest(&self) -> Result<Digest> {
        Ok(Digest::sha256(&self.raw_manifest().await?))
    }

    /// The size of the manifest in bytes.
    async fn size(&self) -> Result<u64> {
        Ok(self.raw_manifest().await?.len() as u64)
    }

    /// The SHA-256 digest of the config blob (the "image ID").
    async fn config_name(&self) -> Result<Digest> {
        Ok(Digest::sha256(&self.raw_config_file().await?))
    }

    /// The parsed image config. Fails for artifacts whose config is not an
    /// image config.
    async fn config_file(&self) -> Result<ImageConfig> {
        Ok(ImageConfig::from_bytes(&self.raw_config_file().await?)?)
    }

    /// The layers, base first.
    async fn layers(&self) -> Result<Vec<Arc<dyn Layer>>> {
        let manifest = self.manifest().await?;
        let mut out = Vec::with_capacity(manifest.layers().len());
        for desc in manifest.layers() {
            out.push(self.layer_by_digest(&desc.digest).await?);
        }
        Ok(out)
    }

    /// Looks up a layer by its uncompressed digest.
    async fn layer_by_diff_id(&self, diff_id: &Digest) -> Result<Arc<dyn Layer>> {
        let digest = crate::partial::diff_id_to_blob(self, diff_id).await?;
        self.layer_by_digest(&digest).await
    }

    /// The config blob as a layer.
    async fn config_layer(&self) -> Result<Arc<dyn Layer>> {
        let manifest = self.manifest().await?;
        let raw = self.raw_config_file().await?;
        Ok(Arc::new(StaticLayer::with_descriptor(
            raw,
            manifest.config().clone(),
        )))
    }

    /// The descriptor of this image. Images that came from an index
    /// override this to carry the platform and annotations.
    async fn descriptor(&self) -> Result<Descriptor> {
        let raw = self.raw_manifest().await?;
        let mt = match probe_media_type(&raw) {
            Some(mt) => mt,
            None => self.media_type().await?,
        };
        let mut d = Descriptor::new(mt, Digest::sha256(&raw), raw.len() as u64);
        if let Ok(m) = Manifest::from_bytes_unchecked(&raw, None) {
            d.artifact_type = crate::partial::artifact_type(&m);
        }
        Ok(d)
    }
}

/// A multi-platform index (or Docker manifest list).
#[async_trait]
pub trait ImageIndex: Send + Sync {
    /// The serialized index manifest, exactly as it is (or will be) stored.
    async fn raw_manifest(&self) -> Result<Vec<u8>>;

    /// Returns the image the index references with `digest`.
    async fn image(&self, digest: &Digest) -> Result<Arc<dyn Image>>;

    /// Returns the nested index the index references with `digest`.
    async fn image_index(&self, digest: &Digest) -> Result<Arc<dyn ImageIndex>>;

    /// Returns a non-image child (e.g. an attestation blob) by digest.
    async fn blob(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        Err(Error::Unsupported(format!(
            "index does not provide non-manifest child {}",
            digest
        )))
    }

    /// The parsed index manifest.
    async fn index_manifest(&self) -> Result<IndexManifest> {
        let raw = self.raw_manifest().await?;
        Ok(IndexManifest::from_bytes_unchecked(&raw, None)?)
    }

    /// The media type of the index manifest.
    async fn media_type(&self) -> Result<MediaType> {
        let raw = self.raw_manifest().await?;
        if let Some(mt) = probe_media_type(&raw) {
            return Ok(mt);
        }
        Ok(IndexManifest::from_bytes_unchecked(&raw, None)?.media_type())
    }

    /// The SHA-256 digest of the index manifest.
    async fn digest(&self) -> Result<Digest> {
        Ok(Digest::sha256(&self.raw_manifest().await?))
    }

    /// The size of the index manifest in bytes.
    async fn size(&self) -> Result<u64> {
        Ok(self.raw_manifest().await?.len() as u64)
    }

    /// The descriptor of this index.
    async fn descriptor(&self) -> Result<Descriptor> {
        let raw = self.raw_manifest().await?;
        let mt = match probe_media_type(&raw) {
            Some(mt) => mt,
            None => self.media_type().await?,
        };
        let mut d = Descriptor::new(mt, Digest::sha256(&raw), raw.len() as u64);
        if let Ok(IndexManifest::Oci(i)) = IndexManifest::from_bytes_unchecked(&raw, None) {
            d.artifact_type = i.artifact_type;
        }
        Ok(d)
    }
}

#[async_trait]
impl<I: Image + ?Sized> Image for Arc<I> {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        (**self).raw_manifest().await
    }
    async fn raw_config_file(&self) -> Result<Vec<u8>> {
        (**self).raw_config_file().await
    }
    async fn layer_by_digest(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        (**self).layer_by_digest(digest).await
    }
    async fn manifest(&self) -> Result<Manifest> {
        (**self).manifest().await
    }
    async fn media_type(&self) -> Result<MediaType> {
        (**self).media_type().await
    }
    async fn digest(&self) -> Result<Digest> {
        (**self).digest().await
    }
    async fn size(&self) -> Result<u64> {
        (**self).size().await
    }
    async fn config_name(&self) -> Result<Digest> {
        (**self).config_name().await
    }
    async fn config_file(&self) -> Result<ImageConfig> {
        (**self).config_file().await
    }
    async fn layers(&self) -> Result<Vec<Arc<dyn Layer>>> {
        (**self).layers().await
    }
    async fn layer_by_diff_id(&self, diff_id: &Digest) -> Result<Arc<dyn Layer>> {
        (**self).layer_by_diff_id(diff_id).await
    }
    async fn config_layer(&self) -> Result<Arc<dyn Layer>> {
        (**self).config_layer().await
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        (**self).descriptor().await
    }
}

#[async_trait]
impl<I: ImageIndex + ?Sized> ImageIndex for Arc<I> {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        (**self).raw_manifest().await
    }
    async fn image(&self, digest: &Digest) -> Result<Arc<dyn Image>> {
        (**self).image(digest).await
    }
    async fn image_index(&self, digest: &Digest) -> Result<Arc<dyn ImageIndex>> {
        (**self).image_index(digest).await
    }
    async fn blob(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        (**self).blob(digest).await
    }
    async fn index_manifest(&self) -> Result<IndexManifest> {
        (**self).index_manifest().await
    }
    async fn media_type(&self) -> Result<MediaType> {
        (**self).media_type().await
    }
    async fn digest(&self) -> Result<Digest> {
        (**self).digest().await
    }
    async fn size(&self) -> Result<u64> {
        (**self).size().await
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        (**self).descriptor().await
    }
}

/// Either an image or an index: something that can be written to a tag
/// (go-containerregistry `remote.Taggable`).
#[derive(Clone)]
pub enum Taggable {
    /// A single image.
    Image(Arc<dyn Image>),
    /// An index.
    Index(Arc<dyn ImageIndex>),
}

impl Taggable {
    /// The raw manifest.
    pub async fn raw_manifest(&self) -> Result<Vec<u8>> {
        match self {
            Taggable::Image(i) => i.raw_manifest().await,
            Taggable::Index(i) => i.raw_manifest().await,
        }
    }

    /// The manifest media type.
    pub async fn media_type(&self) -> Result<MediaType> {
        match self {
            Taggable::Image(i) => i.media_type().await,
            Taggable::Index(i) => i.media_type().await,
        }
    }

    /// The manifest digest.
    pub async fn digest(&self) -> Result<Digest> {
        match self {
            Taggable::Image(i) => i.digest().await,
            Taggable::Index(i) => i.digest().await,
        }
    }

    /// The descriptor.
    pub async fn descriptor(&self) -> Result<Descriptor> {
        match self {
            Taggable::Image(i) => i.descriptor().await,
            Taggable::Index(i) => i.descriptor().await,
        }
    }
}

impl std::fmt::Debug for Taggable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Taggable::Image(_) => f.write_str("Taggable::Image"),
            Taggable::Index(_) => f.write_str("Taggable::Index"),
        }
    }
}
