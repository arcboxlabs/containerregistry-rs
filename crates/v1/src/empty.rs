//! Empty images and indexes (go-containerregistry `pkg/v1/empty`).
//!
//! These are the usual starting points for building images with
//! [`crate::mutate`].

use std::sync::Arc;

use containerregistry_image::{
    ContainerConfig, Descriptor, DockerManifest, ImageConfig, MediaType, OciIndex,
};

use crate::partial::{FixedImage, FixedIndex};
use crate::{Image, ImageIndex};

/// An image with no layers and an empty config, using Docker media types
/// like go-containerregistry's `empty.Image`. Use [`crate::mutate`] to
/// change the media types to OCI.
pub fn image() -> Arc<dyn Image> {
    let mut config = ImageConfig::new("", "");
    config.config = Some(ContainerConfig::default());
    let config = config.to_bytes().expect("serializable config");
    let manifest = DockerManifest::new(
        Descriptor::from_bytes(MediaType::DockerConfig, &config),
        Vec::new(),
    )
    .to_bytes()
    .expect("serializable manifest");
    Arc::new(FixedImage::new(manifest, config, Vec::new()))
}

/// An OCI index with no manifests (go `empty.Index`).
pub fn index() -> Arc<dyn ImageIndex> {
    let manifest = OciIndex::new(Vec::new())
        .to_bytes()
        .expect("serializable index");
    Arc::new(FixedIndex::new(manifest, Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_image_and_index() {
        let img = image();
        assert_eq!(img.media_type().await.unwrap(), MediaType::DockerManifest);
        assert!(img.layers().await.unwrap().is_empty());
        let cfg = img.config_file().await.unwrap();
        assert_eq!(cfg.rootfs.fs_type, "layers");
        assert_eq!(
            img.config_name().await.unwrap(),
            img.manifest().await.unwrap().config().digest
        );

        let idx = index();
        assert_eq!(idx.media_type().await.unwrap(), MediaType::OciIndex);
        assert!(idx.index_manifest().await.unwrap().manifests().is_empty());
    }
}
