//! Random images, indexes and layers for tests and benchmarks
//! (go-containerregistry `pkg/v1/random`).

use std::sync::Arc;

use rand::{Rng, RngCore};

use containerregistry_image::{
    Descriptor, Digest, DockerManifest, History, ImageConfig, MediaType, OciIndex, Platform,
};

use crate::compression::Compression;
use crate::layer::Layer;
use crate::partial::{Child, FixedImage, FixedIndex};
use crate::static_layer::StaticLayer;
use crate::{Image, ImageIndex, Result};

fn random_tar(byte_size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut content = vec![0u8; byte_size];
    rng.fill_bytes(&mut content);
    let name = format!("random_file_{}.txt", rng.random::<u32>());
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_ustar();
    header.set_size(byte_size as u64);
    header.set_mode(0o600);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    builder
        .append_data(&mut header, name, content.as_slice())
        .expect("in-memory tar");
    builder.into_inner().expect("in-memory tar")
}

/// A random layer: a tarball holding one file of `byte_size` random bytes,
/// compressed according to `media_type` (go `random.Layer`).
pub async fn layer(byte_size: usize, media_type: MediaType) -> Result<Arc<dyn Layer>> {
    let tar = random_tar(byte_size);
    let compression = Compression::from_media_type(&media_type).unwrap_or(Compression::Gzip);
    let compressed = crate::read_all(crate::compression::compress(
        Box::pin(std::io::Cursor::new(tar.clone())),
        compression,
        None,
    ))
    .await?;
    Ok(Arc::new(StaticLayer::with_uncompressed(
        compressed, tar, media_type,
    )))
}

/// A random image with `layers` Docker layers of `byte_size` bytes each
/// (go `random.Image`).
pub async fn image(byte_size: usize, layers: usize) -> Result<Arc<dyn Image>> {
    let mut config = ImageConfig::new("amd64", "linux");
    let mut descs = Vec::with_capacity(layers);
    let mut blobs: Vec<Arc<dyn Layer>> = Vec::with_capacity(layers);
    for i in 0..layers {
        let l = layer(byte_size, MediaType::DockerLayerGzip).await?;
        config.rootfs.diff_ids.push(l.diff_id().await?);
        config.history.push(History {
            author: Some("go-containerregistry".to_string()),
            created_by: Some(format!("random layer {}", i)),
            created: Some("1970-01-01T00:00:00Z".to_string()),
            ..Default::default()
        });
        descs.push(l.descriptor().await?);
        blobs.push(l);
    }
    let config = config.to_bytes()?;
    let manifest = DockerManifest::new(
        Descriptor::from_bytes(MediaType::DockerConfig, &config),
        descs,
    )
    .to_bytes()?;
    Ok(Arc::new(FixedImage::new(manifest, config, blobs)))
}

/// A random OCI index of `count` random images (go `random.Index`).
pub async fn index(byte_size: usize, layers: usize, count: usize) -> Result<Arc<dyn ImageIndex>> {
    let mut descs = Vec::with_capacity(count);
    let mut children = Vec::with_capacity(count);
    for i in 0..count {
        let img = image(byte_size, layers).await?;
        let digest: Digest = img.digest().await?;
        let mut d = img.descriptor().await?;
        d.platform = Some(Platform::new(format!("arch{}", i), "linux"));
        descs.push(d);
        children.push((digest, Child::Image(img)));
    }
    let manifest = OciIndex::new(descs).to_bytes()?;
    Ok(Arc::new(FixedIndex::new(manifest, children)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partial::digest_reader;
    use containerregistry_image::Algorithm;

    #[tokio::test]
    async fn random_image_is_consistent() {
        let img = image(1024, 3).await.unwrap();
        let layers = img.layers().await.unwrap();
        assert_eq!(layers.len(), 3);
        let cfg = img.config_file().await.unwrap();
        for (l, diff_id) in layers.iter().zip(&cfg.rootfs.diff_ids) {
            let (d, n) = digest_reader(l.compressed().await.unwrap(), Algorithm::Sha256)
                .await
                .unwrap();
            assert_eq!(d, l.digest().await.unwrap());
            assert_eq!(n, l.size().await.unwrap());
            let (u, _) = digest_reader(l.uncompressed().await.unwrap(), Algorithm::Sha256)
                .await
                .unwrap();
            assert_eq!(&u, diff_id);
        }
        let idx = index(64, 1, 2).await.unwrap();
        assert_eq!(idx.index_manifest().await.unwrap().manifests().len(), 2);
    }
}
