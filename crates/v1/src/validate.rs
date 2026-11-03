//! Consistency checks for images, indexes and layers
//! (go-containerregistry `pkg/v1/validate`).
//!
//! Every blob is streamed and hashed, so validating a multi-gigabyte image
//! needs constant memory. [`Options::fast`] skips reading layer content.

use std::sync::Arc;

use containerregistry_image::{Digest, Manifest};

use crate::compression::Compression;
use crate::layer::Layer;
use crate::partial::{Child, children, digest_reader};
use crate::{Error, Image, ImageIndex, Result};

/// Validation options.
#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
    /// Only check manifests and configs; do not read layer content
    /// (go `validate.Fast`).
    pub fast: bool,
}

fn mismatch(what: &str, want: impl std::fmt::Display, got: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("{} mismatch: expected {}, got {}", what, want, got))
}

/// Checks that a layer's digest, size and (for known compressed layer
/// types) diff ID match its content (`validate.Layer`).
pub async fn layer(l: &dyn Layer, opts: Options) -> Result<()> {
    if opts.fast {
        return Ok(());
    }
    let digest = l.digest().await?;
    let (got, size) = digest_reader(l.compressed().await?, digest.algorithm()).await?;
    if got != digest {
        return Err(mismatch("layer digest", &digest, &got));
    }
    let want_size = l.size().await?;
    if size != want_size {
        return Err(mismatch("layer size", want_size, size));
    }
    let mt = l.media_type().await?;
    if Compression::from_media_type(&mt).is_some() {
        let diff_id = l.diff_id().await?;
        let (got, _) = digest_reader(l.uncompressed().await?, diff_id.algorithm()).await?;
        if got != diff_id {
            return Err(mismatch("layer diff id", &diff_id, &got));
        }
    }
    Ok(())
}

/// Checks an image: manifest and config digests, layer descriptors versus
/// the config's diff IDs, and (unless fast) every layer's content
/// (`validate.Image`).
pub async fn image(img: &dyn Image, opts: Options) -> Result<()> {
    let raw = img.raw_manifest().await?;
    let digest = img.digest().await?;
    let computed = Digest::compute(digest.algorithm(), &raw);
    if computed != digest {
        return Err(mismatch("manifest digest", &digest, &computed));
    }
    if img.size().await? != raw.len() as u64 {
        return Err(mismatch("manifest size", img.size().await?, raw.len()));
    }
    let manifest = Manifest::from_bytes_unchecked(&raw, None)?;
    let config = img.raw_config_file().await?;
    let cd = &manifest.config().digest;
    let got = Digest::compute(cd.algorithm(), &config);
    if &got != cd {
        return Err(mismatch("config digest", cd, &got));
    }
    if manifest.config().size != config.len() as u64 {
        return Err(mismatch(
            "config size",
            manifest.config().size,
            config.len(),
        ));
    }

    let is_image = img.config_file().await.ok().filter(|c| {
        matches!(
            manifest.config().media_type,
            containerregistry_image::MediaType::OciConfig
                | containerregistry_image::MediaType::DockerConfig
        ) || !c.rootfs.diff_ids.is_empty()
    });
    if let Some(cfg) = &is_image
        && cfg.rootfs.diff_ids.len() != manifest.layers().len()
    {
        return Err(mismatch(
            "number of layers vs diff ids",
            manifest.layers().len(),
            cfg.rootfs.diff_ids.len(),
        ));
    }

    for (i, desc) in manifest.layers().iter().enumerate() {
        let l = img.layer_by_digest(&desc.digest).await?;
        if l.digest().await? != desc.digest {
            return Err(mismatch(
                "layer descriptor digest",
                &desc.digest,
                l.digest().await?,
            ));
        }
        if l.size().await? != desc.size {
            return Err(mismatch(
                "layer descriptor size",
                desc.size,
                l.size().await?,
            ));
        }
        if !opts.fast
            && let Some(cfg) = &is_image
            && Compression::from_media_type(&desc.media_type).is_some()
        {
            let want = &cfg.rootfs.diff_ids[i];
            let got = l.diff_id().await?;
            if &got != want {
                return Err(mismatch("layer diff id vs config", want, &got));
            }
        }
        layer(l.as_ref(), opts).await?;
    }
    Ok(())
}

/// Checks an index and, recursively, all of its children
/// (`validate.Index`).
pub async fn index(idx: &dyn ImageIndex, opts: Options) -> Result<()> {
    let raw = idx.raw_manifest().await?;
    let digest = idx.digest().await?;
    let computed = Digest::compute(digest.algorithm(), &raw);
    if computed != digest {
        return Err(mismatch("index digest", &digest, &computed));
    }
    for (desc, child) in children(idx).await? {
        match child {
            Child::Image(i) => {
                check_desc(&desc, &i.raw_manifest().await?)?;
                image(i.as_ref(), opts).await?;
            }
            Child::Index(i) => {
                check_desc(&desc, &i.raw_manifest().await?)?;
                Box::pin(index(i.as_ref(), opts)).await?;
            }
            Child::Blob(b) => layer(b.as_ref(), opts).await?,
        }
    }
    Ok(())
}

fn check_desc(desc: &containerregistry_image::Descriptor, raw: &[u8]) -> Result<()> {
    let got = Digest::compute(desc.digest.algorithm(), raw);
    if got != desc.digest {
        return Err(mismatch("child manifest digest", &desc.digest, &got));
    }
    if desc.size != raw.len() as u64 {
        return Err(mismatch("child manifest size", desc.size, raw.len()));
    }
    Ok(())
}

/// Validates an `Arc` image (convenience for callers holding trait objects).
pub async fn image_arc(img: &Arc<dyn Image>, opts: Options) -> Result<()> {
    image(img.as_ref(), opts).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{empty, mutate, random};
    use containerregistry_image::MediaType;

    #[tokio::test]
    async fn valid_and_invalid() {
        let img = random::image(256, 2).await.unwrap();
        image(img.as_ref(), Options::default()).await.unwrap();
        let idx = random::index(64, 1, 2).await.unwrap();
        index(idx.as_ref(), Options::default()).await.unwrap();

        // A layer whose claimed diff id is wrong.
        let l = random::layer(64, MediaType::DockerLayerGzip).await.unwrap();
        let mut cf = empty::image().config_file().await.unwrap();
        cf.rootfs.diff_ids.push(Digest::sha256(b"wrong"));
        let desc = l.descriptor().await.unwrap();
        let config = cf.to_bytes().unwrap();
        let manifest = containerregistry_image::DockerManifest::new(
            containerregistry_image::Descriptor::from_bytes(MediaType::DockerConfig, &config),
            vec![desc],
        )
        .to_bytes()
        .unwrap();
        let bad = crate::partial::FixedImage::new(manifest, config, vec![l]);
        assert!(image(&bad, Options::default()).await.is_err());
        assert!(image(&bad, Options { fast: true }).await.is_ok());

        let ok = mutate::append_layers(
            empty::image(),
            vec![random::layer(32, MediaType::OciLayerZstd).await.unwrap()],
        );
        image(ok.as_ref(), Options::default()).await.unwrap();
    }
}
