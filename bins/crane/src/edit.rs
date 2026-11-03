//! `crane edit`: edit an image's config, manifest or a file in its
//! filesystem, interactively with `$EDITOR` or from stdin.

use std::io::{IsTerminal, Read, Write};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use clap::Subcommand;

use containerregistry_image::{Digest, MediaType, OciManifest};
use containerregistry_v1::compression::{self, Compression};
use containerregistry_v1::static_layer::StaticLayer;
use containerregistry_v1::{Image, Layer, mutate, probe_media_type};

use crate::util::{Ctx, parse_ref};

#[derive(Subcommand)]
pub enum EditCmd {
    /// Edit an image's config file
    Config {
        /// Image reference
        image: String,
        /// New tag reference (default: the original tag, or a new digest)
        #[arg(short, long)]
        tag: Option<String>,
    },
    /// Edit an image's manifest
    Manifest {
        /// Image reference
        image: String,
        /// New tag reference (default: the original tag, or a new digest)
        #[arg(short, long)]
        tag: Option<String>,
        /// Override the mediaType used as the Content-Type for PUT
        #[arg(short = 'm', long)]
        media_type: Option<String>,
    },
    /// Edit the contents of a file in an image's filesystem
    Fs {
        /// Image reference
        image: String,
        /// The file to edit
        #[arg(short = 'f', long)]
        filename: String,
        /// New tag reference (default: the original tag, or a new digest)
        #[arg(short, long)]
        tag: Option<String>,
    },
}

/// Edits `data` with `$EDITOR` when attached to a terminal, otherwise
/// reads the replacement from stdin (go `crane edit`).
fn edit_bytes(data: &[u8], suffix: &str) -> Result<Vec<u8>> {
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        let mut out = Vec::new();
        std::io::stdin().read_to_end(&mut out)?;
        return Ok(out);
    }
    let mut tmp = tempfile::Builder::new().suffix(suffix).tempfile()?;
    tmp.write_all(data)?;
    tmp.flush()?;
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{} \"$1\"", editor))
        .arg("sh")
        .arg(tmp.path())
        .status()
        .with_context(|| format!("running editor {}", editor))?;
    if !status.success() {
        bail!("editor {} exited with {}", editor, status);
    }
    Ok(std::fs::read(tmp.path())?)
}

/// An image whose manifest and config bytes were replaced; blobs are read
/// from the base.
struct EditedImage {
    base: Arc<dyn Image>,
    manifest: Vec<u8>,
    config: Vec<u8>,
}

#[async_trait]
impl Image for EditedImage {
    async fn raw_manifest(&self) -> containerregistry_v1::Result<Vec<u8>> {
        Ok(self.manifest.clone())
    }
    async fn raw_config_file(&self) -> containerregistry_v1::Result<Vec<u8>> {
        Ok(self.config.clone())
    }
    async fn layer_by_digest(
        &self,
        digest: &Digest,
    ) -> containerregistry_v1::Result<Arc<dyn Layer>> {
        if &self.manifest().await?.config().digest == digest {
            return self.config_layer().await;
        }
        self.base.layer_by_digest(digest).await
    }
}

async fn publish(ctx: &Ctx, img: Arc<dyn Image>, image: &str, tag: Option<&str>) -> Result<()> {
    let r = parse_ref(image)?;
    let dst = tag
        .map(str::to_string)
        .or_else(|| r.is_tag().then(|| image.to_string()));
    println!("{}", ctx.publish(img, dst.as_deref(), &r).await?);
    Ok(())
}

pub async fn run(ctx: &Ctx, cmd: EditCmd) -> Result<()> {
    match cmd {
        EditCmd::Config { image, tag } => {
            let img = ctx.image(&parse_ref(&image)?).await?;
            let config = edit_bytes(&img.raw_config_file().await?, ".json")?;
            let raw = img.raw_manifest().await?;
            let mut m = OciManifest::from_bytes_unchecked(&raw)?;
            m.config.digest = Digest::sha256(&config);
            m.config.size = config.len() as u64;
            m.config.data = None;
            let edited: Arc<dyn Image> = Arc::new(EditedImage {
                base: img,
                manifest: serde_json::to_vec(&m)?,
                config,
            });
            publish(ctx, edited, &image, tag.as_deref()).await
        }
        EditCmd::Manifest {
            image,
            tag,
            media_type,
        } => {
            let r = parse_ref(&image)?;
            let raw = ctx.taggable(&r).await?.raw_manifest().await?;
            let edited = edit_bytes(&raw, ".json")?;
            let mt = match media_type {
                Some(m) => m
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid media type {:?}: {:?}", m, e))?,
                None => probe_media_type(&edited).unwrap_or(MediaType::OciManifest),
            };
            let digest = Digest::sha256(&edited);
            let dst = match tag {
                Some(t) => parse_ref(&t)?,
                None if r.is_tag() => r.clone(),
                None => r.clone().with_new_digest(digest.clone()),
            };
            ctx.client.put_manifest_raw(&dst, &edited, &mt).await?;
            println!("{}", dst.with_new_digest(digest));
            Ok(())
        }
        EditCmd::Fs {
            image,
            filename,
            tag,
        } => {
            let img = ctx.image(&parse_ref(&image)?).await?;
            let name = filename.trim_start_matches('/').to_string();
            let (content, header) = find_file(img.clone(), &name).await?;
            let edited = edit_bytes(&content, "")?;
            let mut h = header.unwrap_or_else(|| {
                let mut h = tar::Header::new_gnu();
                h.set_mode(0o644);
                h.set_entry_type(tar::EntryType::Regular);
                h
            });
            h.set_size(edited.len() as u64);
            let mut b = tar::Builder::new(Vec::new());
            b.append_data(&mut h, &name, edited.as_slice())?;
            let tar = b.into_inner()?;
            let mt = if crate::util::is_oci(&img.media_type().await?) {
                MediaType::OciLayerGzip
            } else {
                MediaType::DockerLayerGzip
            };
            let gz = containerregistry_v1::read_all(compression::compress(
                Box::pin(std::io::Cursor::new(tar.clone())),
                Compression::Gzip,
                None,
            ))
            .await?;
            let layer: Arc<dyn Layer> = Arc::new(StaticLayer::with_uncompressed(gz, tar, mt));
            let out = mutate::append_layers(img, vec![layer]);
            publish(ctx, out, &image, tag.as_deref()).await
        }
    }
}

/// Returns the content and header of `name` in the flattened filesystem,
/// or empty content for a new file.
async fn find_file(img: Arc<dyn Image>, name: &str) -> Result<(Vec<u8>, Option<tar::Header>)> {
    let fs = tokio_util::io::SyncIoBridge::new(mutate::extract(img));
    let name = name.to_string();
    tokio::task::spawn_blocking(move || {
        let mut archive = tar::Archive::new(fs);
        for e in archive.entries()? {
            let mut e = e?;
            let p = e
                .path()?
                .to_string_lossy()
                .trim_start_matches("./")
                .to_string();
            if p == name {
                let mut out = Vec::new();
                e.read_to_end(&mut out)?;
                return Ok((out, Some(e.header().clone())));
            }
        }
        Ok((Vec::new(), None))
    })
    .await?
}
