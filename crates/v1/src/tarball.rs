//! Layers from files and `docker save` tarballs (go-containerregistry
//! `pkg/v1/tarball`).
//!
//! Layers built here re-open their source for every read instead of
//! buffering it, and a layer read from a file whose bytes are already the
//! blob reports [`Layer::local_path`], so registry uploads of it can resume.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio::sync::OnceCell;

use containerregistry_image::{Algorithm, Descriptor, Digest, DockerManifest, MediaType};

use crate::compression::{self, Compression};
use crate::layer::{BoxReader, Layer};
use crate::partial::{FixedImage, blob_to_diff_id, digest_reader};
use crate::pipe::blocking_pipe;
use crate::tarutil;
use crate::{Error, Image, Result};

/// A future that opens a fresh stream.
pub type OpenFuture = Pin<Box<dyn Future<Output = Result<BoxReader>> + Send>>;

/// Re-opens a layer's source (go-containerregistry `tarball.Opener`).
pub type Opener = Arc<dyn Fn() -> OpenFuture + Send + Sync>;

/// Opens the whole file at `path`.
pub fn file_opener(path: impl Into<PathBuf>) -> Opener {
    section_opener(path.into(), 0, None)
}

fn section_opener(path: PathBuf, offset: u64, len: Option<u64>) -> Opener {
    Arc::new(move || {
        let path = path.clone();
        Box::pin(async move {
            let mut f = tokio::fs::File::open(&path).await?;
            if offset > 0 {
                f.seek(SeekFrom::Start(offset)).await?;
            }
            let r: BoxReader = match len {
                Some(n) => Box::pin(f.take(n)),
                None => Box::pin(f),
            };
            Ok(r)
        })
    })
}

/// Options for building a layer (go-containerregistry `tarball.LayerOption`).
#[derive(Clone, Debug, Default)]
pub struct LayerOptions {
    /// Media type of the layer. Defaults to the Docker gzip layer type, the
    /// OCI zstd type for zstd, or the OCI uncompressed type.
    ///
    /// A media type that implies no compression (such as a custom artifact
    /// type) makes the layer opaque: its bytes are stored and hashed as-is.
    pub media_type: Option<MediaType>,
    /// Compression of the stored blob (`WithCompression`).
    pub compression: Option<Compression>,
    /// Compression level (`WithCompressionLevel`).
    pub level: Option<i32>,
    /// Annotations for the layer descriptor.
    pub annotations: BTreeMap<String, String>,
}

/// A layer whose bytes come from an [`Opener`], (re)compressed on the fly
/// when the source compression differs from the target.
pub struct OpenerLayer {
    opener: Opener,
    stored: Compression,
    target: Compression,
    opaque: bool,
    level: Option<i32>,
    media_type: MediaType,
    annotations: BTreeMap<String, String>,
    urls: Vec<String>,
    path: Option<PathBuf>,
    digest: OnceCell<(Digest, u64)>,
    diff_id: OnceCell<Digest>,
}

impl OpenerLayer {
    fn passthrough(&self) -> bool {
        self.opaque || self.stored == self.target
    }

    /// Records a known digest and size, avoiding a hashing pass.
    pub fn with_digest(self, digest: Digest, size: u64) -> Self {
        let _ = self.digest.set((digest, size));
        self
    }

    /// Records a known diff ID, avoiding a hashing pass.
    pub fn with_diff_id(self, diff_id: Digest) -> Self {
        let _ = self.diff_id.set(diff_id);
        self
    }

    fn with_urls(mut self, urls: Vec<String>) -> Self {
        self.urls = urls;
        self
    }

    async fn computed(&self) -> Result<&(Digest, u64)> {
        self.digest
            .get_or_try_init(|| async {
                digest_reader(self.compressed().await?, Algorithm::Sha256).await
            })
            .await
    }
}

#[async_trait]
impl Layer for OpenerLayer {
    async fn digest(&self) -> Result<Digest> {
        Ok(self.computed().await?.0.clone())
    }

    async fn diff_id(&self) -> Result<Digest> {
        if self.opaque {
            return self.digest().await;
        }
        self.diff_id
            .get_or_try_init(|| async {
                Ok(digest_reader(self.uncompressed().await?, Algorithm::Sha256)
                    .await?
                    .0)
            })
            .await
            .cloned()
    }

    async fn compressed(&self) -> Result<BoxReader> {
        let r = (self.opener)().await?;
        if self.passthrough() {
            return Ok(r);
        }
        Ok(compression::compress(
            compression::decompress(r, self.stored),
            self.target,
            self.level,
        ))
    }

    async fn uncompressed(&self) -> Result<BoxReader> {
        let r = (self.opener)().await?;
        if self.opaque {
            return Ok(r);
        }
        Ok(compression::decompress(r, self.stored))
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.computed().await?.1)
    }

    async fn media_type(&self) -> Result<MediaType> {
        Ok(self.media_type.clone())
    }

    async fn descriptor(&self) -> Result<Descriptor> {
        let (digest, size) = self.computed().await?.clone();
        let mut d = Descriptor::new(self.media_type.clone(), digest, size);
        d.annotations = self.annotations.clone();
        d.urls = self.urls.clone();
        Ok(d)
    }

    fn local_path(&self) -> Option<PathBuf> {
        if self.passthrough() {
            self.path.clone()
        } else {
            None
        }
    }
}

async fn detect_opener(opener: &Opener) -> Result<Compression> {
    let mut r = opener().await?;
    let mut prefix = [0u8; 4];
    let mut n = 0;
    while n < 4 {
        let read = r.read(&mut prefix[n..]).await?;
        if read == 0 {
            break;
        }
        n += read;
    }
    Ok(Compression::from_magic(&prefix[..n]))
}

async fn build_layer(
    opener: Opener,
    path: Option<PathBuf>,
    opts: LayerOptions,
) -> Result<OpenerLayer> {
    let implied = opts.media_type.as_ref().map(Compression::from_media_type);
    let opaque = matches!(implied, Some(None));
    let target = opts
        .compression
        .or(implied.flatten())
        .unwrap_or(Compression::Gzip);
    let media_type = opts.media_type.clone().unwrap_or(match target {
        Compression::Gzip => MediaType::DockerLayerGzip,
        Compression::Zstd => MediaType::OciLayerZstd,
        Compression::None => MediaType::OciLayer,
    });
    let stored = if opaque {
        Compression::None
    } else {
        detect_opener(&opener).await?
    };
    Ok(OpenerLayer {
        opener,
        stored,
        target,
        opaque,
        level: opts.level,
        media_type,
        annotations: opts.annotations,
        urls: Vec::new(),
        path,
        digest: OnceCell::new(),
        diff_id: OnceCell::new(),
    })
}

/// A layer from a (possibly compressed) tar file or an opaque blob file
/// (`tarball.LayerFromFile`).
pub async fn layer_from_file(path: impl AsRef<Path>, opts: LayerOptions) -> Result<Arc<dyn Layer>> {
    let path = path.as_ref().to_path_buf();
    let layer = build_layer(file_opener(path.clone()), Some(path), opts).await?;
    Ok(Arc::new(layer))
}

/// A layer whose bytes are the file at `path`, never inspected or
/// recompressed — the right choice for opaque artifact blobs such as VM
/// disk chunks.
pub async fn blob_from_file(
    path: impl AsRef<Path>,
    media_type: MediaType,
) -> Result<Arc<dyn Layer>> {
    let path = path.as_ref().to_path_buf();
    Ok(Arc::new(OpenerLayer {
        opener: file_opener(path.clone()),
        stored: Compression::None,
        target: Compression::None,
        opaque: true,
        level: None,
        media_type,
        annotations: BTreeMap::new(),
        urls: Vec::new(),
        path: Some(path),
        digest: OnceCell::new(),
        diff_id: OnceCell::new(),
    }))
}

/// A layer from an opener (`tarball.LayerFromOpener`).
pub async fn layer_from_opener(opener: Opener, opts: LayerOptions) -> Result<Arc<dyn Layer>> {
    Ok(Arc::new(build_layer(opener, None, opts).await?))
}

/// A layer from a reader. Like go-containerregistry, the content is
/// buffered in memory; prefer [`layer_from_file`] or
/// [`crate::stream::StreamLayer`] for large layers.
pub async fn layer_from_reader(
    mut reader: BoxReader,
    opts: LayerOptions,
) -> Result<Arc<dyn Layer>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    let data = Bytes::from(buf);
    let opener: Opener = Arc::new(move || {
        let data = data.clone();
        Box::pin(async move { Ok(Box::pin(Cursor::new(data)) as BoxReader) })
    });
    layer_from_opener(opener, opts).await
}

// ---------------------------------------------------------------------------
// docker save tarballs

/// One image in a `docker save` tarball's `manifest.json`
/// (`tarball.Descriptor`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TarDescriptor {
    /// Path of the config file in the tarball.
    pub config: String,
    /// Tags of the image.
    #[serde(default, deserialize_with = "null_vec")]
    pub repo_tags: Vec<String>,
    /// Paths of the layer files, base first.
    #[serde(default, deserialize_with = "null_vec")]
    pub layers: Vec<String>,
    /// Foreign layers, keyed by diff ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub layer_sources: BTreeMap<String, Descriptor>,
}

fn null_vec<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

/// A `docker save` tarball's `manifest.json` (`tarball.Manifest`).
pub type TarManifest = Vec<TarDescriptor>;

#[derive(Clone, Debug)]
struct TarEntry {
    offset: u64,
    size: u64,
    link: Option<String>,
}

fn index_tar(path: &Path) -> io::Result<HashMap<String, TarEntry>> {
    let mut f = std::fs::File::open(path)?;
    let mut out = HashMap::new();
    let mut archive = tar::Archive::new(&mut f);
    for entry in archive.entries_with_seek()? {
        let entry = entry?;
        let name = tarutil::clean_path(&tarutil::entry_path(&entry)?);
        let et = entry.header().entry_type();
        let link = if et.is_symlink() || et.is_hard_link() {
            entry
                .link_name()?
                .map(|l| l.to_string_lossy().replace('\\', "/"))
        } else {
            None
        };
        out.insert(
            name,
            TarEntry {
                offset: entry.raw_file_position(),
                size: entry.size(),
                link,
            },
        );
    }
    Ok(out)
}

fn resolve<'a>(entries: &'a HashMap<String, TarEntry>, name: &str) -> Result<&'a TarEntry> {
    let mut name = tarutil::clean_path(name);
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(name.clone()) {
            return Err(Error::Invalid(format!("link cycle detected for {}", name)));
        }
        let e = entries
            .get(&name)
            .ok_or_else(|| Error::NotFound(format!("file {} not found in tar", name)))?;
        match &e.link {
            Some(l) => name = tarutil::join(&tarutil::dir(&name), l),
            None => return Ok(e),
        }
    }
}

fn read_section(path: &Path, e: &TarEntry) -> io::Result<Vec<u8>> {
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(e.offset))?;
    let mut buf = Vec::with_capacity(e.size as usize);
    f.take(e.size).read_to_end(&mut buf)?;
    Ok(buf)
}

fn normalize_tag(t: &str) -> String {
    let t = t
        .strip_prefix("index.docker.io/")
        .or_else(|| t.strip_prefix("docker.io/"))
        .unwrap_or(t);
    let t = t.strip_prefix("library/").unwrap_or(t);
    let last = t.rsplit('/').next().unwrap_or(t);
    if last.contains(':') || t.contains('@') {
        t.to_string()
    } else {
        format!("{}:latest", t)
    }
}

/// Reads the `manifest.json` of a `docker save` tarball.
pub async fn read_manifest(path: impl AsRef<Path>) -> Result<TarManifest> {
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<TarManifest> {
        let entries = index_tar(&path)?;
        let m = resolve(&entries, "manifest.json")?;
        Ok(serde_json::from_slice(&read_section(&path, m)?)?)
    })
    .await
    .map_err(|e| Error::Io(io::Error::other(e)))?
}

/// Loads an image from a `docker save` (or `crane pull`) tarball
/// (`tarball.ImageFromPath`). With `tag`, selects the image carrying that
/// tag; without, the tarball must hold exactly one image.
///
/// Uncompressed layers are gzip-compressed on the fly, so their digests are
/// computed by compressing them once.
pub async fn image_from_path(path: impl AsRef<Path>, tag: Option<&str>) -> Result<Arc<dyn Image>> {
    let path = path.as_ref().to_path_buf();
    let p = path.clone();
    let entries = tokio::task::spawn_blocking(move || index_tar(&p))
        .await
        .map_err(|e| Error::Io(io::Error::other(e)))??;
    let manifest: TarManifest =
        serde_json::from_slice(&read_section(&path, resolve(&entries, "manifest.json")?)?)?;
    let desc = match tag {
        None => {
            if manifest.len() != 1 {
                return Err(Error::Invalid(
                    "tarball must contain only a single image to be used without a tag".to_string(),
                ));
            }
            manifest[0].clone()
        }
        Some(t) => {
            let want = normalize_tag(t);
            manifest
                .iter()
                .find(|d| d.repo_tags.iter().any(|rt| normalize_tag(rt) == want))
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("tag {} in tarball", t)))?
        }
    };
    let config = read_section(&path, resolve(&entries, &desc.config)?)?;
    let cfg = containerregistry_image::ImageConfig::from_bytes(&config)?;

    let mut layers: Vec<Arc<dyn Layer>> = Vec::with_capacity(desc.layers.len());
    let mut descs = Vec::with_capacity(desc.layers.len());
    for (i, file) in desc.layers.iter().enumerate() {
        let e = resolve(&entries, file)?;
        let opener = section_opener(path.clone(), e.offset, Some(e.size));
        let diff_id = cfg.rootfs.diff_ids.get(i).cloned();
        let source = diff_id
            .as_ref()
            .and_then(|d| desc.layer_sources.get(&d.to_string()))
            .cloned();
        let stored = detect_opener(&opener).await?;
        let opts = LayerOptions {
            media_type: Some(match (&source, stored) {
                (Some(s), _) => s.media_type.clone(),
                (None, Compression::Zstd) => MediaType::OciLayerZstd,
                (None, _) => MediaType::DockerLayerGzip,
            }),
            compression: Some(if stored == Compression::None {
                Compression::Gzip
            } else {
                stored
            }),
            ..Default::default()
        };
        let mut layer = build_layer(opener, None, opts).await?;
        if let Some(d) = diff_id {
            layer = layer.with_diff_id(d);
        }
        if let Some(s) = &source {
            layer = layer.with_urls(s.urls.clone());
            if stored != Compression::None {
                layer = layer.with_digest(s.digest.clone(), s.size);
            }
        }
        descs.push(layer.descriptor().await?);
        layers.push(Arc::new(layer));
    }
    let manifest = DockerManifest::new(
        Descriptor::from_bytes(MediaType::DockerConfig, &config),
        descs,
    )
    .to_bytes()?;
    Ok(Arc::new(FixedImage::new(manifest, config, layers)))
}

fn layer_extension(mt: &MediaType) -> &'static str {
    match mt {
        MediaType::OciLayerZstd | MediaType::OciLayerNondistributableZstd => ".tar.zst",
        MediaType::OciLayer | MediaType::OciLayerNondistributable => ".tar",
        _ => ".tar.gz",
    }
}

/// Builds the `manifest.json` for `images`, each with its tags
/// (`tarball.calculateManifest`).
pub async fn manifest_for(images: &[(Vec<String>, Arc<dyn Image>)]) -> Result<TarManifest> {
    let mut m = Vec::with_capacity(images.len());
    for (tags, img) in images {
        let mut layer_files = Vec::new();
        let mut sources = BTreeMap::new();
        let manifest = img.manifest().await?;
        for d in manifest.layers() {
            layer_files.push(format!(
                "{}{}",
                d.digest.hex(),
                layer_extension(&d.media_type)
            ));
            if d.media_type.is_nondistributable() {
                let diff_id = blob_to_diff_id(img.as_ref(), &d.digest).await?;
                sources.insert(diff_id.to_string(), d.clone());
            }
        }
        m.push(TarDescriptor {
            config: img.config_name().await?.to_string(),
            repo_tags: tags.clone(),
            layers: layer_files,
            layer_sources: sources,
        });
    }
    m.sort_by_key(|d| d.repo_tags.join(","));
    Ok(m)
}

fn write_file<W: io::Write, R: Read>(
    b: &mut tar::Builder<W>,
    name: &str,
    size: u64,
    data: R,
) -> io::Result<()> {
    let mut h = tar::Header::new_ustar();
    h.set_size(size);
    h.set_mode(0o644);
    h.set_mtime(0);
    h.set_entry_type(tar::EntryType::Regular);
    b.append_data(&mut h, name, data)
}

/// Streams a `docker save`-compatible tarball of `images`, each with the
/// tags it should carry (`tarball.MultiRefWrite`). Layers shared between
/// images are written once.
pub async fn write(images: Vec<(Vec<String>, Arc<dyn Image>)>) -> Result<BoxReader> {
    if images.is_empty() {
        return Err(Error::Invalid("set of images is empty".to_string()));
    }
    let manifest = serde_json::to_vec(&manifest_for(&images).await?)?;
    let handle = Handle::current();
    Ok(blocking_pipe(move |w| {
        let err = |e: Error| io::Error::other(e.to_string());
        let mut b = tar::Builder::new(&mut *w);
        let mut seen = HashSet::new();
        for (_, img) in &images {
            let name = handle.block_on(img.config_name()).map_err(err)?;
            let cfg = handle.block_on(img.raw_config_file()).map_err(err)?;
            write_file(&mut b, &name.to_string(), cfg.len() as u64, cfg.as_slice())?;
            let m = handle.block_on(img.manifest()).map_err(err)?;
            for d in m.layers() {
                if !seen.insert(d.digest.clone()) {
                    continue;
                }
                let layer = handle
                    .block_on(img.layer_by_digest(&d.digest))
                    .map_err(err)?;
                let r = handle.block_on(layer.compressed()).map_err(err)?;
                let file = format!("{}{}", d.digest.hex(), layer_extension(&d.media_type));
                write_file(
                    &mut b,
                    &file,
                    d.size,
                    tarutil::sync_reader(r, handle.clone()),
                )?;
            }
        }
        write_file(
            &mut b,
            "manifest.json",
            manifest.len() as u64,
            manifest.as_slice(),
        )?;
        b.finish()
    }))
}

/// Writes a tarball of `images` to `path` (`tarball.MultiRefWriteToFile`).
pub async fn write_to_file(
    path: impl AsRef<Path>,
    images: Vec<(Vec<String>, Arc<dyn Image>)>,
) -> Result<()> {
    let mut r = write(images).await?;
    let mut f = tokio::fs::File::create(path).await?;
    tokio::io::copy(&mut r, &mut f).await?;
    f.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mutate, random, read_all, validate};

    #[tokio::test]
    async fn file_layers() {
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("l.tar");
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_size(3);
        b.append_data(&mut h, "x", &b"abc"[..]).unwrap();
        let tar = b.into_inner().unwrap();
        std::fs::write(&tar_path, &tar).unwrap();

        let l = layer_from_file(&tar_path, LayerOptions::default())
            .await
            .unwrap();
        assert_eq!(l.media_type().await.unwrap(), MediaType::DockerLayerGzip);
        assert_eq!(l.diff_id().await.unwrap(), Digest::sha256(&tar));
        assert!(l.local_path().is_none());
        let gz = read_all(l.compressed().await.unwrap()).await.unwrap();
        assert_eq!(l.digest().await.unwrap(), Digest::sha256(&gz));

        let gz_path = dir.path().join("l.tar.gz");
        std::fs::write(&gz_path, &gz).unwrap();
        let l2 = layer_from_file(&gz_path, LayerOptions::default())
            .await
            .unwrap();
        assert_eq!(l2.digest().await.unwrap(), Digest::sha256(&gz));
        assert_eq!(l2.diff_id().await.unwrap(), Digest::sha256(&tar));
        assert_eq!(l2.local_path(), Some(gz_path.clone()));

        let blob = blob_from_file(&gz_path, MediaType::Other("application/x-disk".into()))
            .await
            .unwrap();
        assert_eq!(blob.digest().await.unwrap(), Digest::sha256(&gz));
        assert_eq!(blob.diff_id().await.unwrap(), Digest::sha256(&gz));
    }

    #[tokio::test]
    async fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("img.tar");
        let a = random::image(512, 2).await.unwrap();
        let b = mutate::append_layers(
            a.clone(),
            vec![
                random::layer(256, MediaType::DockerLayerGzip)
                    .await
                    .unwrap(),
            ],
        );
        write_to_file(
            &path,
            vec![
                (vec!["example.com/a:v1".into()], a.clone()),
                (vec!["b:latest".into()], b.clone()),
            ],
        )
        .await
        .unwrap();
        let m = read_manifest(&path).await.unwrap();
        assert_eq!(m.len(), 2);
        let got = image_from_path(&path, Some("example.com/a:v1"))
            .await
            .unwrap();
        assert_eq!(got.digest().await.unwrap(), a.digest().await.unwrap());
        let got_b = image_from_path(&path, Some("b")).await.unwrap();
        assert_eq!(got_b.digest().await.unwrap(), b.digest().await.unwrap());
        validate::image(got_b.as_ref(), validate::Options::default())
            .await
            .unwrap();
        assert!(image_from_path(&path, None).await.is_err());
    }
}
