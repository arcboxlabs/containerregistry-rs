//! Image and index transformations (go-containerregistry `pkg/v1/mutate`).
//!
//! Every function returns a new image or index that lazily computes its
//! manifest and config from its base; blobs are never copied. Layers that
//! need rewriting ([`time`], [`canonical`]) and [`extract`] stream their
//! content, so multi-gigabyte layers are never held in memory.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use tokio::runtime::Handle;
use tokio::sync::OnceCell;

use containerregistry_image::{
    Algorithm, ContainerConfig, Descriptor, Digest, History, ImageConfig, MediaType, OciIndex,
    OciManifest, Platform,
};

use crate::compression::Compression;
use crate::layer::{BoxReader, Layer};
use crate::matcher::Matcher;
use crate::partial::{Child, Described, UncompressedLayer, UncompressedLayerCore, digest_reader};
use crate::pipe::blocking_pipe;
use crate::tarutil;
use crate::{Error, Image, ImageIndex, Result};

const WHITEOUT_PREFIX: &str = ".wh.";
const OPAQUE_WHITEOUT: &str = ".wh..wh..opq";

/// Unix time of go's zero `time.Time` (0001-01-01T00:00:00Z), used by
/// [`canonical`].
pub const CANONICAL_TIME: i64 = -62_135_596_800;

/// A layer to append, with the history entry and descriptor overrides that
/// go with it (go-containerregistry `mutate.Addendum`).
#[derive(Clone, Default)]
pub struct Addendum {
    /// The layer; `None` only for history-only (empty layer) entries.
    pub layer: Option<Arc<dyn Layer>>,
    /// The history entry appended to the config.
    pub history: History,
    /// Annotations for the layer descriptor (replace the layer's own).
    pub annotations: BTreeMap<String, String>,
    /// URLs for the layer descriptor (replace the layer's own).
    pub urls: Vec<String>,
    /// Media type for the layer descriptor.
    pub media_type: Option<MediaType>,
}

impl Addendum {
    /// An addendum for `layer` with an empty history entry.
    pub fn layer(layer: Arc<dyn Layer>) -> Self {
        Self {
            layer: Some(layer),
            ..Default::default()
        }
    }
}

#[derive(Clone, Default)]
struct ImageOverrides {
    adds: Vec<Addendum>,
    config_file: Option<ImageConfig>,
    media_type: Option<MediaType>,
    config_media_type: Option<MediaType>,
    annotations: Option<BTreeMap<String, String>>,
    subject: Option<Descriptor>,
}

struct ImageComputed {
    manifest: Vec<u8>,
    config: Vec<u8>,
    added: HashMap<Digest, Arc<dyn Layer>>,
}

struct MutatedImage {
    base: Arc<dyn Image>,
    o: ImageOverrides,
    computed: OnceCell<ImageComputed>,
}

fn wrap_image(base: Arc<dyn Image>, o: ImageOverrides) -> Arc<dyn Image> {
    Arc::new(MutatedImage {
        base,
        o,
        computed: OnceCell::new(),
    })
}

fn is_image_config(mt: &MediaType) -> bool {
    matches!(mt, MediaType::OciConfig | MediaType::DockerConfig)
}

/// Parses any manifest into the OCI struct, which is a superset of the
/// Docker schema 2 fields, so mutations stack without losing annotations,
/// subjects or artifact types. A Docker manifest without OCI-only fields
/// serializes byte-for-byte like a `DockerManifest`.
fn manifest_struct(raw: &[u8]) -> Result<OciManifest> {
    Ok(OciManifest::from_bytes_unchecked(raw)?)
}

impl MutatedImage {
    async fn compute(&self) -> Result<&ImageComputed> {
        self.computed.get_or_try_init(|| self.build()).await
    }

    async fn build(&self) -> Result<ImageComputed> {
        let raw = self.base.raw_manifest().await?;
        let mut m = manifest_struct(&raw)?;
        let o = &self.o;

        let cfg_mt = o
            .config_media_type
            .clone()
            .unwrap_or_else(|| m.config.media_type.clone());
        let config = if is_image_config(&cfg_mt) {
            let mut cf = match &o.config_file {
                Some(c) => c.clone(),
                None => self.base.config_file().await?,
            };
            for add in &o.adds {
                cf.history.push(add.history.clone());
                if let Some(l) = &add.layer {
                    cf.rootfs.diff_ids.push(l.diff_id().await?);
                }
            }
            let bytes = cf.to_bytes()?;
            m.config.digest = Digest::sha256(&bytes);
            m.config.size = bytes.len() as u64;
            if m.config.data.is_some() {
                m.config.data = Some(base64::engine::general_purpose::STANDARD.encode(&bytes));
            }
            bytes
        } else {
            self.base.raw_config_file().await?
        };

        let mut added = HashMap::new();
        for add in &o.adds {
            let Some(l) = &add.layer else { continue };
            let mut d = l.descriptor().await?;
            if !add.annotations.is_empty() {
                d.annotations = add.annotations.clone();
            }
            if !add.urls.is_empty() {
                d.urls = add.urls.clone();
            }
            if let Some(mt) = &add.media_type {
                d.media_type = mt.clone();
            }
            let layer: Arc<dyn Layer> = Arc::new(Described::new(l.clone(), d.clone()));
            added.insert(d.digest.clone(), layer);
            m.layers.push(d);
        }

        m.config.media_type = cfg_mt;
        if let Some(mt) = &o.media_type {
            m.media_type = Some(mt.clone());
        }
        if let Some(a) = &o.annotations {
            m.annotations.extend(a.clone());
        }
        if let Some(s) = &o.subject {
            m.subject = Some(s.clone());
        }
        Ok(ImageComputed {
            manifest: serde_json::to_vec(&m)?,
            config,
            added,
        })
    }
}

#[async_trait]
impl Image for MutatedImage {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        Ok(self.compute().await?.manifest.clone())
    }

    async fn raw_config_file(&self) -> Result<Vec<u8>> {
        Ok(self.compute().await?.config.clone())
    }

    async fn layer_by_digest(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        let c = self.compute().await?;
        if let Some(l) = c.added.get(digest) {
            return Ok(l.clone());
        }
        if &self.manifest().await?.config().digest == digest {
            return self.config_layer().await;
        }
        self.base.layer_by_digest(digest).await
    }

    /// The base layers followed by the appended ones. This works before
    /// streaming layers have been consumed (go `mutate` falls back the same
    /// way on `stream.ErrNotComputed`), which is what lets writers upload
    /// a [`crate::stream::StreamLayer`] before the manifest exists.
    async fn layers(&self) -> Result<Vec<Arc<dyn Layer>>> {
        match self.compute().await {
            Ok(_) => {
                let manifest = self.manifest().await?;
                let mut out = Vec::with_capacity(manifest.layers().len());
                for d in manifest.layers() {
                    out.push(self.layer_by_digest(&d.digest).await?);
                }
                Ok(out)
            }
            Err(Error::NotComputed(_)) => {
                let mut out = self.base.layers().await?;
                out.extend(self.o.adds.iter().filter_map(|a| a.layer.clone()));
                Ok(out)
            }
            Err(e) => Err(e),
        }
    }
}

fn validate_adds(adds: &[Addendum]) -> Result<()> {
    if adds
        .iter()
        .any(|a| a.layer.is_none() && !a.history.empty_layer)
    {
        return Err(Error::Invalid(
            "unable to add a nil layer to the image".to_string(),
        ));
    }
    Ok(())
}

/// Appends layers with empty history entries (`mutate.AppendLayers`).
pub fn append_layers(base: Arc<dyn Image>, layers: Vec<Arc<dyn Layer>>) -> Arc<dyn Image> {
    let adds = layers.into_iter().map(Addendum::layer).collect();
    wrap_image(
        base,
        ImageOverrides {
            adds,
            ..Default::default()
        },
    )
}

/// Appends layers and history entries (`mutate.Append`).
pub fn append(base: Arc<dyn Image>, adds: Vec<Addendum>) -> Result<Arc<dyn Image>> {
    validate_adds(&adds)?;
    Ok(wrap_image(
        base,
        ImageOverrides {
            adds,
            ..Default::default()
        },
    ))
}

/// Replaces the image config (`mutate.ConfigFile`).
pub fn config_file(base: Arc<dyn Image>, config: ImageConfig) -> Arc<dyn Image> {
    wrap_image(
        base,
        ImageOverrides {
            config_file: Some(config),
            ..Default::default()
        },
    )
}

/// Replaces the container config section of the image config
/// (`mutate.Config`).
pub async fn config(base: Arc<dyn Image>, cfg: ContainerConfig) -> Result<Arc<dyn Image>> {
    let mut cf = base.config_file().await?;
    cf.config = Some(cfg);
    Ok(config_file(base, cf))
}

/// Sets the config's creation time, an RFC 3339 string (`mutate.CreatedAt`).
pub async fn created_at(
    base: Arc<dyn Image>,
    created: impl Into<String>,
) -> Result<Arc<dyn Image>> {
    let mut cf = base.config_file().await?;
    cf.created = Some(created.into());
    Ok(config_file(base, cf))
}

/// Merges annotations into the manifest (`mutate.Annotations`).
pub fn annotations(base: Arc<dyn Image>, annotations: BTreeMap<String, String>) -> Arc<dyn Image> {
    wrap_image(
        base,
        ImageOverrides {
            annotations: Some(annotations),
            ..Default::default()
        },
    )
}

/// Sets the manifest subject (`mutate.Subject`).
pub fn subject(base: Arc<dyn Image>, subject: Descriptor) -> Arc<dyn Image> {
    wrap_image(
        base,
        ImageOverrides {
            subject: Some(subject),
            ..Default::default()
        },
    )
}

/// Sets the manifest media type (`mutate.MediaType`).
pub fn media_type(base: Arc<dyn Image>, media_type: MediaType) -> Arc<dyn Image> {
    wrap_image(
        base,
        ImageOverrides {
            media_type: Some(media_type),
            ..Default::default()
        },
    )
}

/// Sets the config media type (`mutate.ConfigMediaType`).
pub fn config_media_type(base: Arc<dyn Image>, media_type: MediaType) -> Arc<dyn Image> {
    wrap_image(
        base,
        ImageOverrides {
            config_media_type: Some(media_type),
            ..Default::default()
        },
    )
}

// ---------------------------------------------------------------------------
// Indexes

/// A manifest (or blob) to add to an index, with descriptor overrides
/// (go-containerregistry `mutate.IndexAddendum`).
#[derive(Clone)]
pub struct IndexAddendum {
    /// What to add.
    pub add: Child,
    /// Platform for the descriptor.
    pub platform: Option<Platform>,
    /// Annotations for the descriptor (replace the child's own).
    pub annotations: BTreeMap<String, String>,
    /// URLs for the descriptor.
    pub urls: Vec<String>,
    /// Media type for the descriptor.
    pub media_type: Option<MediaType>,
    /// Artifact type for the descriptor.
    pub artifact_type: Option<MediaType>,
}

impl IndexAddendum {
    /// An addendum without overrides.
    pub fn new(add: Child) -> Self {
        Self {
            add,
            platform: None,
            annotations: BTreeMap::new(),
            urls: Vec::new(),
            media_type: None,
            artifact_type: None,
        }
    }

    /// Sets the platform.
    pub fn with_platform(mut self, platform: Platform) -> Self {
        self.platform = Some(platform);
        self
    }

    async fn descriptor(&self) -> Result<Descriptor> {
        let mut d = match &self.add {
            Child::Image(i) => i.descriptor().await?,
            Child::Index(i) => i.descriptor().await?,
            Child::Blob(l) => l.descriptor().await?,
        };
        if let Some(p) = &self.platform {
            d.platform = Some(p.clone());
        }
        if !self.annotations.is_empty() {
            d.annotations = self.annotations.clone();
        }
        if !self.urls.is_empty() {
            d.urls = self.urls.clone();
        }
        if let Some(mt) = &self.media_type {
            d.media_type = mt.clone();
        }
        if let Some(at) = &self.artifact_type {
            d.artifact_type = Some(at.clone());
        }
        Ok(d)
    }
}

#[derive(Clone, Default)]
struct IndexOverrides {
    adds: Vec<IndexAddendum>,
    remove: Option<Matcher>,
    media_type: Option<MediaType>,
    annotations: Option<BTreeMap<String, String>>,
    subject: Option<Descriptor>,
}

struct IndexComputed {
    manifest: Vec<u8>,
    added: HashMap<Digest, Child>,
}

struct MutatedIndex {
    base: Arc<dyn ImageIndex>,
    o: IndexOverrides,
    computed: OnceCell<IndexComputed>,
}

fn wrap_index(base: Arc<dyn ImageIndex>, o: IndexOverrides) -> Arc<dyn ImageIndex> {
    Arc::new(MutatedIndex {
        base,
        o,
        computed: OnceCell::new(),
    })
}

impl MutatedIndex {
    async fn compute(&self) -> Result<&IndexComputed> {
        self.computed.get_or_try_init(|| self.build()).await
    }

    async fn build(&self) -> Result<IndexComputed> {
        let raw = self.base.raw_manifest().await?;
        // The OCI struct is a superset of the Docker manifest list.
        let mut m = OciIndex::from_bytes_unchecked(&raw)?;
        let o = &self.o;
        if let Some(remove) = &o.remove {
            m.manifests.retain(|d| !remove(d));
        }
        let mut added = HashMap::new();
        for add in &o.adds {
            let d = add.descriptor().await?;
            added.insert(d.digest.clone(), add.add.clone());
            m.manifests.push(d);
        }
        if let Some(mt) = &o.media_type {
            m.media_type = Some(mt.clone());
        }
        if let Some(a) = &o.annotations {
            m.annotations.extend(a.clone());
        }
        if let Some(s) = &o.subject {
            m.subject = Some(s.clone());
        }
        Ok(IndexComputed {
            manifest: serde_json::to_vec(&m)?,
            added,
        })
    }
}

#[async_trait]
impl ImageIndex for MutatedIndex {
    async fn raw_manifest(&self) -> Result<Vec<u8>> {
        Ok(self.compute().await?.manifest.clone())
    }

    async fn image(&self, digest: &Digest) -> Result<Arc<dyn Image>> {
        match self.compute().await?.added.get(digest) {
            Some(Child::Image(i)) => Ok(i.clone()),
            Some(_) => Err(Error::Invalid(format!("{} is not an image", digest))),
            None => self.base.image(digest).await,
        }
    }

    async fn image_index(&self, digest: &Digest) -> Result<Arc<dyn ImageIndex>> {
        match self.compute().await?.added.get(digest) {
            Some(Child::Index(i)) => Ok(i.clone()),
            Some(_) => Err(Error::Invalid(format!("{} is not an index", digest))),
            None => self.base.image_index(digest).await,
        }
    }

    async fn blob(&self, digest: &Digest) -> Result<Arc<dyn Layer>> {
        match self.compute().await?.added.get(digest) {
            Some(Child::Blob(l)) => Ok(l.clone()),
            Some(_) => Err(Error::Invalid(format!("{} is not a blob", digest))),
            None => self.base.blob(digest).await,
        }
    }
}

/// Appends manifests to an index (`mutate.AppendManifests`).
pub fn append_manifests(
    base: Arc<dyn ImageIndex>,
    adds: Vec<IndexAddendum>,
) -> Arc<dyn ImageIndex> {
    wrap_index(
        base,
        IndexOverrides {
            adds,
            ..Default::default()
        },
    )
}

/// Removes the manifests matching `matcher` (`mutate.RemoveManifests`).
pub fn remove_manifests(base: Arc<dyn ImageIndex>, matcher: Matcher) -> Arc<dyn ImageIndex> {
    wrap_index(
        base,
        IndexOverrides {
            remove: Some(matcher),
            ..Default::default()
        },
    )
}

/// Sets the index media type (`mutate.IndexMediaType`).
pub fn index_media_type(base: Arc<dyn ImageIndex>, media_type: MediaType) -> Arc<dyn ImageIndex> {
    wrap_index(
        base,
        IndexOverrides {
            media_type: Some(media_type),
            ..Default::default()
        },
    )
}

/// Merges annotations into the index manifest (`mutate.Annotations`).
pub fn index_annotations(
    base: Arc<dyn ImageIndex>,
    annotations: BTreeMap<String, String>,
) -> Arc<dyn ImageIndex> {
    wrap_index(
        base,
        IndexOverrides {
            annotations: Some(annotations),
            ..Default::default()
        },
    )
}

/// Sets the index subject (`mutate.Subject`).
pub fn index_subject(base: Arc<dyn ImageIndex>, subject: Descriptor) -> Arc<dyn ImageIndex> {
    wrap_index(
        base,
        IndexOverrides {
            subject: Some(subject),
            ..Default::default()
        },
    )
}

// ---------------------------------------------------------------------------
// Time, canonical, rebase

/// Formats unix seconds as RFC 3339 UTC (supports years 1..=9999).
pub fn format_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

struct TimeLayerCore {
    inner: Arc<dyn Layer>,
    mtime: u64,
    diff_id: OnceCell<Digest>,
}

#[async_trait]
impl UncompressedLayerCore for TimeLayerCore {
    async fn diff_id(&self) -> Result<Digest> {
        self.diff_id
            .get_or_try_init(|| async {
                Ok(digest_reader(self.uncompressed().await?, Algorithm::Sha256)
                    .await?
                    .0)
            })
            .await
            .cloned()
    }

    async fn uncompressed(&self) -> Result<BoxReader> {
        let input = self.inner.uncompressed().await?;
        let handle = Handle::current();
        let mtime = self.mtime;
        Ok(blocking_pipe(move |w| {
            let mut src = tarutil::sync_reader(input, handle);
            {
                let mut archive = tar::Archive::new(&mut src);
                let mut builder = tar::Builder::new(&mut *w);
                for entry in archive.entries()? {
                    let mut entry = entry?;
                    let mut header = entry.header().clone();
                    header.set_mtime(mtime);
                    let path = tarutil::entry_path(&entry)?;
                    let link = entry.link_name()?.map(|l| l.into_owned());
                    tarutil::append_entry(
                        &mut builder,
                        header,
                        &path,
                        link.as_deref(),
                        &mut entry,
                    )?;
                }
                builder.finish()?;
            }
            // Drain so a verifying source reaches EOF and checks its digest.
            io::copy(&mut src, &mut io::sink())?;
            Ok(())
        }))
    }

    async fn media_type(&self) -> Result<MediaType> {
        self.inner.media_type().await
    }
}

/// A layer whose tar entries all carry modification time `secs`
/// (`mutate.layerTime`). Content is rewritten on the fly when read.
pub async fn layer_time(layer: Arc<dyn Layer>, secs: i64) -> Result<Arc<dyn Layer>> {
    let mt = layer.media_type().await?;
    let compression = Compression::from_media_type(&mt).unwrap_or(Compression::Gzip);
    let core = TimeLayerCore {
        inner: layer,
        mtime: secs.max(0) as u64,
        diff_id: OnceCell::new(),
    };
    Ok(Arc::new(UncompressedLayer::with_compression(
        core,
        compression,
        None,
    )))
}

/// Sets every timestamp in the image (config, history and tar entries) to
/// `secs` since the epoch (`mutate.Time`).
pub async fn time(img: Arc<dyn Image>, secs: i64) -> Result<Arc<dyn Image>> {
    let layers = img.layers().await?;
    let ocf = img.config_file().await?;
    let hist = &ocf.history;
    let n = hist.len().max(layers.len());
    let mut adds = vec![Addendum::default(); n];
    let (mut h, mut a) = (0usize, 0usize);
    for layer in layers {
        let new = layer_time(layer, secs).await?;
        while h < hist.len() && a < n {
            adds[a].history = hist[h].clone();
            h += 1;
            if hist[h - 1].empty_layer {
                a += 1;
                continue;
            }
            break;
        }
        if a < n {
            adds[a].layer = Some(new);
        }
        a += 1;
    }
    while h < hist.len() && a < n {
        adds[a].history = hist[h].clone();
        h += 1;
        a += 1;
    }

    let new = append(crate::empty::image(), adds)?;
    let mut cfg = new.config_file().await?;
    cfg.architecture = ocf.architecture.clone();
    cfg.os = ocf.os.clone();
    cfg.os_version = ocf.os_version.clone();
    cfg.config = ocf.config.clone();
    let t = format_rfc3339(secs);
    cfg.created = Some(t.clone());
    for (i, entry) in cfg.history.iter_mut().enumerate() {
        let orig = hist.get(i).cloned().unwrap_or_default();
        *entry = History {
            created: Some(t.clone()),
            created_by: orig.created_by,
            author: None,
            comment: orig.comment,
            empty_layer: orig.empty_layer,
        };
    }
    Ok(config_file(new, cfg))
}

/// Removes host- and time-dependent data so builds are reproducible
/// (`mutate.Canonical`).
pub async fn canonical(img: Arc<dyn Image>) -> Result<Arc<dyn Image>> {
    let img = time(img, CANONICAL_TIME).await?;
    let mut cfg = img.config_file().await?;
    cfg.container = None;
    if let Some(c) = cfg.config.as_mut() {
        c.hostname = None;
    }
    cfg.docker_version = None;
    Ok(config_file(img, cfg))
}

fn create_addendums(
    start_history: usize,
    start_layer: usize,
    history: &[History],
    layers: &[Arc<dyn Layer>],
) -> Result<Vec<Addendum>> {
    let mut adds = Vec::new();
    let mut li = 0;
    for (hi, h) in history.iter().enumerate() {
        let mut layer = None;
        if !h.empty_layer {
            let l = layers.get(li).ok_or_else(|| {
                Error::Invalid(format!(
                    "malformed image: config history references at least {} non-empty layer(s), but only {} layer(s) are present",
                    li + 1,
                    layers.len()
                ))
            })?;
            layer = Some(l.clone());
            li += 1;
        }
        if hi >= start_history || li >= start_layer {
            adds.push(Addendum {
                layer,
                history: h.clone(),
                ..Default::default()
            });
        }
    }
    for (i, l) in layers.iter().enumerate().skip(li) {
        if i >= start_layer {
            adds.push(Addendum::layer(l.clone()));
        }
    }
    Ok(adds)
}

/// Replaces `old_base`'s layers at the bottom of `orig` with `new_base`'s
/// (`mutate.Rebase`).
pub async fn rebase(
    orig: Arc<dyn Image>,
    old_base: Arc<dyn Image>,
    new_base: Arc<dyn Image>,
) -> Result<Arc<dyn Image>> {
    let orig_layers = orig.layers().await?;
    let old_layers = old_base.layers().await?;
    if old_layers.len() > orig_layers.len() {
        return Err(Error::Invalid(
            "image is not based on old base (too few layers)".to_string(),
        ));
    }
    for (i, (o, b)) in orig_layers.iter().zip(&old_layers).enumerate() {
        if o.digest().await? != b.digest().await? {
            return Err(Error::Invalid(format!(
                "image is not based on old base (layer {} mismatch)",
                i
            )));
        }
    }
    let old_cfg = old_base.config_file().await?;
    let orig_cfg = orig.config_file().await?;
    let new_cfg = new_base.config_file().await?;

    let rebased = config(
        crate::empty::image(),
        orig_cfg.config.clone().unwrap_or_default(),
    )
    .await?;
    let mut cf = rebased.config_file().await?;
    cf.architecture = new_cfg.architecture.clone();
    cf.os = new_cfg.os.clone();
    cf.os_version = new_cfg.os_version.clone();
    let rebased = config_file(rebased, cf);

    let new_layers = new_base.layers().await?;
    let rebased = append(
        rebased,
        create_addendums(0, 0, &new_cfg.history, &new_layers)?,
    )?;
    append(
        rebased,
        create_addendums(
            old_cfg.history.len(),
            old_layers.len() + 1,
            &orig_cfg.history,
            &orig_layers,
        )?,
    )
}

// ---------------------------------------------------------------------------
// Extract

fn in_marked_dir(map: &HashMap<String, bool>, file: &str, only_true: bool) -> bool {
    let mut file = file.to_string();
    while !file.is_empty() {
        let d = tarutil::dir(&file);
        if d == file {
            break;
        }
        match map.get(&d) {
            Some(v) if *v || !only_true => return true,
            _ => {}
        }
        file = d;
    }
    false
}

fn extract_layer<R: Read, W: io::Write>(
    out: &mut tar::Builder<W>,
    file_map: &mut HashMap<String, bool>,
    opaque: &mut HashSet<String>,
    src: &mut R,
) -> io::Result<()> {
    let mut layer_opaque = HashSet::new();
    {
        let mut archive = tar::Archive::new(&mut *src);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let et = entry.header().entry_type();
            if matches!(et, tar::EntryType::XGlobalHeader | tar::EntryType::XHeader) {
                continue;
            }
            let name = tarutil::clean_path(&tarutil::entry_path(&entry)?);
            if tarutil::unsafe_archive_path(&name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsafe tar path {:?}", name),
                ));
            }
            let link = entry.link_name()?.map(|l| l.into_owned());
            if (et.is_symlink() || et.is_hard_link())
                && let Some(l) = &link
                && tarutil::unsafe_relative_link(&name, &l.to_string_lossy())
            {
                continue;
            }
            let mut basename = tarutil::base(&name).to_string();
            let dirname = tarutil::dir(&name);
            if basename == OPAQUE_WHITEOUT {
                layer_opaque.insert(dirname);
                continue;
            }
            let tombstone = basename.starts_with(WHITEOUT_PREFIX);
            if tombstone {
                basename = basename[WHITEOUT_PREFIX.len()..].to_string();
            }
            let key = if et.is_dir() {
                name.clone()
            } else {
                tarutil::join(&dirname, &basename)
            };
            if file_map.contains_key(&key) && !tombstone {
                continue;
            }
            if in_marked_dir(file_map, &key, true) {
                continue;
            }
            let opaque_map: HashMap<String, bool> =
                opaque.iter().map(|d| (d.clone(), true)).collect();
            if in_marked_dir(&opaque_map, &key, true) {
                continue;
            }
            file_map.insert(key, tombstone || !et.is_dir());
            if !tombstone {
                let header = entry.header().clone();
                tarutil::append_entry(out, header, &name, link.as_deref(), &mut entry)?;
            }
        }
    }
    opaque.extend(layer_opaque);
    io::copy(src, &mut io::sink())?;
    Ok(())
}

/// Flattens the image's layers into one tar stream, applying whiteouts
/// (`mutate.Extract`). Entries with paths or relative links escaping the
/// root are rejected or dropped, like go-containerregistry.
pub fn extract(img: Arc<dyn Image>) -> BoxReader {
    let handle = Handle::current();
    blocking_pipe(move |w| {
        let layers = handle
            .block_on(img.layers())
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut builder = tar::Builder::new(&mut *w);
        let mut file_map = HashMap::new();
        let mut opaque = HashSet::new();
        for layer in layers.iter().rev() {
            let r = handle
                .block_on(layer.uncompressed())
                .map_err(|e| io::Error::other(e.to_string()))?;
            let mut src = tarutil::sync_reader(r, handle.clone());
            extract_layer(&mut builder, &mut file_map, &mut opaque, &mut src)?;
        }
        builder.finish()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::static_layer::StaticLayer;
    use crate::{empty, read_all};

    fn tar_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (name, data) in entries {
            let mut h = tar::Header::new_gnu();
            if name.ends_with('/') {
                h.set_entry_type(tar::EntryType::Directory);
                h.set_size(0);
                h.set_mode(0o755);
            } else {
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
            }
            h.set_mtime(1234);
            b.append_data(&mut h, name, *data).unwrap();
        }
        b.into_inner().unwrap()
    }

    fn layer(entries: &[(&str, &[u8])]) -> Arc<dyn Layer> {
        Arc::new(StaticLayer::new(tar_of(entries), MediaType::OciLayer))
    }

    fn names(tar: &[u8]) -> Vec<(String, u64)> {
        let mut a = tar::Archive::new(tar);
        a.entries()
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.path().unwrap().to_string_lossy().into_owned(),
                    e.header().mtime().unwrap(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn append_updates_config_and_manifest() {
        let l1 = layer(&[("a", b"1")]);
        let img = append_layers(empty::image(), vec![l1.clone()]);
        let m = img.manifest().await.unwrap();
        assert_eq!(m.media_type(), MediaType::DockerManifest);
        assert_eq!(m.layers().len(), 1);
        let cf = img.config_file().await.unwrap();
        assert_eq!(cf.rootfs.diff_ids, vec![l1.diff_id().await.unwrap()]);
        assert_eq!(cf.history.len(), 1);
        assert_eq!(
            m.config().digest,
            Digest::sha256(&img.raw_config_file().await.unwrap())
        );
        let got = img
            .layer_by_digest(&l1.digest().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            read_all(got.compressed().await.unwrap())
                .await
                .unwrap()
                .len() as u64,
            l1.size().await.unwrap()
        );

        let oci = media_type(
            annotations(img, BTreeMap::from([("k".into(), "v".into())])),
            MediaType::OciManifest,
        );
        let raw = oci.raw_manifest().await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["mediaType"], "application/vnd.oci.image.manifest.v1+json");
        assert_eq!(v["annotations"]["k"], "v");
    }

    #[tokio::test]
    async fn artifact_config_is_preserved() {
        let cfg = StaticLayer::new(
            b"{\"custom\":1}".to_vec(),
            MediaType::Other("application/x-cfg".into()),
        );
        let manifest = OciManifest::new(cfg.descriptor().await.unwrap(), vec![])
            .to_bytes()
            .unwrap();
        let base: Arc<dyn Image> = Arc::new(crate::partial::FixedImage::new(
            manifest,
            b"{\"custom\":1}".to_vec(),
            vec![],
        ));
        let blob = Arc::new(StaticLayer::new(
            b"disk".to_vec(),
            MediaType::Other("application/x-disk".into()),
        ));
        let img = append_layers(base, vec![blob]);
        assert_eq!(img.raw_config_file().await.unwrap(), b"{\"custom\":1}");
        assert_eq!(img.manifest().await.unwrap().layers().len(), 1);
    }

    #[tokio::test]
    async fn extract_applies_whiteouts() {
        let l1 = layer(&[
            ("dir/", b""),
            ("dir/a", b"a"),
            ("dir/b", b"b"),
            ("keep", b"k"),
            ("opq/", b""),
            ("opq/x", b"x"),
        ]);
        let l2 = layer(&[
            ("dir/.wh.a", b""),
            ("keep", b"k2"),
            ("opq/.wh..wh..opq", b""),
            ("opq/y", b"y"),
        ]);
        let img = append_layers(empty::image(), vec![l1, l2]);
        let out = read_all(extract(img)).await.unwrap();
        let mut got: Vec<String> = names(&out).into_iter().map(|(n, _)| n).collect();
        got.sort();
        assert_eq!(got, vec!["dir", "dir/b", "keep", "opq", "opq/y"]);
    }

    #[tokio::test]
    async fn time_and_canonical() {
        let mut cf = ImageConfig::new("arm64", "linux");
        cf.history.push(History {
            created_by: Some("ENV".into()),
            empty_layer: true,
            ..Default::default()
        });
        let base = config_file(empty::image(), cf);
        let img = append(
            base,
            vec![Addendum {
                layer: Some(layer(&[("f", b"data")])),
                history: History {
                    created_by: Some("COPY".into()),
                    author: Some("me".into()),
                    ..Default::default()
                },
                ..Default::default()
            }],
        )
        .unwrap();
        let t = time(img.clone(), 86_400).await.unwrap();
        let cfg = t.config_file().await.unwrap();
        assert_eq!(cfg.created.as_deref(), Some("1970-01-02T00:00:00Z"));
        assert_eq!(cfg.architecture, "arm64");
        assert_eq!(cfg.history.len(), 2);
        assert!(cfg.history[0].empty_layer);
        assert_eq!(cfg.history[1].created_by.as_deref(), Some("COPY"));
        assert_eq!(cfg.history[1].author, None);
        let layers = t.layers().await.unwrap();
        assert_eq!(layers.len(), 1);
        let u = read_all(layers[0].uncompressed().await.unwrap())
            .await
            .unwrap();
        assert_eq!(names(&u), vec![("f".to_string(), 86_400)]);
        assert_eq!(cfg.rootfs.diff_ids[0], Digest::sha256(&u));
        let c = canonical(img).await.unwrap();
        assert_eq!(
            c.config_file().await.unwrap().created.as_deref(),
            Some("0001-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn rebase_swaps_base_layers() {
        let old = append_layers(empty::image(), vec![layer(&[("old", b"o")])]);
        let new = append_layers(empty::image(), vec![layer(&[("new", b"n")])]);
        let top = layer(&[("top", b"t")]);
        let orig = append_layers(old.clone(), vec![top.clone()]);
        let r = rebase(orig, old, new.clone()).await.unwrap();
        let ls = r.layers().await.unwrap();
        assert_eq!(ls.len(), 2);
        assert_eq!(
            ls[0].digest().await.unwrap(),
            new.layers().await.unwrap()[0].digest().await.unwrap()
        );
        assert_eq!(ls[1].digest().await.unwrap(), top.digest().await.unwrap());
    }

    #[tokio::test]
    async fn index_append_and_remove() {
        let img = append_layers(empty::image(), vec![layer(&[("a", b"a")])]);
        let d = img.digest().await.unwrap();
        let idx = append_manifests(
            empty::index(),
            vec![
                IndexAddendum::new(Child::Image(img))
                    .with_platform(Platform::new("amd64", "linux")),
            ],
        );
        let m = idx.index_manifest().await.unwrap();
        assert_eq!(m.manifests().len(), 1);
        assert_eq!(
            m.manifests()[0].platform.as_ref().unwrap().architecture,
            "amd64"
        );
        assert!(idx.image(&d).await.is_ok());
        let removed = remove_manifests(idx, crate::matcher::digests([d]));
        assert!(
            removed
                .index_manifest()
                .await
                .unwrap()
                .manifests()
                .is_empty()
        );
    }

    #[test]
    fn rfc3339() {
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(CANONICAL_TIME), "0001-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
    }
}
