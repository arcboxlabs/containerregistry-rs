//! Registry-backed images, indexes and layers, and concurrent writers
//! (go-containerregistry `pkg/v1/remote`).
//!
//! Reading: [`get`] fetches a manifest once and can be turned into an
//! [`Image`] or [`ImageIndex`]; [`image`] resolves an index to a platform.
//! Layers are fetched lazily and streamed, with digest verification and
//! automatic resume. [`Layer::download_to`] on a remote layer uses
//! [`Client::download_blob`], so sinks such as an OCI layout get resumable,
//! parallel range downloads.
//!
//! Writing: [`write`], [`write_index`], [`write_layer`] and [`multi_write`]
//! upload blobs concurrently (bounded by [`Options::jobs`]), skip blobs that
//! already exist, mount blobs from other repositories on the same registry,
//! upload local files with resume, and stream everything else. Blobs shared
//! between images are uploaded once.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::{OnceCell, Semaphore};

use containerregistry_image::{Descriptor, Digest, MediaType, Platform};
use containerregistry_v1::partial::{Child, CompressedLayer, CompressedLayerCore};
use containerregistry_v1::{
    BoxReader, Error as V1Error, Image, ImageIndex, Layer, MountSource, Result as V1Result,
    Taggable,
};

use crate::{
    Client, DownloadOptions, Error, Progress, ProgressFn, RawManifest, Reference, UploadOptions,
};

/// Converts registry errors into v1 errors, keeping "not found" distinct.
pub fn into_v1(e: Error) -> V1Error {
    match e {
        Error::NotFound(s) => V1Error::NotFound(s),
        e if e.status_code() == Some(404) => V1Error::NotFound(e.to_string()),
        e => V1Error::source(e),
    }
}

/// Options for reading and writing remote content.
#[derive(Clone)]
pub struct Options {
    /// Platform to select when a reference names an index
    /// (go `remote.WithPlatform`; defaults to `linux/amd64`).
    pub platform: Platform,
    /// Maximum concurrent blob transfers (go `remote.WithJobs`, default 4).
    pub jobs: usize,
    /// Concurrent range requests per blob for [`Layer::download_to`]
    /// (default 4).
    pub download_parallelism: usize,
    /// Upload in chunks of this size, retrying failed chunks; `None`
    /// streams each blob in one request.
    pub chunk_size: Option<usize>,
    /// Aggregate progress over all blobs (go `remote.WithProgress`).
    pub progress: Option<ProgressFn>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            platform: Platform::new("amd64", "linux"),
            jobs: 4,
            download_parallelism: 4,
            chunk_size: None,
            progress: None,
        }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("platform", &self.platform)
            .field("jobs", &self.jobs)
            .field("download_parallelism", &self.download_parallelism)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl Options {
    /// Sets the platform.
    pub fn with_platform(mut self, platform: Platform) -> Self {
        self.platform = platform;
        self
    }

    /// Sets the number of concurrent blob transfers.
    pub fn with_jobs(mut self, jobs: usize) -> Self {
        self.jobs = jobs.max(1);
        self
    }

    /// Sets the progress callback.
    pub fn with_progress(mut self, progress: ProgressFn) -> Self {
        self.progress = Some(progress);
        self
    }
}

// ---------------------------------------------------------------------------
// Reading

/// A fetched manifest of either kind (go `remote.Descriptor`).
#[derive(Clone)]
pub struct RemoteDescriptor {
    client: Client,
    reference: Reference,
    raw: RawManifest,
    descriptor: Option<Descriptor>,
}

impl std::fmt::Debug for RemoteDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDescriptor")
            .field("reference", &self.reference.to_string())
            .field("media_type", &self.raw.media_type)
            .field("digest", &self.raw.digest)
            .finish()
    }
}

impl RemoteDescriptor {
    /// The raw manifest.
    pub fn raw(&self) -> &RawManifest {
        &self.raw
    }

    /// The descriptor of the manifest.
    pub fn descriptor(&self) -> Descriptor {
        self.descriptor
            .clone()
            .unwrap_or_else(|| self.raw.descriptor())
    }

    /// Whether this is an index.
    pub fn is_index(&self) -> bool {
        self.raw.is_index()
    }

    fn repo_ref(&self) -> Reference {
        self.reference
            .clone()
            .with_new_digest(self.raw.digest.clone())
    }

    /// Interprets the manifest as an image. For an index, resolves the child
    /// matching `platform` (go `Descriptor.Image`).
    pub async fn image(&self, platform: &Platform) -> V1Result<Arc<dyn Image>> {
        if !self.is_index() {
            return Ok(Arc::new(RemoteImage::new(
                self.client.clone(),
                self.repo_ref(),
                self.raw.data.clone(),
                Some(self.descriptor()),
            )));
        }
        let idx = self.image_index();
        let manifest = idx.index_manifest().await?;
        let child = manifest
            .manifests()
            .iter()
            .find(|d| {
                d.media_type.is_manifest()
                    && d.platform.as_ref().is_some_and(|p| p.satisfies(platform))
            })
            .ok_or_else(|| {
                V1Error::NotFound(format!(
                    "no child with platform {} in index {}",
                    platform, self.reference
                ))
            })?;
        idx.image(&child.digest).await
    }

    /// Interprets the manifest as an index (go `Descriptor.ImageIndex`).
    pub fn image_index(&self) -> Arc<dyn ImageIndex> {
        Arc::new(RemoteIndex {
            client: self.client.clone(),
            reference: self.repo_ref(),
            raw: self.raw.data.clone(),
            descriptor: self.descriptor(),
        })
    }

    /// The manifest as a [`Taggable`], resolving nothing.
    pub fn taggable(&self) -> Taggable {
        if self.is_index() {
            Taggable::Index(self.image_index())
        } else {
            Taggable::Image(Arc::new(RemoteImage::new(
                self.client.clone(),
                self.repo_ref(),
                self.raw.data.clone(),
                Some(self.descriptor()),
            )))
        }
    }
}

/// Fetches the manifest `reference` points to (go `remote.Get`).
pub async fn get(client: &Client, reference: &Reference) -> V1Result<RemoteDescriptor> {
    let raw = client.get_manifest_raw(reference).await.map_err(into_v1)?;
    Ok(RemoteDescriptor {
        client: client.clone(),
        reference: reference.clone(),
        raw,
        descriptor: None,
    })
}

/// Fetches an image, resolving an index to `options.platform`
/// (go `remote.Image`).
pub async fn image(
    client: &Client,
    reference: &Reference,
    options: &Options,
) -> V1Result<Arc<dyn Image>> {
    get(client, reference).await?.image(&options.platform).await
}

/// Fetches an index (go `remote.Index`).
pub async fn index(client: &Client, reference: &Reference) -> V1Result<Arc<dyn ImageIndex>> {
    let d = get(client, reference).await?;
    if !d.is_index() {
        return Err(V1Error::Invalid(format!(
            "{} is not an index (media type {})",
            reference, d.raw.media_type
        )));
    }
    Ok(d.image_index())
}

/// A layer in a repository, addressed by its descriptor
/// (go `remote.Layer`). The descriptor is trusted for digest, size and
/// media type; content is verified as it streams.
pub fn layer(client: &Client, reference: &Reference, descriptor: Descriptor) -> Arc<dyn Layer> {
    Arc::new(CompressedLayer::new(RemoteBlob {
        client: client.clone(),
        reference: reference.clone(),
        descriptor,
    }))
}

struct RemoteBlob {
    client: Client,
    reference: Reference,
    descriptor: Descriptor,
}

#[async_trait]
impl CompressedLayerCore for RemoteBlob {
    async fn digest(&self) -> V1Result<Digest> {
        Ok(self.descriptor.digest.clone())
    }

    async fn compressed(&self) -> V1Result<BoxReader> {
        let r = self
            .client
            .get_blob_stream(&self.reference, &self.descriptor.digest)
            .await
            .map_err(into_v1)?;
        Ok(Box::pin(r))
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

    fn mount_source(&self) -> Option<MountSource> {
        Some(MountSource {
            registry: self.reference.registry().to_string(),
            repository: self.reference.repository().to_string(),
        })
    }

    async fn download_to(&self, dest: &Path) -> V1Result<bool> {
        let opts = DownloadOptions::default()
            .with_size(self.descriptor.size)
            .with_parallelism(if self.descriptor.size > 64 * 1024 * 1024 {
                4
            } else {
                1
            });
        self.client
            .download_blob(&self.reference, &self.descriptor.digest, dest, opts)
            .await
            .map_err(into_v1)?;
        Ok(true)
    }
}

struct RemoteImage {
    client: Client,
    reference: Reference,
    raw: Vec<u8>,
    descriptor: Option<Descriptor>,
    config: OnceCell<Vec<u8>>,
}

impl RemoteImage {
    fn new(
        client: Client,
        reference: Reference,
        raw: Vec<u8>,
        descriptor: Option<Descriptor>,
    ) -> Self {
        Self {
            client,
            reference,
            raw,
            descriptor,
            config: OnceCell::new(),
        }
    }
}

#[async_trait]
impl Image for RemoteImage {
    async fn raw_manifest(&self) -> V1Result<Vec<u8>> {
        Ok(self.raw.clone())
    }

    async fn raw_config_file(&self) -> V1Result<Vec<u8>> {
        self.config
            .get_or_try_init(|| async {
                let m = self.manifest().await?;
                let cfg = m.config();
                if let Some(data) = &cfg.data {
                    use base64::Engine;
                    if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(data)
                        && Digest::compute(cfg.digest.algorithm(), &b) == cfg.digest
                    {
                        return Ok(b);
                    }
                }
                self.client
                    .get_blob(&self.reference, &cfg.digest)
                    .await
                    .map_err(into_v1)
            })
            .await
            .cloned()
    }

    async fn layer_by_digest(&self, digest: &Digest) -> V1Result<Arc<dyn Layer>> {
        let m = self.manifest().await?;
        let d = containerregistry_v1::partial::find_blob_descriptor(&m, digest)
            .cloned()
            .ok_or_else(|| V1Error::NotFound(format!("blob {} not in manifest", digest)))?;
        Ok(layer(&self.client, &self.reference, d))
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

struct RemoteIndex {
    client: Client,
    reference: Reference,
    raw: Vec<u8>,
    descriptor: Descriptor,
}

impl RemoteIndex {
    async fn child_descriptor(&self, digest: &Digest) -> V1Result<Descriptor> {
        self.index_manifest()
            .await?
            .manifests()
            .iter()
            .find(|d| &d.digest == digest)
            .cloned()
            .ok_or_else(|| V1Error::NotFound(format!("manifest {} not in index", digest)))
    }

    async fn child(&self, digest: &Digest) -> V1Result<RemoteDescriptor> {
        let desc = self.child_descriptor(digest).await?;
        let r = self.reference.clone().with_new_digest(digest.clone());
        let raw = self.client.get_manifest_raw(&r).await.map_err(into_v1)?;
        Ok(RemoteDescriptor {
            client: self.client.clone(),
            reference: r,
            raw,
            descriptor: Some(desc),
        })
    }
}

#[async_trait]
impl ImageIndex for RemoteIndex {
    async fn raw_manifest(&self) -> V1Result<Vec<u8>> {
        Ok(self.raw.clone())
    }

    async fn image(&self, digest: &Digest) -> V1Result<Arc<dyn Image>> {
        let c = self.child(digest).await?;
        Ok(Arc::new(RemoteImage::new(
            self.client.clone(),
            c.repo_ref(),
            c.raw.data.clone(),
            Some(c.descriptor()),
        )))
    }

    async fn image_index(&self, digest: &Digest) -> V1Result<Arc<dyn ImageIndex>> {
        Ok(self.child(digest).await?.image_index())
    }

    async fn blob(&self, digest: &Digest) -> V1Result<Arc<dyn Layer>> {
        let d = self.child_descriptor(digest).await?;
        Ok(layer(&self.client, &self.reference, d))
    }

    async fn descriptor(&self) -> V1Result<Descriptor> {
        Ok(self.descriptor.clone())
    }
}

// ---------------------------------------------------------------------------
// Writing

struct Progressor {
    total: AtomicU64,
    complete: AtomicU64,
    callback: ProgressFn,
}

impl Progressor {
    fn report(&self) {
        (self.callback)(Progress {
            total: self.total.load(Ordering::Relaxed),
            complete: self.complete.load(Ordering::Relaxed),
        });
    }

    /// A per-blob callback that adds its deltas to the aggregate.
    fn blob(self: &Arc<Self>) -> ProgressFn {
        let me = self.clone();
        let last = Arc::new(AtomicU64::new(0));
        Arc::new(move |p: Progress| {
            let prev = last.swap(p.complete, Ordering::Relaxed);
            if p.complete > prev {
                me.complete.fetch_add(p.complete - prev, Ordering::Relaxed);
            }
            me.report();
        })
    }
}

/// A blob in a repository: (repository, digest).
type BlobKey = (String, Digest);

/// Shared state of one write operation: the concurrency limit and the
/// blobs already uploaded (or being uploaded) per repository.
struct Writer {
    client: Client,
    options: Options,
    semaphore: Semaphore,
    done: Mutex<HashMap<BlobKey, Arc<OnceCell<()>>>>,
    progress: Option<Arc<Progressor>>,
}

impl Writer {
    fn new(client: &Client, options: &Options) -> Arc<Self> {
        Arc::new(Self {
            client: client.clone(),
            options: options.clone(),
            semaphore: Semaphore::new(options.jobs.max(1)),
            done: Mutex::new(HashMap::new()),
            progress: options.progress.clone().map(|callback| {
                Arc::new(Progressor {
                    total: AtomicU64::new(0),
                    complete: AtomicU64::new(0),
                    callback,
                })
            }),
        })
    }

    fn upload_options(&self) -> UploadOptions {
        let mut o = UploadOptions {
            chunk_size: self.options.chunk_size,
            ..Default::default()
        };
        if let Some(p) = &self.progress {
            o.progress = Some(p.blob());
        }
        o
    }

    async fn upload_blob(&self, repo: &Reference, layer: Arc<dyn Layer>) -> V1Result<()> {
        // Streaming layers only know their digest after being consumed.
        let known = match layer.digest().await {
            Ok(d) => Some(d),
            Err(V1Error::NotComputed(_)) => None,
            Err(e) => return Err(e),
        };
        let Some(digest) = known else {
            let _permit = self.semaphore.acquire().await.expect("semaphore");
            let r = layer.compressed().await?;
            self.client
                .push_blob_stream(repo, r, self.upload_options())
                .await
                .map_err(into_v1)?;
            return Ok(());
        };
        let cell = {
            let mut done = self.done.lock().expect("lock");
            done.entry((repo.repository().to_string(), digest.clone()))
                .or_default()
                .clone()
        };
        cell.get_or_try_init(|| self.upload_known(repo, layer, digest))
            .await?;
        Ok(())
    }

    async fn upload_known(
        &self,
        repo: &Reference,
        layer: Arc<dyn Layer>,
        digest: Digest,
    ) -> V1Result<()> {
        let _permit = self.semaphore.acquire().await.expect("semaphore");
        let size = layer.size().await?;
        if let Some(p) = &self.progress {
            p.total.fetch_add(size, Ordering::Relaxed);
        }
        if self
            .client
            .blob_exists(repo, &digest)
            .await
            .map_err(into_v1)?
            .is_some()
        {
            if let Some(p) = &self.progress {
                p.complete.fetch_add(size, Ordering::Relaxed);
                p.report();
            }
            return Ok(());
        }
        let mut opts = self.upload_options().with_digest(digest).with_size(size);
        if let Some(m) = layer.mount_source()
            && m.registry == repo.registry()
            && m.repository != repo.repository()
        {
            opts = opts.with_mount_from(m.repository);
        }
        let res = match layer.local_path() {
            Some(path) => self.client.push_blob_file(repo, &path, opts).await,
            None => {
                let r = layer.compressed().await?;
                self.client.push_blob_stream(repo, r, opts).await
            }
        };
        let uploaded = res.map_err(into_v1)?;
        if uploaded.status != crate::UploadStatus::Uploaded
            && let Some(p) = &self.progress
        {
            p.complete.fetch_add(size, Ordering::Relaxed);
            p.report();
        }
        Ok(())
    }

    async fn write_image(&self, repo: &Reference, img: Arc<dyn Image>) -> V1Result<Digest> {
        let layers = img.layers().await?;
        let futs: Vec<_> = layers
            .into_iter()
            .map(|l| self.upload_blob(repo, l))
            .collect();
        futures::stream::iter(futs)
            .buffer_unordered(self.options.jobs.max(1))
            .try_collect::<Vec<_>>()
            .await?;
        // The config goes last: for streamed layers its digest (and the
        // manifest) only exist once the layers were consumed.
        let cfg = img.config_layer().await?;
        self.upload_blob(repo, cfg).await?;
        let raw = img.raw_manifest().await?;
        Ok(Digest::sha256(&raw))
    }

    async fn put(&self, reference: &Reference, raw: &[u8], mt: &MediaType) -> V1Result<()> {
        self.client
            .put_manifest_raw(reference, raw, mt)
            .await
            .map_err(into_v1)?;
        Ok(())
    }

    fn write_index<'a>(
        &'a self,
        repo: &'a Reference,
        idx: Arc<dyn ImageIndex>,
    ) -> futures::future::BoxFuture<'a, V1Result<()>> {
        Box::pin(async move {
            let children = containerregistry_v1::partial::children(idx.as_ref()).await?;
            let futs: Vec<_> = children
                .into_iter()
                .map(|(desc, child)| self.write_child(repo, desc, child))
                .collect();
            futures::stream::iter(futs)
                .buffer_unordered(self.options.jobs.max(1))
                .try_collect::<Vec<_>>()
                .await?;
            Ok(())
        })
    }

    fn write_child<'a>(
        &'a self,
        repo: &'a Reference,
        desc: Descriptor,
        child: Child,
    ) -> futures::future::BoxFuture<'a, V1Result<()>> {
        Box::pin(async move {
            let child_ref = repo.clone().with_new_digest(desc.digest.clone());
            let raw = match child {
                Child::Blob(l) => return self.upload_blob(repo, l).await,
                // Skip children the registry already has.
                _ if self.client.head_manifest(&child_ref).await.is_ok() => return Ok(()),
                Child::Image(i) => {
                    self.write_image(repo, i.clone()).await?;
                    i.raw_manifest().await?
                }
                Child::Index(i) => {
                    self.write_index(repo, i.clone()).await?;
                    i.raw_manifest().await?
                }
            };
            self.put(&child_ref, &raw, &desc.media_type).await
        })
    }

    async fn write_taggable(&self, reference: &Reference, t: &Taggable) -> V1Result<()> {
        match t {
            Taggable::Image(i) => {
                self.write_image(reference, i.clone()).await?;
            }
            Taggable::Index(i) => self.write_index(reference, i.clone()).await?,
        }
        let raw = t.raw_manifest().await?;
        let mt = t.media_type().await?;
        self.put(reference, &raw, &mt).await
    }
}

/// Uploads an image's blobs concurrently and then its manifest to
/// `reference` (go `remote.Write`).
pub async fn write(
    client: &Client,
    reference: &Reference,
    img: Arc<dyn Image>,
    options: &Options,
) -> V1Result<()> {
    Writer::new(client, options)
        .write_taggable(reference, &Taggable::Image(img))
        .await
}

/// Uploads an index, its children and their blobs, then the index
/// manifest (go `remote.WriteIndex`).
pub async fn write_index(
    client: &Client,
    reference: &Reference,
    idx: Arc<dyn ImageIndex>,
    options: &Options,
) -> V1Result<()> {
    Writer::new(client, options)
        .write_taggable(reference, &Taggable::Index(idx))
        .await
}

/// Uploads an image or index (go `remote.Push`).
pub async fn push(
    client: &Client,
    reference: &Reference,
    t: &Taggable,
    options: &Options,
) -> V1Result<()> {
    Writer::new(client, options)
        .write_taggable(reference, t)
        .await
}

/// Uploads a single blob to the repository of `reference`
/// (go `remote.WriteLayer`).
pub async fn write_layer(
    client: &Client,
    reference: &Reference,
    layer: Arc<dyn Layer>,
    options: &Options,
) -> V1Result<()> {
    Writer::new(client, options)
        .upload_blob(reference, layer)
        .await
}

/// Uploads only the manifest of `t`; its blobs must already exist
/// (go `remote.Put`).
pub async fn put(client: &Client, reference: &Reference, t: &Taggable) -> V1Result<()> {
    let raw = t.raw_manifest().await?;
    let mt = t.media_type().await?;
    client
        .put_manifest_raw(reference, &raw, &mt)
        .await
        .map_err(into_v1)?;
    Ok(())
}

/// Writes several images or indexes, sharing one concurrency limit and
/// uploading each blob once per repository (go `remote.MultiWrite`).
pub async fn multi_write(
    client: &Client,
    items: Vec<(Reference, Taggable)>,
    options: &Options,
) -> V1Result<()> {
    let w = Writer::new(client, options);
    futures::stream::iter(items.iter().map(|(r, t)| w.write_taggable(r, t)))
        .buffer_unordered(options.jobs.max(1))
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}
