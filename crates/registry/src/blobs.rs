//! Blob reads: existence checks, in-memory and streaming downloads, ranged
//! reads, and resumable / parallel downloads to files.

use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio_util::io::StreamReader;
use tracing::{debug, instrument, warn};

use containerregistry_image::{Digest, Hasher};

use crate::client::{Client, Target, delete_scope};
use crate::metrics::Operation;
use crate::progress::{Progress, ProgressFn};
use crate::reference::Reference;
use crate::{Error, Result};

/// How many times a broken download stream is resumed with a `Range`
/// request before giving up.
const DEFAULT_STREAM_RESUMES: u32 = 5;

/// A streaming blob download.
///
/// Implements [`AsyncRead`] and [`AsyncBufRead`]. For full-blob reads
/// (`Client::get_blob_stream`) the digest is verified incrementally and a
/// mismatch is reported as an `InvalidData` I/O error *at end of stream*, so
/// consumers must read to EOF before trusting the data. Interrupted
/// connections are transparently resumed with HTTP `Range` requests.
pub struct BlobReader {
    inner: StreamReader<BoxStream<'static, io::Result<Bytes>>, Bytes>,
    size: Option<u64>,
}

impl BlobReader {
    /// Returns the number of bytes this reader will produce, when known.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Converts the reader into a stream of chunks.
    pub fn into_stream(self) -> BoxStream<'static, io::Result<Bytes>> {
        self.inner.into_inner()
    }
}

impl std::fmt::Debug for BlobReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader")
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for BlobReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncBufRead for BlobReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.inner).consume(amt)
    }
}

/// Recovers a crate [`Error`] that was wrapped into an [`io::Error`] by a
/// [`BlobReader`] (e.g. a digest mismatch), or wraps the I/O error.
pub fn error_from_io(e: io::Error) -> Error {
    if e.get_ref().is_some_and(|inner| inner.is::<Error>()) {
        match e.into_inner().map(|b| b.downcast::<Error>()) {
            Some(Ok(err)) => *err,
            Some(Err(other)) => Error::Io(io::Error::other(other)),
            None => Error::Io(io::Error::other("unknown error")),
        }
    } else {
        Error::Io(e)
    }
}

fn io_err(e: Error) -> io::Error {
    let kind = match &e {
        Error::DigestMismatch { .. } => io::ErrorKind::InvalidData,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, e)
}

/// State of a (resumable) ranged download.
struct Download {
    client: Client,
    target: Target,
    url: String,
    digest: Digest,
    /// Next absolute offset to deliver.
    pos: u64,
    /// Exclusive end offset, once known.
    end: Option<u64>,
    body: Option<BoxStream<'static, reqwest::Result<Bytes>>>,
    /// Bytes to discard because the server ignored our `Range` header.
    skip: u64,
    resumes_left: u32,
    hasher: Option<Hasher>,
    progress: Option<ProgressFn>,
    done: bool,
}

impl Download {
    /// Issues the GET for the remaining range.
    async fn open(&mut self) -> Result<()> {
        let range = match self.end {
            Some(end) if self.pos > 0 || self.end.is_some() => {
                if self.pos >= end {
                    self.body = Some(futures::stream::empty().boxed());
                    return Ok(());
                }
                Some(format!("bytes={}-{}", self.pos, end - 1))
            }
            _ if self.pos > 0 => Some(format!("bytes={}-", self.pos)),
            _ => None,
        };
        let url = self.url.clone();
        let http = self.client.http.clone();
        let resp = self
            .client
            .send(&self.target, Some(Operation::GetBlob), true, || {
                let rb = http.get(&url);
                match &range {
                    Some(r) => rb.header(RANGE, r),
                    None => rb,
                }
            })
            .await?;

        match resp.status() {
            StatusCode::PARTIAL_CONTENT => {
                if self.end.is_none() {
                    self.end = parse_content_range_total(resp.headers());
                }
            }
            StatusCode::OK => {
                // Full content: either we asked for everything, or the server
                // ignored the Range header and we must skip what we have.
                self.skip = self.pos;
                if self.end.is_none() {
                    self.end = resp.content_length();
                }
            }
            StatusCode::RANGE_NOT_SATISFIABLE if range.is_some() => {
                // Nothing left to read (e.g. resuming a complete file).
                if self.end.is_none() {
                    self.end = Some(self.pos);
                }
                self.body = Some(futures::stream::empty().boxed());
                return Ok(());
            }
            _ => {
                return Err(self
                    .client
                    .error_from_response(resp, "GET blob", &self.digest.to_string())
                    .await);
            }
        }
        self.body = Some(resp.bytes_stream().boxed());
        Ok(())
    }

    async fn next_chunk(&mut self) -> Option<io::Result<Bytes>> {
        loop {
            if self.done {
                return None;
            }
            if self.body.is_none()
                && let Err(e) = self.open().await
            {
                if e.is_retryable() && self.resumes_left > 0 {
                    self.resumes_left -= 1;
                    tokio::time::sleep(self.client.backoff(0)).await;
                    continue;
                }
                self.done = true;
                return Some(Err(io_err(e)));
            }
            let body = self.body.as_mut().expect("body opened");
            match body.next().await {
                Some(Ok(mut chunk)) => {
                    if self.skip > 0 {
                        let n = (self.skip as usize).min(chunk.len());
                        let _ = chunk.split_to(n);
                        self.skip -= n as u64;
                    }
                    if let Some(end) = self.end {
                        let remaining = end.saturating_sub(self.pos);
                        if (chunk.len() as u64) > remaining {
                            chunk.truncate(remaining as usize);
                        }
                    }
                    if chunk.is_empty() {
                        if self.end.is_some_and(|end| self.pos >= end) {
                            self.body = Some(futures::stream::empty().boxed());
                        }
                        continue;
                    }
                    if let Some(h) = &mut self.hasher {
                        h.update(&chunk);
                    }
                    self.pos += chunk.len() as u64;
                    if let Some(p) = &self.progress {
                        p(Progress {
                            total: self.end.unwrap_or(0),
                            complete: self.pos,
                        });
                    }
                    return Some(Ok(chunk));
                }
                Some(Err(e)) => {
                    self.body = None;
                    if self.resumes_left > 0 {
                        self.resumes_left -= 1;
                        warn!(pos = self.pos, error = %e, "blob stream interrupted, resuming");
                        tokio::time::sleep(self.client.backoff(0)).await;
                        continue;
                    }
                    self.done = true;
                    return Some(Err(io_err(self.client.map_transport_error(e))));
                }
                None => {
                    self.body = None;
                    if let Some(end) = self.end
                        && self.pos < end
                    {
                        if self.resumes_left > 0 {
                            self.resumes_left -= 1;
                            debug!(pos = self.pos, end, "blob stream ended early, resuming");
                            continue;
                        }
                        self.done = true;
                        return Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "blob {} truncated at {} of {} bytes",
                                self.digest, self.pos, end
                            ),
                        )));
                    }
                    self.done = true;
                    if let Some(h) = self.hasher.take() {
                        let actual = h.finalize();
                        if actual != self.digest {
                            return Some(Err(io_err(Error::DigestMismatch {
                                expected: self.digest.to_string(),
                                actual: actual.to_string(),
                            })));
                        }
                    }
                    return None;
                }
            }
        }
    }
}

fn parse_content_range_total(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    // Content-Range: bytes 0-99/1234
    let v = headers.get(CONTENT_RANGE)?.to_str().ok()?;
    let total = v.rsplit('/').next()?;
    total.trim().parse().ok()
}

fn range_bounds(range: impl RangeBounds<u64>) -> (u64, Option<u64>) {
    let start = match range.start_bound() {
        Bound::Included(s) => *s,
        Bound::Excluded(s) => s + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(e) => Some(e + 1),
        Bound::Excluded(e) => Some(*e),
        Bound::Unbounded => None,
    };
    (start, end)
}

/// Options for [`Client::download_blob`].
#[derive(Clone)]
pub struct DownloadOptions {
    /// Expected blob size; fetched with `HEAD` when unknown and needed.
    pub size: Option<u64>,
    /// Number of concurrent range requests for a single blob (1 = sequential).
    pub parallelism: usize,
    /// Size of each part for parallel downloads.
    pub part_size: u64,
    /// Progress callback.
    pub progress: Option<ProgressFn>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            size: None,
            parallelism: 1,
            part_size: 64 * 1024 * 1024,
            progress: None,
        }
    }
}

impl std::fmt::Debug for DownloadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadOptions")
            .field("size", &self.size)
            .field("parallelism", &self.parallelism)
            .field("part_size", &self.part_size)
            .finish_non_exhaustive()
    }
}

impl DownloadOptions {
    /// Sets the expected size.
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Sets the number of parallel range requests.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Sets the part size for parallel downloads.
    pub fn with_part_size(mut self, part_size: u64) -> Self {
        self.part_size = part_size.max(1);
        self
    }

    /// Sets a progress callback.
    pub fn with_progress(mut self, progress: ProgressFn) -> Self {
        self.progress = Some(progress);
        self
    }
}

/// Persistent state for a parallel download, stored next to the partial file.
#[derive(Debug, Serialize, Deserialize)]
struct PartsState {
    digest: String,
    size: u64,
    part_size: u64,
    done: Vec<bool>,
}

fn partial_path(dest: &Path) -> PathBuf {
    let mut s = dest.as_os_str().to_owned();
    s.push(".partial");
    PathBuf::from(s)
}

fn parts_state_path(dest: &Path) -> PathBuf {
    let mut s = dest.as_os_str().to_owned();
    s.push(".partial.json");
    PathBuf::from(s)
}

impl Client {
    /// Checks if a blob exists in the registry.
    ///
    /// Returns the size if the blob exists.
    #[instrument(skip(self), level = "debug", fields(reference = %reference, digest = %digest))]
    pub async fn head_blob(&self, reference: &Reference, digest: &Digest) -> Result<u64> {
        let start = Instant::now();
        let result = async {
            let url = self.url(
                reference.registry(),
                &format!("/{}/blobs/{}", reference.repository(), digest),
            );
            let resp = self
                .send(
                    &Target::pull(reference),
                    Some(Operation::HeadBlob),
                    true,
                    || self.http.head(&url),
                )
                .await?;
            match resp.status() {
                StatusCode::OK => Ok(resp
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)),
                _ => Err(self
                    .error_from_response(resp, "HEAD blob", &digest.to_string())
                    .await),
            }
        }
        .await;
        self.observe(Operation::HeadBlob, start, &result, 0);
        result
    }

    /// Returns the blob size if it exists, `None` if the registry reports it
    /// missing.
    pub async fn blob_exists(&self, reference: &Reference, digest: &Digest) -> Result<Option<u64>> {
        match self.head_blob(reference, digest).await {
            Ok(size) => Ok(Some(size)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Gets a blob from the registry into memory, verifying its digest.
    ///
    /// For large blobs use [`Client::get_blob_stream`] or
    /// [`Client::download_blob`].
    #[instrument(skip(self), level = "debug", fields(reference = %reference, digest = %digest))]
    pub async fn get_blob(&self, reference: &Reference, digest: &Digest) -> Result<Vec<u8>> {
        let mut reader = self.get_blob_stream(reference, digest).await?;
        let mut data = Vec::with_capacity(reader.size().unwrap_or(0).min(1 << 26) as usize);
        reader.read_to_end(&mut data).await.map_err(error_from_io)?;
        Ok(data)
    }

    /// Opens a streaming download of a whole blob.
    ///
    /// The returned reader verifies the digest as it goes (reporting a
    /// mismatch at EOF) and resumes interrupted transfers with `Range`
    /// requests. Memory use is bounded regardless of blob size.
    pub async fn get_blob_stream(
        &self,
        reference: &Reference,
        digest: &Digest,
    ) -> Result<BlobReader> {
        self.open_blob(
            reference,
            digest,
            0,
            None,
            Some(Hasher::new(digest.algorithm())),
            None,
        )
        .await
    }

    /// Opens a streaming download of a byte range of a blob (HTTP `Range`).
    ///
    /// Partial content cannot be verified against the blob digest, so the
    /// reader does not check it. Servers that ignore `Range` are handled by
    /// discarding the leading bytes.
    pub async fn get_blob_range(
        &self,
        reference: &Reference,
        digest: &Digest,
        range: impl RangeBounds<u64>,
    ) -> Result<BlobReader> {
        let (start, end) = range_bounds(range);
        if let Some(end) = end
            && end < start
        {
            return Err(Error::UnexpectedStatus {
                status: 416,
                message: format!("invalid range {}..{}", start, end),
            });
        }
        self.open_blob(reference, digest, start, end, None, None)
            .await
    }

    /// Opens a download starting at `start`. When `hasher` is given it must
    /// already contain bytes `0..start`, and the digest is verified at EOF.
    pub(crate) async fn open_blob(
        &self,
        reference: &Reference,
        digest: &Digest,
        start: u64,
        end: Option<u64>,
        hasher: Option<Hasher>,
        progress: Option<ProgressFn>,
    ) -> Result<BlobReader> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/blobs/{}", reference.repository(), digest),
        );
        let mut dl = Download {
            client: self.clone(),
            target: Target::pull(reference),
            url,
            digest: digest.clone(),
            pos: start,
            end,
            body: None,
            skip: 0,
            resumes_left: DEFAULT_STREAM_RESUMES,
            hasher,
            progress,
            done: false,
        };
        let op_start = Instant::now();
        // Open eagerly so that 404/401 surface from this call.
        if let Err(e) = dl.open().await {
            self.record_failure(Operation::GetBlob, op_start);
            return Err(e);
        }
        let size = dl.end.map(|e| e.saturating_sub(start));
        self.record_success(Operation::GetBlob, op_start, size.unwrap_or(0));
        let stream = futures::stream::unfold(dl, |mut dl| async move {
            dl.next_chunk().await.map(|item| (item, dl))
        })
        .fuse()
        .boxed();
        Ok(BlobReader {
            inner: StreamReader::new(stream),
            size,
        })
    }

    /// Downloads a blob to `dest`, verifying its digest.
    ///
    /// Data is written to `<dest>.partial` and renamed into place only after
    /// verification, so `dest` never holds unverified content. If a previous
    /// attempt left a partial file, the download resumes from it:
    ///
    /// - sequential mode (`parallelism == 1`): the existing prefix is hashed
    ///   and the rest is fetched with a `Range` request;
    /// - parallel mode: the blob is split into `part_size` ranges fetched
    ///   concurrently; completed parts are recorded in
    ///   `<dest>.partial.json` and skipped on resume.
    ///
    /// Returns the blob size.
    pub async fn download_blob(
        &self,
        reference: &Reference,
        digest: &Digest,
        dest: &Path,
        options: DownloadOptions,
    ) -> Result<u64> {
        let size = match options.size {
            Some(s) => Some(s),
            None if options.parallelism > 1 => Some(self.head_blob(reference, digest).await?),
            None => None,
        };
        let parallel = options.parallelism > 1
            && size.is_some_and(|s| s > options.part_size.saturating_mul(2));
        let n = if parallel {
            self.download_parallel(reference, digest, dest, size.unwrap_or(0), &options)
                .await?
        } else {
            self.download_sequential(reference, digest, dest, size, &options)
                .await?
        };
        tokio::fs::rename(partial_path(dest), dest).await?;
        let _ = tokio::fs::remove_file(parts_state_path(dest)).await;
        Ok(n)
    }

    async fn download_sequential(
        &self,
        reference: &Reference,
        digest: &Digest,
        dest: &Path,
        size: Option<u64>,
        options: &DownloadOptions,
    ) -> Result<u64> {
        let partial = partial_path(dest);
        if let Some(parent) = partial.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // A parts-state file means the partial file has holes; start over.
        if tokio::fs::try_exists(parts_state_path(dest)).await? {
            let _ = tokio::fs::remove_file(&partial).await;
            let _ = tokio::fs::remove_file(parts_state_path(dest)).await;
        }
        let mut existing = match tokio::fs::metadata(&partial).await {
            Ok(m) => m.len(),
            Err(_) => 0,
        };
        if size.is_some_and(|s| existing > s) {
            existing = 0;
        }
        let hasher = if existing > 0 {
            let path = partial.clone();
            let alg = digest.algorithm();
            tokio::task::spawn_blocking(move || -> io::Result<Hasher> {
                let mut h = Hasher::new(alg);
                let mut f = std::fs::File::open(&path)?;
                io::copy(&mut io::Read::take(&mut f, existing), &mut h)?;
                Ok(h)
            })
            .await
            .map_err(|e| Error::Io(io::Error::other(e)))??
        } else {
            Hasher::new(digest.algorithm())
        };

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&partial)
            .await?;
        file.set_len(existing).await?;
        file.seek(io::SeekFrom::Start(existing)).await?;
        if existing > 0 {
            debug!(existing, "resuming blob download");
        }

        let mut reader = self
            .open_blob(
                reference,
                digest,
                existing,
                size,
                Some(hasher),
                options.progress.clone(),
            )
            .await?;
        let copied = tokio::io::copy_buf(&mut reader, &mut file).await;
        file.flush().await?;
        match copied {
            Ok(n) => Ok(existing + n),
            Err(e) => {
                let err = error_from_io(e);
                if matches!(err, Error::DigestMismatch { .. }) {
                    // Corrupt data must not be resumed from.
                    let _ = tokio::fs::remove_file(&partial).await;
                }
                Err(err)
            }
        }
    }

    async fn download_parallel(
        &self,
        reference: &Reference,
        digest: &Digest,
        dest: &Path,
        size: u64,
        options: &DownloadOptions,
    ) -> Result<u64> {
        let partial = partial_path(dest);
        let state_path = parts_state_path(dest);
        if let Some(parent) = partial.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let part_size = options.part_size;
        let parts = size.div_ceil(part_size) as usize;

        let loaded = match tokio::fs::read(&state_path).await {
            Ok(data) => serde_json::from_slice::<PartsState>(&data).ok(),
            Err(_) => None,
        }
        .filter(|s| {
            s.digest == digest.to_string()
                && s.size == size
                && s.part_size == part_size
                && s.done.len() == parts
        })
        .filter(|_| partial.exists());
        if loaded.is_none() {
            let _ = tokio::fs::remove_file(&partial).await;
        }
        let state = loaded.unwrap_or_else(|| PartsState {
            digest: digest.to_string(),
            size,
            part_size,
            done: vec![false; parts],
        });

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&partial)
            .await?;
        file.set_len(size).await?;
        drop(file);
        tokio::fs::write(&state_path, serde_json::to_vec(&state)?).await?;

        let completed = Arc::new(AtomicU64::new(
            (0..parts)
                .filter(|i| state.done[*i])
                .map(|i| part_len(i, part_size, size))
                .sum(),
        ));
        let pending: Vec<usize> = (0..parts).filter(|i| !state.done[*i]).collect();
        if !pending.is_empty() {
            debug!(pending = pending.len(), parts, "downloading blob parts");
        }
        let state = Arc::new(tokio::sync::Mutex::new(state));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(options.parallelism));
        let mut tasks = tokio::task::JoinSet::new();
        for i in pending {
            let client = self.clone();
            let reference = reference.clone();
            let digest = digest.clone();
            let partial = partial.clone();
            let state = state.clone();
            let state_path = state_path.clone();
            let semaphore = semaphore.clone();
            let completed = completed.clone();
            let progress = options.progress.clone();
            tasks.spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore");
                let start = i as u64 * part_size;
                let end = (start + part_size).min(size);
                let mut reader = client
                    .open_blob(&reference, &digest, start, Some(end), None, None)
                    .await?;
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&partial)
                    .await?;
                file.seek(io::SeekFrom::Start(start)).await?;
                let mut buf = vec![0u8; 256 * 1024];
                let mut written = 0u64;
                loop {
                    let n = reader.read(&mut buf).await.map_err(error_from_io)?;
                    if n == 0 {
                        break;
                    }
                    file.write_all(&buf[..n]).await?;
                    written += n as u64;
                    let done = completed.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
                    if let Some(p) = &progress {
                        p(Progress {
                            total: size,
                            complete: done,
                        });
                    }
                }
                file.flush().await?;
                file.sync_data().await?;
                if written != end - start {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("part {} short: {} of {} bytes", i, written, end - start),
                    )));
                }
                let mut s = state.lock().await;
                s.done[i] = true;
                tokio::fs::write(&state_path, serde_json::to_vec(&*s)?).await?;
                Ok::<(), Error>(())
            });
        }
        let mut first_err = None;
        while let Some(res) = tasks.join_next().await {
            let res = res
                .map_err(|e| Error::Io(io::Error::other(e)))
                .and_then(|r| r);
            if let Err(e) = res
                && first_err.is_none()
            {
                first_err = Some(e);
                // Let in-flight parts finish so their progress is recorded.
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }

        // Verify the assembled file.
        let path = partial.clone();
        let alg = digest.algorithm();
        let (actual, n) = tokio::task::spawn_blocking(move || {
            let f = std::fs::File::open(&path)?;
            Digest::from_reader(alg, io::BufReader::with_capacity(1 << 20, f))
        })
        .await
        .map_err(|e| Error::Io(io::Error::other(e)))??;
        if &actual != digest {
            let _ = tokio::fs::remove_file(&partial).await;
            let _ = tokio::fs::remove_file(&state_path).await;
            return Err(Error::DigestMismatch {
                expected: digest.to_string(),
                actual: actual.to_string(),
            });
        }
        Ok(n)
    }

    /// Deletes a blob by digest.
    pub async fn delete_blob(&self, reference: &Reference, digest: &Digest) -> Result<()> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/blobs/{}", reference.repository(), digest),
        );
        let target = Target::push(reference).with_scope(delete_scope(reference.repository()));
        let response = self
            .send(&target, None, true, || self.http.delete(&url))
            .await?;
        match response.status() {
            StatusCode::ACCEPTED | StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            _ => Err(self
                .error_from_response(response, "DELETE blob", &digest.to_string())
                .await),
        }
    }
}

fn part_len(i: usize, part_size: u64, size: u64) -> u64 {
    let start = i as u64 * part_size;
    (start + part_size).min(size) - start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_bounds() {
        assert_eq!(range_bounds(..), (0, None));
        assert_eq!(range_bounds(5..), (5, None));
        assert_eq!(range_bounds(5..10), (5, Some(10)));
        assert_eq!(range_bounds(5..=10), (5, Some(11)));
    }

    #[test]
    fn test_error_from_io_roundtrip() {
        let e = io_err(Error::DigestMismatch {
            expected: "a".into(),
            actual: "b".into(),
        });
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(matches!(error_from_io(e), Error::DigestMismatch { .. }));
        assert!(matches!(error_from_io(io::Error::other("x")), Error::Io(_)));
    }
}
