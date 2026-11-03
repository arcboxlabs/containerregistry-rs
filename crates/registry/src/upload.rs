//! Blob uploads: monolithic, chunked, streaming, resumable, and cross-repo
//! mounts.
//!
//! The building block is [`BlobUpload`], an upload session at a registry
//! `Location`. Higher-level helpers ([`Client::push_blob_stream`],
//! [`Client::push_blob_file`]) drive a session from an `AsyncRead` or a file,
//! computing the digest while the bytes stream past, so blobs never have to
//! fit in memory.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tracing::{debug, instrument, warn};

use containerregistry_image::{Algorithm, Digest, Hasher};

use crate::client::{Client, Target, pull_scope};
use crate::metrics::Operation;
use crate::progress::{Progress, ProgressFn};
use crate::reference::Reference;
use crate::{Error, Result};

/// Called after every committed chunk with the session state, so callers can
/// persist it and resume the upload after a crash with
/// [`UploadOptions::resume_location`].
pub type CheckpointFn = Arc<dyn Fn(&UploadCheckpoint) + Send + Sync>;

/// A resumable point in an upload session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadCheckpoint {
    /// The upload session URL.
    pub location: String,
    /// Number of bytes the registry has committed.
    pub offset: u64,
}

/// How a blob ended up in the registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadStatus {
    /// The bytes were uploaded.
    Uploaded,
    /// The registry already had the blob; nothing was sent.
    AlreadyExists,
    /// The blob was mounted from another repository.
    Mounted,
}

/// Result of a blob push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadedBlob {
    /// Digest of the blob.
    pub digest: Digest,
    /// Size of the blob in bytes (0 when it already existed or was mounted
    /// and the size was not known).
    pub size: u64,
    /// How the blob got there.
    pub status: UploadStatus,
}

/// Options for [`Client::push_blob_stream`] and [`Client::push_blob_file`].
#[derive(Clone, Default)]
pub struct UploadOptions {
    /// Expected digest. When set, an existing blob is detected with `HEAD`
    /// and skipped, mounts can be attempted, and the uploaded bytes are
    /// checked before the upload is committed.
    pub digest: Option<Digest>,
    /// Expected size, checked before the upload is committed.
    pub size: Option<u64>,
    /// Send the data as a series of `PATCH` requests of this many bytes
    /// (with `Content-Range`). Each chunk is retried individually, so a
    /// transient failure costs at most one chunk. `None` streams the data
    /// in a single `PATCH`, which every registry supports.
    pub chunk_size: Option<usize>,
    /// Repositories (on the same registry) to try mounting the blob from.
    pub mount_from: Vec<String>,
    /// Digest algorithm when `digest` is not given (default SHA-256).
    pub algorithm: Option<Algorithm>,
    /// Progress callback.
    pub progress: Option<ProgressFn>,
    /// Checkpoint callback (see [`CheckpointFn`]).
    pub checkpoint: Option<CheckpointFn>,
    /// Resume an existing upload session (file uploads only).
    pub resume_location: Option<String>,
}

impl std::fmt::Debug for UploadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadOptions")
            .field("digest", &self.digest)
            .field("size", &self.size)
            .field("chunk_size", &self.chunk_size)
            .field("mount_from", &self.mount_from)
            .field("resume_location", &self.resume_location)
            .finish_non_exhaustive()
    }
}

impl UploadOptions {
    /// Sets the expected digest.
    pub fn with_digest(mut self, digest: Digest) -> Self {
        self.digest = Some(digest);
        self
    }

    /// Sets the expected size.
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Enables chunked uploads with the given chunk size.
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = Some(chunk_size.max(1));
        self
    }

    /// Adds a repository to try mounting from.
    pub fn with_mount_from(mut self, repository: impl Into<String>) -> Self {
        self.mount_from.push(repository.into());
        self
    }

    /// Sets a progress callback.
    pub fn with_progress(mut self, progress: ProgressFn) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Sets a checkpoint callback.
    pub fn with_checkpoint(mut self, checkpoint: CheckpointFn) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// Resumes the given upload session.
    pub fn with_resume_location(mut self, location: impl Into<String>) -> Self {
        self.resume_location = Some(location.into());
        self
    }
}

/// Result of a mount attempt.
#[derive(Debug)]
pub enum MountResult {
    /// The blob was mounted.
    Mounted,
    /// The registry declined and opened an upload session instead.
    Upload(Box<BlobUpload>),
}

/// An open blob upload session.
pub struct BlobUpload {
    client: Client,
    reference: Reference,
    target: Target,
    location: String,
    offset: u64,
}

impl std::fmt::Debug for BlobUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobUpload")
            .field("reference", &self.reference.to_string())
            .field("location", &self.location)
            .field("offset", &self.offset)
            .finish()
    }
}

/// Parses an upload `Range` header (`0-<last>`; some registries prefix
/// `bytes=`) into the next offset, given the offset we believe is current.
fn parse_upload_range(headers: &reqwest::header::HeaderMap, local_offset: u64) -> Option<u64> {
    let value = headers.get(RANGE)?.to_str().ok()?.trim();
    let value = value.strip_prefix("bytes=").unwrap_or(value);
    let (_, end) = value.split_once('-')?;
    let end: u64 = end.trim().parse().ok()?;
    // distribution reports "0-0" both for an empty upload and for one byte.
    if end == 0 && local_offset == 0 {
        return Some(0);
    }
    Some(end + 1)
}

impl BlobUpload {
    /// The session URL (persist it to resume later).
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Bytes committed so far, as last reported by the registry.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The current checkpoint.
    pub fn checkpoint(&self) -> UploadCheckpoint {
        UploadCheckpoint {
            location: self.location.clone(),
            offset: self.offset,
        }
    }

    fn update_location(&mut self, headers: &reqwest::header::HeaderMap) -> Result<()> {
        if let Some(loc) = headers.get(LOCATION).and_then(|v| v.to_str().ok()) {
            self.location = self
                .client
                .resolve_location(self.reference.registry(), loc)?;
        }
        Ok(())
    }

    /// Queries the registry for the committed offset (`GET <location>`).
    pub async fn status(&mut self) -> Result<u64> {
        let url = self.location.clone();
        let resp = self
            .client
            .send(&self.target, Some(Operation::PutBlob), true, || {
                self.client.http.get(&url)
            })
            .await?;
        match resp.status() {
            StatusCode::NO_CONTENT | StatusCode::ACCEPTED | StatusCode::OK => {
                self.update_location(resp.headers())?;
                self.offset = parse_upload_range(resp.headers(), self.offset).unwrap_or(0);
                Ok(self.offset)
            }
            _ => Err(self
                .client
                .error_from_response(resp, "GET upload status", &self.reference.to_string())
                .await),
        }
    }

    /// Uploads `data` as the next chunk (`PATCH` with `Content-Range`).
    ///
    /// Transient failures are retried; before each retry the committed
    /// offset is re-read so that bytes the registry already has are not sent
    /// twice.
    pub async fn write_chunk(&mut self, data: Bytes) -> Result<()> {
        let chunk_start = self.offset;
        let chunk_end = chunk_start + data.len() as u64;
        let mut attempts = 0u32;
        while self.offset < chunk_end {
            let body = data.slice((self.offset - chunk_start) as usize..);
            let range = format!("{}-{}", self.offset, chunk_end - 1);
            let url = self.location.clone();
            let len = body.len();
            let result = self
                .client
                .send(&self.target, Some(Operation::PutBlob), false, || {
                    self.client
                        .http
                        .patch(&url)
                        .header(CONTENT_TYPE, "application/octet-stream")
                        .header("Content-Range", range.as_str())
                        .header(CONTENT_LENGTH, len)
                        .body(body.clone())
                })
                .await;
            let failure = match result {
                Ok(resp)
                    if matches!(
                        resp.status(),
                        StatusCode::ACCEPTED | StatusCode::NO_CONTENT | StatusCode::CREATED
                    ) =>
                {
                    self.update_location(resp.headers())?;
                    let next = parse_upload_range(resp.headers(), chunk_end).unwrap_or(chunk_end);
                    if next < self.offset || next > chunk_end {
                        return Err(Error::UnexpectedStatus {
                            status: resp.status().as_u16(),
                            message: format!(
                                "registry reported upload offset {} outside chunk {}..{}",
                                next, self.offset, chunk_end
                            ),
                        });
                    }
                    self.offset = next;
                    continue;
                }
                Ok(resp) if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE => {
                    // Our idea of the offset is wrong; ask the registry.
                    let err = self
                        .client
                        .error_from_response(resp, "PATCH blob", &self.reference.to_string())
                        .await;
                    if attempts >= self.client.config.retries {
                        return Err(err);
                    }
                    err
                }
                Ok(resp) => {
                    let err = self
                        .client
                        .error_from_response(resp, "PATCH blob", &self.reference.to_string())
                        .await;
                    if !err.is_retryable() || attempts >= self.client.config.retries {
                        return Err(err);
                    }
                    err
                }
                Err(e) => {
                    if !e.is_retryable() || attempts >= self.client.config.retries {
                        return Err(e);
                    }
                    e
                }
            };
            warn!(offset = self.offset, error = %failure, "chunk upload failed, resuming");
            tokio::time::sleep(self.client.backoff(attempts)).await;
            attempts += 1;
            let committed = self.status().await?;
            if committed < chunk_start || committed > chunk_end {
                return Err(Error::UnexpectedStatus {
                    status: 416,
                    message: format!(
                        "cannot resume chunk {}..{}: registry has {} bytes",
                        chunk_start, chunk_end, committed
                    ),
                });
            }
        }
        Ok(())
    }

    /// Streams `body` in a single `PATCH` without `Content-Range`.
    ///
    /// The body cannot be replayed, so failures are returned to the caller
    /// (which can call [`BlobUpload::status`] and continue from the
    /// committed offset).
    pub async fn write_stream(&mut self, body: reqwest::Body) -> Result<()> {
        let rb = self
            .client
            .http
            .patch(&self.location)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(body);
        let resp = self.client.send_once(&self.target, rb).await?;
        match resp.status() {
            StatusCode::ACCEPTED | StatusCode::NO_CONTENT | StatusCode::CREATED => {
                self.update_location(resp.headers())?;
                if let Some(next) = parse_upload_range(resp.headers(), 1) {
                    self.offset = next;
                }
                Ok(())
            }
            _ => Err(self
                .client
                .error_from_response(resp, "PATCH blob", &self.reference.to_string())
                .await),
        }
    }

    fn commit_url(&self, digest: &Digest) -> String {
        let separator = if self.location.contains('?') {
            "&"
        } else {
            "?"
        };
        format!("{}{}digest={}", self.location, separator, digest)
    }

    /// Completes the upload (`PUT <location>?digest=`).
    pub async fn finish(self, digest: &Digest) -> Result<Digest> {
        self.finish_with(digest, Bytes::new()).await
    }

    /// Completes the upload, sending `data` as the final (or only) bytes —
    /// a monolithic upload when no chunks were written.
    pub async fn finish_with(self, digest: &Digest, data: Bytes) -> Result<Digest> {
        let url = self.commit_url(digest);
        let resp = self
            .client
            .send(&self.target, Some(Operation::PutBlob), true, || {
                self.client
                    .http
                    .put(&url)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, data.len())
                    .body(data.clone())
            })
            .await?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::NO_CONTENT | StatusCode::OK => Ok(digest.clone()),
            StatusCode::NOT_FOUND => {
                // A retried commit after a lost response finds the session
                // gone; the blob may nevertheless be there.
                if self
                    .client
                    .blob_exists(&self.reference, digest)
                    .await?
                    .is_some()
                {
                    return Ok(digest.clone());
                }
                Err(self
                    .client
                    .error_from_response(resp, "PUT blob", &self.reference.to_string())
                    .await)
            }
            _ => Err(self
                .client
                .error_from_response(resp, "PUT blob", &self.reference.to_string())
                .await),
        }
    }

    /// Cancels the upload session (`DELETE <location>`).
    pub async fn cancel(self) -> Result<()> {
        let url = self.location.clone();
        let resp = self
            .client
            .send(&self.target, None, true, || self.client.http.delete(&url))
            .await?;
        match resp.status() {
            s if s.is_success() || s == StatusCode::NOT_FOUND => Ok(()),
            _ => Err(self
                .client
                .error_from_response(resp, "DELETE upload", &self.reference.to_string())
                .await),
        }
    }
}

/// Reads until `buf` holds `limit` bytes or the reader is exhausted.
async fn read_up_to<R: AsyncRead + Unpin>(reader: &mut R, limit: usize) -> io::Result<BytesMut> {
    let mut buf = BytesMut::with_capacity(limit);
    while buf.len() < limit {
        let n = (&mut *reader)
            .take((limit - buf.len()) as u64)
            .read_buf(&mut buf)
            .await?;
        if n == 0 {
            break;
        }
    }
    Ok(buf)
}

impl Client {
    fn upload_session(
        &self,
        reference: &Reference,
        target: Target,
        location: String,
    ) -> BlobUpload {
        BlobUpload {
            client: self.clone(),
            reference: reference.clone(),
            target,
            location,
            offset: 0,
        }
    }

    async fn location_from(
        &self,
        reference: &Reference,
        resp: &reqwest::Response,
    ) -> Result<String> {
        let loc = resp
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Error::UnexpectedStatus {
                status: resp.status().as_u16(),
                message: "missing Location header in upload response".to_string(),
            })?;
        self.resolve_location(reference.registry(), loc)
    }

    /// Opens a new upload session (`POST /v2/<name>/blobs/uploads/`).
    pub async fn start_blob_upload(&self, reference: &Reference) -> Result<BlobUpload> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/blobs/uploads/", reference.repository()),
        );
        let target = Target::push(reference);
        let resp = self
            .send(&target, Some(Operation::PutBlob), true, || {
                self.http.post(&url).header(CONTENT_LENGTH, 0)
            })
            .await?;
        match resp.status() {
            StatusCode::ACCEPTED => {
                let location = self.location_from(reference, &resp).await?;
                Ok(self.upload_session(reference, target, location))
            }
            _ => Err(self
                .error_from_response(resp, "POST blob upload", &reference.to_string())
                .await),
        }
    }

    /// Reattaches to an existing upload session and reads its offset.
    pub async fn resume_blob_upload(
        &self,
        reference: &Reference,
        location: &str,
    ) -> Result<BlobUpload> {
        let location = self.resolve_location(reference.registry(), location)?;
        let mut upload = self.upload_session(reference, Target::push(reference), location);
        upload.status().await?;
        Ok(upload)
    }

    /// Sends a mount request; returns `None` when mounted, or the upload
    /// session location (if any) the registry opened instead.
    async fn mount_request(
        &self,
        reference: &Reference,
        from_repo: &str,
        digest: &Digest,
    ) -> Result<Option<Option<String>>> {
        let url = self.url(
            reference.registry(),
            &format!("/{}/blobs/uploads/", reference.repository()),
        );
        let target = Target::push(reference).with_scope(pull_scope(from_repo));
        let resp = self
            .send(&target, Some(Operation::PutBlob), true, || {
                self.http.post(&url).header(CONTENT_LENGTH, 0).query(&[
                    ("mount", digest.to_string()),
                    ("from", from_repo.to_string()),
                ])
            })
            .await?;
        match resp.status() {
            StatusCode::CREATED => Ok(None),
            StatusCode::ACCEPTED => Ok(Some(self.location_from(reference, &resp).await.ok())),
            _ => Err(self
                .error_from_response(resp, "mount blob", &reference.to_string())
                .await),
        }
    }

    /// Attempts a cross-repository mount of `digest` from `from_repo`,
    /// returning the upload session the registry opened if it declined.
    pub async fn mount_or_start_upload(
        &self,
        reference: &Reference,
        from_repo: &str,
        digest: &Digest,
    ) -> Result<MountResult> {
        match self.mount_request(reference, from_repo, digest).await? {
            None => Ok(MountResult::Mounted),
            Some(Some(location)) => Ok(MountResult::Upload(Box::new(self.upload_session(
                reference,
                Target::push(reference),
                location,
            )))),
            Some(None) => Ok(MountResult::Upload(Box::new(
                self.start_blob_upload(reference).await?,
            ))),
        }
    }

    /// Mounts a blob from another repository in the same registry.
    ///
    /// Returns true if the blob was mounted, false if the registry requested an upload.
    pub async fn mount_blob(
        &self,
        reference: &Reference,
        from_repo: &str,
        digest: &Digest,
    ) -> Result<bool> {
        Ok(self
            .mount_request(reference, from_repo, digest)
            .await?
            .is_none())
    }

    /// Uploads a blob held in memory (monolithic `POST` + `PUT`).
    ///
    /// For large blobs use [`Client::push_blob_stream`] or
    /// [`Client::push_blob_file`].
    #[instrument(skip(self, data), level = "debug", fields(reference = %reference, size = data.len()))]
    pub async fn put_blob(&self, reference: &Reference, data: &[u8]) -> Result<Digest> {
        let digest = Digest::sha256(data);
        self.put_blob_with_digest(reference, data, &digest).await
    }

    /// Uploads a blob held in memory with a specified digest.
    ///
    /// Skips the upload when the registry already has the blob.
    #[instrument(skip(self, data), level = "debug", fields(reference = %reference, digest = %digest, size = data.len()))]
    pub async fn put_blob_with_digest(
        &self,
        reference: &Reference,
        data: &[u8],
        digest: &Digest,
    ) -> Result<Digest> {
        let start = Instant::now();
        let result = async {
            let computed = Digest::compute(digest.algorithm(), data);
            if &computed != digest {
                return Err(Error::DigestMismatch {
                    expected: digest.to_string(),
                    actual: computed.to_string(),
                });
            }
            if self.head_blob(reference, digest).await.is_ok() {
                return Ok((digest.clone(), 0));
            }
            let upload = self.start_blob_upload(reference).await?;
            upload
                .finish_with(digest, Bytes::copy_from_slice(data))
                .await?;
            Ok((digest.clone(), data.len() as u64))
        }
        .await;
        match &result {
            Ok((_, n)) => self.record_upload(Operation::PutBlob, start, *n),
            Err(_) => self.record_failure(Operation::PutBlob, start),
        }
        result.map(|(d, _)| d)
    }

    /// Uploads a blob using chunked PATCH requests.
    pub async fn put_blob_chunked(
        &self,
        reference: &Reference,
        data: &[u8],
        chunk_size: usize,
    ) -> Result<Digest> {
        let digest = Digest::sha256(data);
        self.put_blob_chunked_with_digest(reference, data, &digest, chunk_size)
            .await
    }

    /// Uploads a blob using chunked PATCH requests with a specified digest.
    pub async fn put_blob_chunked_with_digest(
        &self,
        reference: &Reference,
        data: &[u8],
        digest: &Digest,
        chunk_size: usize,
    ) -> Result<Digest> {
        let start = Instant::now();
        let result = async {
            let computed = Digest::compute(digest.algorithm(), data);
            if &computed != digest {
                return Err(Error::DigestMismatch {
                    expected: digest.to_string(),
                    actual: computed.to_string(),
                });
            }
            let mut upload = self.start_blob_upload(reference).await?;
            let data = Bytes::copy_from_slice(data);
            for chunk_start in (0..data.len()).step_by(chunk_size.max(1)) {
                let end = (chunk_start + chunk_size.max(1)).min(data.len());
                upload.write_chunk(data.slice(chunk_start..end)).await?;
            }
            upload.finish(digest).await
        }
        .await;
        match &result {
            Ok(_) => self.record_upload(Operation::PutBlob, start, data.len() as u64),
            Err(_) => self.record_failure(Operation::PutBlob, start),
        }
        result
    }

    /// Checks for an existing blob and tries mounts; returns either a final
    /// result or an upload session to write to.
    async fn prepare_upload(
        &self,
        reference: &Reference,
        options: &UploadOptions,
    ) -> Result<std::result::Result<UploadedBlob, BlobUpload>> {
        if let Some(digest) = &options.digest {
            if let Some(size) = self.blob_exists(reference, digest).await? {
                return Ok(Ok(UploadedBlob {
                    digest: digest.clone(),
                    size,
                    status: UploadStatus::AlreadyExists,
                }));
            }
            let mut session = None;
            for (i, from) in options.mount_from.iter().enumerate() {
                if from == reference.repository() {
                    continue;
                }
                match self.mount_or_start_upload(reference, from, digest).await {
                    Ok(MountResult::Mounted) => {
                        return Ok(Ok(UploadedBlob {
                            digest: digest.clone(),
                            size: options.size.unwrap_or(0),
                            status: UploadStatus::Mounted,
                        }));
                    }
                    Ok(MountResult::Upload(u)) if i + 1 == options.mount_from.len() => {
                        session = Some(*u);
                    }
                    Ok(MountResult::Upload(u)) => {
                        let _ = u.cancel().await;
                    }
                    Err(e) => debug!(from = %from, error = %e, "mount failed"),
                }
            }
            if let Some(u) = session {
                return Ok(Err(u));
            }
        }
        Ok(Err(self.start_blob_upload(reference).await?))
    }

    /// Uploads everything readable from `reader`, computing the digest on
    /// the fly.
    ///
    /// With [`UploadOptions::chunk_size`] the data is sent in chunks, each
    /// buffered in memory (so memory use is one chunk) and retried on
    /// failure; otherwise it is streamed in a single `PATCH`.
    pub async fn push_blob_stream<R>(
        &self,
        reference: &Reference,
        reader: R,
        options: UploadOptions,
    ) -> Result<UploadedBlob>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let start = Instant::now();
        let result = async {
            let upload = match self.prepare_upload(reference, &options).await? {
                Ok(done) => return Ok(done),
                Err(upload) => upload,
            };
            let alg = options
                .digest
                .as_ref()
                .map(|d| d.algorithm())
                .or(options.algorithm)
                .unwrap_or(Algorithm::Sha256);
            self.upload_reader(upload, reader, Hasher::new(alg), 0, &options)
                .await
        }
        .await;
        match &result {
            Ok(b) if b.status == UploadStatus::Uploaded => {
                self.record_upload(Operation::PutBlob, start, b.size)
            }
            Ok(_) => self.record_success(Operation::PutBlob, start, 0),
            Err(_) => self.record_failure(Operation::PutBlob, start),
        }
        result
    }

    /// Writes `reader` (positioned at `offset` of the blob, with `hasher`
    /// covering bytes before it) to `upload` and commits.
    async fn upload_reader<R>(
        &self,
        mut upload: BlobUpload,
        reader: R,
        hasher: Hasher,
        offset: u64,
        options: &UploadOptions,
    ) -> Result<UploadedBlob>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (hasher, total) = self
            .write_from_reader(&mut upload, reader, hasher, offset, options)
            .await?;
        self.commit(upload, hasher, total, options).await
    }

    /// Sends the reader's bytes to the session; returns the final hasher and
    /// total size.
    async fn write_from_reader<R>(
        &self,
        upload: &mut BlobUpload,
        mut reader: R,
        mut hasher: Hasher,
        offset: u64,
        options: &UploadOptions,
    ) -> Result<(Hasher, u64)>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let total_hint = options.size.unwrap_or(0);
        upload.offset = offset;
        if let Some(chunk_size) = options.chunk_size {
            loop {
                let buf = read_up_to(&mut reader, chunk_size).await?;
                if buf.is_empty() {
                    break;
                }
                let last = buf.len() < chunk_size;
                hasher.update(&buf);
                upload.write_chunk(buf.freeze()).await?;
                if let Some(p) = &options.progress {
                    p(Progress {
                        total: total_hint,
                        complete: upload.offset,
                    });
                }
                if let Some(c) = &options.checkpoint {
                    c(&upload.checkpoint());
                }
                if last {
                    break;
                }
            }
            Ok((hasher, upload.offset))
        } else {
            // Stream in one PATCH; hash and count as bytes leave.
            let shared = Arc::new(Mutex::new((hasher, offset)));
            let tap = shared.clone();
            let progress = options.progress.clone();
            let stream = ReaderStream::with_capacity(reader, 256 * 1024).map(move |chunk| {
                if let Ok(chunk) = &chunk {
                    let mut g = tap.lock().expect("hasher lock");
                    g.0.update(chunk);
                    g.1 += chunk.len() as u64;
                    if let Some(p) = &progress {
                        p(Progress {
                            total: total_hint,
                            complete: g.1,
                        });
                    }
                }
                chunk
            });
            upload
                .write_stream(reqwest::Body::wrap_stream(stream))
                .await?;
            let (hasher, total) = shared.lock().expect("hasher lock").clone();
            upload.offset = total;
            if let Some(c) = &options.checkpoint {
                c(&upload.checkpoint());
            }
            Ok((hasher, total))
        }
    }

    async fn commit(
        &self,
        upload: BlobUpload,
        hasher: Hasher,
        total: u64,
        options: &UploadOptions,
    ) -> Result<UploadedBlob> {
        let digest = hasher.finalize();
        if let Some(size) = options.size
            && size != total
        {
            let _ = upload.cancel().await;
            return Err(Error::Image(containerregistry_image::Error::SizeMismatch {
                expected: size,
                actual: total,
            }));
        }
        if let Some(expected) = &options.digest
            && expected != &digest
        {
            let _ = upload.cancel().await;
            return Err(Error::DigestMismatch {
                expected: expected.to_string(),
                actual: digest.to_string(),
            });
        }
        let digest = upload.finish(&digest).await?;
        Ok(UploadedBlob {
            digest,
            size: total,
            status: UploadStatus::Uploaded,
        })
    }

    /// Uploads a file, resuming after failures.
    ///
    /// On a transient error the committed offset is read back from the
    /// registry, the file is re-positioned and the upload continues; the
    /// digest of the already-committed prefix is recomputed from the file.
    /// The same mechanism resumes a session from a previous process via
    /// [`UploadOptions::resume_location`] (see [`UploadOptions::checkpoint`]).
    pub async fn push_blob_file(
        &self,
        reference: &Reference,
        path: &Path,
        options: UploadOptions,
    ) -> Result<UploadedBlob> {
        let start = Instant::now();
        let result = self.push_blob_file_inner(reference, path, options).await;
        match &result {
            Ok(b) if b.status == UploadStatus::Uploaded => {
                self.record_upload(Operation::PutBlob, start, b.size)
            }
            Ok(_) => self.record_success(Operation::PutBlob, start, 0),
            Err(_) => self.record_failure(Operation::PutBlob, start),
        }
        result
    }

    async fn push_blob_file_inner(
        &self,
        reference: &Reference,
        path: &Path,
        mut options: UploadOptions,
    ) -> Result<UploadedBlob> {
        let size = tokio::fs::metadata(path).await?.len();
        if options.size.is_some_and(|s| s != size) {
            return Err(Error::Image(containerregistry_image::Error::SizeMismatch {
                expected: options.size.unwrap_or(0),
                actual: size,
            }));
        }
        options.size = Some(size);
        let alg = options
            .digest
            .as_ref()
            .map(|d| d.algorithm())
            .or(options.algorithm)
            .unwrap_or(Algorithm::Sha256);

        let mut upload = match &options.resume_location {
            Some(loc) => match self.resume_blob_upload(reference, loc).await {
                Ok(u) => u,
                Err(Error::NotFound(_)) => match self.prepare_upload(reference, &options).await? {
                    Ok(done) => return Ok(done),
                    Err(u) => u,
                },
                Err(e) => return Err(e),
            },
            None => match self.prepare_upload(reference, &options).await? {
                Ok(done) => return Ok(done),
                Err(u) => u,
            },
        };

        let mut attempts = 0u32;
        loop {
            let offset = upload.offset;
            if offset > size {
                return Err(Error::UnexpectedStatus {
                    status: 416,
                    message: format!("registry has {} bytes of a {} byte file", offset, size),
                });
            }
            let hasher = hash_file_prefix(path, offset, alg).await?;
            let mut file = tokio::fs::File::open(path).await?;
            file.seek(io::SeekFrom::Start(offset)).await?;
            match self
                .write_from_reader(&mut upload, file, hasher, offset, &options)
                .await
            {
                Ok((hasher, total)) => return self.commit(upload, hasher, total, &options).await,
                Err(e) if e.is_retryable() && attempts < self.config.retries => {
                    warn!(error = %e, "file upload interrupted, resuming");
                    tokio::time::sleep(self.backoff(attempts)).await;
                    attempts += 1;
                    match upload.status().await {
                        Ok(_) => {}
                        Err(Error::NotFound(_)) => {
                            upload = self.start_blob_upload(reference).await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Hashes the first `len` bytes of a file.
async fn hash_file_prefix(path: &Path, len: u64, alg: Algorithm) -> Result<Hasher> {
    if len == 0 {
        return Ok(Hasher::new(alg));
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> io::Result<Hasher> {
        let mut h = Hasher::new(alg);
        let f = std::fs::File::open(path)?;
        let mut limited = io::Read::take(io::BufReader::with_capacity(1 << 20, f), len);
        let n = io::copy(&mut limited, &mut h)?;
        if n != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file shorter than committed upload",
            ));
        }
        Ok(h)
    })
    .await
    .map_err(|e| Error::Io(io::Error::other(e)))?
    .map_err(Error::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_parse_upload_range() {
        let mut h = HeaderMap::new();
        h.insert(RANGE, HeaderValue::from_static("0-9"));
        assert_eq!(parse_upload_range(&h, 5), Some(10));
        h.insert(RANGE, HeaderValue::from_static("bytes=0-99"));
        assert_eq!(parse_upload_range(&h, 5), Some(100));
        h.insert(RANGE, HeaderValue::from_static("0-0"));
        assert_eq!(parse_upload_range(&h, 0), Some(0));
        assert_eq!(parse_upload_range(&h, 1), Some(1));
        assert_eq!(parse_upload_range(&HeaderMap::new(), 3), None);
    }
}
