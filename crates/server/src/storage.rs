//! Blob storage (in memory or on disk) and upload sessions.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use containerregistry_image::{Algorithm, Digest, Hasher};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

use crate::error::RegistryError;

/// Buffer size for disk writes and reads.
const IO_BUFFER: usize = 1 << 20;

/// Where blob contents are kept.
#[derive(Debug)]
pub(crate) enum BlobStore {
    /// Blobs keyed by digest string.
    Memory(RwLock<HashMap<String, Bytes>>),
    /// Blobs stored at `<dir>/<algorithm>/<hex>` (the go-containerregistry
    /// `NewDiskBlobHandler` layout).
    Disk(PathBuf),
}

/// An open blob.
pub(crate) enum BlobData {
    Memory(Bytes),
    File(tokio::fs::File),
}

fn algorithm_name(alg: Algorithm) -> &'static str {
    match alg {
        Algorithm::Sha256 => "sha256",
        Algorithm::Sha384 => "sha384",
        Algorithm::Sha512 => "sha512",
    }
}

impl BlobStore {
    fn blob_path(dir: &Path, digest: &Digest) -> PathBuf {
        dir.join(algorithm_name(digest.algorithm()))
            .join(digest.hex())
    }

    /// Returns the size of the blob, if present.
    pub(crate) async fn stat(&self, digest: &Digest) -> io::Result<Option<u64>> {
        match self {
            BlobStore::Memory(map) => Ok(map
                .read()
                .expect("blob lock")
                .get(&digest.to_string())
                .map(|b| b.len() as u64)),
            BlobStore::Disk(dir) => match tokio::fs::metadata(Self::blob_path(dir, digest)).await {
                Ok(m) if m.is_file() => Ok(Some(m.len())),
                Ok(_) => Ok(None),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            },
        }
    }

    /// Opens the blob for reading, returning it with its size.
    pub(crate) async fn open(&self, digest: &Digest) -> io::Result<Option<(BlobData, u64)>> {
        match self {
            BlobStore::Memory(map) => Ok(map
                .read()
                .expect("blob lock")
                .get(&digest.to_string())
                .map(|b| (BlobData::Memory(b.clone()), b.len() as u64))),
            BlobStore::Disk(dir) => match tokio::fs::File::open(Self::blob_path(dir, digest)).await
            {
                Ok(f) => {
                    let size = f.metadata().await?.len();
                    Ok(Some((BlobData::File(f), size)))
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            },
        }
    }

    /// Deletes the blob; returns false if it did not exist.
    pub(crate) async fn delete(&self, digest: &Digest) -> io::Result<bool> {
        match self {
            BlobStore::Memory(map) => Ok(map
                .write()
                .expect("blob lock")
                .remove(&digest.to_string())
                .is_some()),
            BlobStore::Disk(dir) => {
                match tokio::fs::remove_file(Self::blob_path(dir, digest)).await {
                    Ok(()) => Ok(true),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

enum Sink {
    Memory(BytesMut),
    File {
        path: PathBuf,
        file: BufWriter<tokio::fs::File>,
    },
}

/// Bytes received so far for an upload session.
///
/// A running sha256 is kept as data arrives; other algorithms re-read the
/// data once at commit time.
pub(crate) struct Upload {
    pub(crate) repo: String,
    pub(crate) size: u64,
    hasher: Hasher,
    sink: Sink,
}

impl Drop for Upload {
    fn drop(&mut self) {
        // Committed uploads were renamed away; anything left is garbage.
        if let Sink::File { path, .. } = &self.sink {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Upload {
    pub(crate) async fn new(store: &BlobStore, repo: &str, id: &str) -> io::Result<Self> {
        let sink = match store {
            BlobStore::Memory(_) => Sink::Memory(BytesMut::new()),
            BlobStore::Disk(dir) => {
                // Same directory as the blobs so the final rename never
                // crosses devices.
                let path = dir.join(format!("upload-{id}"));
                let file = tokio::fs::File::create(&path).await?;
                Sink::File {
                    path,
                    file: BufWriter::with_capacity(IO_BUFFER, file),
                }
            }
        };
        Ok(Self {
            repo: repo.to_string(),
            size: 0,
            hasher: Hasher::new(Algorithm::Sha256),
            sink,
        })
    }

    async fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        match &mut self.sink {
            Sink::Memory(buf) => buf.extend_from_slice(chunk),
            Sink::File { file, .. } => file.write_all(chunk).await?,
        }
        self.hasher.update(chunk);
        self.size += chunk.len() as u64;
        Ok(())
    }

    /// Streams a request body into the upload, chunk by chunk.
    ///
    /// Bytes received before a client disconnect stay committed, so the
    /// client can query the offset and resume.
    pub(crate) async fn append(&mut self, body: Body) -> Result<u64, RegistryError> {
        let mut stream = body.into_data_stream();
        let mut written = 0u64;
        let mut result = Ok(());
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    if let Err(e) = self.write(&chunk).await {
                        result = Err(RegistryError::from(e));
                        break;
                    }
                    written += chunk.len() as u64;
                }
                Err(e) => {
                    result = Err(RegistryError::new(
                        axum::http::StatusCode::BAD_REQUEST,
                        "BLOB_UPLOAD_INVALID",
                        format!("reading request body: {e}"),
                    ));
                    break;
                }
            }
        }
        if let Sink::File { file, .. } = &mut self.sink {
            file.flush().await?;
        }
        result.map(|()| written)
    }

    /// Verifies the contents against `expected` and moves them into `store`.
    pub(crate) async fn commit(
        mut self,
        store: &BlobStore,
        expected: &Digest,
    ) -> Result<(), RegistryError> {
        let actual = match expected.algorithm() {
            Algorithm::Sha256 => self.hasher.clone().finalize(),
            alg => match &mut self.sink {
                Sink::Memory(buf) => Digest::compute(alg, buf),
                Sink::File { path, .. } => hash_file(path, alg).await?,
            },
        };
        if &actual != expected {
            return Err(RegistryError::digest_invalid(format!(
                "digest does not match contents: expected {expected}, computed {actual}"
            ))
            .with_detail(serde_json::json!({ "expected": expected.to_string(), "actual": actual.to_string() })));
        }
        match (&mut self.sink, store) {
            (Sink::Memory(buf), BlobStore::Memory(map)) => {
                let data = std::mem::take(buf).freeze();
                map.write()
                    .expect("blob lock")
                    .insert(expected.to_string(), data);
            }
            (Sink::File { path, file }, BlobStore::Disk(dir)) => {
                file.flush().await?;
                file.get_ref().sync_all().await?;
                let dest = BlobStore::blob_path(dir, expected);
                if let Some(parent) = dest.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::rename(&*path, dest).await?;
            }
            _ => unreachable!("upload sink always matches its store"),
        }
        Ok(())
    }
}

async fn hash_file(path: &Path, alg: Algorithm) -> io::Result<Digest> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Hasher::new(alg);
    let mut buf = vec![0u8; IO_BUFFER];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// An upload session. `None` once committed or cancelled.
pub(crate) type Session = Arc<tokio::sync::Mutex<Option<Upload>>>;

/// Open upload sessions keyed by id.
#[derive(Default)]
pub(crate) struct Uploads {
    sessions: Mutex<HashMap<String, Session>>,
}

impl Uploads {
    pub(crate) fn insert(&self, id: String, upload: Upload) {
        self.sessions
            .lock()
            .expect("uploads lock")
            .insert(id, Arc::new(tokio::sync::Mutex::new(Some(upload))));
    }

    pub(crate) fn get(&self, id: &str) -> Option<Session> {
        self.sessions.lock().expect("uploads lock").get(id).cloned()
    }

    pub(crate) fn remove(&self, id: &str) {
        self.sessions.lock().expect("uploads lock").remove(id);
    }
}

/// Returns a random 128-bit hex id.
pub(crate) fn random_id() -> String {
    format!("{:016x}{:016x}", random_u64(), random_u64())
}

/// A random number from std's per-process randomly keyed SipHash.
pub(crate) fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher as _};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default(),
    );
    h.finish()
}
