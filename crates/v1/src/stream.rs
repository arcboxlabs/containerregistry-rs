//! Single-pass streaming layers (go-containerregistry `pkg/v1/stream`).
//!
//! A [`StreamLayer`] wraps an uncompressed byte stream that can be read only
//! once. Its digest, diff ID and size are computed while it is consumed
//! (typically by an upload), so a layer of unknown size can be pushed
//! without staging it on disk or in memory.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, ReadBuf};

use containerregistry_image::{Algorithm, Digest, Hasher, MediaType};

use crate::compression::{self, Compression};
use crate::layer::{BoxReader, Layer};
use crate::{Error, Result};

/// An [`AsyncRead`] adapter that hashes and counts bytes and reports the
/// result once the inner reader hits EOF.
pub struct HashingReader<R> {
    inner: R,
    hasher: Option<Hasher>,
    count: u64,
    on_eof: Option<Box<dyn FnOnce(Digest, u64) + Send>>,
}

impl<R> HashingReader<R> {
    /// Wraps `inner`; `on_eof` receives the digest and byte count.
    pub fn new(
        inner: R,
        algorithm: Algorithm,
        on_eof: impl FnOnce(Digest, u64) + Send + 'static,
    ) -> Self {
        Self {
            inner,
            hasher: Some(Hasher::new(algorithm)),
            count: 0,
            on_eof: Some(Box::new(on_eof)),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for HashingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let new = &buf.filled()[before..];
                if new.is_empty() {
                    if buf.remaining() > 0
                        && let (Some(h), Some(f)) = (this.hasher.take(), this.on_eof.take())
                    {
                        f(h.finalize(), this.count);
                    }
                } else if let Some(h) = &mut this.hasher {
                    h.update(new);
                    this.count += new.len() as u64;
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Computed {
    digest: Option<Digest>,
    diff_id: Option<Digest>,
    size: Option<u64>,
}

/// A layer computed from a single-use uncompressed stream.
pub struct StreamLayer {
    reader: Mutex<Option<BoxReader>>,
    computed: Arc<Mutex<Computed>>,
    media_type: MediaType,
    compression: Compression,
    level: Option<i32>,
}

impl StreamLayer {
    /// Creates a gzip-compressed OCI layer from an uncompressed stream.
    pub fn new(reader: BoxReader) -> Self {
        Self {
            reader: Mutex::new(Some(reader)),
            computed: Arc::new(Mutex::new(Computed::default())),
            media_type: MediaType::OciLayerGzip,
            compression: Compression::Gzip,
            level: None,
        }
    }

    /// Sets the media type (go `stream.WithMediaType`).
    pub fn with_media_type(mut self, media_type: MediaType) -> Self {
        if let Some(c) = Compression::from_media_type(&media_type) {
            self.compression = c;
        }
        self.media_type = media_type;
        self
    }

    /// Sets the compression level (go `stream.WithCompressionLevel`).
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    /// Returns true once the stream has been fully read.
    pub fn is_consumed(&self) -> bool {
        self.computed.lock().expect("lock").size.is_some()
    }

    fn get<T>(&self, f: impl Fn(&Computed) -> Option<T>, what: &str) -> Result<T> {
        f(&self.computed.lock().expect("lock")).ok_or_else(|| Error::NotComputed(what.to_string()))
    }
}

#[async_trait]
impl Layer for StreamLayer {
    async fn digest(&self) -> Result<Digest> {
        self.get(|c| c.digest.clone(), "digest")
    }

    async fn diff_id(&self) -> Result<Digest> {
        self.get(|c| c.diff_id.clone(), "diff id")
    }

    /// Returns the compressed stream; may only be called once.
    async fn compressed(&self) -> Result<BoxReader> {
        let reader = self
            .reader
            .lock()
            .expect("lock")
            .take()
            .ok_or(Error::Consumed)?;
        let c1 = self.computed.clone();
        let uncompressed = HashingReader::new(reader, Algorithm::Sha256, move |d, _| {
            c1.lock().expect("lock").diff_id = Some(d);
        });
        let compressed =
            compression::compress(Box::pin(uncompressed), self.compression, self.level);
        let c2 = self.computed.clone();
        Ok(Box::pin(HashingReader::new(
            compressed,
            Algorithm::Sha256,
            move |d, n| {
                let mut g = c2.lock().expect("lock");
                g.digest = Some(d);
                g.size = Some(n);
            },
        )))
    }

    async fn uncompressed(&self) -> Result<BoxReader> {
        Err(Error::Unsupported(
            "stream layers can only be read compressed".to_string(),
        ))
    }

    async fn size(&self) -> Result<u64> {
        self.get(|c| c.size, "size")
    }

    async fn media_type(&self) -> Result<MediaType> {
        Ok(self.media_type.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::read_all;
    use std::io::Cursor;

    #[tokio::test]
    async fn computes_after_consumption() {
        let data = b"hello stream layer".repeat(1000);
        let layer = StreamLayer::new(Box::pin(Cursor::new(data.clone())));
        assert!(matches!(layer.digest().await, Err(Error::NotComputed(_))));
        let compressed = read_all(layer.compressed().await.unwrap()).await.unwrap();
        assert!(matches!(layer.compressed().await, Err(Error::Consumed)));
        assert_eq!(layer.digest().await.unwrap(), Digest::sha256(&compressed));
        assert_eq!(layer.size().await.unwrap(), compressed.len() as u64);
        assert_eq!(layer.diff_id().await.unwrap(), Digest::sha256(&data));
        let round = read_all(compression::decompress(
            Box::pin(Cursor::new(compressed)),
            Compression::Gzip,
        ))
        .await
        .unwrap();
        assert_eq!(round, data);
    }
}
