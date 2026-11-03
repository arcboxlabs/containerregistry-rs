//! In-memory layers (go-containerregistry `pkg/v1/static`).

use std::io::Cursor;

use async_trait::async_trait;
use bytes::Bytes;

use containerregistry_image::{Descriptor, Digest, MediaType};

use crate::Result;
use crate::layer::{BoxReader, Layer};

/// A layer backed by bytes in memory.
///
/// By default the bytes are opaque: compressed and uncompressed forms are
/// identical and `diff_id == digest`. [`StaticLayer::with_uncompressed`]
/// creates a layer with distinct compressed and uncompressed forms.
#[derive(Clone, Debug)]
pub struct StaticLayer {
    data: Bytes,
    uncompressed: Option<(Bytes, Digest)>,
    descriptor: Descriptor,
}

impl StaticLayer {
    /// Creates a layer from bytes with the given media type.
    pub fn new(data: impl Into<Bytes>, media_type: MediaType) -> Self {
        let data = data.into();
        let descriptor = Descriptor::from_bytes(media_type, &data);
        Self {
            data,
            uncompressed: None,
            descriptor,
        }
    }

    /// Creates a layer from bytes and a descriptor describing them
    /// (annotations etc. are preserved). The descriptor's digest is trusted.
    pub fn with_descriptor(data: impl Into<Bytes>, descriptor: Descriptor) -> Self {
        Self {
            data: data.into(),
            uncompressed: None,
            descriptor,
        }
    }

    /// Creates a layer from its compressed bytes and the matching
    /// uncompressed bytes.
    pub fn with_uncompressed(
        compressed: impl Into<Bytes>,
        uncompressed: impl Into<Bytes>,
        media_type: MediaType,
    ) -> Self {
        let data = compressed.into();
        let uncompressed = uncompressed.into();
        let diff_id = Digest::sha256(&uncompressed);
        let descriptor = Descriptor::from_bytes(media_type, &data);
        Self {
            data,
            uncompressed: Some((uncompressed, diff_id)),
            descriptor,
        }
    }

    /// The (compressed) bytes.
    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

#[async_trait]
impl Layer for StaticLayer {
    async fn digest(&self) -> Result<Digest> {
        Ok(self.descriptor.digest.clone())
    }
    async fn diff_id(&self) -> Result<Digest> {
        Ok(match &self.uncompressed {
            Some((_, d)) => d.clone(),
            None => self.descriptor.digest.clone(),
        })
    }
    async fn compressed(&self) -> Result<BoxReader> {
        Ok(Box::pin(Cursor::new(self.data.clone())))
    }
    async fn uncompressed(&self) -> Result<BoxReader> {
        let b = match &self.uncompressed {
            Some((b, _)) => b.clone(),
            None => self.data.clone(),
        };
        Ok(Box::pin(Cursor::new(b)))
    }
    async fn size(&self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }
    async fn media_type(&self) -> Result<MediaType> {
        Ok(self.descriptor.media_type.clone())
    }
    async fn descriptor(&self) -> Result<Descriptor> {
        let mut d = self.descriptor.clone();
        d.size = self.data.len() as u64;
        Ok(d)
    }
    async fn uncompressed_size(&self) -> Result<Option<u64>> {
        Ok(Some(match &self.uncompressed {
            Some((b, _)) => b.len() as u64,
            None => self.data.len() as u64,
        }))
    }
}
