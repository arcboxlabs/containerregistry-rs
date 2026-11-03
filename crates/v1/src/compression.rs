//! Compression detection and streaming (de)compression
//! (go-containerregistry `pkg/compression`, `internal/gzip`, `internal/zstd`).

use std::io::Cursor;

use async_compression::Level;
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder, ZstdDecoder, ZstdEncoder};
use tokio::io::{AsyncReadExt, BufReader};

use containerregistry_image::MediaType;

use crate::Result;
use crate::layer::BoxReader;

const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];

/// A layer compression algorithm.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Compression {
    /// Uncompressed.
    None,
    /// gzip (the default for image layers).
    #[default]
    Gzip,
    /// zstd.
    Zstd,
}

impl Compression {
    /// The compression implied by a layer media type, if it names one.
    ///
    /// Custom (artifact) media types return `None` here: their bytes are
    /// opaque and must not be decompressed.
    pub fn from_media_type(media_type: &MediaType) -> Option<Compression> {
        match media_type {
            MediaType::OciLayerGzip
            | MediaType::OciLayerNondistributableGzip
            | MediaType::DockerLayerGzip
            | MediaType::DockerForeignLayer => Some(Compression::Gzip),
            MediaType::OciLayerZstd | MediaType::OciLayerNondistributableZstd => {
                Some(Compression::Zstd)
            }
            MediaType::OciLayer | MediaType::OciLayerNondistributable => Some(Compression::None),
            MediaType::Other(s) if s.ends_with("+gzip") || s.ends_with(".gzip") => {
                Some(Compression::Gzip)
            }
            MediaType::Other(s) if s.ends_with("+zstd") => Some(Compression::Zstd),
            _ => None,
        }
    }

    /// Detects the compression from leading magic bytes.
    pub fn from_magic(prefix: &[u8]) -> Compression {
        if prefix.starts_with(GZIP_MAGIC) {
            Compression::Gzip
        } else if prefix.starts_with(ZSTD_MAGIC) {
            Compression::Zstd
        } else {
            Compression::None
        }
    }

    /// The OCI layer media type for this compression.
    pub fn oci_layer_media_type(self) -> MediaType {
        match self {
            Compression::None => MediaType::OciLayer,
            Compression::Gzip => MediaType::OciLayerGzip,
            Compression::Zstd => MediaType::OciLayerZstd,
        }
    }
}

impl std::str::FromStr for Compression {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip),
            "zstd" => Ok(Compression::Zstd),
            other => Err(crate::Error::Unsupported(format!("compression {}", other))),
        }
    }
}

/// Peeks at the start of `reader` and returns the detected compression and
/// a reader that still yields every byte.
pub async fn detect(mut reader: BoxReader) -> Result<(Compression, BoxReader)> {
    let mut prefix = Vec::with_capacity(4);
    while prefix.len() < 4 {
        let mut b = [0u8; 4];
        let n = reader.read(&mut b[..4 - prefix.len()]).await?;
        if n == 0 {
            break;
        }
        prefix.extend_from_slice(&b[..n]);
    }
    let c = Compression::from_magic(&prefix);
    Ok((c, Box::pin(Cursor::new(prefix).chain(reader))))
}

/// Wraps `reader` with a streaming decompressor.
pub fn decompress(reader: BoxReader, compression: Compression) -> BoxReader {
    match compression {
        Compression::None => reader,
        Compression::Gzip => {
            let mut d = GzipDecoder::new(BufReader::with_capacity(256 * 1024, reader));
            d.multiple_members(true);
            Box::pin(d)
        }
        Compression::Zstd => {
            let mut d = ZstdDecoder::new(BufReader::with_capacity(256 * 1024, reader));
            d.multiple_members(true);
            Box::pin(d)
        }
    }
}

/// Decompresses `reader` if it starts with a gzip or zstd header.
pub async fn maybe_decompress(reader: BoxReader) -> Result<BoxReader> {
    let (c, reader) = detect(reader).await?;
    Ok(decompress(reader, c))
}

/// Wraps `reader` with a streaming compressor. `level` uses the algorithm's
/// scale (gzip 0-9, zstd 1-22); `None` uses the default.
pub fn compress(reader: BoxReader, compression: Compression, level: Option<i32>) -> BoxReader {
    let level = level.map(Level::Precise).unwrap_or(Level::Default);
    match compression {
        Compression::None => reader,
        Compression::Gzip => Box::pin(GzipEncoder::with_quality(
            BufReader::with_capacity(256 * 1024, reader),
            level,
        )),
        Compression::Zstd => Box::pin(ZstdEncoder::with_quality(
            BufReader::with_capacity(256 * 1024, reader),
            level,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::read_all;

    #[tokio::test]
    async fn roundtrip_and_detect() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        for c in [Compression::None, Compression::Gzip, Compression::Zstd] {
            let compressed = read_all(compress(Box::pin(Cursor::new(data.clone())), c, None))
                .await
                .unwrap();
            let (detected, r) = detect(Box::pin(Cursor::new(compressed))).await.unwrap();
            assert_eq!(detected, c);
            assert_eq!(read_all(decompress(r, detected)).await.unwrap(), data);
        }
    }

    #[test]
    fn media_type_mapping() {
        assert_eq!(
            Compression::from_media_type(&MediaType::DockerLayerGzip),
            Some(Compression::Gzip)
        );
        assert_eq!(
            Compression::from_media_type(&MediaType::Other(
                "application/vnd.cirruslabs.tart.disk.v2".into()
            )),
            None
        );
    }
}
