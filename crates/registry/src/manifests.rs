//! Manifest operations: get/head/put/delete/tag, raw and typed.

use std::time::Instant;

use reqwest::StatusCode;
use reqwest::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use serde::Deserialize;
use tracing::instrument;

use containerregistry_image::{Descriptor, Digest, ImageIndex, Manifest, MediaType};

use crate::client::{Client, Target, delete_scope};
use crate::metrics::Operation;
use crate::reference::Reference;
use crate::{Error, Result};

/// Maximum manifest size accepted from a registry (matches
/// go-containerregistry's limit).
const MAX_MANIFEST_SIZE: usize = 100 * 1024 * 1024;

/// A manifest or image index returned from a registry.
///
/// Since a tag can point to either a single manifest or a multi-platform
/// image index, this enum represents both possibilities.
#[derive(Clone, Debug)]
pub enum ManifestOrIndex {
    /// A single manifest (OCI or Docker).
    Manifest(Box<Manifest>),
    /// An image index / manifest list (OCI or Docker).
    Index(Box<ImageIndex>),
}

impl ManifestOrIndex {
    /// Parses bytes as either a manifest or index, auto-detecting the format.
    ///
    /// This validates media types strictly; see [`ManifestOrIndex::from_raw`]
    /// for the lenient variant used for registry responses.
    pub fn from_bytes(data: &[u8]) -> crate::Result<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MediaTypeProbe {
            media_type: Option<String>,
        }

        let probe: MediaTypeProbe = serde_json::from_slice(data)?;

        match probe.media_type.as_deref() {
            Some("application/vnd.oci.image.index.v1+json")
            | Some("application/vnd.docker.distribution.manifest.list.v2+json") => Ok(
                ManifestOrIndex::Index(Box::new(ImageIndex::from_bytes(data)?)),
            ),
            Some("application/vnd.oci.image.manifest.v1+json")
            | Some("application/vnd.docker.distribution.manifest.v2+json") => Ok(
                ManifestOrIndex::Manifest(Box::new(Manifest::from_bytes(data)?)),
            ),
            _ => {
                // Try index first (has "manifests" array), then manifest
                if let Ok(index) = ImageIndex::from_bytes(data) {
                    Ok(ManifestOrIndex::Index(Box::new(index)))
                } else {
                    Ok(ManifestOrIndex::Manifest(Box::new(Manifest::from_bytes(
                        data,
                    )?)))
                }
            }
        }
    }

    /// Parses registry content without media-type validation.
    ///
    /// The kind is chosen from the document's `mediaType`, then
    /// `content_type` (the HTTP header), then the presence of a `manifests`
    /// array. Custom artifact config/layer types are accepted.
    pub fn from_raw(data: &[u8], content_type: Option<&MediaType>) -> crate::Result<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Probe {
            media_type: Option<MediaType>,
            manifests: Option<serde_json::Value>,
        }
        let probe: Probe = serde_json::from_slice(data)?;
        let mt = probe.media_type.as_ref().or(content_type);
        let is_index = match mt {
            Some(mt) if mt.is_index() => true,
            Some(mt) if mt.is_manifest() => false,
            _ => probe.manifests.is_some(),
        };
        if is_index {
            Ok(ManifestOrIndex::Index(Box::new(
                ImageIndex::from_bytes_unchecked(data, mt)?,
            )))
        } else {
            Ok(ManifestOrIndex::Manifest(Box::new(
                Manifest::from_bytes_unchecked(data, mt)?,
            )))
        }
    }

    /// Returns true if this is a manifest.
    pub fn is_manifest(&self) -> bool {
        matches!(self, ManifestOrIndex::Manifest(_))
    }

    /// Returns true if this is an index.
    pub fn is_index(&self) -> bool {
        matches!(self, ManifestOrIndex::Index(_))
    }

    /// Returns the manifest if this is a manifest, or None.
    pub fn as_manifest(&self) -> Option<&Manifest> {
        match self {
            ManifestOrIndex::Manifest(m) => Some(m),
            ManifestOrIndex::Index(_) => None,
        }
    }

    /// Returns the index if this is an index, or None.
    pub fn as_index(&self) -> Option<&ImageIndex> {
        match self {
            ManifestOrIndex::Manifest(_) => None,
            ManifestOrIndex::Index(i) => Some(i),
        }
    }

    /// Converts into the manifest if this is a manifest.
    pub fn into_manifest(self) -> Option<Manifest> {
        match self {
            ManifestOrIndex::Manifest(m) => Some(*m),
            ManifestOrIndex::Index(_) => None,
        }
    }

    /// Converts into the index if this is an index.
    pub fn into_index(self) -> Option<ImageIndex> {
        match self {
            ManifestOrIndex::Manifest(_) => None,
            ManifestOrIndex::Index(i) => Some(*i),
        }
    }
}

/// A manifest exactly as served by the registry.
///
/// Keeping the original bytes matters: re-serializing a parsed manifest can
/// change whitespace or field order and therefore its digest, which breaks
/// copies of indexes and signatures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawManifest {
    /// The manifest bytes.
    pub data: Vec<u8>,
    /// The media type (from `mediaType` or the `Content-Type` header).
    pub media_type: MediaType,
    /// The content digest.
    pub digest: Digest,
}

impl RawManifest {
    /// Creates a raw manifest, computing its SHA-256 digest.
    pub fn new(data: Vec<u8>, media_type: MediaType) -> Self {
        let digest = Digest::sha256(&data);
        Self {
            data,
            media_type,
            digest,
        }
    }

    /// Parses the manifest (leniently).
    pub fn parse(&self) -> Result<ManifestOrIndex> {
        ManifestOrIndex::from_raw(&self.data, Some(&self.media_type))
    }

    /// Returns true if this is an index / manifest list.
    pub fn is_index(&self) -> bool {
        self.media_type.is_index()
    }

    /// Returns a descriptor for this manifest.
    pub fn descriptor(&self) -> Descriptor {
        Descriptor::new(
            self.media_type.clone(),
            self.digest.clone(),
            self.data.len() as u64,
        )
    }
}

/// Response of a manifest PUT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutManifestResponse {
    /// SHA-256 digest of the uploaded manifest.
    pub digest: Digest,
    /// Value of the `OCI-Subject` header: set when the registry indexed the
    /// manifest's `subject` for the referrers API.
    pub oci_subject: Option<Digest>,
}

/// Default `Accept` media types for manifest requests.
pub const DEFAULT_MANIFEST_ACCEPT: &[&str] = &[
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v1+prettyjws",
    "application/vnd.docker.distribution.manifest.v1+json",
];

fn accept_header(accept: &[MediaType]) -> HeaderValue {
    let joined = if accept.is_empty() {
        DEFAULT_MANIFEST_ACCEPT.join(", ")
    } else {
        accept
            .iter()
            .map(|m| m.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    HeaderValue::from_str(&joined).unwrap_or_else(|_| HeaderValue::from_static("*/*"))
}

fn content_media_type(headers: &reqwest::header::HeaderMap) -> Option<MediaType> {
    let ct = headers.get(CONTENT_TYPE)?.to_str().ok()?;
    let mt = ct.split(';').next().unwrap_or(ct).trim();
    if mt.is_empty() { None } else { mt.parse().ok() }
}

fn manifest_path(reference: &Reference) -> String {
    format!(
        "/{}/manifests/{}",
        reference.repository(),
        reference.reference()
    )
}

impl Client {
    /// Gets a manifest or index from the registry.
    ///
    /// Returns the manifest/index and its content digest. Note that multi-architecture
    /// tags may return an ImageIndex rather than a Manifest.
    #[instrument(skip(self), level = "debug", fields(reference = %reference))]
    pub async fn get_manifest(&self, reference: &Reference) -> Result<(ManifestOrIndex, Digest)> {
        let raw = self.get_manifest_raw(reference).await?;
        let parsed = raw.parse()?;
        Ok((parsed, raw.digest))
    }

    /// Gets a manifest exactly as stored, with its media type and digest.
    pub async fn get_manifest_raw(&self, reference: &Reference) -> Result<RawManifest> {
        self.get_manifest_raw_with_accept(reference, &[]).await
    }

    /// Like [`Client::get_manifest_raw`] with a custom `Accept` list (empty
    /// means the default list of OCI and Docker manifest types).
    pub async fn get_manifest_raw_with_accept(
        &self,
        reference: &Reference,
        accept: &[MediaType],
    ) -> Result<RawManifest> {
        let start = Instant::now();
        let result = self.get_manifest_raw_inner(reference, accept).await;
        let bytes = result.as_ref().map(|r| r.data.len() as u64).unwrap_or(0);
        self.observe(Operation::GetManifest, start, &result, bytes);
        result
    }

    async fn get_manifest_raw_inner(
        &self,
        reference: &Reference,
        accept: &[MediaType],
    ) -> Result<RawManifest> {
        let url = self.url(reference.registry(), &manifest_path(reference));
        let accept = accept_header(accept);
        let response = self
            .send(
                &Target::pull(reference),
                Some(Operation::GetManifest),
                true,
                || self.http.get(&url).header(ACCEPT, accept.clone()),
            )
            .await?;

        if response.status() != StatusCode::OK {
            return Err(self
                .error_from_response(response, "GET manifest", &reference.to_string())
                .await);
        }

        let status = response.status().as_u16();
        let header_digest = Self::parse_optional_digest_header(response.headers(), status)?;
        let content_type = content_media_type(response.headers());
        if let Some(len) = response.content_length()
            && len as usize > MAX_MANIFEST_SIZE
        {
            return Err(Error::UnexpectedStatus {
                status,
                message: format!("manifest too large: {} bytes", len),
            });
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| self.map_transport_error(e))?;
        let computed_sha256 = Digest::sha256(&body);

        if let Some(ref header) = header_digest {
            let computed = Self::digest_for_algorithm(header.algorithm(), &body, &computed_sha256);
            if header != &computed {
                return Err(Error::DigestMismatch {
                    expected: header.to_string(),
                    actual: computed.to_string(),
                });
            }
        }

        if let Some(expected) = reference.digest() {
            let computed =
                Self::digest_for_algorithm(expected.algorithm(), &body, &computed_sha256);
            if &computed != expected {
                return Err(Error::DigestMismatch {
                    expected: expected.to_string(),
                    actual: computed.to_string(),
                });
            }
        }

        let digest = if let Some(header) = header_digest {
            header
        } else if let Some(expected) = reference.digest() {
            expected.clone()
        } else {
            computed_sha256
        };

        let media_type = body_media_type(&body)
            .or(content_type)
            .unwrap_or(MediaType::OciManifest);

        Ok(RawManifest {
            data: body.to_vec(),
            media_type,
            digest,
        })
    }

    /// Gets a manifest's metadata without downloading the full body.
    ///
    /// Returns the content type, digest, and size.
    #[instrument(skip(self), level = "debug", fields(reference = %reference))]
    pub async fn head_manifest(&self, reference: &Reference) -> Result<(MediaType, Digest, u64)> {
        let d = self.head_manifest_descriptor(reference).await?;
        Ok((d.media_type, d.digest, d.size))
    }

    /// Gets a manifest descriptor via `HEAD` (go-containerregistry `remote.Head`).
    pub async fn head_manifest_descriptor(&self, reference: &Reference) -> Result<Descriptor> {
        let start = Instant::now();
        let result = async {
            let url = self.url(reference.registry(), &manifest_path(reference));
            let accept = accept_header(&[]);
            let resp = self
                .send(
                    &Target::pull(reference),
                    Some(Operation::HeadManifest),
                    true,
                    || self.http.head(&url).header(ACCEPT, accept.clone()),
                )
                .await?;
            if resp.status() != StatusCode::OK {
                return Err(self
                    .error_from_response(resp, "HEAD manifest", &reference.to_string())
                    .await);
            }
            let status = resp.status().as_u16();
            let media_type = content_media_type(resp.headers()).unwrap_or(MediaType::OciManifest);
            let digest =
                Self::parse_optional_digest_header(resp.headers(), status)?.ok_or_else(|| {
                    Error::UnexpectedStatus {
                        status,
                        message: "missing docker-content-digest header".to_string(),
                    }
                })?;
            let size = resp
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            if let Some(expected) = reference.digest()
                && expected != &digest
            {
                return Err(Error::DigestMismatch {
                    expected: expected.to_string(),
                    actual: digest.to_string(),
                });
            }
            Ok(Descriptor::new(media_type, digest, size))
        }
        .await;
        self.observe(Operation::HeadManifest, start, &result, 0);
        result
    }

    /// Puts a manifest to the registry.
    ///
    /// Returns the digest of the uploaded manifest. The manifest is
    /// serialized canonically; use [`Client::put_manifest_raw`] to upload
    /// exact bytes.
    #[instrument(skip(self, manifest), level = "debug", fields(reference = %reference))]
    pub async fn put_manifest(&self, reference: &Reference, manifest: &Manifest) -> Result<Digest> {
        let body = manifest.to_bytes()?;
        self.put_manifest_raw(reference, &body, &manifest.media_type())
            .await
            .map(|r| r.digest)
    }

    /// Puts an image index (manifest list) to the registry.
    ///
    /// Returns the digest of the uploaded index.
    #[instrument(skip(self, index), level = "debug", fields(reference = %reference))]
    pub async fn put_index(&self, reference: &Reference, index: &ImageIndex) -> Result<Digest> {
        let body = index.to_bytes()?;
        self.put_manifest_raw(reference, &body, &index.media_type())
            .await
            .map(|r| r.digest)
    }

    /// Uploads manifest bytes verbatim with the given media type.
    pub async fn put_manifest_raw(
        &self,
        reference: &Reference,
        data: &[u8],
        media_type: &MediaType,
    ) -> Result<PutManifestResponse> {
        let start = Instant::now();
        let result = self
            .put_manifest_raw_inner(reference, data, media_type)
            .await;
        match &result {
            Ok(_) => self.record_upload(Operation::PutManifest, start, data.len() as u64),
            Err(_) => self.record_failure(Operation::PutManifest, start),
        }
        result
    }

    async fn put_manifest_raw_inner(
        &self,
        reference: &Reference,
        data: &[u8],
        media_type: &MediaType,
    ) -> Result<PutManifestResponse> {
        let computed_sha256 = Digest::sha256(data);

        if let Some(expected_digest) = reference.digest() {
            let computed =
                Self::digest_for_algorithm(expected_digest.algorithm(), data, &computed_sha256);
            if &computed != expected_digest {
                return Err(Error::DigestMismatch {
                    expected: expected_digest.to_string(),
                    actual: computed.to_string(),
                });
            }
        }

        let url = self.url(reference.registry(), &manifest_path(reference));
        let body = bytes::Bytes::copy_from_slice(data);
        let content_type = media_type.as_str().to_string();
        let response = self
            .send(
                &Target::push(reference),
                Some(Operation::PutManifest),
                true,
                || {
                    self.http
                        .put(&url)
                        .header(CONTENT_TYPE, content_type.as_str())
                        .body(body.clone())
                },
            )
            .await?;

        match response.status() {
            StatusCode::CREATED | StatusCode::OK | StatusCode::ACCEPTED => {
                let status = response.status().as_u16();
                if let Some(header_digest) =
                    Self::parse_optional_digest_header(response.headers(), status)?
                {
                    let computed = Self::digest_for_algorithm(
                        header_digest.algorithm(),
                        data,
                        &computed_sha256,
                    );
                    if header_digest != computed {
                        return Err(Error::DigestMismatch {
                            expected: computed.to_string(),
                            actual: header_digest.to_string(),
                        });
                    }
                }
                let oci_subject = response
                    .headers()
                    .get("oci-subject")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse().ok());
                Ok(PutManifestResponse {
                    digest: computed_sha256,
                    oci_subject,
                })
            }
            _ => Err(self
                .error_from_response(response, "PUT manifest", &reference.to_string())
                .await),
        }
    }

    /// Deletes a manifest by tag or digest reference.
    pub async fn delete_manifest(&self, reference: &Reference) -> Result<()> {
        let url = self.url(reference.registry(), &manifest_path(reference));
        let target = Target::push(reference).with_scope(delete_scope(reference.repository()));
        let response = self
            .send(&target, None, true, || self.http.delete(&url))
            .await?;

        match response.status() {
            StatusCode::ACCEPTED | StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            _ => Err(self
                .error_from_response(response, "DELETE manifest", &reference.to_string())
                .await),
        }
    }

    /// Points `tag` (in the same repository) at the manifest referenced by
    /// `reference`, copying the manifest bytes verbatim (`crane tag`).
    pub async fn tag(&self, reference: &Reference, tag: &str) -> Result<Digest> {
        let raw = self.get_manifest_raw(reference).await?;
        let dst = reference.clone().with_new_tag(tag);
        self.put_manifest_raw(&dst, &raw.data, &raw.media_type)
            .await
            .map(|r| r.digest)
    }
}

fn body_media_type(body: &[u8]) -> Option<MediaType> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Probe {
        media_type: Option<MediaType>,
    }
    serde_json::from_slice::<Probe>(body).ok()?.media_type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_raw_accepts_artifact_manifest() {
        // A Tart-style VM image: OCI manifest with custom config/layer types.
        let json = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
            "config":{"mediaType":"application/vnd.cirruslabs.tart.config.v1","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},
            "layers":[{"mediaType":"application/vnd.cirruslabs.tart.disk.v2","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2,
                "annotations":{"org.cirruslabs.tart.uncompressed-size":"536870912"}}]}"#;
        let parsed = ManifestOrIndex::from_raw(json, None).unwrap();
        let m = parsed.as_manifest().unwrap();
        assert_eq!(
            m.layers()[0].media_type.as_str(),
            "application/vnd.cirruslabs.tart.disk.v2"
        );
    }

    #[test]
    fn from_raw_uses_content_type_without_media_type_field() {
        let json = br#"{"schemaVersion":2,"manifests":[]}"#;
        assert!(ManifestOrIndex::from_raw(json, None).unwrap().is_index());
        let json = br#"{"schemaVersion":2,"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","size":2},"layers":[]}"#;
        let parsed = ManifestOrIndex::from_raw(json, Some(&MediaType::DockerManifest)).unwrap();
        assert!(matches!(parsed.as_manifest(), Some(Manifest::Docker(_))));
    }
}
