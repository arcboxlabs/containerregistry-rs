//! OCI image index and Docker manifest list types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Descriptor, Digest, Error, MediaType, Result};

/// OCI image index schema version.
pub const OCI_INDEX_SCHEMA_VERSION: u32 = 2;

/// An OCI image index (multi-architecture manifest list).
///
/// An image index contains references to platform-specific manifests,
/// allowing a single image reference to support multiple architectures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OciIndex {
    /// Schema version (must be 2).
    pub schema_version: u32,

    /// Media type of the index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<MediaType>,

    /// Artifact type (OCI 1.1 extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,

    /// References to platform-specific manifests.
    pub manifests: Vec<Descriptor>,

    /// Subject descriptor for OCI referrers (OCI 1.1 extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Descriptor>,

    /// Optional annotations (uses BTreeMap for deterministic serialization).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Docker manifest list.
///
/// The Docker-specific multi-architecture manifest format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DockerManifestList {
    /// Schema version (must be 2).
    pub schema_version: u32,

    /// Media type of the manifest list (defaults to the Docker manifest list
    /// type when absent).
    #[serde(default = "docker_manifest_list_media_type")]
    pub media_type: MediaType,

    /// References to platform-specific manifests.
    pub manifests: Vec<Descriptor>,
}

fn docker_manifest_list_media_type() -> MediaType {
    MediaType::DockerManifestList
}

impl OciIndex {
    /// Creates a new OCI index with the given manifests.
    pub fn new(manifests: Vec<Descriptor>) -> Self {
        Self {
            schema_version: OCI_INDEX_SCHEMA_VERSION,
            media_type: Some(MediaType::OciIndex),
            artifact_type: None,
            manifests,
            subject: None,
            annotations: BTreeMap::new(),
        }
    }

    /// Adds an annotation to the index.
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Sets the artifact type (OCI 1.1 extension).
    pub fn with_artifact_type(mut self, artifact_type: MediaType) -> Self {
        self.artifact_type = Some(artifact_type);
        self
    }

    /// Sets the subject descriptor (OCI 1.1 extension).
    pub fn with_subject(mut self, subject: Descriptor) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Parses an OCI index from JSON bytes.
    ///
    /// This validates that the schemaVersion is 2 and the mediaType (if present)
    /// is an OCI index type.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let index: Self = serde_json::from_slice(data)?;
        index.validate()?;
        Ok(index)
    }

    /// Parses an OCI index from JSON bytes without validation.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(Error::from)
    }

    /// Validates the index structure.
    ///
    /// This checks:
    /// - schemaVersion is 2
    /// - mediaType is present and is OCI index type
    /// - manifest descriptors have valid manifest/index media types
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OCI_INDEX_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "invalid schemaVersion: expected {}, got {}",
                OCI_INDEX_SCHEMA_VERSION, self.schema_version
            )));
        }

        // mediaType is required for OCI indexes
        match &self.media_type {
            Some(MediaType::OciIndex) => {}
            Some(mt) => {
                return Err(Error::InvalidManifest(format!(
                    "invalid mediaType for OCI index: {}",
                    mt
                )));
            }
            None => {
                return Err(Error::InvalidManifest(
                    "missing required mediaType field".to_string(),
                ));
            }
        }

        // Validate manifest descriptor media types
        for (i, manifest) in self.manifests.iter().enumerate() {
            // Unknown (`Other`) types are allowed: the image spec requires
            // implementations to ignore descriptors they do not understand.
            if !manifest.media_type.is_manifest()
                && !manifest.media_type.is_index()
                && !matches!(manifest.media_type, MediaType::Other(_))
            {
                return Err(Error::InvalidManifest(format!(
                    "invalid manifests[{}] mediaType: expected manifest or index type, got {}",
                    i, manifest.media_type
                )));
            }
        }

        Ok(())
    }

    /// Serializes the index to canonical JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Error::from)
    }

    /// Computes the digest of this index.
    pub fn digest(&self) -> Result<Digest> {
        let bytes = self.to_bytes()?;
        Ok(Digest::sha256(&bytes))
    }

    /// Returns the size of the serialized index.
    pub fn size(&self) -> Result<u64> {
        let bytes = self.to_bytes()?;
        Ok(bytes.len() as u64)
    }

    /// Finds a manifest matching the given platform.
    ///
    /// Returns the first manifest that matches the architecture and OS.
    /// If variant is specified in the platform, it must also match.
    pub fn find_platform(
        &self,
        arch: &str,
        os: &str,
        variant: Option<&str>,
    ) -> Option<&Descriptor> {
        self.manifests.iter().find(|m| {
            if let Some(ref p) = m.platform {
                let arch_match = p.architecture == arch;
                let os_match = p.os == os;
                let variant_match = match (variant, &p.variant) {
                    (Some(v), Some(pv)) => v == pv,
                    (Some(_), None) => false,
                    (None, _) => true,
                };
                arch_match && os_match && variant_match
            } else {
                false
            }
        })
    }

    /// Converts this index to a Docker manifest list.
    ///
    /// This is a shallow conversion that only converts manifest descriptor
    /// media types. The actual manifests pointed to by the descriptors are
    /// not modified.
    ///
    /// # Panics
    ///
    /// Panics if any manifest descriptors have media types that cannot be
    /// converted to Docker format. Use `try_to_docker()` for fallible conversion.
    #[deprecated(
        since = "0.1.0",
        note = "Use try_to_docker() for safe conversion with error handling"
    )]
    pub fn to_docker(&self) -> DockerManifestList {
        self.try_to_docker()
            .expect("cannot convert index with incompatible manifest types to Docker format")
    }

    /// Attempts to convert this index to a Docker manifest list.
    ///
    /// This is a shallow conversion that only converts manifest descriptor
    /// media types. The actual manifests pointed to by the descriptors are
    /// not modified.
    ///
    /// Returns an error if any manifest descriptors have media types that
    /// cannot be converted to Docker format.
    pub fn try_to_docker(&self) -> Result<DockerManifestList> {
        // Check that all manifest descriptors have convertible media types
        for (i, m) in self.manifests.iter().enumerate() {
            if matches!(
                m.media_type,
                MediaType::OciIndex | MediaType::DockerManifestList
            ) {
                return Err(Error::InvalidManifest(format!(
                    "manifests[{}] has media type {} which cannot be converted to Docker manifest list",
                    i, m.media_type
                )));
            }
            if !m.media_type.is_docker_compatible() {
                return Err(Error::InvalidManifest(format!(
                    "manifests[{}] has media type {} which cannot be converted to Docker format",
                    i, m.media_type
                )));
            }
        }

        Ok(DockerManifestList {
            schema_version: self.schema_version,
            media_type: MediaType::DockerManifestList,
            manifests: self
                .manifests
                .iter()
                .map(|m| Descriptor {
                    media_type: m.media_type.to_docker(),
                    ..m.clone()
                })
                .collect(),
        })
    }
}

impl DockerManifestList {
    /// Creates a new Docker manifest list with the given manifests.
    pub fn new(manifests: Vec<Descriptor>) -> Self {
        Self {
            schema_version: OCI_INDEX_SCHEMA_VERSION,
            media_type: MediaType::DockerManifestList,
            manifests,
        }
    }

    /// Parses a Docker manifest list from JSON bytes.
    ///
    /// This validates that the schemaVersion is 2 and the mediaType is a Docker
    /// manifest list type.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let list: Self = serde_json::from_slice(data)?;
        list.validate()?;
        Ok(list)
    }

    /// Parses a Docker manifest list from JSON bytes without validation.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(Error::from)
    }

    /// Validates the manifest list structure.
    ///
    /// This checks:
    /// - schemaVersion is 2
    /// - mediaType is Docker manifest list type
    /// - manifest descriptors have valid manifest media types
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OCI_INDEX_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "invalid schemaVersion: expected {}, got {}",
                OCI_INDEX_SCHEMA_VERSION, self.schema_version
            )));
        }

        if !matches!(self.media_type, MediaType::DockerManifestList) {
            return Err(Error::InvalidManifest(format!(
                "invalid mediaType for Docker manifest list: {}",
                self.media_type
            )));
        }

        // Validate manifest descriptor media types (Docker manifest only)
        for (i, manifest) in self.manifests.iter().enumerate() {
            if !matches!(manifest.media_type, MediaType::DockerManifest) {
                return Err(Error::InvalidManifest(format!(
                    "invalid manifests[{}] mediaType for Docker manifest list: expected Docker manifest, got {}",
                    i, manifest.media_type
                )));
            }
        }

        Ok(())
    }

    /// Serializes the manifest list to canonical JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Error::from)
    }

    /// Computes the digest of this manifest list.
    pub fn digest(&self) -> Result<Digest> {
        let bytes = self.to_bytes()?;
        Ok(Digest::sha256(&bytes))
    }

    /// Returns the size of the serialized manifest list.
    pub fn size(&self) -> Result<u64> {
        let bytes = self.to_bytes()?;
        Ok(bytes.len() as u64)
    }

    /// Finds a manifest matching the given platform.
    pub fn find_platform(
        &self,
        arch: &str,
        os: &str,
        variant: Option<&str>,
    ) -> Option<&Descriptor> {
        self.manifests.iter().find(|m| {
            if let Some(ref p) = m.platform {
                let arch_match = p.architecture == arch;
                let os_match = p.os == os;
                let variant_match = match (variant, &p.variant) {
                    (Some(v), Some(pv)) => v == pv,
                    (Some(_), None) => false,
                    (None, _) => true,
                };
                arch_match && os_match && variant_match
            } else {
                false
            }
        })
    }

    /// Converts this manifest list to an OCI index.
    pub fn to_oci(&self) -> OciIndex {
        OciIndex {
            schema_version: self.schema_version,
            media_type: Some(MediaType::OciIndex),
            artifact_type: None,
            manifests: self
                .manifests
                .iter()
                .map(|m| Descriptor {
                    media_type: m.media_type.to_oci(),
                    ..m.clone()
                })
                .collect(),
            subject: None,
            annotations: BTreeMap::new(),
        }
    }
}

/// A unified index type that can represent either OCI or Docker formats.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Boxing would hurt API ergonomics
pub enum ImageIndex {
    Oci(OciIndex),
    Docker(DockerManifestList),
}

impl ImageIndex {
    /// Parses an image index from JSON bytes, auto-detecting the format.
    ///
    /// Detection is based on the mediaType field:
    /// - Docker manifest list: `application/vnd.docker.distribution.manifest.list.v2+json`
    /// - OCI index: `application/vnd.oci.image.index.v1+json`
    ///
    /// Returns an error if mediaType is missing or unrecognized.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(default, rename = "mediaType")]
            media_type: Option<String>,
        }

        let probe: Probe = serde_json::from_slice(data)?;

        match probe.media_type.as_deref() {
            Some("application/vnd.docker.distribution.manifest.list.v2+json") => {
                Ok(ImageIndex::Docker(DockerManifestList::from_bytes(data)?))
            }
            Some("application/vnd.oci.image.index.v1+json") => {
                Ok(ImageIndex::Oci(OciIndex::from_bytes(data)?))
            }
            Some(other) => Err(Error::UnsupportedMediaType(other.to_string())),
            None => Err(Error::InvalidManifest(
                "missing mediaType field; cannot auto-detect index format".to_string(),
            )),
        }
    }

    /// Parses an index without validating media types.
    ///
    /// The format is chosen from the document's `mediaType` field, falling
    /// back to `media_type` (typically the HTTP `Content-Type`) and finally to
    /// OCI.
    pub fn from_bytes_unchecked(data: &[u8], media_type: Option<&MediaType>) -> Result<Self> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(default, rename = "mediaType")]
            media_type: Option<MediaType>,
        }
        let probe: Probe = serde_json::from_slice(data)?;
        match probe.media_type.as_ref().or(media_type) {
            Some(MediaType::DockerManifestList) => Ok(ImageIndex::Docker(
                DockerManifestList::from_bytes_unchecked(data)?,
            )),
            _ => Ok(ImageIndex::Oci(OciIndex::from_bytes_unchecked(data)?)),
        }
    }

    /// Returns mutable access to the manifest descriptors.
    pub fn manifests_mut(&mut self) -> &mut Vec<Descriptor> {
        match self {
            ImageIndex::Oci(i) => &mut i.manifests,
            ImageIndex::Docker(i) => &mut i.manifests,
        }
    }

    /// Returns the index annotations (empty for Docker manifest lists).
    pub fn annotations(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            ImageIndex::Oci(i) => Some(&i.annotations),
            ImageIndex::Docker(_) => None,
        }
    }

    /// Returns the subject descriptor (OCI 1.1), if any.
    pub fn subject(&self) -> Option<&Descriptor> {
        match self {
            ImageIndex::Oci(i) => i.subject.as_ref(),
            ImageIndex::Docker(_) => None,
        }
    }

    /// Returns the manifest descriptors.
    pub fn manifests(&self) -> &[Descriptor] {
        match self {
            ImageIndex::Oci(i) => &i.manifests,
            ImageIndex::Docker(i) => &i.manifests,
        }
    }

    /// Returns the media type of this index.
    pub fn media_type(&self) -> MediaType {
        match self {
            ImageIndex::Oci(_) => MediaType::OciIndex,
            ImageIndex::Docker(_) => MediaType::DockerManifestList,
        }
    }

    /// Computes the digest of this index.
    pub fn digest(&self) -> Result<Digest> {
        match self {
            ImageIndex::Oci(i) => i.digest(),
            ImageIndex::Docker(i) => i.digest(),
        }
    }

    /// Serializes the index to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            ImageIndex::Oci(i) => i.to_bytes(),
            ImageIndex::Docker(i) => i.to_bytes(),
        }
    }

    /// Finds a manifest matching the given platform.
    pub fn find_platform(
        &self,
        arch: &str,
        os: &str,
        variant: Option<&str>,
    ) -> Option<&Descriptor> {
        match self {
            ImageIndex::Oci(i) => i.find_platform(arch, os, variant),
            ImageIndex::Docker(i) => i.find_platform(arch, os, variant),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Platform;

    fn sample_manifest_descriptor(arch: &str, os: &str) -> Descriptor {
        Descriptor::new(
            MediaType::OciManifest,
            "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                .parse()
                .unwrap(),
            1234,
        )
        .with_platform(Platform::new(arch, os))
    }

    #[test]
    fn test_oci_index_create() {
        let index = OciIndex::new(vec![
            sample_manifest_descriptor("amd64", "linux"),
            sample_manifest_descriptor("arm64", "linux"),
        ]);

        assert_eq!(index.schema_version, 2);
        assert_eq!(index.media_type, Some(MediaType::OciIndex));
        assert_eq!(index.manifests.len(), 2);
    }

    #[test]
    fn test_oci_index_roundtrip() {
        let index = OciIndex::new(vec![
            sample_manifest_descriptor("amd64", "linux"),
            sample_manifest_descriptor("arm64", "linux"),
        ])
        .with_annotation("org.opencontainers.image.title", "test");

        let bytes = index.to_bytes().unwrap();
        let parsed = OciIndex::from_bytes(&bytes).unwrap();

        assert_eq!(index, parsed);
    }

    #[test]
    fn test_oci_index_annotations_deterministic() {
        let index1 = OciIndex::new(vec![sample_manifest_descriptor("amd64", "linux")])
            .with_annotation("z-key", "value1")
            .with_annotation("a-key", "value2");

        let index2 = OciIndex::new(vec![sample_manifest_descriptor("amd64", "linux")])
            .with_annotation("a-key", "value2")
            .with_annotation("z-key", "value1");

        assert_eq!(
            index1.digest().unwrap(),
            index2.digest().unwrap(),
            "digest should be deterministic regardless of annotation insertion order"
        );
    }

    #[test]
    fn test_oci_index_validation_schema_version() {
        let mut index = OciIndex::new(vec![]);
        index.schema_version = 1;

        let err = index.validate().unwrap_err();
        assert!(err.to_string().contains("schemaVersion"));
    }

    #[test]
    fn test_oci_index_validation_media_type() {
        let mut index = OciIndex::new(vec![]);
        index.media_type = Some(MediaType::DockerManifestList);

        let err = index.validate().unwrap_err();
        assert!(err.to_string().contains("mediaType"));
    }

    #[test]
    fn test_oci_index_find_platform() {
        let index = OciIndex::new(vec![
            sample_manifest_descriptor("amd64", "linux"),
            sample_manifest_descriptor("arm64", "linux"),
        ]);

        let found = index.find_platform("arm64", "linux", None);
        assert!(found.is_some());
        assert_eq!(
            found.unwrap().platform.as_ref().unwrap().architecture,
            "arm64"
        );

        let not_found = index.find_platform("s390x", "linux", None);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_oci_index_find_platform_with_variant() {
        let mut desc = sample_manifest_descriptor("arm", "linux");
        desc.platform = Some(Platform::new("arm", "linux").with_variant("v7"));

        let index = OciIndex::new(vec![desc]);

        // Should find with matching variant
        let found = index.find_platform("arm", "linux", Some("v7"));
        assert!(found.is_some());

        // Should not find with different variant
        let not_found = index.find_platform("arm", "linux", Some("v8"));
        assert!(not_found.is_none());

        // Should find without specifying variant
        let found_no_variant = index.find_platform("arm", "linux", None);
        assert!(found_no_variant.is_some());
    }

    #[test]
    fn test_docker_manifest_list_roundtrip() {
        let list = DockerManifestList::new(vec![
            Descriptor::new(
                MediaType::DockerManifest,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                1234,
            )
            .with_platform(Platform::new("amd64", "linux")),
        ]);

        let bytes = list.to_bytes().unwrap();
        let parsed = DockerManifestList::from_bytes(&bytes).unwrap();

        assert_eq!(list, parsed);
    }

    #[test]
    fn test_docker_manifest_list_validation() {
        let mut list = DockerManifestList::new(vec![]);
        list.media_type = MediaType::OciIndex;

        let err = list.validate().unwrap_err();
        assert!(err.to_string().contains("mediaType"));
    }

    #[test]
    fn test_image_index_auto_detect_oci() {
        let oci = OciIndex::new(vec![sample_manifest_descriptor("amd64", "linux")]);
        let bytes = oci.to_bytes().unwrap();

        let parsed = ImageIndex::from_bytes(&bytes).unwrap();
        assert!(matches!(parsed, ImageIndex::Oci(_)));
    }

    #[test]
    fn test_image_index_auto_detect_docker() {
        let docker = DockerManifestList::new(vec![
            Descriptor::new(
                MediaType::DockerManifest,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                1234,
            )
            .with_platform(Platform::new("amd64", "linux")),
        ]);
        let bytes = docker.to_bytes().unwrap();

        let parsed = ImageIndex::from_bytes(&bytes).unwrap();
        assert!(matches!(parsed, ImageIndex::Docker(_)));
    }

    #[test]
    fn test_image_index_auto_detect_missing_media_type() {
        // Index without mediaType should error
        let json = r#"{"schemaVersion":2,"manifests":[]}"#;
        let err = ImageIndex::from_bytes(json.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("missing mediaType"));
    }

    #[test]
    fn test_oci_to_docker_index_conversion() {
        let oci = OciIndex::new(vec![sample_manifest_descriptor("amd64", "linux")]);
        let docker = oci.try_to_docker().expect("conversion should succeed");

        assert_eq!(docker.media_type, MediaType::DockerManifestList);
        assert_eq!(docker.manifests[0].media_type, MediaType::DockerManifest);
    }
}
