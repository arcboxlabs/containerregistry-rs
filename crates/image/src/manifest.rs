//! OCI and Docker image manifest types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Descriptor, Digest, Error, MediaType, Result};

/// OCI image manifest schema version.
pub const OCI_MANIFEST_SCHEMA_VERSION: u32 = 2;

/// An OCI image manifest.
///
/// This represents a single-platform container image, containing
/// references to the config blob and layer blobs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OciManifest {
    /// Schema version (must be 2).
    pub schema_version: u32,

    /// Media type of the manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<MediaType>,

    /// Artifact type (OCI 1.1 artifacts extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,

    /// Reference to the image config blob.
    pub config: Descriptor,

    /// References to the layer blobs, in order from base to top.
    pub layers: Vec<Descriptor>,

    /// Subject descriptor for OCI referrers (OCI 1.1 extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Descriptor>,

    /// Optional annotations (uses BTreeMap for deterministic serialization).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Docker v2 schema 2 manifest.
///
/// This is the Docker-specific manifest format, which is similar to
/// the OCI format but uses different media types.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DockerManifest {
    /// Schema version (must be 2).
    pub schema_version: u32,

    /// Media type of the manifest (defaults to the Docker manifest type when
    /// absent, as older registries omit it).
    #[serde(default = "docker_manifest_media_type")]
    pub media_type: MediaType,

    /// Reference to the image config blob.
    pub config: Descriptor,

    /// References to the layer blobs, in order from base to top.
    pub layers: Vec<Descriptor>,
}

fn docker_manifest_media_type() -> MediaType {
    MediaType::DockerManifest
}

impl OciManifest {
    /// Creates a new OCI manifest with the given config and layers.
    pub fn new(config: Descriptor, layers: Vec<Descriptor>) -> Self {
        Self {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: Some(MediaType::OciManifest),
            artifact_type: None,
            config,
            layers,
            subject: None,
            annotations: BTreeMap::new(),
        }
    }

    /// Adds an annotation to the manifest.
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Sets the artifact type (OCI 1.1 extension).
    pub fn with_artifact_type(mut self, artifact_type: MediaType) -> Self {
        self.artifact_type = Some(artifact_type);
        self
    }

    /// Sets the subject descriptor for referrers (OCI 1.1 extension).
    pub fn with_subject(mut self, subject: Descriptor) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Parses an OCI manifest from JSON bytes.
    ///
    /// This validates that the schemaVersion is 2 and the mediaType (if present)
    /// is an OCI manifest type.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(data)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parses an OCI manifest from JSON bytes without validation.
    ///
    /// Use this when you need to parse potentially non-conformant manifests.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(Error::from)
    }

    /// Validates the manifest structure.
    ///
    /// This checks:
    /// - schemaVersion is 2
    /// - mediaType is present and is OCI manifest type
    /// - config has an OCI config media type or a custom (artifact) type
    /// - layers have OCI layer media types or custom (artifact) types
    ///
    /// Docker media types are rejected inside an OCI manifest.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OCI_MANIFEST_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "invalid schemaVersion: expected {}, got {}",
                OCI_MANIFEST_SCHEMA_VERSION, self.schema_version
            )));
        }

        // mediaType is required for OCI manifests
        match &self.media_type {
            Some(MediaType::OciManifest) => {}
            Some(mt) => {
                return Err(Error::InvalidManifest(format!(
                    "invalid mediaType for OCI manifest: {}",
                    mt
                )));
            }
            None => {
                return Err(Error::InvalidManifest(
                    "missing required mediaType field".to_string(),
                ));
            }
        }

        // Validate config media type (OCI-only)
        // Arbitrary (`Other`) media types are allowed: OCI 1.1 artifacts such
        // as Tart VM images use custom config and layer types.
        if !matches!(
            self.config.media_type,
            MediaType::OciConfig | MediaType::OciEmptyJson | MediaType::Other(_)
        ) {
            return Err(Error::InvalidManifest(format!(
                "invalid config mediaType for OCI manifest: expected OCI config type, got {}",
                self.config.media_type
            )));
        }

        // Validate layer media types (OCI-only)
        for (i, layer) in self.layers.iter().enumerate() {
            if !matches!(
                layer.media_type,
                MediaType::OciLayerGzip
                    | MediaType::OciLayer
                    | MediaType::OciLayerZstd
                    | MediaType::OciLayerNondistributable
                    | MediaType::OciLayerNondistributableGzip
                    | MediaType::OciLayerNondistributableZstd
                    | MediaType::OciEmptyJson
                    | MediaType::Other(_)
            ) {
                return Err(Error::InvalidManifest(format!(
                    "invalid layer[{}] mediaType for OCI manifest: expected OCI layer type, got {}",
                    i, layer.media_type
                )));
            }
        }

        Ok(())
    }

    /// Serializes the manifest to canonical JSON bytes.
    ///
    /// The output is deterministic for digest computation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // Use compact JSON for canonical representation
        serde_json::to_vec(self).map_err(Error::from)
    }

    /// Computes the digest of this manifest.
    pub fn digest(&self) -> Result<Digest> {
        let bytes = self.to_bytes()?;
        Ok(Digest::sha256(&bytes))
    }

    /// Returns the size of the serialized manifest.
    pub fn size(&self) -> Result<u64> {
        let bytes = self.to_bytes()?;
        Ok(bytes.len() as u64)
    }

    /// Converts this manifest to a Docker manifest.
    ///
    /// # Panics
    ///
    /// Panics if any layers use compression formats not supported by Docker
    /// (uncompressed tar, zstd, non-distributable). Use `try_to_docker()` for
    /// fallible conversion.
    #[deprecated(
        since = "0.1.0",
        note = "Use try_to_docker() for safe conversion with error handling"
    )]
    pub fn to_docker(&self) -> DockerManifest {
        self.try_to_docker()
            .expect("cannot convert manifest with incompatible layers to Docker format")
    }

    /// Attempts to convert this manifest to a Docker manifest.
    ///
    /// Returns an error if any layers use compression formats not supported
    /// by Docker (uncompressed tar, zstd).
    pub fn try_to_docker(&self) -> Result<DockerManifest> {
        // Check for incompatible layer types
        for layer in &self.layers {
            match &layer.media_type {
                MediaType::OciLayer => {
                    return Err(Error::InvalidManifest(
                        "cannot convert uncompressed OCI layer to Docker format".to_string(),
                    ));
                }
                MediaType::OciLayerZstd => {
                    return Err(Error::InvalidManifest(
                        "cannot convert zstd-compressed OCI layer to Docker format".to_string(),
                    ));
                }
                MediaType::OciLayerNondistributable
                | MediaType::OciLayerNondistributableGzip
                | MediaType::OciLayerNondistributableZstd => {
                    return Err(Error::InvalidManifest(
                        "cannot convert non-distributable OCI layer to Docker format".to_string(),
                    ));
                }
                _ => {}
            }
        }

        // Perform the actual conversion
        Ok(DockerManifest {
            schema_version: self.schema_version,
            media_type: MediaType::DockerManifest,
            config: Descriptor {
                media_type: self.config.media_type.to_docker(),
                ..self.config.clone()
            },
            layers: self
                .layers
                .iter()
                .map(|l| Descriptor {
                    media_type: l.media_type.to_docker(),
                    ..l.clone()
                })
                .collect(),
        })
    }
}

impl DockerManifest {
    /// Creates a new Docker manifest with the given config and layers.
    pub fn new(config: Descriptor, layers: Vec<Descriptor>) -> Self {
        Self {
            schema_version: OCI_MANIFEST_SCHEMA_VERSION,
            media_type: MediaType::DockerManifest,
            config,
            layers,
        }
    }

    /// Parses a Docker manifest from JSON bytes.
    ///
    /// This validates that the schemaVersion is 2 and the mediaType is a Docker
    /// manifest type.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let manifest: Self = serde_json::from_slice(data)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parses a Docker manifest from JSON bytes without validation.
    pub fn from_bytes_unchecked(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(Error::from)
    }

    /// Validates the manifest structure.
    ///
    /// This checks:
    /// - schemaVersion is 2
    /// - mediaType is Docker manifest type
    /// - config has a valid config media type
    /// - layers have valid layer media types
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OCI_MANIFEST_SCHEMA_VERSION {
            return Err(Error::InvalidManifest(format!(
                "invalid schemaVersion: expected {}, got {}",
                OCI_MANIFEST_SCHEMA_VERSION, self.schema_version
            )));
        }

        if !matches!(self.media_type, MediaType::DockerManifest) {
            return Err(Error::InvalidManifest(format!(
                "invalid mediaType for Docker manifest: {}",
                self.media_type
            )));
        }

        // Validate config media type (Docker-only)
        if !matches!(self.config.media_type, MediaType::DockerConfig) {
            return Err(Error::InvalidManifest(format!(
                "invalid config mediaType for Docker manifest: expected Docker config type, got {}",
                self.config.media_type
            )));
        }

        // Validate layer media types (Docker-only)
        for (i, layer) in self.layers.iter().enumerate() {
            if !matches!(
                layer.media_type,
                MediaType::DockerLayerGzip | MediaType::DockerForeignLayer
            ) {
                return Err(Error::InvalidManifest(format!(
                    "invalid layer[{}] mediaType for Docker manifest: expected Docker layer type, got {}",
                    i, layer.media_type
                )));
            }
        }

        Ok(())
    }

    /// Serializes the manifest to canonical JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Error::from)
    }

    /// Computes the digest of this manifest.
    pub fn digest(&self) -> Result<Digest> {
        let bytes = self.to_bytes()?;
        Ok(Digest::sha256(&bytes))
    }

    /// Returns the size of the serialized manifest.
    pub fn size(&self) -> Result<u64> {
        let bytes = self.to_bytes()?;
        Ok(bytes.len() as u64)
    }

    /// Converts this manifest to an OCI manifest.
    pub fn to_oci(&self) -> OciManifest {
        OciManifest {
            schema_version: self.schema_version,
            media_type: Some(MediaType::OciManifest),
            artifact_type: None,
            config: Descriptor {
                media_type: self.config.media_type.to_oci(),
                ..self.config.clone()
            },
            layers: self
                .layers
                .iter()
                .map(|l| Descriptor {
                    media_type: l.media_type.to_oci(),
                    ..l.clone()
                })
                .collect(),
            subject: None,
            annotations: BTreeMap::new(),
        }
    }
}

/// A unified manifest type that can represent either OCI or Docker formats.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Boxing would hurt API ergonomics
pub enum Manifest {
    Oci(OciManifest),
    Docker(DockerManifest),
}

impl Manifest {
    /// Parses a manifest from JSON bytes, auto-detecting the format.
    ///
    /// Detection is based on the mediaType field:
    /// - Docker manifest: `application/vnd.docker.distribution.manifest.v2+json`
    /// - OCI manifest: `application/vnd.oci.image.manifest.v1+json`
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
            Some("application/vnd.docker.distribution.manifest.v2+json") => {
                Ok(Manifest::Docker(DockerManifest::from_bytes(data)?))
            }
            Some("application/vnd.oci.image.manifest.v1+json") => {
                Ok(Manifest::Oci(OciManifest::from_bytes(data)?))
            }
            Some(other) => Err(Error::UnsupportedMediaType(other.to_string())),
            None => Err(Error::InvalidManifest(
                "missing mediaType field; cannot auto-detect manifest format".to_string(),
            )),
        }
    }

    /// Parses a manifest without validating media types.
    ///
    /// The format is chosen from the document's `mediaType` field, falling
    /// back to `media_type` (typically the HTTP `Content-Type`) and finally to
    /// OCI. Use this for content received from registries, which may predate
    /// the `mediaType` field or carry custom artifact types.
    pub fn from_bytes_unchecked(data: &[u8], media_type: Option<&MediaType>) -> Result<Self> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(default, rename = "mediaType")]
            media_type: Option<MediaType>,
        }
        let probe: Probe = serde_json::from_slice(data)?;
        match probe.media_type.as_ref().or(media_type) {
            Some(MediaType::DockerManifest) => Ok(Manifest::Docker(
                DockerManifest::from_bytes_unchecked(data)?,
            )),
            _ => Ok(Manifest::Oci(OciManifest::from_bytes_unchecked(data)?)),
        }
    }

    /// Returns the subject descriptor (OCI 1.1), if any.
    pub fn subject(&self) -> Option<&Descriptor> {
        match self {
            Manifest::Oci(m) => m.subject.as_ref(),
            Manifest::Docker(_) => None,
        }
    }

    /// Returns the artifact type (OCI 1.1), if any.
    pub fn artifact_type(&self) -> Option<&MediaType> {
        match self {
            Manifest::Oci(m) => m.artifact_type.as_ref(),
            Manifest::Docker(_) => None,
        }
    }

    /// Returns the manifest annotations (empty for Docker manifests).
    pub fn annotations(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Manifest::Oci(m) => Some(&m.annotations),
            Manifest::Docker(_) => None,
        }
    }

    /// Returns mutable access to the config descriptor.
    pub fn config_mut(&mut self) -> &mut Descriptor {
        match self {
            Manifest::Oci(m) => &mut m.config,
            Manifest::Docker(m) => &mut m.config,
        }
    }

    /// Returns mutable access to the layer descriptors.
    pub fn layers_mut(&mut self) -> &mut Vec<Descriptor> {
        match self {
            Manifest::Oci(m) => &mut m.layers,
            Manifest::Docker(m) => &mut m.layers,
        }
    }

    /// Returns the config descriptor.
    pub fn config(&self) -> &Descriptor {
        match self {
            Manifest::Oci(m) => &m.config,
            Manifest::Docker(m) => &m.config,
        }
    }

    /// Returns the layer descriptors.
    pub fn layers(&self) -> &[Descriptor] {
        match self {
            Manifest::Oci(m) => &m.layers,
            Manifest::Docker(m) => &m.layers,
        }
    }

    /// Returns the media type of this manifest.
    pub fn media_type(&self) -> MediaType {
        match self {
            Manifest::Oci(_) => MediaType::OciManifest,
            Manifest::Docker(_) => MediaType::DockerManifest,
        }
    }

    /// Computes the digest of this manifest.
    pub fn digest(&self) -> Result<Digest> {
        match self {
            Manifest::Oci(m) => m.digest(),
            Manifest::Docker(m) => m.digest(),
        }
    }

    /// Serializes the manifest to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Manifest::Oci(m) => m.to_bytes(),
            Manifest::Docker(m) => m.to_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config_descriptor() -> Descriptor {
        Descriptor::new(
            MediaType::OciConfig,
            "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                .parse()
                .unwrap(),
            2,
        )
    }

    fn sample_layer_descriptor() -> Descriptor {
        Descriptor::new(
            MediaType::OciLayerGzip,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .unwrap(),
            0,
        )
    }

    #[test]
    fn test_oci_manifest_create() {
        let manifest =
            OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()]);

        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.media_type, Some(MediaType::OciManifest));
        assert_eq!(manifest.layers.len(), 1);
    }

    #[test]
    fn test_oci_manifest_roundtrip() {
        let manifest =
            OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()])
                .with_annotation("org.opencontainers.image.title", "test");

        let bytes = manifest.to_bytes().unwrap();
        let parsed = OciManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest, parsed);
    }

    #[test]
    fn test_oci_manifest_digest_stability() {
        let manifest =
            OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()]);

        let digest1 = manifest.digest().unwrap();
        let digest2 = manifest.digest().unwrap();

        assert_eq!(digest1, digest2);
    }

    #[test]
    fn test_oci_manifest_annotations_deterministic() {
        // Add annotations in different orders, verify digest is the same
        let manifest1 =
            OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()])
                .with_annotation("z-key", "value1")
                .with_annotation("a-key", "value2");

        let manifest2 =
            OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()])
                .with_annotation("a-key", "value2")
                .with_annotation("z-key", "value1");

        assert_eq!(
            manifest1.digest().unwrap(),
            manifest2.digest().unwrap(),
            "digest should be deterministic regardless of annotation insertion order"
        );
    }

    #[test]
    fn test_oci_manifest_validation_schema_version() {
        let mut manifest = OciManifest::new(sample_config_descriptor(), vec![]);
        manifest.schema_version = 1;

        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("schemaVersion"));
    }

    #[test]
    fn test_oci_manifest_validation_media_type() {
        let mut manifest = OciManifest::new(sample_config_descriptor(), vec![]);
        manifest.media_type = Some(MediaType::DockerManifest);

        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("mediaType"));
    }

    #[test]
    fn test_docker_manifest_roundtrip() {
        let manifest = DockerManifest::new(
            Descriptor::new(
                MediaType::DockerConfig,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                2,
            ),
            vec![Descriptor::new(
                MediaType::DockerLayerGzip,
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .parse()
                    .unwrap(),
                0,
            )],
        );

        let bytes = manifest.to_bytes().unwrap();
        let parsed = DockerManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest, parsed);
    }

    #[test]
    fn test_docker_manifest_validation() {
        let mut manifest = DockerManifest::new(
            Descriptor::new(
                MediaType::DockerConfig,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                2,
            ),
            vec![],
        );
        manifest.media_type = MediaType::OciManifest;

        let err = manifest.validate().unwrap_err();
        assert!(err.to_string().contains("mediaType"));
    }

    #[test]
    fn test_manifest_auto_detect_oci() {
        let oci = OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()]);
        let bytes = oci.to_bytes().unwrap();

        let parsed = Manifest::from_bytes(&bytes).unwrap();
        assert!(matches!(parsed, Manifest::Oci(_)));
    }

    #[test]
    fn test_manifest_auto_detect_docker() {
        let docker = DockerManifest::new(
            Descriptor::new(
                MediaType::DockerConfig,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                2,
            ),
            vec![],
        );
        let bytes = docker.to_bytes().unwrap();

        let parsed = Manifest::from_bytes(&bytes).unwrap();
        assert!(matches!(parsed, Manifest::Docker(_)));
    }

    #[test]
    fn test_manifest_auto_detect_missing_media_type() {
        // Manifest without mediaType should error
        let json = r#"{"schemaVersion":2,"config":{},"layers":[]}"#;
        let err = Manifest::from_bytes(json.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("missing mediaType"));
    }

    #[test]
    fn test_oci_to_docker_conversion() {
        let oci = OciManifest::new(sample_config_descriptor(), vec![sample_layer_descriptor()]);
        let docker = oci.try_to_docker().expect("conversion should succeed");

        assert_eq!(docker.media_type, MediaType::DockerManifest);
        assert_eq!(docker.config.media_type, MediaType::DockerConfig);
        assert_eq!(docker.layers[0].media_type, MediaType::DockerLayerGzip);
    }

    #[test]
    fn test_try_to_docker_with_zstd_layer() {
        let oci = OciManifest::new(
            sample_config_descriptor(),
            vec![Descriptor::new(
                MediaType::OciLayerZstd,
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .parse()
                    .unwrap(),
                0,
            )],
        );

        let err = oci.try_to_docker().unwrap_err();
        assert!(err.to_string().contains("zstd"));
    }

    #[test]
    fn test_try_to_docker_with_uncompressed_layer() {
        let oci = OciManifest::new(
            sample_config_descriptor(),
            vec![Descriptor::new(
                MediaType::OciLayer,
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .parse()
                    .unwrap(),
                0,
            )],
        );

        let err = oci.try_to_docker().unwrap_err();
        assert!(err.to_string().contains("uncompressed"));
    }

    #[test]
    fn test_docker_to_oci_conversion() {
        let docker = DockerManifest::new(
            Descriptor::new(
                MediaType::DockerConfig,
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .parse()
                    .unwrap(),
                2,
            ),
            vec![Descriptor::new(
                MediaType::DockerLayerGzip,
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .parse()
                    .unwrap(),
                0,
            )],
        );
        let oci = docker.to_oci();

        assert_eq!(oci.media_type, Some(MediaType::OciManifest));
        assert_eq!(oci.config.media_type, MediaType::OciConfig);
        assert_eq!(oci.layers[0].media_type, MediaType::OciLayerGzip);
    }

    #[test]
    fn test_oci_manifest_with_artifact_type() {
        let manifest = OciManifest::new(sample_config_descriptor(), vec![]).with_artifact_type(
            MediaType::Other("application/vnd.example.artifact".to_string()),
        );

        let bytes = manifest.to_bytes().unwrap();
        let parsed = OciManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest.artifact_type, parsed.artifact_type);
    }

    #[test]
    fn test_oci_manifest_with_subject() {
        let subject = Descriptor::new(
            MediaType::OciManifest,
            "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                .parse()
                .unwrap(),
            1234,
        );

        let manifest =
            OciManifest::new(sample_config_descriptor(), vec![]).with_subject(subject.clone());

        let bytes = manifest.to_bytes().unwrap();
        let parsed = OciManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest.subject, parsed.subject);
    }
}
