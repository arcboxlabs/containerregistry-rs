//! OCI content descriptor type.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Digest, Error, MediaType};

/// An OCI content descriptor.
///
/// Descriptors are used to reference content by digest, and include
/// the media type and size for validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    /// The media type of the referenced content.
    pub media_type: MediaType,

    /// The digest of the referenced content.
    pub digest: Digest,

    /// The size in bytes of the referenced content.
    pub size: u64,

    /// Optional URLs for downloading the content (OCI extension).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,

    /// Optional annotations (uses BTreeMap for deterministic serialization).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,

    /// Optional embedded data (base64-encoded, OCI extension).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    /// Optional platform specification (used in index manifests).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,

    /// Optional artifact type of the referenced manifest (OCI 1.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_type: Option<MediaType>,
}

/// Platform specification for multi-arch images.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Platform {
    /// The CPU architecture.
    pub architecture: String,

    /// The operating system.
    pub os: String,

    /// Optional OS version.
    #[serde(
        default,
        rename = "os.version",
        skip_serializing_if = "Option::is_none"
    )]
    pub os_version: Option<String>,

    /// Optional OS features.
    #[serde(default, rename = "os.features", skip_serializing_if = "Vec::is_empty")]
    pub os_features: Vec<String>,

    /// Optional architecture variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,

    /// Optional features (Docker manifest list extension).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
}

impl Descriptor {
    /// Creates a new descriptor with the given media type, digest, and size.
    pub fn new(media_type: MediaType, digest: Digest, size: u64) -> Self {
        Self {
            media_type,
            digest,
            size,
            urls: Vec::new(),
            annotations: BTreeMap::new(),
            data: None,
            platform: None,
            artifact_type: None,
        }
    }

    /// Creates a descriptor for `data`, computing its SHA-256 digest and size.
    pub fn from_bytes(media_type: MediaType, data: &[u8]) -> Self {
        Self::new(media_type, Digest::sha256(data), data.len() as u64)
    }

    /// Sets the artifact type (OCI 1.1).
    pub fn with_artifact_type(mut self, artifact_type: MediaType) -> Self {
        self.artifact_type = Some(artifact_type);
        self
    }

    /// Adds an annotation to the descriptor.
    pub fn with_annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Adds a URL for content download.
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.urls.push(url.into());
        self
    }

    /// Sets the platform for the descriptor.
    pub fn with_platform(mut self, platform: Platform) -> Self {
        self.platform = Some(platform);
        self
    }
}

impl Platform {
    /// Creates a new platform specification.
    pub fn new(architecture: impl Into<String>, os: impl Into<String>) -> Self {
        Self {
            architecture: architecture.into(),
            os: os.into(),
            os_version: None,
            os_features: Vec::new(),
            variant: None,
            features: Vec::new(),
        }
    }

    /// Sets the variant for the platform.
    pub fn with_variant(mut self, variant: impl Into<String>) -> Self {
        self.variant = Some(variant.into());
        self
    }

    /// Sets the OS version.
    pub fn with_os_version(mut self, version: impl Into<String>) -> Self {
        self.os_version = Some(version.into());
        self
    }

    /// Adds an OS feature.
    pub fn with_os_feature(mut self, feature: impl Into<String>) -> Self {
        self.os_features.push(feature.into());
        self
    }

    /// Adds a feature (Docker extension).
    pub fn with_feature(mut self, feature: impl Into<String>) -> Self {
        self.features.push(feature.into());
        self
    }

    /// Returns true if this platform satisfies `spec`.
    ///
    /// Empty fields in `spec` match anything; feature lists in `spec` must be
    /// subsets of this platform's lists. This mirrors go-containerregistry's
    /// `Platform.Satisfies`.
    pub fn satisfies(&self, spec: &Platform) -> bool {
        fn field(want: &str, have: &str) -> bool {
            want.is_empty() || want == have
        }
        fn opt(want: &Option<String>, have: &Option<String>) -> bool {
            field(want.as_deref().unwrap_or(""), have.as_deref().unwrap_or(""))
        }
        fn list(want: &[String], have: &[String]) -> bool {
            want.iter().all(|w| have.contains(w))
        }
        field(&spec.os, &self.os)
            && field(&spec.architecture, &self.architecture)
            && opt(&spec.variant, &self.variant)
            && opt(&spec.os_version, &self.os_version)
            && list(&spec.os_features, &self.os_features)
            && list(&spec.features, &self.features)
    }

    /// Returns true if both platforms are equal, ignoring feature order.
    pub fn equals(&self, other: &Platform) -> bool {
        fn sorted(v: &[String]) -> Vec<&String> {
            let mut v: Vec<&String> = v.iter().collect();
            v.sort();
            v
        }
        self.os == other.os
            && self.architecture == other.architecture
            && self.variant.as_deref().unwrap_or("") == other.variant.as_deref().unwrap_or("")
            && self.os_version.as_deref().unwrap_or("") == other.os_version.as_deref().unwrap_or("")
            && sorted(&self.os_features) == sorted(&other.os_features)
            && sorted(&self.features) == sorted(&other.features)
    }
}

impl fmt::Display for Platform {
    /// Formats as `os[/arch[/variant]][:osversion]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.os.is_empty() {
            return Ok(());
        }
        f.write_str(&self.os)?;
        if !self.architecture.is_empty() {
            write!(f, "/{}", self.architecture)?;
        }
        if let Some(v) = self.variant.as_deref().filter(|v| !v.is_empty()) {
            write!(f, "/{}", v)?;
        }
        if let Some(v) = self.os_version.as_deref().filter(|v| !v.is_empty()) {
            write!(f, ":{}", v)?;
        }
        Ok(())
    }
}

impl FromStr for Platform {
    type Err = Error;

    /// Parses `os[/arch[/variant]][:osversion]`, e.g. `linux/arm64/v8`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (rest, os_version) = match s.split_once(':') {
            Some((rest, v)) => (rest, Some(v.to_string())),
            None => (s, None),
        };
        let parts: Vec<&str> = rest.split('/').collect();
        if parts.len() > 3 {
            return Err(Error::InvalidPlatform(format!(
                "too many slashes in platform spec: {}",
                s
            )));
        }
        let mut p = Platform::new(
            parts.get(1).copied().unwrap_or(""),
            parts.first().copied().unwrap_or(""),
        );
        p.variant = parts.get(2).map(|v| v.to_string());
        p.os_version = os_version;
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_descriptor_serde() {
        let desc = Descriptor::new(
            MediaType::OciLayerGzip,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
            1234,
        );

        let json = serde_json::to_string_pretty(&desc).unwrap();
        let parsed: Descriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(desc, parsed);
    }

    #[test]
    fn test_descriptor_with_platform() {
        let desc = Descriptor::new(
            MediaType::OciManifest,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
            5678,
        )
        .with_platform(Platform::new("amd64", "linux"));

        let json = serde_json::to_string(&desc).unwrap();
        assert!(json.contains("amd64"));
        assert!(json.contains("linux"));
    }

    #[test]
    fn test_descriptor_with_urls() {
        let desc = Descriptor::new(
            MediaType::OciLayerGzip,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
            1234,
        )
        .with_url("https://example.com/blob");

        let json = serde_json::to_string(&desc).unwrap();
        assert!(json.contains("urls"));
        assert!(json.contains("https://example.com/blob"));
    }

    #[test]
    fn test_annotations_deterministic_order() {
        // Add annotations in different orders, verify serialization is the same
        let desc1 = Descriptor::new(
            MediaType::OciLayerGzip,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
            1234,
        )
        .with_annotation("z-key", "value1")
        .with_annotation("a-key", "value2");

        let desc2 = Descriptor::new(
            MediaType::OciLayerGzip,
            "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .unwrap(),
            1234,
        )
        .with_annotation("a-key", "value2")
        .with_annotation("z-key", "value1");

        let json1 = serde_json::to_string(&desc1).unwrap();
        let json2 = serde_json::to_string(&desc2).unwrap();

        assert_eq!(
            json1, json2,
            "annotations should serialize in deterministic order"
        );
    }
}
