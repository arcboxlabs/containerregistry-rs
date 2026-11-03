//! OCI image configuration types.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{Digest, Error, Result};

/// Deserialize a value that may be JSON `null`, treating null as T::default().
/// Docker/OCI configs frequently contain `"Volumes": null` or `"Labels": null`
/// instead of omitting the field or using an empty object/array.
fn null_as_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(|v| v.unwrap_or_default())
}

/// OCI image configuration.
///
/// This contains the execution parameters for the container,
/// along with the history and layer diff IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageConfig {
    /// The CPU architecture.
    pub architecture: String,

    /// The operating system.
    pub os: String,

    /// Optional OS version.
    #[serde(
        default,
        rename = "os.version",
        alias = "os_version",
        skip_serializing_if = "Option::is_none"
    )]
    pub os_version: Option<String>,

    /// Optional OS features required by the image.
    #[serde(
        default,
        rename = "os.features",
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub os_features: Vec<String>,

    /// Optional architecture variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,

    /// Container runtime configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<ContainerConfig>,

    /// Layer content hashes (uncompressed).
    pub rootfs: RootFs,

    /// Build history.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub history: Vec<History>,

    /// Creation timestamp (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    /// Author of the image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// ID of the container used to build the image (Docker).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,

    /// Docker version used to build the image (Docker).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_version: Option<String>,

    /// Fields not modelled above (e.g. `container_config`), preserved so that
    /// a parse/serialize round trip does not drop data.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Empty object used for ports and volumes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyObject {}

/// Health check configuration for the container.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Healthcheck {
    /// The test to perform (CMD or CMD-SHELL).
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub test: Vec<String>,

    /// Interval between health checks (nanoseconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<i64>,

    /// Timeout for each health check (nanoseconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<i64>,

    /// Number of retries before marking unhealthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<i32>,

    /// Start period for the container (nanoseconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_period: Option<i64>,

    /// Interval between health checks during start period (nanoseconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_interval: Option<i64>,
}

/// Container runtime configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerConfig {
    /// Hostname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// Domain name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domainname: Option<String>,

    /// User to run as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Exposed ports.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub exposed_ports: BTreeMap<String, EmptyObject>,

    /// Environment variables.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub env: Vec<String>,

    /// Entrypoint command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,

    /// Default command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,

    /// Volumes.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub volumes: BTreeMap<String, EmptyObject>,

    /// Working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    /// Labels.
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub labels: BTreeMap<String, String>,

    /// Stop signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,

    /// Health check configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<Healthcheck>,

    /// Dockerfile ONBUILD triggers.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "null_as_default"
    )]
    pub on_build: Vec<String>,

    /// Shell for shell-form RUN commands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<Vec<String>>,

    /// Windows-specific: whether args should be escaped.
    #[serde(default, skip_serializing_if = "is_false")]
    pub args_escaped: bool,

    /// Fields not modelled above (e.g. `Tty`, `Image`), preserved so that a
    /// parse/serialize round trip does not drop data.
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}
/// Root filesystem configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootFs {
    /// Type of rootfs (always "layers").
    #[serde(rename = "type")]
    pub fs_type: String,

    /// Layer diff IDs (uncompressed content hashes).
    pub diff_ids: Vec<Digest>,
}

/// History entry for a layer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct History {
    /// Creation timestamp (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    /// Command that created this layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,

    /// Author of this layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Commit message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Whether this is an empty layer (no filesystem changes).
    #[serde(default, skip_serializing_if = "is_false")]
    pub empty_layer: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl ImageConfig {
    /// Creates a new image config with the given architecture and OS.
    pub fn new(architecture: impl Into<String>, os: impl Into<String>) -> Self {
        Self {
            architecture: architecture.into(),
            os: os.into(),
            os_version: None,
            os_features: Vec::new(),
            variant: None,
            config: None,
            rootfs: RootFs {
                fs_type: "layers".to_string(),
                diff_ids: Vec::new(),
            },
            history: Vec::new(),
            created: None,
            author: None,
            container: None,
            docker_version: None,
            extra: BTreeMap::new(),
        }
    }

    /// Parses an image config from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(Error::from)
    }

    /// Serializes the config to canonical JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Error::from)
    }

    /// Computes the digest of this config.
    pub fn digest(&self) -> Result<Digest> {
        let bytes = self.to_bytes()?;
        Ok(Digest::sha256(&bytes))
    }

    /// Validates the image config for strict OCI requirements.
    ///
    /// This currently checks:
    /// - rootfs.type is "layers"
    pub fn validate(&self) -> Result<()> {
        if self.rootfs.fs_type != "layers" {
            return Err(Error::InvalidConfig(format!(
                "invalid rootfs.type: expected \"layers\", got \"{}\"",
                self.rootfs.fs_type
            )));
        }
        Ok(())
    }

    /// Returns the size of the serialized config.
    pub fn size(&self) -> Result<u64> {
        let bytes = self.to_bytes()?;
        Ok(bytes.len() as u64)
    }

    /// Adds a layer diff ID.
    pub fn with_layer(mut self, diff_id: Digest) -> Self {
        self.rootfs.diff_ids.push(diff_id);
        self
    }

    /// Sets the container configuration.
    pub fn with_config(mut self, config: ContainerConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Adds a history entry.
    pub fn with_history(mut self, history: History) -> Self {
        self.history.push(history);
        self
    }

    /// Returns the labels from the container config, if any.
    pub fn labels(&self) -> Option<&BTreeMap<String, String>> {
        self.config.as_ref().map(|c| &c.labels)
    }

    /// Returns the entrypoint from the container config, if any.
    pub fn entrypoint(&self) -> Option<&[String]> {
        self.config.as_ref().and_then(|c| c.entrypoint.as_deref())
    }

    /// Returns the cmd from the container config, if any.
    pub fn cmd(&self) -> Option<&[String]> {
        self.config.as_ref().and_then(|c| c.cmd.as_deref())
    }

    /// Returns the environment variables from the container config, if any.
    pub fn env(&self) -> Option<&[String]> {
        self.config.as_ref().map(|c| c.env.as_slice())
    }
}

impl ContainerConfig {
    /// Creates a new container config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the entrypoint.
    pub fn with_entrypoint(mut self, entrypoint: Vec<String>) -> Self {
        self.entrypoint = Some(entrypoint);
        self
    }

    /// Sets the default command.
    pub fn with_cmd(mut self, cmd: Vec<String>) -> Self {
        self.cmd = Some(cmd);
        self
    }

    /// Adds an environment variable.
    pub fn with_env(mut self, var: impl Into<String>) -> Self {
        self.env.push(var.into());
        self
    }

    /// Adds a label.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Sets the working directory.
    pub fn with_working_dir(mut self, dir: impl Into<String>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    /// Sets the user.
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }
}

impl History {
    /// Creates a new history entry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the created_by field.
    pub fn with_created_by(mut self, created_by: impl Into<String>) -> Self {
        self.created_by = Some(created_by.into());
        self
    }

    /// Marks this as an empty layer.
    pub fn as_empty_layer(mut self) -> Self {
        self.empty_layer = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_config_create() {
        let config = ImageConfig::new("amd64", "linux");

        assert_eq!(config.architecture, "amd64");
        assert_eq!(config.os, "linux");
        assert_eq!(config.rootfs.fs_type, "layers");
    }

    #[test]
    fn test_image_config_roundtrip() {
        let config = ImageConfig::new("amd64", "linux")
            .with_layer(
                "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    .parse()
                    .unwrap(),
            )
            .with_config(
                ContainerConfig::new()
                    .with_entrypoint(vec!["/bin/sh".to_string()])
                    .with_env("PATH=/usr/bin:/bin".to_string())
                    .with_label("version", "1.0"),
            );

        let bytes = config.to_bytes().unwrap();
        let parsed = ImageConfig::from_bytes(&bytes).unwrap();

        assert_eq!(config, parsed);
    }

    #[test]
    fn test_image_config_digest_stability() {
        let config = ImageConfig::new("amd64", "linux");

        let digest1 = config.digest().unwrap();
        let digest2 = config.digest().unwrap();

        assert_eq!(digest1, digest2);
    }

    #[test]
    fn test_container_config_builder() {
        let config = ContainerConfig::new()
            .with_entrypoint(vec!["/bin/sh".to_string()])
            .with_cmd(vec!["-c".to_string(), "echo hello".to_string()])
            .with_env("FOO=bar")
            .with_working_dir("/app")
            .with_user("nobody")
            .with_label("maintainer", "test@example.com");

        assert_eq!(config.entrypoint, Some(vec!["/bin/sh".to_string()]));
        assert_eq!(
            config.cmd,
            Some(vec!["-c".to_string(), "echo hello".to_string()])
        );
        assert_eq!(config.env, vec!["FOO=bar"]);
        assert_eq!(config.working_dir, Some("/app".to_string()));
        assert_eq!(config.user, Some("nobody".to_string()));
        assert_eq!(
            config.labels.get("maintainer"),
            Some(&"test@example.com".to_string())
        );
    }

    #[test]
    fn test_history_builder() {
        let history = History::new()
            .with_created_by("ADD file:abc123 /")
            .as_empty_layer();

        assert_eq!(history.created_by, Some("ADD file:abc123 /".to_string()));
        assert!(history.empty_layer);
    }

    #[test]
    fn test_image_config_accessors() {
        let config = ImageConfig::new("amd64", "linux").with_config(
            ContainerConfig::new()
                .with_entrypoint(vec!["/app".to_string()])
                .with_cmd(vec!["--help".to_string()])
                .with_env("DEBUG=1")
                .with_label("version", "1.0"),
        );

        assert_eq!(config.entrypoint(), Some(&["/app".to_string()][..]));
        assert_eq!(config.cmd(), Some(&["--help".to_string()][..]));
        assert_eq!(config.env(), Some(&["DEBUG=1".to_string()][..]));
        assert_eq!(
            config.labels().and_then(|l| l.get("version")),
            Some(&"1.0".to_string())
        );
    }
}
