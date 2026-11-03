//! Docker config file parsing.
//!
//! This module parses Docker's `config.json` file which contains registry
//! credentials and credential helper configuration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Credential, Error, Result};

/// Docker config file structure.
///
/// This represents the contents of `~/.docker/config.json` or the file
/// pointed to by `DOCKER_CONFIG`. Unknown top-level keys (`proxies`,
/// `HttpHeaders`, ...) are preserved so that [`DockerConfig::save_to`] does
/// not drop them.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DockerConfig {
    /// Direct authentication entries (base64-encoded or plaintext).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub auths: HashMap<String, AuthEntry>,

    /// Default credential store for all registries.
    #[serde(
        default,
        rename = "credsStore",
        skip_serializing_if = "Option::is_none"
    )]
    pub creds_store: Option<String>,

    /// Per-registry credential helpers.
    #[serde(
        default,
        rename = "credHelpers",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub cred_helpers: HashMap<String, String>,

    /// Other keys, preserved verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// An auth entry from the Docker config's "auths" section.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AuthEntry {
    /// Base64-encoded "username:password" string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,

    /// Username (if stored separately).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Password (if stored separately).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Identity token for OAuth2.
    #[serde(
        default,
        rename = "identitytoken",
        skip_serializing_if = "Option::is_none"
    )]
    pub identity_token: Option<String>,

    /// Registry token (deprecated).
    #[serde(
        default,
        rename = "registrytoken",
        skip_serializing_if = "Option::is_none"
    )]
    pub registry_token: Option<String>,
}

impl DockerConfig {
    /// Loads the Docker config from the default location.
    ///
    /// Checks `DOCKER_CONFIG` environment variable first, then falls back
    /// to `~/.docker/config.json`.
    pub fn load() -> Result<Self> {
        let path = Self::default_config_path()?;
        Self::load_from(&path)
    }

    /// Loads the Docker config from a specific path.
    pub fn load_from(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                // Return empty config if file doesn't exist
                return Error::Io(e);
            }
            Error::Io(e)
        })?;

        Self::from_json(&contents)
    }

    /// Parses a Docker config from JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| Error::ConfigParse(e.to_string()))
    }

    /// Returns the default Docker config file path.
    ///
    /// Checks `DOCKER_CONFIG` environment variable first, then uses
    /// `~/.docker/config.json`.
    pub fn default_config_path() -> Result<PathBuf> {
        // Check DOCKER_CONFIG env var
        if let Ok(docker_config) = std::env::var("DOCKER_CONFIG") {
            let path = PathBuf::from(docker_config);
            return Ok(path.join("config.json"));
        }

        // Fall back to ~/.docker/config.json
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .map_err(|_| Error::ConfigParse("could not determine home directory".to_string()))?;

        Ok(PathBuf::from(home).join(".docker").join("config.json"))
    }

    /// Writes the config to `path` (creating parent directories), with
    /// owner-only permissions on Unix.
    pub fn save_to(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_vec_pretty(self)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, data)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Loads the config at `path`, or returns an empty config if it does not
    /// exist.
    pub fn load_or_default(path: &Path) -> Result<Self> {
        match Self::load_from(path) {
            Ok(c) => Ok(c),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Returns the config key used for `registry` (`https://index.docker.io/v1/`
    /// for Docker Hub, the registry host otherwise).
    pub fn auth_key(registry: &str) -> String {
        match normalize_registry(registry).as_str() {
            "index.docker.io" => DOCKER_HUB_AUTH_KEY.to_string(),
            other => other.to_string(),
        }
    }

    /// Stores `credential` for `registry`.
    ///
    /// When a credential helper is configured for the registry the secret is
    /// stored through the helper (as `docker login` does); otherwise it is
    /// written base64-encoded into `auths`. Call [`DockerConfig::save_to`]
    /// afterwards to persist the file.
    pub fn store_credential(&mut self, registry: &str, credential: &Credential) -> Result<()> {
        let key = Self::auth_key(registry);
        if let Some(helper) = self.get_credential_helper(registry) {
            let helper = crate::CredentialHelper::new(helper);
            helper.store(&crate::helper::normalize_server_url(&key), credential)?;
            // Docker leaves an empty entry so `docker logout` knows about it.
            self.auths.insert(key, AuthEntry::default());
            return Ok(());
        }
        let entry = match credential {
            Credential::Basic { username, password } => {
                use base64::Engine;
                AuthEntry {
                    auth: Some(
                        base64::engine::general_purpose::STANDARD
                            .encode(format!("{}:{}", username, password)),
                    ),
                    ..Default::default()
                }
            }
            Credential::IdentityToken(token) => AuthEntry {
                identity_token: Some(token.clone()),
                ..Default::default()
            },
            Credential::Bearer(token) => AuthEntry {
                registry_token: Some(token.clone()),
                ..Default::default()
            },
            Credential::Anonymous => {
                return Err(Error::InvalidCredentials(
                    "cannot store anonymous credentials".to_string(),
                ));
            }
        };
        self.auths.insert(key, entry);
        Ok(())
    }

    /// Removes stored credentials for `registry` (from `auths` and, when
    /// configured, the credential helper). Returns true if anything was
    /// removed.
    pub fn erase_credential(&mut self, registry: &str) -> Result<bool> {
        let normalized = normalize_registry(registry);
        let keys: Vec<String> = self
            .auths
            .keys()
            .filter(|k| normalize_registry(k) == normalized)
            .cloned()
            .collect();
        let mut removed = !keys.is_empty();
        for k in keys {
            self.auths.remove(&k);
        }
        if let Some(helper) = self.get_credential_helper(registry) {
            let helper = crate::CredentialHelper::new(helper);
            match helper.erase(&crate::helper::normalize_server_url(&Self::auth_key(
                registry,
            ))) {
                Ok(()) => removed = true,
                Err(Error::HelperNotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(removed)
    }

    /// Gets the credential helper name for a registry.
    ///
    /// Returns the registry-specific helper from `credHelpers` if present,
    /// otherwise falls back to `credsStore`.
    pub fn get_credential_helper(&self, registry: &str) -> Option<&str> {
        // Check registry-specific helper first
        if let Some(helper) = self.cred_helpers.get(registry) {
            return Some(helper.as_str());
        }

        // Normalize registry and try again
        let normalized = normalize_registry(registry);
        if let Some(helper) = self.cred_helpers.get(&normalized) {
            return Some(helper.as_str());
        }

        // Fall back to global credential store
        self.creds_store.as_deref()
    }

    /// Gets credentials from the `auths` section for a registry.
    pub fn get_auth(&self, registry: &str) -> Option<Credential> {
        // Try exact match first
        if let Some(entry) = self.auths.get(registry)
            && let Some(cred) = entry.to_credential()
        {
            return Some(cred);
        }

        // Normalize the input registry
        let normalized = normalize_registry(registry);

        // Try to find a matching entry by normalizing each auths key
        for (key, entry) in &self.auths {
            let normalized_key = normalize_registry(key);
            if normalized_key == normalized
                && let Some(cred) = entry.to_credential()
            {
                return Some(cred);
            }
        }

        None
    }
}

impl AuthEntry {
    /// Converts this auth entry to a Credential.
    pub fn to_credential(&self) -> Option<Credential> {
        // Check for identity token first
        if let Some(ref token) = self.identity_token
            && !token.is_empty()
        {
            return Some(Credential::identity_token(token));
        }

        // Check for registry token
        if let Some(ref token) = self.registry_token
            && !token.is_empty()
        {
            return Some(Credential::bearer(token));
        }

        // Check for base64-encoded auth
        if let Some(ref auth) = self.auth
            && !auth.is_empty()
        {
            return Self::decode_auth(auth);
        }

        // Check for separate username/password
        if let (Some(username), Some(password)) = (&self.username, &self.password)
            && !username.is_empty()
        {
            return Some(Credential::basic(username, password));
        }

        None
    }

    /// Decodes a base64-encoded "username:password" auth string.
    fn decode_auth(auth: &str) -> Option<Credential> {
        use base64::Engine;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(auth)
            .ok()?;
        let decoded_str = String::from_utf8(decoded).ok()?;

        // Split on first colon (password may contain colons)
        let (username, password) = decoded_str.split_once(':')?;

        if username.is_empty() {
            None
        } else {
            Some(Credential::basic(username, password))
        }
    }
}

/// The key Docker uses for Docker Hub credentials, hardcoded for historical
/// reasons.
pub const DOCKER_HUB_AUTH_KEY: &str = "https://index.docker.io/v1/";

/// Normalizes a registry hostname for lookup.
///
/// Docker Hub can be referenced as "docker.io", "index.docker.io",
/// "registry-1.docker.io", etc.
pub(crate) fn normalize_registry(registry: &str) -> String {
    let registry = registry
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let registry = registry.trim_end_matches('/');

    // Remove common path suffixes like /v1, /v2
    let registry = registry.trim_end_matches("/v1").trim_end_matches("/v2");

    // Normalize Docker Hub references
    match registry {
        "docker.io" | "registry-1.docker.io" | "registry.hub.docker.com" => {
            "index.docker.io".to_string()
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::ENV_LOCK;

    #[test]
    fn test_parse_empty_config() {
        let config: DockerConfig = serde_json::from_str("{}").unwrap();
        assert!(config.auths.is_empty());
        assert!(config.creds_store.is_none());
        assert!(config.cred_helpers.is_empty());
    }

    #[test]
    fn test_parse_auths_base64() {
        let json = r#"{
            "auths": {
                "https://index.docker.io/v1/": {
                    "auth": "dXNlcjpwYXNz"
                }
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();
        let entry = config.auths.get("https://index.docker.io/v1/").unwrap();
        let cred = entry.to_credential().unwrap();

        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));
    }

    #[test]
    fn test_parse_auths_plaintext() {
        let json = r#"{
            "auths": {
                "gcr.io": {
                    "username": "myuser",
                    "password": "mypass"
                }
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();
        let entry = config.auths.get("gcr.io").unwrap();
        let cred = entry.to_credential().unwrap();

        assert_eq!(cred.username(), Some("myuser"));
        assert_eq!(cred.password(), Some("mypass"));
    }

    #[test]
    fn test_parse_creds_store() {
        let json = r#"{
            "credsStore": "osxkeychain"
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.creds_store.as_deref(), Some("osxkeychain"));
    }

    #[test]
    fn test_parse_cred_helpers() {
        let json = r#"{
            "credHelpers": {
                "gcr.io": "gcloud",
                "123456789.dkr.ecr.us-east-1.amazonaws.com": "ecr-login"
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.cred_helpers.get("gcr.io"),
            Some(&"gcloud".to_string())
        );
    }

    #[test]
    fn test_get_credential_helper() {
        let json = r#"{
            "credsStore": "osxkeychain",
            "credHelpers": {
                "gcr.io": "gcloud"
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();

        // Registry-specific helper takes precedence
        assert_eq!(config.get_credential_helper("gcr.io"), Some("gcloud"));

        // Falls back to credsStore for other registries
        assert_eq!(
            config.get_credential_helper("docker.io"),
            Some("osxkeychain")
        );
    }

    #[test]
    fn test_store_and_erase_credential_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.json");
        std::fs::write(&path, r#"{"proxies":{"default":{"httpProxy":"x"}}}"#).unwrap();

        let mut config = DockerConfig::load_or_default(&path).unwrap();
        config
            .store_credential("docker.io", &Credential::basic("u", "p:w"))
            .unwrap();
        config.save_to(&path).unwrap();

        let reloaded = DockerConfig::load_from(&path).unwrap();
        assert!(reloaded.auths.contains_key(DOCKER_HUB_AUTH_KEY));
        let cred = reloaded.get_auth("index.docker.io").unwrap();
        assert_eq!(cred.password(), Some("p:w"));
        assert!(
            reloaded.extra.contains_key("proxies"),
            "unknown keys preserved"
        );

        let mut reloaded = reloaded;
        assert!(reloaded.erase_credential("registry-1.docker.io").unwrap());
        assert!(reloaded.get_auth("index.docker.io").is_none());
        assert!(!reloaded.erase_credential("index.docker.io").unwrap());
    }

    #[test]
    fn test_normalize_registry() {
        assert_eq!(normalize_registry("docker.io"), "index.docker.io");
        assert_eq!(
            normalize_registry("registry-1.docker.io"),
            "index.docker.io"
        );
        assert_eq!(normalize_registry("gcr.io"), "gcr.io");
        assert_eq!(normalize_registry("https://gcr.io/"), "gcr.io");
    }

    #[test]
    fn test_get_auth_normalized() {
        let json = r#"{
            "auths": {
                "https://index.docker.io/v1/": {
                    "auth": "dXNlcjpwYXNz"
                }
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();

        // Should find via various Docker Hub references
        assert!(config.get_auth("index.docker.io").is_some());
    }

    #[test]
    fn test_decode_auth_with_colon_in_password() {
        // "user:pass:with:colons" base64 encoded
        let auth = "dXNlcjpwYXNzOndpdGg6Y29sb25z";
        let cred = AuthEntry::decode_auth(auth).unwrap();
        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass:with:colons"));
    }

    #[test]
    fn test_load_uses_docker_config_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.json");

        std::fs::write(
            &config_path,
            r#"{"auths":{"example.com":{"username":"user","password":"pass"}}}"#,
        )
        .unwrap();

        let prev = std::env::var("DOCKER_CONFIG").ok();
        unsafe {
            std::env::set_var("DOCKER_CONFIG", temp.path());
        }

        let config = DockerConfig::load().unwrap();
        let cred = config.get_auth("example.com").unwrap();
        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));

        if let Some(value) = prev {
            unsafe {
                std::env::set_var("DOCKER_CONFIG", value);
            }
        } else {
            unsafe {
                std::env::remove_var("DOCKER_CONFIG");
            }
        }
    }
}
