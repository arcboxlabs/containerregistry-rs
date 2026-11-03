//! Keychains: pluggable credential sources (go-containerregistry `authn.Keychain`).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::DockerConfig;
use crate::helper::{CredentialHelper, normalize_server_url};
use crate::{AuthResolver, Credential, Result};

/// A source of credentials for registries.
///
/// Implementations must be cheap to call repeatedly; registry clients cache
/// the result per registry (and repository).
pub trait Keychain: Send + Sync {
    /// Resolves the credential to use for `registry` (a host such as
    /// `ghcr.io` or `localhost:5000`).
    fn resolve(&self, registry: &str) -> Result<Credential>;

    /// Resolves the credential for a specific repository. Defaults to
    /// [`Keychain::resolve`]; keychains that support repository-scoped
    /// entries (`ghcr.io/org/repo`) override it.
    fn resolve_repository(&self, registry: &str, repository: &str) -> Result<Credential> {
        let _ = repository;
        self.resolve(registry)
    }
}

impl fmt::Debug for dyn Keychain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Keychain")
    }
}

/// A static credential is a keychain that returns itself for every registry.
impl Keychain for Credential {
    fn resolve(&self, _registry: &str) -> Result<Credential> {
        Ok(self.clone())
    }
}

impl Keychain for AuthResolver {
    fn resolve(&self, registry: &str) -> Result<Credential> {
        AuthResolver::resolve(self, registry)
    }

    fn resolve_repository(&self, registry: &str, repository: &str) -> Result<Credential> {
        // Docker config files may contain repository-scoped keys; go-containerregistry
        // checks `registry/repository` before `registry`.
        let scoped = format!("{}/{}", registry, repository);
        if let Some(config) = self.config()
            && let Some(cred) = config.get_auth(&scoped)
        {
            return Ok(cred);
        }
        AuthResolver::resolve(self, registry)
    }
}

impl<K: Keychain + ?Sized> Keychain for Arc<K> {
    fn resolve(&self, registry: &str) -> Result<Credential> {
        (**self).resolve(registry)
    }

    fn resolve_repository(&self, registry: &str, repository: &str) -> Result<Credential> {
        (**self).resolve_repository(registry, repository)
    }
}

/// Tries several keychains in order and returns the first non-anonymous
/// credential (go-containerregistry `authn.NewMultiKeychain`).
#[derive(Clone, Default)]
pub struct MultiKeychain {
    keychains: Vec<Arc<dyn Keychain>>,
}

impl MultiKeychain {
    /// Creates a multi-keychain from the given keychains.
    pub fn new(keychains: Vec<Arc<dyn Keychain>>) -> Self {
        Self { keychains }
    }

    /// Appends a keychain.
    pub fn with(mut self, keychain: Arc<dyn Keychain>) -> Self {
        self.keychains.push(keychain);
        self
    }
}

impl Keychain for MultiKeychain {
    fn resolve(&self, registry: &str) -> Result<Credential> {
        for k in &self.keychains {
            let cred = k.resolve(registry)?;
            if !cred.is_anonymous() {
                return Ok(cred);
            }
        }
        Ok(Credential::Anonymous)
    }

    fn resolve_repository(&self, registry: &str, repository: &str) -> Result<Credential> {
        for k in &self.keychains {
            let cred = k.resolve_repository(registry, repository)?;
            if !cred.is_anonymous() {
                return Ok(cred);
            }
        }
        Ok(Credential::Anonymous)
    }
}

/// A keychain backed by a single credential helper binary
/// (go-containerregistry `authn.NewKeychainFromHelper`).
#[derive(Clone, Debug)]
pub struct HelperKeychain {
    helper: CredentialHelper,
}

impl HelperKeychain {
    /// Creates a keychain that invokes `docker-credential-<name>`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            helper: CredentialHelper::new(name),
        }
    }
}

impl Keychain for HelperKeychain {
    fn resolve(&self, registry: &str) -> Result<Credential> {
        match self.helper.get(&normalize_server_url(registry)) {
            Ok(c) => Ok(c),
            Err(crate::Error::HelperNotFound(_)) => Ok(Credential::Anonymous),
            Err(e) => Err(e),
        }
    }
}

/// Returns the default keychain: the Docker config (`$DOCKER_CONFIG` or
/// `~/.docker/config.json`), falling back to Podman's `auth.json`
/// (`$REGISTRY_AUTH_FILE`, `$XDG_RUNTIME_DIR/containers/auth.json`,
/// `$XDG_CONFIG_HOME/containers/auth.json`), then anonymous.
pub fn default_keychain() -> AuthResolver {
    match DockerConfig::load() {
        Ok(config) => AuthResolver::with_config(config),
        Err(_) => match podman_auth_path().and_then(|p| DockerConfig::load_from(&p).ok()) {
            Some(config) => AuthResolver::with_config(config),
            None => AuthResolver::with_config(DockerConfig::default()),
        },
    }
}

fn podman_auth_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("REGISTRY_AUTH_FILE") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        let p = PathBuf::from(dir).join("containers/auth.json");
        if p.is_file() {
            return Some(p);
        }
    }
    let config_dir = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".config"))
        })?;
    let p = config_dir.join("containers/auth.json");
    p.is_file().then_some(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Credential);
    impl Keychain for Fixed {
        fn resolve(&self, _registry: &str) -> Result<Credential> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn multi_keychain_returns_first_non_anonymous() {
        let mk = MultiKeychain::new(vec![
            Arc::new(Fixed(Credential::Anonymous)),
            Arc::new(Fixed(Credential::basic("a", "b"))),
            Arc::new(Fixed(Credential::basic("c", "d"))),
        ]);
        assert_eq!(mk.resolve("x").unwrap().username(), Some("a"));
        assert!(
            MultiKeychain::default()
                .resolve("x")
                .unwrap()
                .is_anonymous()
        );
    }

    #[test]
    fn resolver_prefers_repository_scoped_entry() {
        let config = DockerConfig::from_json(
            r#"{"auths":{
                "ghcr.io": {"username":"registry","password":"p"},
                "ghcr.io/org/repo": {"username":"scoped","password":"p"}
            }}"#,
        )
        .unwrap();
        let resolver = AuthResolver::with_config(config);
        let scoped = resolver.resolve_repository("ghcr.io", "org/repo").unwrap();
        assert_eq!(scoped.username(), Some("scoped"));
        let other = resolver.resolve_repository("ghcr.io", "org/other").unwrap();
        assert_eq!(other.username(), Some("registry"));
    }
}
