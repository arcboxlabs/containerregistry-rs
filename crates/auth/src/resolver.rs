//! Credential resolution with configurable sources.
//!
//! The resolver follows this order of precedence:
//! 1. Explicit credentials (if provided)
//! 2. Docker config `auths` section
//! 3. Registry-specific credential helper (`credHelpers`)
//! 4. Default credential store (`credsStore`)
//! 5. Anonymous access

use crate::config::DockerConfig;
use crate::helper::{CredentialHelper, normalize_server_url};
use crate::{Credential, Result};

/// A resolver for container registry credentials.
///
/// The resolver checks multiple sources in order of precedence to find
/// credentials for a given registry.
#[derive(Clone, Debug)]
pub struct AuthResolver {
    /// Docker config (if loaded).
    config: Option<DockerConfig>,

    /// Explicit credentials to use (overrides all other sources).
    explicit: Option<Credential>,
}

impl AuthResolver {
    /// Creates a new resolver with the default Docker config.
    ///
    /// Loads the config from `DOCKER_CONFIG` or `~/.docker/config.json`.
    /// If the config doesn't exist, uses an empty config.
    ///
    /// When no Docker config is found, Podman's `auth.json` locations are
    /// consulted (see [`crate::default_keychain`]).
    pub fn new() -> Self {
        crate::default_keychain()
    }

    /// Creates a new resolver with a specific Docker config.
    pub fn with_config(config: DockerConfig) -> Self {
        Self {
            config: Some(config),
            explicit: None,
        }
    }

    /// Creates a new resolver that always returns anonymous credentials.
    pub fn anonymous() -> Self {
        Self {
            config: None,
            explicit: Some(Credential::Anonymous),
        }
    }

    /// Returns the loaded Docker config, if any.
    pub fn config(&self) -> Option<&DockerConfig> {
        self.config.as_ref()
    }

    /// Sets explicit credentials that override all other sources.
    pub fn with_explicit(mut self, credential: Credential) -> Self {
        self.explicit = Some(credential);
        self
    }

    /// Resolves credentials for a registry.
    ///
    /// Returns the credential to use, following the resolution order:
    /// 1. Explicit credentials
    /// 2. Docker config auths
    /// 3. Registry-specific helper
    /// 4. Default credential store
    /// 5. Anonymous
    pub fn resolve(&self, registry: &str) -> Result<Credential> {
        // 1. Check explicit credentials
        if let Some(ref cred) = self.explicit {
            return Ok(cred.clone());
        }

        let config = match &self.config {
            Some(c) => c,
            None => return Ok(Credential::Anonymous),
        };

        // 2. Check Docker config auths
        if let Some(cred) = config.get_auth(registry) {
            return Ok(cred);
        }

        // 3 & 4. Check credential helpers
        if let Some(helper_name) = config.get_credential_helper(registry) {
            let helper = CredentialHelper::new(helper_name);
            let server_url = normalize_server_url(registry);

            match helper.get(&server_url) {
                Ok(cred) if !cred.is_anonymous() => return Ok(cred),
                Ok(_) => {} // Anonymous means helper didn't find credentials, continue
                Err(crate::Error::HelperNotFound(_)) => {} // Helper not installed, continue
                Err(e) => return Err(e), // Real error
            }
        }

        // 5. Fall back to anonymous
        Ok(Credential::Anonymous)
    }

    /// Resolves credentials for a registry, returning anonymous on any error.
    ///
    /// This is useful when you want to try authenticated access but fall back
    /// to anonymous if there are any issues with credential resolution.
    pub fn resolve_or_anonymous(&self, registry: &str) -> Credential {
        self.resolve(registry).unwrap_or(Credential::Anonymous)
    }
}

impl Default for AuthResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::ENV_LOCK;

    #[test]
    fn test_resolver_explicit_credentials() {
        let resolver = AuthResolver::anonymous().with_explicit(Credential::basic("user", "pass"));

        let cred = resolver.resolve("any-registry.io").unwrap();
        assert_eq!(cred.username(), Some("user"));
    }

    #[test]
    fn test_resolver_anonymous() {
        let resolver = AuthResolver::anonymous();
        let cred = resolver.resolve("any-registry.io").unwrap();
        assert!(cred.is_anonymous());
    }

    #[test]
    fn test_resolver_from_config_auths() {
        let config = DockerConfig::from_json(
            r#"{
            "auths": {
                "gcr.io": {
                    "auth": "dXNlcjpwYXNz"
                }
            }
        }"#,
        )
        .unwrap();

        let resolver = AuthResolver::with_config(config);
        let cred = resolver.resolve("gcr.io").unwrap();

        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));
    }

    #[test]
    fn test_resolver_falls_back_to_anonymous() {
        let config = DockerConfig::from_json(r#"{}"#).unwrap();

        let resolver = AuthResolver::with_config(config);
        let cred = resolver.resolve("unknown-registry.io").unwrap();

        assert!(cred.is_anonymous());
    }

    #[test]
    fn test_resolver_resolve_or_anonymous() {
        let resolver = AuthResolver::anonymous();
        let cred = resolver.resolve_or_anonymous("any-registry.io");
        assert!(cred.is_anonymous());
    }

    #[test]
    fn test_resolver_auths_precede_helpers() {
        let config = DockerConfig::from_json(
            r#"{
            "auths": {
                "example.com": { "username": "user", "password": "pass" }
            },
            "credHelpers": {
                "example.com": "fake"
            }
        }"#,
        )
        .unwrap();

        let resolver = AuthResolver::with_config(config);
        let cred = resolver.resolve("example.com").unwrap();
        assert_eq!(cred.username(), Some("user"));
    }

    #[test]
    fn test_resolver_uses_credential_helper_when_no_auths() {
        let _guard = ENV_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let helper_path = temp.path().join("docker-credential-fake");

        std::fs::write(
            &helper_path,
            r#"#!/bin/sh
read server
if [ "$server" = "https://example.com" ]; then
  echo '{"Username":"helper","Secret":"secret"}'
  exit 0
fi
echo "credentials not found" 1>&2
exit 1
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&helper_path).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&helper_path, perms).unwrap();
        }

        let prev_path = std::env::var("PATH").ok();
        let new_path = format!(
            "{}:{}",
            temp.path().to_string_lossy(),
            prev_path.clone().unwrap_or_default()
        );
        unsafe {
            std::env::set_var("PATH", new_path);
        }

        let config = DockerConfig::from_json(
            r#"{
            "credHelpers": {
                "example.com": "fake"
            }
        }"#,
        )
        .unwrap();

        let resolver = AuthResolver::with_config(config);
        let cred = resolver.resolve("example.com").unwrap();
        assert_eq!(cred.username(), Some("helper"));
        assert_eq!(cred.password(), Some("secret"));

        if let Some(value) = prev_path {
            unsafe {
                std::env::set_var("PATH", value);
            }
        } else {
            unsafe {
                std::env::remove_var("PATH");
            }
        }
    }
}
