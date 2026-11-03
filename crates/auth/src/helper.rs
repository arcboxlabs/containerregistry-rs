//! Credential helper invocation.
//!
//! Docker credential helpers are external programs that follow a specific
//! protocol for getting/storing/erasing credentials.
//!
//! The helper is invoked as `docker-credential-<name>` and communicates
//! via stdin/stdout using JSON.

use std::io::Write;
use std::process::{Command, Stdio};

use crate::credential::HelperCredential;
use crate::{Credential, Error, Result};

/// A credential helper that can retrieve credentials for registries.
#[derive(Clone, Debug)]
pub struct CredentialHelper {
    /// The helper name (e.g., "osxkeychain", "gcloud", "ecr-login").
    name: String,
}

impl CredentialHelper {
    /// Creates a new credential helper with the given name.
    ///
    /// The helper will be invoked as `docker-credential-{name}`.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Returns the helper name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the full helper binary name.
    fn binary_name(&self) -> String {
        format!("docker-credential-{}", self.name)
    }

    /// Gets credentials for a registry server.
    ///
    /// Invokes the helper with the "get" action, passing the server URL
    /// via stdin.
    pub fn get(&self, server_url: &str) -> Result<Credential> {
        let binary = self.binary_name();

        let mut child = Command::new(&binary)
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::HelperNotFound(binary.clone())
                } else {
                    Error::HelperFailure(format!("failed to spawn {}: {}", binary, e))
                }
            })?;

        // Write server URL to stdin
        if let Some(mut stdin) = child.stdin.take() {
            // Docker helpers expect the URL with newline
            writeln!(stdin, "{}", server_url).map_err(|e| {
                Error::HelperFailure(format!("failed to write to {}: {}", binary, e))
            })?;
        }

        let output = child
            .wait_with_output()
            .map_err(|e| Error::HelperFailure(format!("failed to read from {}: {}", binary, e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);

            // Check for "credentials not found" which is not really an error
            let combined = format!("{}{}", stderr, stdout).to_lowercase();
            if combined.contains("credentials not found")
                || combined.contains("not found")
                || combined.contains("no credentials")
            {
                return Ok(Credential::Anonymous);
            }

            return Err(Error::HelperFailure(format!(
                "{} exited with status {}: {}",
                binary,
                output.status,
                stderr.trim()
            )));
        }

        // Parse the JSON response
        let response: HelperCredential = serde_json::from_slice(&output.stdout).map_err(|e| {
            Error::HelperFailure(format!(
                "{} returned invalid JSON: {} (output: {})",
                binary,
                e,
                String::from_utf8_lossy(&output.stdout)
            ))
        })?;

        Ok(response.into_credential())
    }

    /// Runs the helper with `action`, writing `input` to stdin, and returns
    /// stdout.
    fn run(&self, action: &str, input: &[u8]) -> Result<Vec<u8>> {
        let binary = self.binary_name();
        let mut child = Command::new(&binary)
            .arg(action)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::HelperNotFound(binary.clone())
                } else {
                    Error::HelperFailure(format!("failed to spawn {}: {}", binary, e))
                }
            })?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(input).map_err(|e| {
                Error::HelperFailure(format!("failed to write to {}: {}", binary, e))
            })?;
        }
        let output = child
            .wait_with_output()
            .map_err(|e| Error::HelperFailure(format!("failed to read from {}: {}", binary, e)))?;
        if !output.status.success() {
            return Err(Error::HelperFailure(format!(
                "{} {} exited with status {}: {}{}",
                binary,
                action,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                String::from_utf8_lossy(&output.stdout).trim()
            )));
        }
        Ok(output.stdout)
    }

    /// Stores a credential for `server_url` (the helper `store` action).
    pub fn store(&self, server_url: &str, credential: &Credential) -> Result<()> {
        let (username, secret) = match credential {
            Credential::Basic { username, password } => (username.as_str(), password.as_str()),
            // Docker stores identity tokens with the magic username "<token>".
            Credential::IdentityToken(t) | Credential::Bearer(t) => ("<token>", t.as_str()),
            Credential::Anonymous => {
                return Err(Error::InvalidCredentials(
                    "cannot store anonymous credentials".to_string(),
                ));
            }
        };
        let payload = serde_json::json!({
            "ServerURL": server_url,
            "Username": username,
            "Secret": secret,
        });
        self.run("store", &serde_json::to_vec(&payload)?)?;
        Ok(())
    }

    /// Erases the credential for `server_url` (the helper `erase` action).
    pub fn erase(&self, server_url: &str) -> Result<()> {
        self.run("erase", format!("{}\n", server_url).as_bytes())?;
        Ok(())
    }

    /// Lists stored server URLs and their usernames (the helper `list`
    /// action).
    pub fn list(&self) -> Result<std::collections::HashMap<String, String>> {
        let out = self.run("list", b"")?;
        Ok(serde_json::from_slice(&out)?)
    }

    /// Checks if the helper binary exists and is executable.
    pub fn is_available(&self) -> bool {
        let binary = self.binary_name();

        // Try to run with --help or version to check if it exists
        Command::new(&binary)
            .arg("version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|_| true)
            .unwrap_or_else(|_| {
                // Some helpers don't support "version", try just running with no args
                Command::new(&binary)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .is_ok()
            })
    }
}

/// Normalizes a registry URL for credential helper lookup.
///
/// Credential helpers expect URLs in a specific format.
pub fn normalize_server_url(registry: &str) -> String {
    let registry = registry.trim();

    // If it already has a scheme, use as-is
    if registry.starts_with("https://") || registry.starts_with("http://") {
        return registry.to_string();
    }

    // Add https:// prefix
    format!("https://{}", registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::ENV_LOCK;

    #[test]
    fn test_helper_binary_name() {
        let helper = CredentialHelper::new("osxkeychain");
        assert_eq!(helper.binary_name(), "docker-credential-osxkeychain");
    }

    #[test]
    fn test_normalize_server_url() {
        assert_eq!(normalize_server_url("gcr.io"), "https://gcr.io");
        assert_eq!(normalize_server_url("https://gcr.io"), "https://gcr.io");
        assert_eq!(
            normalize_server_url("http://localhost:5000"),
            "http://localhost:5000"
        );
    }

    // Integration tests for actual helpers would go here but require
    // the helpers to be installed. See the fake helper tests below.

    #[test]
    fn test_helper_not_found() {
        let helper = CredentialHelper::new("nonexistent-helper-12345");
        let result = helper.get("https://example.com");
        assert!(matches!(result, Err(Error::HelperNotFound(_))));
    }

    #[test]
    fn test_helper_fake_success_not_found_error() {
        let _guard = ENV_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let helper_path = temp.path().join("docker-credential-fake");

        std::fs::write(
            &helper_path,
            r#"#!/bin/sh
read server
if [ "$server" = "https://success" ]; then
  echo '{"Username":"user","Secret":"pass"}'
  exit 0
fi
if [ "$server" = "https://notfound" ]; then
  echo "credentials not found" 1>&2
  exit 1
fi
echo "boom" 1>&2
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

        let helper = CredentialHelper::new("fake");

        let cred = helper.get("https://success").unwrap();
        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));

        let cred = helper.get("https://notfound").unwrap();
        assert!(cred.is_anonymous());

        let err = helper.get("https://error").unwrap_err();
        assert!(matches!(err, Error::HelperFailure(_)));

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
