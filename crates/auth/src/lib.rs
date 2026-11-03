//! Docker config and credential helper integration.
//!
//! This crate provides functionality for resolving container registry
//! credentials from Docker config files and credential helpers.
//!
//! # Overview
//!
//! The main types in this crate are:
//! - [`AuthResolver`] - Resolves credentials for a registry
//! - [`Credential`] - A credential (anonymous, basic, bearer, etc.)
//! - [`DockerConfig`] - Parsed Docker config file
//!
//! # Example
//!
//! ```no_run
//! use containerregistry_auth::{AuthResolver, Credential};
//!
//! // Create a resolver that loads from default Docker config
//! let resolver = AuthResolver::new();
//!
//! // Resolve credentials for a registry
//! let cred = resolver.resolve("gcr.io")?;
//!
//! // Use the credential
//! if let Some(header) = cred.authorization_header() {
//!     println!("Authorization: {}", header);
//! }
//! # Ok::<(), containerregistry_auth::Error>(())
//! ```
//!
//! # Resolution Order
//!
//! The resolver checks multiple sources in order of precedence:
//! 1. Explicit credentials (if set via [`AuthResolver::with_explicit`])
//! 2. Docker config `auths` section (base64-encoded or plaintext)
//! 3. Registry-specific credential helper (`credHelpers`)
//! 4. Default credential store (`credsStore`)
//! 5. Anonymous access (fallback)

mod config;
mod credential;
mod error;
mod helper;
mod keychain;
mod resolver;

pub use config::{AuthEntry, DOCKER_HUB_AUTH_KEY, DockerConfig};
pub use credential::{Credential, HelperCredential};
pub use error::Error;
pub use helper::{CredentialHelper, normalize_server_url};
pub use keychain::{HelperKeychain, Keychain, MultiKeychain, default_keychain};
pub use resolver::AuthResolver;

/// Result type for auth operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::Mutex;

    pub static ENV_LOCK: Mutex<()> = Mutex::new(());
}
